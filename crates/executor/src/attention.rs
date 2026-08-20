//! Attention scheduler v0: keep pending input requests from going stale.
//!
//! Two behaviors, both derived from the input-request convention
//! (`website/docs-src/concepts/input-requests.md`) and both deliberately
//! minimal starting points:
//!
//! - **Nudges**: when a relayed request has waited longer than a threshold,
//!   re-post a reminder to the bound channel, at most once per interval.
//!   Nudge bookkeeping is Exo-private and therefore recorded as a namespaced
//!   custom event (`exo.input_request_nudged`), in contrast to the shared
//!   convention events themselves.
//! - **Ordering**: scheduled tasks whose conversation has an aged pending
//!   request are started first within a scheduler pass, so blocked
//!   conversations surface before routine work.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use exoharness::{
    ConversationHandle, DateTimeUtc, EventData, EventKind, EventQuery, EventQueryDirection,
};
use serde::Deserialize;

use crate::adapter::runtime::send_adapter_message_with_handles;
use crate::adapter::{AdapterRecord, AdapterStore};
use crate::conversation_events::record_host_event;
use crate::input_requests::{PendingInputRequest, pending_input_requests};
use crate::scheduler_types::ScheduledTaskRecord;
use crate::{Harness, HarnessAgent, HarnessConversation};

/// Exo-private bookkeeping event: a reminder was re-posted for a pending
/// input request. Namespaced, unlike the convention events, because no other
/// harness needs to understand it.
pub const EXO_INPUT_REQUEST_NUDGED_EVENT: &str = "exo.input_request_nudged";

const NUDGE_SCAN_PAGE_LIMIT: u32 = 200;

#[derive(Debug, Clone, Copy)]
pub struct AttentionOptions {
    /// A pending request older than this is considered aged: it is eligible
    /// for a nudge and prioritizes its conversation's scheduled work.
    pub nudge_after: Duration,
    /// Minimum spacing between nudges for the same request.
    pub nudge_interval: Duration,
}

impl Default for AttentionOptions {
    fn default() -> Self {
        Self {
            nudge_after: Duration::from_secs(300),
            nudge_interval: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InputRequestNudge {
    pub agent_slug: String,
    pub conversation_slug: String,
    pub request_id: String,
    pub age_seconds: u64,
}

/// One attention pass over every conversation: re-nudge relayed requests that
/// aged past the threshold. Per-conversation failures are logged and skipped
/// so one broken conversation cannot silence the rest.
pub async fn run_attention_pass(
    harness: &dyn Harness,
    adapters: &AdapterStore,
    options: &AttentionOptions,
) -> Result<Vec<InputRequestNudge>> {
    let mut nudges = Vec::new();
    for agent_record in harness.list_agents().await? {
        let Some(agent) = harness.get_agent(&agent_record.id.to_string()).await? else {
            continue;
        };
        for conversation_record in agent.list_conversations().await? {
            let Some(conversation) = agent
                .get_conversation(&conversation_record.id.to_string())
                .await?
            else {
                continue;
            };
            let result = nudge_conversation(
                agent.as_ref(),
                conversation.as_ref(),
                adapters,
                options,
                &mut nudges,
            )
            .await;
            if let Err(error) = result {
                tracing::warn!(
                    conversation_id = %conversation_record.id,
                    %error,
                    "attention pass failed for conversation"
                );
            }
        }
    }
    Ok(nudges)
}

async fn nudge_conversation(
    agent: &dyn HarnessAgent,
    conversation: &dyn HarnessConversation,
    adapters: &AdapterStore,
    options: &AttentionOptions,
    nudges: &mut Vec<InputRequestNudge>,
) -> Result<()> {
    let handle = conversation.exoharness_handle();
    let pending = pending_input_requests(handle.as_ref()).await?;
    if !pending
        .iter()
        .any(|request| request.payload.adapter_id.is_some())
    {
        return Ok(());
    }
    let last_nudges = last_nudge_times(handle.as_ref()).await?;
    for request in &pending {
        let Some(adapter_id) = request.payload.adapter_id.as_deref() else {
            // Not relayed anywhere; there is no channel to remind.
            continue;
        };
        let Some(age) = request_age(request) else {
            continue;
        };
        if age < options.nudge_after {
            continue;
        }
        if let Some(nudged_at) = last_nudges.get(&request.payload.request_id)
            && age_since(*nudged_at).is_none_or(|since| since < options.nudge_interval)
        {
            continue;
        }
        let Some(adapter) = adapters.get_adapter(adapter_id).await? else {
            continue;
        };
        if !adapter.enabled {
            continue;
        }
        send_nudge(agent, conversation, adapters, &adapter, request).await?;
        record_host_event(
            handle.as_ref(),
            EXO_INPUT_REQUEST_NUDGED_EVENT,
            serde_json::json!({ "request_id": request.payload.request_id }),
        )
        .await?;
        nudges.push(InputRequestNudge {
            agent_slug: agent.record().slug.clone(),
            conversation_slug: conversation.record().slug.clone(),
            request_id: request.payload.request_id.clone(),
            age_seconds: age.as_secs(),
        });
    }
    Ok(())
}

async fn send_nudge(
    agent: &dyn HarnessAgent,
    conversation: &dyn HarnessConversation,
    adapters: &AdapterStore,
    adapter: &AdapterRecord,
    request: &PendingInputRequest,
) -> Result<()> {
    let text = format!(
        "Reminder — still waiting on an answer to:\n\n{}",
        request.payload.prompt
    );
    send_adapter_message_with_handles(
        agent.exoharness_handle().as_ref(),
        conversation.exoharness_handle().as_ref(),
        adapters,
        adapter,
        &text,
        request.payload.target.as_deref(),
        Vec::new(),
    )
    .await?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct NudgePayload {
    request_id: String,
}

/// Most recent nudge time per request id, from the conversation's own log.
async fn last_nudge_times(
    conversation: &dyn ConversationHandle,
) -> Result<HashMap<String, DateTimeUtc>> {
    let mut nudged = HashMap::new();
    let mut cursor = None;
    loop {
        let result = conversation
            .get_events(Some(EventQuery {
                cursor,
                direction: Some(EventQueryDirection::Asc),
                limit: Some(NUDGE_SCAN_PAGE_LIMIT),
                session_id: None,
                turn_id: None,
                types: Some(vec![EventKind::custom(EXO_INPUT_REQUEST_NUDGED_EVENT)]),
            }))
            .await?;
        for event in &result.events {
            let EventData::Custom { payload, .. } = &event.data else {
                continue;
            };
            if let Ok(payload) = serde_json::from_value::<NudgePayload>(payload.clone()) {
                nudged.insert(payload.request_id, event.created_at);
            }
        }
        match result.cursor {
            Some(next) if !result.events.is_empty() => cursor = Some(next),
            _ => break,
        }
    }
    Ok(nudged)
}

/// Starts due tasks whose conversation has an aged pending input request
/// before the rest. v0: runs stay concurrent, so this is a start-order
/// priority, not strict sequencing; lookup failures degrade to the original
/// order.
pub async fn order_due_tasks_for_attention(
    harness: &dyn Harness,
    due: Vec<ScheduledTaskRecord>,
    aged_after: Duration,
) -> Vec<ScheduledTaskRecord> {
    let mut blocked = HashSet::new();
    let mut checked = HashSet::new();
    for task in &due {
        let key = (task.agent_id.clone(), task.conversation_id.clone());
        if !checked.insert(key.clone()) {
            continue;
        }
        match conversation_has_aged_request(
            harness,
            &task.agent_id,
            &task.conversation_id,
            aged_after,
        )
        .await
        {
            Ok(true) => {
                blocked.insert(key);
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(
                    conversation_id = %task.conversation_id,
                    %error,
                    "failed to check pending input requests for scheduling order"
                );
            }
        }
    }
    order_tasks_by_attention(due, &blocked)
}

fn order_tasks_by_attention(
    due: Vec<ScheduledTaskRecord>,
    blocked: &HashSet<(String, String)>,
) -> Vec<ScheduledTaskRecord> {
    let mut due = due;
    due.sort_by_key(|task| {
        !blocked.contains(&(task.agent_id.clone(), task.conversation_id.clone()))
    });
    due
}

async fn conversation_has_aged_request(
    harness: &dyn Harness,
    agent_id: &str,
    conversation_id: &str,
    aged_after: Duration,
) -> Result<bool> {
    let Some(agent) = harness.get_agent(agent_id).await? else {
        return Ok(false);
    };
    let Some(conversation) = agent.get_conversation(conversation_id).await? else {
        return Ok(false);
    };
    let pending = pending_input_requests(conversation.exoharness_handle().as_ref()).await?;
    Ok(pending
        .iter()
        .any(|request| request_age(request).is_some_and(|age| age >= aged_after)))
}

fn request_age(request: &PendingInputRequest) -> Option<Duration> {
    age_since(request.requested_at)
}

fn age_since(instant: DateTimeUtc) -> Option<Duration> {
    (Utc::now() - instant).to_std().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler_types::NewScheduledTask;

    fn task(id: &str, agent_id: &str, conversation_id: &str) -> ScheduledTaskRecord {
        let mut task = ScheduledTaskRecord::new(
            NewScheduledTask {
                agent_id: agent_id.to_string(),
                conversation_id: conversation_id.to_string(),
                name: id.to_string(),
                schedule: "@every 60s".to_string(),
                sandbox_mode: None,
                setup_command: None,
                command: vec!["true".to_string()],
                report_prompt: "report".to_string(),
                max_output_bytes: None,
                missed: None,
            },
            0,
        )
        .unwrap();
        task.id = id.to_string();
        task
    }

    #[test]
    fn blocked_conversations_surface_first_and_order_is_stable() {
        let due = vec![
            task("task-1", "agent-1", "conversation-a"),
            task("task-2", "agent-1", "conversation-b"),
            task("task-3", "agent-1", "conversation-a"),
            task("task-4", "agent-2", "conversation-c"),
        ];
        let blocked = HashSet::from([
            ("agent-1".to_string(), "conversation-b".to_string()),
            ("agent-2".to_string(), "conversation-c".to_string()),
        ]);

        let ordered = order_tasks_by_attention(due, &blocked);
        let ids: Vec<&str> = ordered.iter().map(|task| task.id.as_str()).collect();
        assert_eq!(ids, vec!["task-2", "task-4", "task-1", "task-3"]);
    }

    #[test]
    fn empty_blocked_set_preserves_order() {
        let due = vec![
            task("task-1", "agent-1", "conversation-a"),
            task("task-2", "agent-1", "conversation-b"),
        ];
        let ordered = order_tasks_by_attention(due, &HashSet::new());
        let ids: Vec<&str> = ordered.iter().map(|task| task.id.as_str()).collect();
        assert_eq!(ids, vec!["task-1", "task-2"]);
    }
}
