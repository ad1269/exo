//! Host-side helpers for the input-request event convention: a pair of
//! `Custom` events (`input_requested` / `input_resolved`) that mark a
//! conversation as blocked on a human and record how the block was settled.
//! See `website/docs-src/concepts/input-requests.md` for the specification;
//! the TypeScript twin lives in `exoharness/typescript/harness/input-requests.ts`.
//! The event names are deliberately un-namespaced: they are candidates for
//! the exoharness event vocabulary if the convention is adopted more widely,
//! so promotion must be wire-compatible.

use anyhow::Result;
use exoharness::{
    ConversationHandle, DateTimeUtc, EventData, EventId, EventKind, EventQuery, EventQueryDirection,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::conversation_events::record_host_event;

pub const INPUT_REQUESTED_EVENT: &str = "input_requested";
pub const INPUT_RESOLVED_EVENT: &str = "input_resolved";

const PENDING_SCAN_PAGE_LIMIT: u32 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputRequestKind {
    Question,
    ToolConfirmation,
    Feedback,
    Auth,
}

impl InputRequestKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Question => "question",
            Self::ToolConfirmation => "tool_confirmation",
            Self::Feedback => "feedback",
            Self::Auth => "auth",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputResolution {
    Answered,
    Cancelled,
    Expired,
}

/// Payload of an `input_requested` event. Field names are the snake_case
/// wire format the spec defines. The payload is open: unknown fields from
/// other producers are ignored on read, and `adapter_id` / `target` are Exo
/// extension fields set when the request was relayed to an external channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputRequestedPayload {
    pub request_id: String,
    pub kind: InputRequestKind,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

/// Payload of an `input_resolved` event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputResolvedPayload {
    pub request_id: String,
    pub resolution: InputResolution,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_by: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PendingInputRequest {
    pub event_id: EventId,
    pub requested_at: DateTimeUtc,
    pub payload: InputRequestedPayload,
}

/// The conversation's pending set: `input_requested` events without a
/// matching `input_resolved`, oldest first. Reads only the two convention
/// kinds, paging by cursor so per-turn traffic is never scanned. Malformed
/// payloads and resolutions without a matching request are ignored.
pub async fn pending_input_requests(
    conversation: &dyn ConversationHandle,
) -> Result<Vec<PendingInputRequest>> {
    let mut pending: Vec<PendingInputRequest> = Vec::new();
    let mut cursor = None;
    loop {
        let result = conversation
            .get_events(Some(EventQuery {
                cursor,
                direction: Some(EventQueryDirection::Asc),
                limit: Some(PENDING_SCAN_PAGE_LIMIT),
                session_id: None,
                turn_id: None,
                types: Some(vec![
                    EventKind::custom(INPUT_REQUESTED_EVENT),
                    EventKind::custom(INPUT_RESOLVED_EVENT),
                ]),
            }))
            .await?;
        for event in &result.events {
            let EventData::Custom {
                event_type,
                payload,
            } = &event.data
            else {
                continue;
            };
            if event_type == INPUT_REQUESTED_EVENT {
                let Ok(payload) = serde_json::from_value::<InputRequestedPayload>(payload.clone())
                else {
                    continue;
                };
                if !pending
                    .iter()
                    .any(|request| request.payload.request_id == payload.request_id)
                {
                    pending.push(PendingInputRequest {
                        event_id: event.id,
                        requested_at: event.created_at,
                        payload,
                    });
                }
            } else if event_type == INPUT_RESOLVED_EVENT
                && let Ok(payload) = serde_json::from_value::<InputResolvedPayload>(payload.clone())
            {
                pending.retain(|request| request.payload.request_id != payload.request_id);
            }
        }
        match result.cursor {
            Some(next) if !result.events.is_empty() => cursor = Some(next),
            _ => break,
        }
    }
    Ok(pending)
}

/// Appends an `input_resolved` event for a settled request.
pub async fn record_input_resolved(
    conversation: &dyn ConversationHandle,
    payload: &InputResolvedPayload,
) -> Result<()> {
    record_host_event(
        conversation,
        INPUT_RESOLVED_EVENT,
        serde_json::to_value(payload)?,
    )
    .await
}

/// The oldest pending request that was relayed through this adapter and, when
/// the request pinned a target, through this target. Requests without adapter
/// context are never claimed by an adapter reply — they belong to whoever
/// created them (REPL, CLI, another client).
pub fn match_pending_adapter_request<'a>(
    pending: &'a [PendingInputRequest],
    adapter_id: &str,
    target: &str,
) -> Option<&'a PendingInputRequest> {
    pending.iter().find(|request| {
        request.payload.adapter_id.as_deref() == Some(adapter_id)
            && request
                .payload
                .target
                .as_deref()
                .is_none_or(|requested| requested == target)
    })
}

/// Converts an inbound adapter reply into the resolution of the oldest
/// matching pending request, if there is one. Returns the resolved request so
/// the caller can decide what else the reply should do (for the reference
/// integration: nothing — the blocked turn consumes the answer).
pub async fn resolve_pending_adapter_input_request(
    conversation: &dyn ConversationHandle,
    adapter_id: &str,
    target: &str,
    resolved_by: Option<&str>,
    answer_text: &str,
) -> Result<Option<PendingInputRequest>> {
    let pending = pending_input_requests(conversation).await?;
    let Some(request) = match_pending_adapter_request(&pending, adapter_id, target) else {
        return Ok(None);
    };
    let request = request.clone();
    record_input_resolved(
        conversation,
        &InputResolvedPayload {
            request_id: request.payload.request_id.clone(),
            resolution: InputResolution::Answered,
            answer: Some(Value::String(answer_text.to_string())),
            resolved_by: resolved_by.map(ToOwned::to_owned),
        },
    )
    .await?;
    Ok(Some(request))
}

#[cfg(test)]
mod tests {
    use exoharness::{BasicExoHarness, ExoHarness, NewAgentRequest, NewConversationRequest};
    use tempfile::TempDir;

    use super::*;
    use crate::test_support::local_test_config;

    async fn test_conversation(
        tempdir: &TempDir,
    ) -> std::sync::Arc<dyn exoharness::ConversationHandle> {
        let exoharness = BasicExoHarness::new(local_test_config(tempdir.path().join("exoharness")))
            .await
            .unwrap();
        let agent = exoharness
            .new_agent(NewAgentRequest {
                slug: "agent".to_string(),
                name: "Agent".to_string(),
            })
            .await
            .unwrap();
        agent
            .new_conversation(NewConversationRequest {
                slug: Some("conversation".to_string()),
                name: Some("Conversation".to_string()),
            })
            .await
            .unwrap()
    }

    async fn record_request(
        conversation: &dyn ConversationHandle,
        request_id: &str,
        adapter_id: Option<&str>,
        target: Option<&str>,
    ) {
        record_host_event(
            conversation,
            INPUT_REQUESTED_EVENT,
            serde_json::to_value(InputRequestedPayload {
                request_id: request_id.to_string(),
                kind: InputRequestKind::Question,
                prompt: format!("prompt for {request_id}"),
                answer_schema: None,
                tool_call_id: None,
                adapter_id: adapter_id.map(ToOwned::to_owned),
                target: target.map(ToOwned::to_owned),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn pending_set_tracks_requests_until_resolved() {
        let tempdir = TempDir::new().unwrap();
        let conversation = test_conversation(&tempdir).await;

        record_request(conversation.as_ref(), "req-1", None, None).await;
        record_request(conversation.as_ref(), "req-2", None, None).await;
        let pending = pending_input_requests(conversation.as_ref()).await.unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].payload.request_id, "req-1");

        record_input_resolved(
            conversation.as_ref(),
            &InputResolvedPayload {
                request_id: "req-1".to_string(),
                resolution: InputResolution::Answered,
                answer: Some(Value::String("yes".to_string())),
                resolved_by: Some("martin".to_string()),
            },
        )
        .await
        .unwrap();
        let pending = pending_input_requests(conversation.as_ref()).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].payload.request_id, "req-2");
    }

    #[tokio::test]
    async fn ignores_malformed_payloads_and_unmatched_resolutions() {
        let tempdir = TempDir::new().unwrap();
        let conversation = test_conversation(&tempdir).await;

        record_host_event(
            conversation.as_ref(),
            INPUT_REQUESTED_EVENT,
            serde_json::json!({ "not": "a request" }),
        )
        .await
        .unwrap();
        record_host_event(
            conversation.as_ref(),
            INPUT_RESOLVED_EVENT,
            serde_json::json!({ "request_id": "req-unknown", "resolution": "answered" }),
        )
        .await
        .unwrap();
        record_request(conversation.as_ref(), "req-1", None, None).await;

        let pending = pending_input_requests(conversation.as_ref()).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].payload.request_id, "req-1");
    }

    #[tokio::test]
    async fn adapter_reply_resolves_only_matching_relayed_requests() {
        let tempdir = TempDir::new().unwrap();
        let conversation = test_conversation(&tempdir).await;

        // Not relayed: an adapter reply must never claim it.
        record_request(conversation.as_ref(), "req-local", None, None).await;
        // Relayed through another channel of the same adapter.
        record_request(
            conversation.as_ref(),
            "req-other-channel",
            Some("adapter-1"),
            Some("channel-2"),
        )
        .await;
        // Relayed through the replying channel.
        record_request(
            conversation.as_ref(),
            "req-match",
            Some("adapter-1"),
            Some("channel-1"),
        )
        .await;

        let resolved = resolve_pending_adapter_input_request(
            conversation.as_ref(),
            "adapter-1",
            "channel-1",
            Some("martin"),
            "the answer",
        )
        .await
        .unwrap()
        .expect("reply should resolve the matching request");
        assert_eq!(resolved.payload.request_id, "req-match");

        let pending = pending_input_requests(conversation.as_ref()).await.unwrap();
        let pending_ids: Vec<&str> = pending
            .iter()
            .map(|request| request.payload.request_id.as_str())
            .collect();
        assert_eq!(pending_ids, vec!["req-local", "req-other-channel"]);

        // No matching pending request left for this channel.
        let unresolved = resolve_pending_adapter_input_request(
            conversation.as_ref(),
            "adapter-1",
            "channel-1",
            None,
            "another reply",
        )
        .await
        .unwrap();
        assert!(unresolved.is_none());
    }

    #[test]
    fn matches_oldest_pending_request_and_untargeted_relays() {
        let request = |id: &str, adapter: Option<&str>, target: Option<&str>| PendingInputRequest {
            event_id: exoharness::Uuid7::now(),
            requested_at: chrono::Utc::now(),
            payload: InputRequestedPayload {
                request_id: id.to_string(),
                kind: InputRequestKind::Question,
                prompt: "?".to_string(),
                answer_schema: None,
                tool_call_id: None,
                adapter_id: adapter.map(ToOwned::to_owned),
                target: target.map(ToOwned::to_owned),
            },
        };
        let pending = vec![
            request("req-local", None, None),
            request("req-untargeted", Some("adapter-1"), None),
            request("req-targeted", Some("adapter-1"), Some("channel-1")),
        ];

        // An untargeted relay accepts a reply from any of the adapter's
        // targets; it is older, so it wins over the targeted one.
        let matched = match_pending_adapter_request(&pending, "adapter-1", "channel-1").unwrap();
        assert_eq!(matched.payload.request_id, "req-untargeted");
        assert!(match_pending_adapter_request(&pending, "adapter-2", "channel-1").is_none());
    }
}
