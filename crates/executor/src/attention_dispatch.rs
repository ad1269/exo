//! The impure half of the attention scheduler: producers hand
//! [`deliver_via_inbox`] an item, and the append plus a dispatch pass turn
//! pending items into conversation turns. The pure half —
//! [`decide`](crate::attention::decide) and the item types — lives in
//! [`crate::attention`]; nothing here decides anything.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Result;
use exoharness::Uuid7;
use lingua::Message;
use lingua::universal::UserContent;

use crate::attention::{
    Action, AttentionBackend, AttentionState, ConversationState, DispatchLease,
    FileAttentionBackend, InboxItem, LEASE_TTL_MS, SandboxState, decide,
};
use crate::{HarnessConversation, SendRequest, now_ms};

/// The wiring flag: set, wakes flow through the attention inbox rooted here;
/// unset, they take the legacy `send_conversation_wakeup` funnel. Read once
/// per process. Call this only at the outermost producer call sites and
/// thread the result explicitly, so tests never touch process env.
pub fn attention_dir_from_env() -> Option<PathBuf> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| {
        std::env::var("EXO_ATTENTION_DIR")
            .ok()
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    })
    .clone()
}

/// Append one item and run a dispatch pass over its conversation.
///
/// Errors propagate on both halves deliberately: callers' at-least-once retry
/// semantics depend on a failed append surfacing (a redelivered call dedupes
/// at append), and a failed dispatch leaves the item pending for the caller's
/// retry or the next append to drain.
pub async fn deliver_via_inbox(
    dir: &Path,
    conversation: &dyn HarnessConversation,
    item: InboxItem,
) -> Result<()> {
    let backend: Arc<dyn AttentionBackend> = Arc::new(FileAttentionBackend::new(dir));
    let conversation_id = item.conversation_id.clone();
    backend.append_inbox_item(item).await?;
    dispatch_conversation(&backend, conversation, &conversation_id).await
}

/// A dispatch pass with no append: drains whatever is already pending. The
/// startup sweep for items stranded by a dispatcher that died holding the
/// lease — an appender that found the lease taken has already returned, so
/// without a sweep the item waits for the next append, which for a one-shot
/// fire may never come.
pub async fn sweep_via_inbox(dir: &Path, conversation: &dyn HarnessConversation) -> Result<()> {
    let backend: Arc<dyn AttentionBackend> = Arc::new(FileAttentionBackend::new(dir));
    let conversation_id = conversation.record().id.to_string();
    dispatch_conversation(&backend, conversation, &conversation_id).await
}

async fn dispatch_conversation(
    backend: &Arc<dyn AttentionBackend>,
    conversation: &dyn HarnessConversation,
    conversation_id: &str,
) -> Result<()> {
    let mut open_turn = |items: Vec<InboxItem>| async move {
        // The dispatch lease is the serializer on this path — no wakeup file
        // lock, unlike `send_conversation_wakeup`.
        let result = conversation
            .send(SendRequest {
                input: vec![Message::User {
                    content: UserContent::String(compose_drained_prompt(&items)),
                }],
                session_id: None,
            })
            .await?;
        conversation.close_session(result.session_id).await?;
        Ok(())
    };
    run_dispatch(backend, conversation_id, &mut open_turn).await
}

/// One item's prompt travels verbatim — parity with the single-wake behavior
/// of the legacy funnel. Coalesced items are joined in drain order.
fn compose_drained_prompt(items: &[InboxItem]) -> String {
    items
        .iter()
        .map(|item| item.prompt.as_str())
        .collect::<Vec<_>>()
        .join("\n\n---\n\n")
}

/// Acquire the conversation's dispatch lease and drain it, handing each turn
/// to `open_turn`; return without dispatching when another dispatcher holds
/// the lease. After releasing, a non-empty re-list means an append raced the
/// release, so the loop takes the lease back rather than stranding the item.
async fn run_dispatch<F, Fut>(
    backend: &Arc<dyn AttentionBackend>,
    conversation_id: &str,
    open_turn: &mut F,
) -> Result<()>
where
    F: FnMut(Vec<InboxItem>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    loop {
        let Some(lease) = backend.acquire_dispatch_lease(conversation_id).await? else {
            tracing::trace!(
                conversation_id,
                "another dispatcher holds the conversation; item left for it"
            );
            return Ok(());
        };
        let outcome = drain_while_leased(backend, &lease, open_turn).await;
        backend.release_dispatch_lease(&lease).await?;
        outcome?;
        if backend.list_pending(conversation_id).await?.is_empty() {
            return Ok(());
        }
    }
}

/// The decision state is fixed at quiescent-over-idle: this process is not
/// running a turn when it dispatches, and scheduled commands still run before
/// their fire is appended, so [`SandboxState::CommandRunning`] is not yet
/// reachable from here. Moving scheduled commands under the dispatcher is
/// future work.
const DISPATCH_STATE: ConversationState = ConversationState {
    attention: AttentionState::Quiescent,
    sandbox: SandboxState::Idle,
};

async fn drain_while_leased<F, Fut>(
    backend: &Arc<dyn AttentionBackend>,
    lease: &DispatchLease,
    open_turn: &mut F,
) -> Result<()>
where
    F: FnMut(Vec<InboxItem>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    loop {
        let pending = backend.list_pending(&lease.conversation_id).await?;
        match decide(DISPATCH_STATE, &pending, now_ms()) {
            Action::OpenTurn { drain } | Action::FlushBatch { drain } => {
                let turn_ref = Uuid7::now().to_string();
                let transitioned = backend.mark_drained(&drain, &turn_ref).await?;
                if transitioned.is_empty() {
                    // Everything was already drained elsewhere; nothing to
                    // hand a turn. The re-list shows what is actually left.
                    continue;
                }
                let items: Vec<InboxItem> = drain
                    .iter()
                    .filter(|item_id| transitioned.contains(item_id))
                    .filter_map(|item_id| pending.iter().find(|item| item.item_id == *item_id))
                    .cloned()
                    .collect();
                // A turn mid-send cannot be killed, so a failed renewal only
                // logs: the successor's re-pend on acquire covers redelivery
                // if the lease is really gone.
                let renewal = spawn_lease_renewal(Arc::clone(backend), lease.clone());
                let sent = open_turn(items).await;
                renewal.abort();
                sent?;
                backend.mark_acknowledged(&transitioned).await?;
            }
            Action::Wait { until_ms } => {
                let now = now_ms();
                if until_ms > now {
                    tokio::time::sleep(Duration::from_millis(until_ms - now)).await;
                }
                // The lease may have timed out during the sleep; a successor
                // would re-pend and drain, so stop rather than double-drain.
                if !backend.renew_dispatch_lease(lease).await? {
                    return Ok(());
                }
            }
            Action::Nothing => return Ok(()),
            Action::OfferInterrupt { .. } => {
                debug_assert!(
                    false,
                    "OfferInterrupt is unreachable from a quiescent decision"
                );
                return Ok(());
            }
        }
    }
}

fn spawn_lease_renewal(
    backend: Arc<dyn AttentionBackend>,
    lease: DispatchLease,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(LEASE_TTL_MS / 3));
        // The first tick fires immediately; the lease was just acquired or
        // renewed, so skip it.
        interval.tick().await;
        loop {
            interval.tick().await;
            match backend.renew_dispatch_lease(&lease).await {
                Ok(true) => {}
                Ok(false) => tracing::error!(
                    conversation_id = %lease.conversation_id,
                    "dispatch lease lost mid-turn; continuing the send"
                ),
                Err(error) => tracing::error!(
                    conversation_id = %lease.conversation_id,
                    %error,
                    "dispatch lease renewal failed; continuing the send"
                ),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use tempfile::TempDir;

    use super::*;
    use crate::attention::{Attention, InMemoryAttentionBackend, ProducerKind, ProducerRef};

    const CONVERSATION: &str = "conversation-1";

    fn item(dedupe_key: &str, appended_at_ms: u64) -> InboxItem {
        InboxItem {
            item_id: Uuid7::now(),
            conversation_id: CONVERSATION.to_string(),
            producer: ProducerRef {
                kind: ProducerKind::Agent,
                id: "test-producer".to_string(),
            },
            dedupe_key: dedupe_key.to_string(),
            attention: Attention::Wake,
            native: Value::Null,
            prompt: format!("prompt for {dedupe_key}"),
            artifacts: Vec::new(),
            appended_at_ms,
        }
    }

    #[test]
    fn one_item_composes_to_exactly_its_prompt() {
        let single = item("single", 10);
        assert_eq!(
            compose_drained_prompt(std::slice::from_ref(&single)),
            single.prompt
        );
    }

    #[test]
    fn coalesced_items_compose_joined_in_drain_order() {
        let first = item("first", 10);
        let second = item("second", 20);
        assert_eq!(
            compose_drained_prompt(&[first, second]),
            "prompt for first\n\n---\n\nprompt for second"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_appends_drain_into_one_turn() {
        let backend: Arc<dyn AttentionBackend> = Arc::new(InMemoryAttentionBackend::new());
        backend.append_inbox_item(item("fire", 10)).await.unwrap();
        backend
            .append_inbox_item(item("message", 20))
            .await
            .unwrap();

        let turns = std::sync::Mutex::new(Vec::<String>::new());
        let mut open_turn = |items: Vec<InboxItem>| {
            turns.lock().unwrap().push(compose_drained_prompt(&items));
            async { Ok(()) }
        };
        run_dispatch(&backend, CONVERSATION, &mut open_turn)
            .await
            .unwrap();

        assert_eq!(
            *turns.lock().unwrap(),
            vec!["prompt for fire\n\n---\n\nprompt for message".to_string()]
        );
        assert!(backend.list_pending(CONVERSATION).await.unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_redelivered_append_dedupes_into_one_turn_with_one_item() {
        let backend: Arc<dyn AttentionBackend> = Arc::new(InMemoryAttentionBackend::new());
        backend
            .append_inbox_item(item("task-1:1000", 10))
            .await
            .unwrap();
        let redelivered = backend
            .append_inbox_item(item("task-1:1000", 20))
            .await
            .unwrap();
        assert!(redelivered.deduplicated);

        let turns = std::sync::Mutex::new(Vec::<usize>::new());
        let mut open_turn = |items: Vec<InboxItem>| {
            turns.lock().unwrap().push(items.len());
            async { Ok(()) }
        };
        run_dispatch(&backend, CONVERSATION, &mut open_turn)
            .await
            .unwrap();
        assert_eq!(*turns.lock().unwrap(), vec![1]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_held_lease_blocks_the_pass_and_leaves_the_item_pending() {
        let backend: Arc<dyn AttentionBackend> = Arc::new(InMemoryAttentionBackend::new());
        backend.append_inbox_item(item("wake", 10)).await.unwrap();
        let other_dispatcher = backend
            .acquire_dispatch_lease(CONVERSATION)
            .await
            .unwrap()
            .expect("lease should be granted");

        let turns = std::sync::Mutex::new(0usize);
        let mut open_turn = |_items: Vec<InboxItem>| {
            *turns.lock().unwrap() += 1;
            async { Ok(()) }
        };
        run_dispatch(&backend, CONVERSATION, &mut open_turn)
            .await
            .unwrap();
        // No turn opened, and the item is left pending for the lease holder.
        assert_eq!(*turns.lock().unwrap(), 0);
        assert_eq!(backend.list_pending(CONVERSATION).await.unwrap().len(), 1);

        backend
            .release_dispatch_lease(&other_dispatcher)
            .await
            .unwrap();
        run_dispatch(&backend, CONVERSATION, &mut open_turn)
            .await
            .unwrap();
        assert_eq!(*turns.lock().unwrap(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_send_that_dies_before_acknowledging_is_redelivered_on_the_next_acquire() {
        // The file backend, because re-pending drained items on acquire is
        // its crash-recovery move; the in-memory backend dies with the
        // process it would recover.
        let dir = TempDir::new().unwrap();
        let backend: Arc<dyn AttentionBackend> = Arc::new(FileAttentionBackend::new(dir.path()));
        backend.append_inbox_item(item("wake", 10)).await.unwrap();

        let mut failing_turn =
            |_items: Vec<InboxItem>| async { Err(anyhow::anyhow!("model exploded")) };
        let error = run_dispatch(&backend, CONVERSATION, &mut failing_turn)
            .await
            .expect_err("the failed send must surface");
        assert!(error.to_string().contains("model exploded"));
        // Drained but never acknowledged: invisible to list_pending until a
        // new acquire re-pends it.
        assert!(backend.list_pending(CONVERSATION).await.unwrap().is_empty());

        let turns = std::sync::Mutex::new(Vec::<String>::new());
        let mut open_turn = |items: Vec<InboxItem>| {
            turns.lock().unwrap().push(compose_drained_prompt(&items));
            async { Ok(()) }
        };
        run_dispatch(&backend, CONVERSATION, &mut open_turn)
            .await
            .unwrap();
        assert_eq!(*turns.lock().unwrap(), vec!["prompt for wake".to_string()]);
        assert!(backend.list_pending(CONVERSATION).await.unwrap().is_empty());
    }
}
