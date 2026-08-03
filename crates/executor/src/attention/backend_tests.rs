use exoharness::Uuid7;
use serde_json::Value;
use tempfile::TempDir;

use super::{
    Attention, AttentionBackend, FileAttentionBackend, InMemoryAttentionBackend, InboxItem,
    ProducerKind, ProducerRef,
};

#[derive(Clone, Copy)]
enum BackendKind {
    InMemory,
    File,
}

/// Keeps the file backend's directory alive for the test's duration.
struct TestBackend {
    backend: Box<dyn AttentionBackend>,
    _dir: Option<TempDir>,
}

fn make_backend(kind: BackendKind) -> TestBackend {
    match kind {
        BackendKind::InMemory => TestBackend {
            backend: Box::new(InMemoryAttentionBackend::new()),
            _dir: None,
        },
        BackendKind::File => {
            let dir = TempDir::new().expect("tempdir");
            TestBackend {
                backend: Box::new(FileAttentionBackend::new(dir.path())),
                _dir: Some(dir),
            }
        }
    }
}

/// Run every shared backend test against every implementation.
///
/// A coordinator-backed backend adds a module here and inherits the whole
/// suite unchanged — the discipline exoharness#113 uses to hold its in-memory
/// and file-backed stores to one set of semantics. These are written against
/// `&dyn AttentionBackend` for the same reason: nothing in them may depend on
/// the storage medium.
macro_rules! attention_backend_suite {
    ($($test:ident),* $(,)?) => {
        mod in_memory {
            $(
                #[tokio::test(flavor = "current_thread")]
                async fn $test() {
                    let backend = super::make_backend(super::BackendKind::InMemory);
                    super::$test(backend.backend.as_ref()).await;
                }
            )*
        }
        mod file {
            $(
                #[tokio::test(flavor = "current_thread")]
                async fn $test() {
                    let backend = super::make_backend(super::BackendKind::File);
                    super::$test(backend.backend.as_ref()).await;
                }
            )*
        }
    };
}

attention_backend_suite!(
    append_is_idempotent_on_conversation_and_dedupe_key,
    dedupe_keys_are_scoped_to_their_conversation,
    pending_items_come_back_oldest_first,
    drained_items_leave_pending_and_transition_once,
    acknowledged_requires_drained_and_transitions_once,
    dispatch_lease_is_exclusive_until_released,
    a_stale_lease_can_neither_renew_nor_release,
);

fn item(
    conversation_id: &str,
    dedupe_key: &str,
    attention: Attention,
    appended_at_ms: u64,
) -> InboxItem {
    InboxItem {
        item_id: Uuid7::now(),
        conversation_id: conversation_id.to_string(),
        producer: ProducerRef {
            kind: ProducerKind::Agent,
            id: "test-producer".to_string(),
        },
        dedupe_key: dedupe_key.to_string(),
        attention,
        native: Value::Null,
        prompt: format!("prompt for {dedupe_key}"),
        artifacts: Vec::new(),
        appended_at_ms,
    }
}

async fn append_is_idempotent_on_conversation_and_dedupe_key(backend: &dyn AttentionBackend) {
    let first = item("conversation", "task-1:1000", Attention::Wake, 10);
    let first_outcome = backend
        .append_inbox_item(first.clone())
        .await
        .expect("append");
    assert!(!first_outcome.deduplicated);
    assert_eq!(first_outcome.item_id, first.item_id);

    // The same fact appended again — a redelivery — mints a fresh item id and
    // must still collapse onto the item already there.
    let redelivered = item("conversation", "task-1:1000", Attention::Wake, 20);
    assert_ne!(redelivered.item_id, first.item_id);
    let second_outcome = backend
        .append_inbox_item(redelivered)
        .await
        .expect("append");
    assert!(second_outcome.deduplicated);
    assert_eq!(second_outcome.item_id, first.item_id);

    let pending = backend.list_pending("conversation").await.expect("pending");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].item_id, first.item_id);
    assert_eq!(pending[0].appended_at_ms, 10);
}

async fn dedupe_keys_are_scoped_to_their_conversation(backend: &dyn AttentionBackend) {
    for conversation in ["conversation-a", "conversation-b"] {
        let outcome = backend
            .append_inbox_item(item(conversation, "shared-key", Attention::Wake, 10))
            .await
            .expect("append");
        assert!(!outcome.deduplicated);
    }
    for conversation in ["conversation-a", "conversation-b"] {
        assert_eq!(
            backend
                .list_pending(conversation)
                .await
                .expect("pending")
                .len(),
            1
        );
    }
}

async fn pending_items_come_back_oldest_first(backend: &dyn AttentionBackend) {
    // Appended newest-first, and with a tie, so ordering cannot come from
    // append order alone nor from the timestamp alone.
    let late = item("conversation", "late", Attention::Wake, 300);
    let tie_first = item("conversation", "tie-first", Attention::Wake, 100);
    let tie_second = item("conversation", "tie-second", Attention::Wake, 100);
    for pending in [&late, &tie_first, &tie_second] {
        backend
            .append_inbox_item(pending.clone())
            .await
            .expect("append");
    }

    let pending = backend.list_pending("conversation").await.expect("pending");
    let keys: Vec<&str> = pending
        .iter()
        .map(|item| item.dedupe_key.as_str())
        .collect();
    assert_eq!(keys, vec!["tie-first", "tie-second", "late"]);
}

async fn drained_items_leave_pending_and_transition_once(backend: &dyn AttentionBackend) {
    let first = item("conversation", "first", Attention::Wake, 10);
    let second = item("conversation", "second", Attention::Wake, 20);
    let held = item("conversation", "held", Attention::Wake, 30);
    for pending in [&first, &second, &held] {
        backend
            .append_inbox_item(pending.clone())
            .await
            .expect("append");
    }

    let drained = backend
        .mark_drained(&[first.item_id, second.item_id], "turn-1")
        .await
        .expect("drain");
    assert_eq!(drained, vec![first.item_id, second.item_id]);

    let pending = backend.list_pending("conversation").await.expect("pending");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].item_id, held.item_id);

    // Replaying the drain — the same turn re-opened after a crash — must not
    // hand the same items to a second turn.
    let replayed = backend
        .mark_drained(&[first.item_id, second.item_id], "turn-2")
        .await
        .expect("drain again");
    assert!(replayed.is_empty());
    assert_eq!(
        backend
            .list_pending("conversation")
            .await
            .expect("pending")
            .len(),
        1
    );
}

async fn acknowledged_requires_drained_and_transitions_once(backend: &dyn AttentionBackend) {
    let drained_item = item("conversation", "drained", Attention::Wake, 10);
    let pending_item = item("conversation", "pending", Attention::Wake, 20);
    for pending in [&drained_item, &pending_item] {
        backend
            .append_inbox_item(pending.clone())
            .await
            .expect("append");
    }
    backend
        .mark_drained(&[drained_item.item_id], "turn-1")
        .await
        .expect("drain");

    // A pending item was never handed to a turn, so it has no commit to
    // acknowledge and is left where it is.
    let acknowledged = backend
        .mark_acknowledged(&[drained_item.item_id, pending_item.item_id])
        .await
        .expect("acknowledge");
    assert_eq!(acknowledged, vec![drained_item.item_id]);
    assert_eq!(
        backend
            .list_pending("conversation")
            .await
            .expect("pending")
            .len(),
        1
    );

    let replayed = backend
        .mark_acknowledged(&[drained_item.item_id])
        .await
        .expect("acknowledge again");
    assert!(replayed.is_empty());
}

async fn dispatch_lease_is_exclusive_until_released(backend: &dyn AttentionBackend) {
    let held = backend
        .acquire_dispatch_lease("conversation")
        .await
        .expect("acquire")
        .expect("lease should be granted");
    assert!(
        backend
            .acquire_dispatch_lease("conversation")
            .await
            .expect("second acquire")
            .is_none()
    );
    // Another conversation is a separate resource entirely.
    assert!(
        backend
            .acquire_dispatch_lease("other-conversation")
            .await
            .expect("acquire other")
            .is_some()
    );

    assert!(backend.renew_dispatch_lease(&held).await.expect("renew"));
    assert!(
        backend
            .release_dispatch_lease(&held)
            .await
            .expect("release")
    );
    assert!(
        backend
            .acquire_dispatch_lease("conversation")
            .await
            .expect("acquire after release")
            .is_some()
    );
}

async fn a_stale_lease_can_neither_renew_nor_release(backend: &dyn AttentionBackend) {
    let stale = backend
        .acquire_dispatch_lease("conversation")
        .await
        .expect("acquire")
        .expect("lease should be granted");
    backend
        .release_dispatch_lease(&stale)
        .await
        .expect("release");
    let live = backend
        .acquire_dispatch_lease("conversation")
        .await
        .expect("reacquire")
        .expect("lease should be granted again");
    assert_ne!(stale.token, live.token);

    // The fencing token: the previous acquisition cannot renew the live lease
    // back to life, nor release someone else's.
    assert!(!backend.renew_dispatch_lease(&stale).await.expect("renew"));
    assert!(
        !backend
            .release_dispatch_lease(&stale)
            .await
            .expect("release")
    );
    assert!(backend.renew_dispatch_lease(&live).await.expect("renew"));
}
