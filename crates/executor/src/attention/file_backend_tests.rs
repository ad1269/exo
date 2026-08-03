//! Cross-process behavior only: everything a [`FileAttentionBackend`] shares
//! with the in-memory backend is covered by the shared suite in
//! `backend_tests`, which runs against both. What is tested here is the part
//! only files provide — two backend instances over one directory agreeing.

use exoharness::Uuid7;
use serde_json::Value;
use tempfile::TempDir;

use super::file_backend::{LeaseRecord, lease_expired, write_json_file};
use super::{
    Attention, AttentionBackend, FileAttentionBackend, InboxItem, ProducerKind, ProducerRef,
};

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

#[tokio::test(flavor = "current_thread")]
async fn append_dedupes_across_two_instances_over_one_directory() {
    let dir = TempDir::new().expect("tempdir");
    let first_process = FileAttentionBackend::new(dir.path());
    let second_process = FileAttentionBackend::new(dir.path());

    let original = item("task-1:1000", 10);
    let outcome = first_process
        .append_inbox_item(original.clone())
        .await
        .expect("append");
    assert!(!outcome.deduplicated);

    // The other process redelivers the same fact with a fresh item id; the
    // dedupe file on disk is the shared truth.
    let redelivered = item("task-1:1000", 20);
    let outcome = second_process
        .append_inbox_item(redelivered)
        .await
        .expect("append redelivery");
    assert!(outcome.deduplicated);
    assert_eq!(outcome.item_id, original.item_id);

    let pending = second_process
        .list_pending(CONVERSATION)
        .await
        .expect("pending");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].item_id, original.item_id);
}

#[tokio::test(flavor = "current_thread")]
async fn a_lease_held_by_one_instance_blocks_another() {
    let dir = TempDir::new().expect("tempdir");
    let first_process = FileAttentionBackend::new(dir.path());
    let second_process = FileAttentionBackend::new(dir.path());

    let lease = first_process
        .acquire_dispatch_lease(CONVERSATION)
        .await
        .expect("acquire")
        .expect("lease should be granted");
    assert!(
        second_process
            .acquire_dispatch_lease(CONVERSATION)
            .await
            .expect("second acquire")
            .is_none()
    );

    assert!(
        first_process
            .release_dispatch_lease(&lease)
            .await
            .expect("release")
    );
    assert!(
        second_process
            .acquire_dispatch_lease(CONVERSATION)
            .await
            .expect("acquire after release")
            .is_some()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn an_expired_lease_is_taken_over_and_drained_items_re_pend() {
    let dir = TempDir::new().expect("tempdir");
    let dead_process = FileAttentionBackend::new(dir.path());
    let successor = FileAttentionBackend::new(dir.path());

    let committed = item("committed", 10);
    let in_flight = item("in-flight", 20);
    for pending in [&committed, &in_flight] {
        dead_process
            .append_inbox_item(pending.clone())
            .await
            .expect("append");
    }
    dead_process
        .mark_drained(&[committed.item_id, in_flight.item_id], "turn-1")
        .await
        .expect("drain");
    dead_process
        .mark_acknowledged(&[committed.item_id])
        .await
        .expect("acknowledge");

    // The holder died mid-turn: its lease sits on disk, expired.
    write_json_file(
        &dir.path().join(CONVERSATION).join("lease.json"),
        &LeaseRecord {
            conversation_id: CONVERSATION.to_string(),
            token: Uuid7::now().to_string(),
            holder_pid: 0,
            expires_at_ms: 1,
        },
    )
    .await
    .expect("write expired lease");

    let lease = successor
        .acquire_dispatch_lease(CONVERSATION)
        .await
        .expect("acquire")
        .expect("expired lease should be taken over");
    assert!(successor.renew_dispatch_lease(&lease).await.expect("renew"));

    // The drained-but-unacknowledged item came back; the acknowledged one is
    // committed history and stays put.
    let pending = successor.list_pending(CONVERSATION).await.expect("pending");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].item_id, in_flight.item_id);
}

#[tokio::test(flavor = "current_thread")]
async fn a_fresh_acquire_re_pends_items_a_previous_holder_left_drained() {
    let dir = TempDir::new().expect("tempdir");
    let backend = FileAttentionBackend::new(dir.path());

    let orphan = item("orphan", 10);
    backend
        .append_inbox_item(orphan.clone())
        .await
        .expect("append");
    let lease = backend
        .acquire_dispatch_lease(CONVERSATION)
        .await
        .expect("acquire")
        .expect("lease should be granted");
    backend
        .mark_drained(&[orphan.item_id], "turn-1")
        .await
        .expect("drain");
    // Released without acknowledging — the turn never committed.
    backend
        .release_dispatch_lease(&lease)
        .await
        .expect("release");

    backend
        .acquire_dispatch_lease(CONVERSATION)
        .await
        .expect("reacquire")
        .expect("lease should be granted again");
    let pending = backend.list_pending(CONVERSATION).await.expect("pending");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].item_id, orphan.item_id);
}

// Every process saw the same expired lease; at most one may end up holding.
// The steal-then-restore in acquire is what this pins: without it a slow
// loser renames away the winner's fresh lease and a second dispatcher runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_takeovers_of_one_expired_lease_grant_at_most_one_winner() {
    for _ in 0..8 {
        let dir = TempDir::new().unwrap();
        tokio::fs::create_dir_all(dir.path().join(CONVERSATION))
            .await
            .expect("create conversation dir");
        write_json_file(
            &dir.path().join(CONVERSATION).join("lease.json"),
            &LeaseRecord {
                conversation_id: CONVERSATION.to_string(),
                token: Uuid7::now().to_string(),
                holder_pid: 0,
                expires_at_ms: 1,
            },
        )
        .await
        .expect("write expired lease");

        let acquires = (0..4).map(|_| {
            let path = dir.path().to_path_buf();
            tokio::spawn(async move {
                FileAttentionBackend::new(path)
                    .acquire_dispatch_lease(CONVERSATION)
                    .await
                    .expect("acquire")
            })
        });
        let mut winners = Vec::new();
        for acquire in acquires.collect::<Vec<_>>() {
            if let Some(lease) = acquire.await.expect("join") {
                winners.push(lease);
            }
        }
        assert_eq!(winners.len(), 1, "exactly one racer may take the lease");
        // The survivor's token is the one on disk: its renew must succeed.
        let survivor = FileAttentionBackend::new(dir.path());
        assert!(
            survivor
                .renew_dispatch_lease(&winners[0])
                .await
                .expect("renew")
        );
    }
}

#[test]
fn a_lease_expires_strictly_after_its_deadline() {
    let record = LeaseRecord {
        conversation_id: CONVERSATION.to_string(),
        token: "token".to_string(),
        holder_pid: 0,
        expires_at_ms: 1_000,
    };
    assert!(!lease_expired(&record, 999));
    assert!(!lease_expired(&record, 1_000));
    assert!(lease_expired(&record, 1_001));
}
