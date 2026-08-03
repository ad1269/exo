use serde_json::Value;

use super::{
    Action, AdapterInboundMessage, Attention, AttentionBackend, AttentionState, ConversationState,
    DispatchLease, InMemoryAttentionBackend, InboxItem, SandboxState, decide,
    inbox_item_from_adapter_message, inbox_item_from_fire,
};
use crate::{AdapterConfig, AdapterRecord, AdapterSource, NewAdapter, ScheduledFireRecord};

const CONVERSATION: &str = "conversation-1";
const T0: u64 = 1_700_000_000_000;
const BATCH_MAX_WAIT_MS: u64 = 60_000;
const QUIESCENT_IDLE: ConversationState = ConversationState {
    attention: AttentionState::Quiescent,
    sandbox: SandboxState::Idle,
};

/// The entire runtime half of the loop: renew the lease, read what is
/// pending, ask the pure core what to do, and do it. Everything that
/// *decides* anything is in [`decide`]; this carries the decision out and
/// hands the drained items to a turn. No real turn and no executor — the turn
/// sink is a closure.
async fn dispatch_once<F>(
    backend: &dyn AttentionBackend,
    lease: &DispatchLease,
    state: ConversationState,
    now_ms: u64,
    open_turn: &mut F,
) -> Action
where
    F: FnMut(&[InboxItem]) -> String,
{
    assert!(
        backend.renew_dispatch_lease(lease).await.expect("renew"),
        "the dispatcher must still hold its lease to decide"
    );
    let pending = backend
        .list_pending(&lease.conversation_id)
        .await
        .expect("pending");
    let action = decide(state, &pending, now_ms);
    let drain = match &action {
        Action::OpenTurn { drain } | Action::FlushBatch { drain } => drain.clone(),
        Action::OfferInterrupt { .. } | Action::Wait { .. } | Action::Nothing => return action,
    };
    let drained: Vec<InboxItem> = drain
        .iter()
        .map(|item_id| {
            pending
                .iter()
                .find(|item| item.item_id == *item_id)
                .expect("drained items come from the pending list")
                .clone()
        })
        .collect();
    let turn_ref = open_turn(&drained);
    backend
        .mark_drained(&drain, &turn_ref)
        .await
        .expect("mark drained");
    backend
        .mark_acknowledged(&drain)
        .await
        .expect("mark acknowledged");
    action
}

fn fire(task_id: &str, task_name: &str, slot_ms: u64, prompt: &str) -> ScheduledFireRecord {
    ScheduledFireRecord {
        task_id: task_id.to_string(),
        task_name: task_name.to_string(),
        slot_ms,
        run_id: format!("run-{slot_ms}"),
        agent_id: "agent-1".to_string(),
        conversation_id: CONVERSATION.to_string(),
        prompt: prompt.to_string(),
        fired_at_ms: slot_ms,
    }
}

fn adapter() -> AdapterRecord {
    AdapterRecord::new(
        NewAdapter {
            agent_id: "agent-1".to_string(),
            conversation_id: CONVERSATION.to_string(),
            name: "chat".to_string(),
            source: AdapterSource::Library,
            config: AdapterConfig {
                adapter_type: "exochat".to_string(),
                worker_command: vec!["node".to_string(), "worker.js".to_string()],
                initialization: Value::Null,
                state_dir: None,
                secret_env: Vec::new(),
            },
        },
        T0,
    )
    .expect("adapter record")
}

fn inbound<'a>(
    message_id: &'a str,
    text: &'a str,
    metadata: &'a Value,
) -> AdapterInboundMessage<'a> {
    AdapterInboundMessage {
        target: "#room",
        sender: Some("ad"),
        message_id,
        text,
        metadata,
        attachments: &[],
    }
}

/// The whole loop against the process-local backend: two producers coalesce
/// into one turn, a redelivered fire never becomes a second item, a held
/// batch item waits out its own bound and then flushes on it, and a second
/// dispatcher cannot run while the first holds the lease.
#[tokio::test(flavor = "current_thread")]
async fn the_dispatcher_coalesces_two_producers_holds_a_batch_item_and_flushes_it_on_its_bound() {
    let backend = InMemoryAttentionBackend::new();
    let adapter = adapter();
    let metadata = Value::Null;
    let health_check = fire("task-health", "health check", T0, "health check output");

    // A scheduled fire and a human message arrive while the conversation is
    // quiescent, five milliseconds apart.
    let fire_item = inbox_item_from_fire(&health_check, Attention::Wake, T0).expect("fire item");
    assert!(
        !backend
            .append_inbox_item(fire_item.clone())
            .await
            .expect("append fire")
            .deduplicated
    );

    let message_item = inbox_item_from_adapter_message(
        &adapter,
        inbound("platform-42", "are we up?", &metadata),
        Attention::Wake,
        T0 + 5,
    )
    .expect("message item");
    assert!(
        !backend
            .append_inbox_item(message_item.clone())
            .await
            .expect("append message")
            .deduplicated
    );

    // The scheduler restarts and redelivers the same fire. Same task, same
    // slot, same dedupe key — it must collapse onto the item already pending
    // even though it arrives with a fresh item id.
    let redelivered =
        inbox_item_from_fire(&health_check, Attention::Wake, T0 + 7).expect("redelivered item");
    assert_ne!(redelivered.item_id, fire_item.item_id);
    let redelivered_append = backend
        .append_inbox_item(redelivered)
        .await
        .expect("append redelivery");
    assert!(redelivered_append.deduplicated);
    assert_eq!(redelivered_append.item_id, fire_item.item_id);

    let lease = backend
        .acquire_dispatch_lease(CONVERSATION)
        .await
        .expect("acquire")
        .expect("lease should be granted");
    // A second process reaching for the same conversation gets nothing, which
    // is the whole point of the lease: one dispatcher deciding at a time.
    assert!(
        backend
            .acquire_dispatch_lease(CONVERSATION)
            .await
            .expect("second acquire")
            .is_none()
    );

    let mut turns: Vec<Vec<String>> = Vec::new();
    let mut open_turn = |items: &[InboxItem]| {
        turns.push(items.iter().map(|item| item.prompt.clone()).collect());
        format!("turn-{}", turns.len())
    };

    // One turn, both items, oldest first: coalescing is draining.
    let opened = dispatch_once(&backend, &lease, QUIESCENT_IDLE, T0 + 10, &mut open_turn).await;
    assert_eq!(
        opened,
        Action::OpenTurn {
            drain: vec![fire_item.item_id, message_item.item_id]
        }
    );
    assert!(
        backend
            .list_pending(CONVERSATION)
            .await
            .expect("pending")
            .is_empty()
    );

    // Redelivery after the drain is still one item: the dedupe key outlives
    // the drain, so a restarted scheduler cannot replay a turn.
    let late_redelivery =
        inbox_item_from_fire(&health_check, Attention::Wake, T0 + 20).expect("late redelivery");
    assert!(
        backend
            .append_inbox_item(late_redelivery)
            .await
            .expect("append late redelivery")
            .deduplicated
    );
    assert!(
        backend
            .list_pending(CONVERSATION)
            .await
            .expect("pending")
            .is_empty()
    );

    // A background scrape maps to Batch: held for coalescing, bounded by the
    // wait it declares.
    let scrape = fire("task-scrape", "background scrape", T0 + 30, "scrape output");
    let scrape_item = inbox_item_from_fire(
        &scrape,
        Attention::Batch {
            max_wait_ms: BATCH_MAX_WAIT_MS,
        },
        T0 + 30,
    )
    .expect("scrape item");
    backend
        .append_inbox_item(scrape_item.clone())
        .await
        .expect("append scrape");

    let held = dispatch_once(&backend, &lease, QUIESCENT_IDLE, T0 + 40, &mut open_turn).await;
    assert_eq!(
        held,
        Action::Wait {
            until_ms: T0 + 30 + BATCH_MAX_WAIT_MS
        }
    );
    assert_eq!(
        backend
            .list_pending(CONVERSATION)
            .await
            .expect("pending")
            .len(),
        1
    );

    // At its bound it flushes on its own, with no second producer needed to
    // carry it: that is the starvation guarantee.
    let flushed = dispatch_once(
        &backend,
        &lease,
        QUIESCENT_IDLE,
        T0 + 30 + BATCH_MAX_WAIT_MS,
        &mut open_turn,
    )
    .await;
    assert_eq!(
        flushed,
        Action::FlushBatch {
            drain: vec![scrape_item.item_id]
        }
    );

    let idle = dispatch_once(
        &backend,
        &lease,
        QUIESCENT_IDLE,
        T0 + 200_000,
        &mut open_turn,
    )
    .await;
    assert_eq!(idle, Action::Nothing);

    // Two turns for four appends, and the fire and the message shared the
    // first one.
    assert_eq!(
        turns,
        vec![
            vec!["health check output".to_string(), "are we up?".to_string()],
            vec!["scrape output".to_string()],
        ]
    );

    // The lease is given up only by its holder, and the next dispatcher can
    // then take it.
    assert!(
        backend
            .release_dispatch_lease(&lease)
            .await
            .expect("release")
    );
    assert!(
        backend
            .acquire_dispatch_lease(CONVERSATION)
            .await
            .expect("acquire after release")
            .is_some()
    );
}
