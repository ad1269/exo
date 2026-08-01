use exoharness::Uuid7;
use serde_json::Value;

use super::{
    Action, Attention, ConversationState, InboxItem, ItemId, ProducerKind, ProducerRef, decide,
};

fn item(dedupe_key: &str, attention: Attention, appended_at_ms: u64) -> InboxItem {
    InboxItem {
        item_id: Uuid7::now(),
        conversation_id: "conversation".to_string(),
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

fn ids(items: &[InboxItem]) -> Vec<ItemId> {
    items.iter().map(|item| item.item_id).collect()
}

#[test]
fn an_empty_inbox_asks_for_nothing_in_every_state() {
    for state in [
        ConversationState::TurnRunning,
        ConversationState::AtRoundBoundary,
        ConversationState::Quiescent,
    ] {
        assert_eq!(decide(state, &[], 1_000), Action::Nothing);
    }
}

#[test]
fn a_running_turn_mid_round_admits_nothing() {
    let pending = vec![
        item("interrupt", Attention::Interrupt, 10),
        item("wake", Attention::Wake, 20),
        item("overdue-batch", Attention::Batch { max_wait_ms: 5 }, 30),
    ];
    assert_eq!(
        decide(ConversationState::TurnRunning, &pending, 10_000),
        Action::Nothing
    );
}

#[test]
fn a_round_boundary_offers_interrupts_and_only_interrupts() {
    let pending = vec![
        item("wake", Attention::Wake, 10),
        item("interrupt-late", Attention::Interrupt, 30),
        item("batch", Attention::Batch { max_wait_ms: 1 }, 15),
        item("interrupt-early", Attention::Interrupt, 20),
    ];
    let expected = vec![pending[3].item_id, pending[1].item_id];
    assert_eq!(
        decide(ConversationState::AtRoundBoundary, &pending, 10_000),
        Action::OfferInterrupt { items: expected }
    );
}

#[test]
fn a_round_boundary_with_no_interrupt_waits_for_the_turn_to_end() {
    // The batch bound below is long past, and it still cannot open a turn:
    // one is already running.
    let pending = vec![
        item("wake", Attention::Wake, 10),
        item("batch", Attention::Batch { max_wait_ms: 5 }, 20),
    ];
    assert_eq!(
        decide(ConversationState::AtRoundBoundary, &pending, 10_000),
        Action::Nothing
    );
}

#[test]
fn quiescent_with_a_wake_opens_a_turn() {
    let pending = vec![item("wake", Attention::Wake, 10)];
    assert_eq!(
        decide(ConversationState::Quiescent, &pending, 1_000),
        Action::OpenTurn {
            drain: ids(&pending)
        }
    );
}

#[test]
fn quiescent_with_an_interrupt_opens_a_turn_rather_than_waiting_for_a_boundary() {
    let pending = vec![item("interrupt", Attention::Interrupt, 10)];
    assert_eq!(
        decide(ConversationState::Quiescent, &pending, 1_000),
        Action::OpenTurn {
            drain: ids(&pending)
        }
    );
}

#[test]
fn one_wake_drains_every_pending_item_into_one_turn() {
    // The coalescing property: a fire, a human message and three held batch
    // items produce ONE turn with five results, not five turns.
    let pending = vec![
        item("fire", Attention::Wake, 10),
        item("message", Attention::Wake, 20),
        item(
            "scrape-a",
            Attention::Batch {
                max_wait_ms: 600_000,
            },
            30,
        ),
        item(
            "scrape-b",
            Attention::Batch {
                max_wait_ms: 600_000,
            },
            40,
        ),
        item(
            "scrape-c",
            Attention::Batch {
                max_wait_ms: 600_000,
            },
            50,
        ),
    ];
    let action = decide(ConversationState::Quiescent, &pending, 1_000);
    assert_eq!(
        action,
        Action::OpenTurn {
            drain: ids(&pending)
        }
    );
}

#[test]
fn drain_order_is_append_order_whatever_order_the_backend_hands_over() {
    let first = item("first", Attention::Wake, 10);
    let second = item("second", Attention::Wake, 20);
    let third = item("third", Attention::Wake, 30);
    let shuffled = vec![third.clone(), first.clone(), second.clone()];
    assert_eq!(
        decide(ConversationState::Quiescent, &shuffled, 1_000),
        Action::OpenTurn {
            drain: vec![first.item_id, second.item_id, third.item_id]
        }
    );
}

#[test]
fn held_batch_items_wait_until_the_earliest_bound_expires() {
    let pending = vec![
        item("far", Attention::Batch { max_wait_ms: 5_000 }, 1_000),
        item("near", Attention::Batch { max_wait_ms: 500 }, 1_000),
    ];
    assert_eq!(
        decide(ConversationState::Quiescent, &pending, 1_200),
        Action::Wait { until_ms: 1_500 }
    );
}

#[test]
fn a_batch_bound_flushes_on_the_millisecond_it_expires() {
    let pending = vec![item("batch", Attention::Batch { max_wait_ms: 500 }, 1_000)];
    assert_eq!(
        decide(ConversationState::Quiescent, &pending, 1_499),
        Action::Wait { until_ms: 1_500 }
    );
    assert_eq!(
        decide(ConversationState::Quiescent, &pending, 1_500),
        Action::FlushBatch {
            drain: ids(&pending)
        }
    );
}

#[test]
fn an_expired_bound_flushes_every_held_item_not_just_the_expired_one() {
    let pending = vec![
        item("expired", Attention::Batch { max_wait_ms: 500 }, 1_000),
        item(
            "still-held",
            Attention::Batch {
                max_wait_ms: 900_000,
            },
            1_100,
        ),
    ];
    assert_eq!(
        decide(ConversationState::Quiescent, &pending, 2_000),
        Action::FlushBatch {
            drain: ids(&pending)
        }
    );
}

#[test]
fn a_bound_too_large_to_represent_reads_as_never_rather_than_as_overdue() {
    let pending = vec![item(
        "batch",
        Attention::Batch {
            max_wait_ms: u64::MAX,
        },
        10,
    )];
    assert_eq!(
        decide(ConversationState::Quiescent, &pending, 1_000),
        Action::Wait { until_ms: u64::MAX }
    );
}
