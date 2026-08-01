use exoharness::Uuid7;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Identity of an inbox item. Minted at append; UUIDv7, so lexicographic
/// order is mint order.
pub type ItemId = Uuid7;

/// Who appended an item.
///
/// A human arrives *via* an adapter — a chat platform, the CLI — so there is
/// no `human` kind: the adapter knows it carries a human and maps
/// accordingly.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProducerKind {
    Scheduler,
    Adapter,
    Agent,
}

/// The producer that appended an item, and which one of its kind.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProducerRef {
    pub kind: ProducerKind,
    /// Producer-scoped id: the task id for a fire, the adapter id for an
    /// inbound message.
    pub id: String,
}

/// What the runtime may mechanically do with a pending item — not a rank.
///
/// A rank across producers would be fake precision: there is no true ordering
/// between "the scheduler thinks this is critical" and "a human sent a DM".
/// What *is* closed is the set of actions available to the runtime, so that
/// is the one cross-producer field. Everything else a producer means by
/// "priority" stays in [`InboxItem::native`] and travels opaquely for the
/// agent to read.
///
/// Cross-producer contention then answers itself: two `Wake` items — a
/// scheduled fire and a human message — enter the *same* drained turn, tagged
/// with producer and native context, and the agent decides what to address
/// first. The runtime decides when items reach the surface; the agent decides
/// what deserves attention within the turn.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Attention {
    /// May enter a RUNNING turn at the next round boundary. Steering is this
    /// value, not a separate mechanism.
    Interrupt,
    /// Opens a turn when the conversation is quiescent. FIFO among peers.
    Wake,
    /// Held for coalescing; flushed with the next turn, or on its own once
    /// `max_wait_ms` has elapsed since the item was appended. The starvation
    /// bound is declared rather than emergent.
    Batch { max_wait_ms: u64 },
}

/// A conversation artifact the item's payload refers to.
///
/// The id is the substrate's artifact id — the same string
/// `ScheduledTaskRunRecord::result_artifact_id` carries. Nothing in this
/// module interprets it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRef {
    pub id: String,
}

/// An inbox item: one thing that has happened and is owed the agent's
/// attention.
///
/// Identity is the *logical fact*, not a delivery attempt. Two appends of the
/// same fact — a redelivered fire, a re-emitted platform message — carry the
/// same `dedupe_key` and collapse to one item; see
/// [`AttentionBackend::append_inbox_item`](super::AttentionBackend::append_inbox_item).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InboxItem {
    pub item_id: ItemId,
    pub conversation_id: String,
    pub producer: ProducerRef,
    /// Per-OCCURRENCE dedupe key: `(task_id, slot_ms)` for a scheduled fire,
    /// the platform message id for adapter inbound. A key scoped wider than
    /// one occurrence collapses distinct deliveries into one item and loses
    /// work; a key scoped narrower than one occurrence lets a redelivery
    /// wake the conversation twice.
    pub dedupe_key: String,
    pub attention: Attention,
    /// Producer-native semantics, preserved opaquely: the task's declared
    /// urgency, the platform thread ref, whatever the producer means. The
    /// agent sees this; the runtime never interprets it.
    pub native: Value,
    /// Rendered payload — the fire's report prompt, the message text.
    pub prompt: String,
    pub artifacts: Vec<ArtifactRef>,
    /// When the item entered the inbox, which is not when the fact occurred:
    /// a fire redelivered after a crash was fired long before it was
    /// appended. Drain order is append order.
    pub appended_at_ms: u64,
}

/// Where an item is in the drain lifecycle.
///
/// Exactly-once delivery to the agent is these transitions and nothing else —
/// there is no second dedupe layer under them. `Pending` items are what
/// [`AttentionBackend::list_pending`](super::AttentionBackend::list_pending)
/// returns; `Drained` means a turn was opened over the item; `Acknowledged`
/// means that turn committed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InboxItemStatus {
    Pending,
    Drained {
        /// The turn the item was drained into. Opaque to this layer: a
        /// coordinator-backed backend would put its turn id here.
        turn_ref: String,
    },
    Acknowledged {
        turn_ref: String,
    },
}

/// Outcome of an append.
///
/// `item_id` is the id of the item now in the inbox — on a duplicate that is
/// the id of the item already there, not the one the caller just minted, so a
/// producer can name what it appended either way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendOutcome {
    pub item_id: ItemId,
    /// True when `(conversation_id, dedupe_key)` was already present and this
    /// append changed nothing.
    pub deduplicated: bool,
}

/// A held per-conversation dispatch lease: the right to be the one dispatcher
/// deciding for this conversation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DispatchLease {
    pub conversation_id: String,
    /// Fencing token, minted per acquisition. A holder that lost the lease and
    /// took it again presents a different token, so a renew or release left
    /// over from the previous acquisition cannot touch the live one.
    pub token: String,
}
