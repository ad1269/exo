//! The attention scheduler: what reaches the agent, when, in what order, and
//! coalesced how.
//!
//! Producers — the scheduler, adapters, the agent itself — append **inbox
//! items** to a conversation instead of racing each other for a wake lock.
//! Each item declares an [`Attention`] value, and a single dispatcher per
//! conversation drains pending items into turns. Four properties make the
//! layer trustworthy:
//!
//! - **identity at append** — the dedupe key each producer established at its
//!   own crossing travels up, so a redelivered fire or a re-emitted platform
//!   message cannot become two items;
//! - **ordering** — arrival order survives; FIFO among peers;
//! - **attention semantics** — [`Attention`] is the closed set of things the
//!   runtime can mechanically *do* with a pending item, not a rank.
//!   Everything else a producer means by "priority" rides opaquely in
//!   [`InboxItem::native`], which the agent reads and the runtime never
//!   interprets;
//! - **liveness** — every item is drained by some turn, with
//!   [`Attention::Batch`]'s `max_wait_ms` as the explicit starvation bound.
//!
//! The split those add up to: the runtime decides when items reach the
//! surface, the agent decides what deserves attention within the turn.
//!
//! One abstraction, [`AttentionBackend`], sits underneath; everything else
//! here is plain data and pure functions. [`decide`] in particular is a
//! function of `(state, pending, now_ms)` with no clock of its own, so the
//! decision table is exhaustively testable and a recorded sequence replays.
//!
//! The module is inert: it implements the semantics and nothing inside the
//! crate calls it. Wiring the three wake producers onto it — retiring the
//! `conversation_wakeup` lock file — is a separate change.

mod backend;
mod dispatcher;
mod producers;
mod types;

#[cfg(test)]
mod backend_tests;
#[cfg(test)]
mod dispatcher_tests;
#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod producers_tests;

pub use backend::{AttentionBackend, InMemoryAttentionBackend};
pub use dispatcher::{Action, ConversationState, decide};
pub use producers::{
    AdapterInboundMessage, AdapterNative, SchedulerNative, adapter_dedupe_key, fire_dedupe_key,
    inbox_item_from_adapter_message, inbox_item_from_fire,
};
pub use types::{
    AppendOutcome, ArtifactRef, Attention, DispatchLease, InboxItem, InboxItemStatus, ItemId,
    ProducerKind, ProducerRef,
};
