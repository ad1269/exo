use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use exoharness::Uuid7;

use super::types::{AppendOutcome, DispatchLease, InboxItem, InboxItemStatus, ItemId};

/// The two hard primitives the attention scheduler needs from storage, and
/// nothing else.
///
/// 1. **Idempotent durable append** — an item lands exactly once per
///    `(conversation_id, dedupe_key)`. A conditional put.
/// 2. **A per-conversation dispatch lease** — one dispatcher decision at a
///    time, across processes. The CLI, the scheduler runner and the adapter
///    runtime are separate processes, which is the entire reason a wake lock
///    file exists today.
///
/// Those two are exactly the turn coordinator's charter in exoharness#113:
/// its conditional puts are this trait's idempotent append, and its
/// per-conversation lease is this trait's dispatch lease. The durable,
/// cross-process implementation of this trait is that coordinator's
/// territory, and this module deliberately ships only the process-local
/// [`InMemoryAttentionBackend`] rather than growing a second, competing
/// queue with its own semantics. Nothing here imports from it.
///
/// Everything above this line is semantics and stays fixed; durability below
/// it upgrades.
#[async_trait]
pub trait AttentionBackend: Send + Sync {
    /// Append an item, idempotently on `(conversation_id, dedupe_key)`.
    ///
    /// A second append of the same key changes nothing and reports
    /// [`AppendOutcome::deduplicated`], returning the id of the item already
    /// in the inbox. This is where identity established at the producer's own
    /// crossing — a fire's `(task_id, slot_ms)`, a platform message id —
    /// stops a redelivery from becoming a second wake.
    async fn append_inbox_item(&self, item: InboxItem) -> Result<AppendOutcome>;

    /// Undrained items for a conversation, oldest first by `appended_at_ms`.
    /// Items appended in the same millisecond keep append order.
    async fn list_pending(&self, conversation_id: &str) -> Result<Vec<InboxItem>>;

    /// Move items to [`InboxItemStatus::Drained`] as a turn opens over them.
    ///
    /// Returns the ids that actually made the transition. An id already
    /// drained is not returned and not touched, so replaying a drain is a
    /// no-op rather than a second turn — the exactly-once property is this
    /// status transition, not a dedupe layer beneath it.
    async fn mark_drained(&self, item_ids: &[ItemId], turn_ref: &str) -> Result<Vec<ItemId>>;

    /// Move drained items to [`InboxItemStatus::Acknowledged`] as the turn
    /// commits. Returns the ids that made the transition; an id that is not
    /// currently drained is left alone.
    async fn mark_acknowledged(&self, item_ids: &[ItemId]) -> Result<Vec<ItemId>>;

    /// Take the conversation's dispatch lease, or `None` when another holder
    /// has it. Granting it to a second caller is the one failure this trait
    /// exists to prevent.
    async fn acquire_dispatch_lease(&self, conversation_id: &str) -> Result<Option<DispatchLease>>;

    /// Keep a held lease alive. Returns false when the lease is no longer
    /// held, at which point the caller must stop dispatching.
    ///
    /// A process-local backend has no expiry to extend — a holder that dies
    /// takes the whole process with it — so this is a liveness check there.
    /// The renewal exists in the contract because a durable backend times
    /// leases out to survive a holder that died in another process, and
    /// honest renewals have to be able to keep it.
    async fn renew_dispatch_lease(&self, lease: &DispatchLease) -> Result<bool>;

    /// Give the lease up. Returns false when the caller no longer held it.
    async fn release_dispatch_lease(&self, lease: &DispatchLease) -> Result<bool>;
}

/// Process-local [`AttentionBackend`]: identical semantics, no durability, no
/// cross-process scope.
///
/// The same shape exoharness#113 gives its own in-memory fallback — one set
/// of semantics, expressed once, over whichever store is available — and the
/// same honest limits. Two processes each holding one of these agree about
/// nothing, so this is a test and single-process substrate, not the answer
/// to the wake lock file it is modelled on.
#[derive(Default)]
pub struct InMemoryAttentionBackend {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Append order. Kept as a vector because that order *is* the FIFO
    /// contract, and a stable sort by `appended_at_ms` over it resolves ties
    /// back to it.
    items: Vec<StoredItem>,
    /// `(conversation_id, dedupe_key)` -> the item that claimed it.
    dedupe: HashMap<(String, String), ItemId>,
    /// `conversation_id` -> live lease token.
    leases: HashMap<String, String>,
}

struct StoredItem {
    item: InboxItem,
    status: InboxItemStatus,
}

impl InMemoryAttentionBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("attention backend state poisoned")
    }
}

#[async_trait]
impl AttentionBackend for InMemoryAttentionBackend {
    async fn append_inbox_item(&self, item: InboxItem) -> Result<AppendOutcome> {
        let mut state = self.lock();
        let key = (item.conversation_id.clone(), item.dedupe_key.clone());
        if let Some(existing) = state.dedupe.get(&key) {
            return Ok(AppendOutcome {
                item_id: *existing,
                deduplicated: true,
            });
        }
        let item_id = item.item_id;
        state.dedupe.insert(key, item_id);
        state.items.push(StoredItem {
            item,
            status: InboxItemStatus::Pending,
        });
        Ok(AppendOutcome {
            item_id,
            deduplicated: false,
        })
    }

    async fn list_pending(&self, conversation_id: &str) -> Result<Vec<InboxItem>> {
        let state = self.lock();
        let mut pending: Vec<InboxItem> = state
            .items
            .iter()
            .filter(|stored| {
                stored.item.conversation_id == conversation_id
                    && matches!(stored.status, InboxItemStatus::Pending)
            })
            .map(|stored| stored.item.clone())
            .collect();
        // Stable, so items sharing a millisecond come back in append order.
        pending.sort_by_key(|item| item.appended_at_ms);
        Ok(pending)
    }

    async fn mark_drained(&self, item_ids: &[ItemId], turn_ref: &str) -> Result<Vec<ItemId>> {
        let mut state = self.lock();
        let mut transitioned = Vec::new();
        for stored in &mut state.items {
            if item_ids.contains(&stored.item.item_id)
                && matches!(stored.status, InboxItemStatus::Pending)
            {
                stored.status = InboxItemStatus::Drained {
                    turn_ref: turn_ref.to_string(),
                };
                transitioned.push(stored.item.item_id);
            }
        }
        Ok(transitioned)
    }

    async fn mark_acknowledged(&self, item_ids: &[ItemId]) -> Result<Vec<ItemId>> {
        let mut state = self.lock();
        let mut transitioned = Vec::new();
        for stored in &mut state.items {
            if !item_ids.contains(&stored.item.item_id) {
                continue;
            }
            let InboxItemStatus::Drained { turn_ref } = &stored.status else {
                continue;
            };
            stored.status = InboxItemStatus::Acknowledged {
                turn_ref: turn_ref.clone(),
            };
            transitioned.push(stored.item.item_id);
        }
        Ok(transitioned)
    }

    async fn acquire_dispatch_lease(&self, conversation_id: &str) -> Result<Option<DispatchLease>> {
        let mut state = self.lock();
        if state.leases.contains_key(conversation_id) {
            return Ok(None);
        }
        let token = Uuid7::now().to_string();
        state
            .leases
            .insert(conversation_id.to_string(), token.clone());
        Ok(Some(DispatchLease {
            conversation_id: conversation_id.to_string(),
            token,
        }))
    }

    async fn renew_dispatch_lease(&self, lease: &DispatchLease) -> Result<bool> {
        let state = self.lock();
        Ok(state.leases.get(&lease.conversation_id) == Some(&lease.token))
    }

    async fn release_dispatch_lease(&self, lease: &DispatchLease) -> Result<bool> {
        let mut state = self.lock();
        if state.leases.get(&lease.conversation_id) != Some(&lease.token) {
            return Ok(false);
        }
        state.leases.remove(&lease.conversation_id);
        Ok(true)
    }
}
