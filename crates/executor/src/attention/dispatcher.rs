use super::types::{Attention, InboxItem, ItemId};

/// Where the conversation is, from the dispatcher's point of view.
///
/// A conversation has two serial resources — the agent's attention and the
/// sandbox's world state — and one dispatcher owns both, so this is the whole
/// state space it needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationState {
    /// A turn is executing and is mid-round. Nothing can enter it, and no new
    /// turn may open.
    TurnRunning,
    /// A turn is executing and has reached a round boundary — the one point
    /// at which an [`Attention::Interrupt`] item may join it. The TypeScript
    /// harness re-materializes the prompt from the event log every round, so
    /// entry here costs no new machinery.
    AtRoundBoundary,
    /// No turn is running.
    Quiescent,
}

/// What the dispatcher should do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Open one turn draining these items, oldest first. All pending items go
    /// in, whatever their attention value: coalescing *is* draining, so N
    /// pending items produce one turn with N results, not N turns.
    OpenTurn { drain: Vec<ItemId> },
    /// Offer these interrupt items to the running turn at its round boundary.
    OfferInterrupt { items: Vec<ItemId> },
    /// Open one turn because a [`Attention::Batch`] item hit its `max_wait_ms`
    /// bound.
    ///
    /// Mechanically the same as [`Action::OpenTurn`] — one turn draining all
    /// pending items, FIFO — and reported separately because the trigger is
    /// different: `OpenTurn` is attention arriving, `FlushBatch` is the
    /// starvation bound expiring with nothing but held items to show for it.
    /// Telling them apart is what makes the liveness guarantee observable
    /// rather than inferred.
    FlushBatch { drain: Vec<ItemId> },
    /// Nothing to do until this wall-clock instant, when a batch bound
    /// expires. The dispatcher may also be woken earlier by an append.
    Wait { until_ms: u64 },
    /// Nothing to do, and no clock will change that — either the inbox is
    /// empty, or a turn is running and the pending items cannot enter it. The
    /// next decision point is the turn ending or an item arriving.
    Nothing,
}

/// The whole dispatcher decision, as a pure function.
///
/// No clock of its own: `now_ms` is a parameter, so the decision table is
/// exhaustively testable and a recorded sequence of `(state, pending, now_ms)`
/// replays to the same actions forever.
///
/// The contract it encodes:
///
/// - a running turn admits [`Attention::Interrupt`] items, and only at a
///   round boundary;
/// - quiescent, with any `Wake` or `Interrupt` pending, drains *everything*
///   pending into one turn — held `Batch` items ride along, which is what
///   "flushed with the next turn" means;
/// - quiescent, with only `Batch` items pending, waits until the earliest
///   `appended_at_ms + max_wait_ms` and then flushes;
/// - liveness: every pending item is drained by some turn. A `Wake` or
///   `Interrupt` is drained by the next quiescent decision; a `Batch` item is
///   drained no later than its own bound, because that bound is what the
///   `Wait` is computed from.
pub fn decide(state: ConversationState, pending: &[InboxItem], now_ms: u64) -> Action {
    if pending.is_empty() {
        return Action::Nothing;
    }
    match state {
        ConversationState::TurnRunning => Action::Nothing,
        ConversationState::AtRoundBoundary => {
            let items: Vec<ItemId> = fifo(pending)
                .into_iter()
                .filter(|item| item.attention == Attention::Interrupt)
                .map(|item| item.item_id)
                .collect();
            if items.is_empty() {
                Action::Nothing
            } else {
                Action::OfferInterrupt { items }
            }
        }
        ConversationState::Quiescent => {
            let ordered = fifo(pending);
            let drain: Vec<ItemId> = ordered.iter().map(|item| item.item_id).collect();
            if ordered
                .iter()
                .any(|item| matches!(item.attention, Attention::Wake | Attention::Interrupt))
            {
                return Action::OpenTurn { drain };
            }
            let earliest_flush = ordered
                .iter()
                .filter_map(|item| batch_deadline_ms(item))
                .min()
                .expect("only batch items remain, so at least one deadline exists");
            if earliest_flush <= now_ms {
                Action::FlushBatch { drain }
            } else {
                Action::Wait {
                    until_ms: earliest_flush,
                }
            }
        }
    }
}

/// When a `Batch` item must be flushed by; `None` for anything else.
///
/// Saturating, so a `max_wait_ms` large enough to overflow a `u64` of
/// milliseconds reads as "effectively never" rather than wrapping into a
/// deadline in the past and flushing immediately.
fn batch_deadline_ms(item: &InboxItem) -> Option<u64> {
    match item.attention {
        Attention::Batch { max_wait_ms } => Some(item.appended_at_ms.saturating_add(max_wait_ms)),
        Attention::Interrupt | Attention::Wake => None,
    }
}

/// Pending items oldest first. The sort is stable, so items sharing a
/// millisecond keep the order the backend produced them in — which is append
/// order — and the function is total over any input slice rather than
/// trusting the caller to have sorted.
fn fifo(pending: &[InboxItem]) -> Vec<&InboxItem> {
    let mut ordered: Vec<&InboxItem> = pending.iter().collect();
    ordered.sort_by_key(|item| item.appended_at_ms);
    ordered
}
