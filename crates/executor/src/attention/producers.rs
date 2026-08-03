use anyhow::Result;
use exoharness::Uuid7;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::types::{Attention, InboxItem, ProducerKind, ProducerRef};
use crate::{AdapterAttachment, AdapterRecord, ScheduledFireRecord};

/// A scheduled fire's producer-native context, preserved for the agent.
///
/// The runtime never reads this. It exists so a drained turn can tell the
/// agent *which* task fired, for which slot, and how late — the things a
/// closed [`Attention`] set deliberately does not encode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerNative {
    pub task_id: String,
    pub task_name: String,
    pub slot_ms: u64,
    pub run_id: String,
    pub agent_id: String,
    /// When the fire happened, as against [`InboxItem::appended_at_ms`]. A
    /// fire redelivered after a crash was fired long before it reached the
    /// inbox, and the agent should be able to see the gap.
    pub fired_at_ms: u64,
}

/// An adapter message's producer-native context, preserved for the agent:
/// which adapter, which platform target, who sent it, and whatever metadata
/// the platform itself attached.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdapterNative {
    pub adapter_id: String,
    pub adapter_type: String,
    pub target: String,
    pub sender: Option<String>,
    pub message_id: String,
    pub metadata: Value,
    pub attachments: Vec<AdapterAttachment>,
}

/// The fields of one inbound platform message, borrowed from whatever the
/// adapter runtime received.
///
/// A plain view of the runtime's own worker message event, not a layer over
/// it: it keeps this mapping's signature to types the crate exports, and it
/// makes the platform message id required by the type rather than checked at
/// run time. A message the platform gave no id has no per-occurrence identity
/// to dedupe on — which is exactly the condition under which
/// `AdapterStore::record_inbound_message_once` does not dedupe it at ingest
/// either — so it cannot become an inbox item at all.
#[derive(Debug, Clone, Copy)]
pub struct AdapterInboundMessage<'a> {
    pub target: &'a str,
    pub sender: Option<&'a str>,
    pub message_id: &'a str,
    pub text: &'a str,
    pub metadata: &'a Value,
    pub attachments: &'a [AdapterAttachment],
}

/// The dedupe key for a scheduled fire: `(task_id, slot_ms)`.
///
/// The same identity [`ScheduledFireRecord`] already carries — its own
/// redelivery refuses a slot already delivered — lifted so it travels with
/// the item. Keyed on the run id instead, a redelivery of one slot would
/// become two items.
pub fn fire_dedupe_key(task_id: &str, slot_ms: u64) -> String {
    format!("{task_id}:{slot_ms}")
}

/// The dedupe key for an adapter inbound message.
///
/// The platform message id, qualified by the adapter and target it arrived
/// on — the same triple `AdapterStore::record_inbound_message_once` dedupes
/// on at ingest, because a platform message id is only unique within its own
/// adapter and target.
pub fn adapter_dedupe_key(adapter_id: &str, target: &str, message_id: &str) -> String {
    format!("{adapter_id}:{target}:{message_id}")
}

/// Map a scheduled fire onto an inbox item.
///
/// `attention` is the caller's, not this function's: how a task's fires map
/// is a per-producer decision that belongs with the task — a health check to
/// [`Attention::Interrupt`], a routine job to [`Attention::Wake`], a
/// background scrape to [`Attention::Batch`]. Semantics stay in the syntax at
/// the producer; the runtime executes mechanism.
///
/// `artifacts` is empty because a fire record carries its rendered prompt
/// rather than a pointer to the run: redelivery must not re-run the command,
/// so there is nothing to point at that the prompt does not already say.
pub fn inbox_item_from_fire(
    fire: &ScheduledFireRecord,
    attention: Attention,
    now_ms: u64,
) -> Result<InboxItem> {
    let native = SchedulerNative {
        task_id: fire.task_id.clone(),
        task_name: fire.task_name.clone(),
        slot_ms: fire.slot_ms,
        run_id: fire.run_id.clone(),
        agent_id: fire.agent_id.clone(),
        fired_at_ms: fire.fired_at_ms,
    };
    Ok(InboxItem {
        item_id: Uuid7::now(),
        conversation_id: fire.conversation_id.clone(),
        producer: ProducerRef {
            kind: ProducerKind::Scheduler,
            id: fire.task_id.clone(),
        },
        dedupe_key: fire_dedupe_key(&fire.task_id, fire.slot_ms),
        attention,
        native: serde_json::to_value(native)?,
        prompt: fire.prompt.clone(),
        artifacts: Vec::new(),
        appended_at_ms: now_ms,
    })
}

/// Map an adapter's inbound platform message onto an inbox item.
///
/// The mirror of [`inbox_item_from_fire`] for the other producer. A human
/// arrives *via* an adapter, so the producer kind is
/// [`ProducerKind::Adapter`] whoever is on the far end, and how a given
/// adapter maps its traffic — DM to [`Attention::Wake`], ambient channel
/// mention to [`Attention::Batch`] — is that adapter's configuration, which
/// is why `attention` is a parameter here too.
///
/// The prompt is the message as the platform sent it. Whether the item
/// carries that or the composed wakeup prompt is a wiring decision; the
/// mapping takes what the message carries.
pub fn inbox_item_from_adapter_message(
    adapter: &AdapterRecord,
    message: AdapterInboundMessage<'_>,
    attention: Attention,
    now_ms: u64,
) -> Result<InboxItem> {
    let native = AdapterNative {
        adapter_id: adapter.id.clone(),
        adapter_type: adapter.config.adapter_type.clone(),
        target: message.target.to_string(),
        sender: message.sender.map(str::to_string),
        message_id: message.message_id.to_string(),
        metadata: message.metadata.clone(),
        attachments: message.attachments.to_vec(),
    };
    Ok(InboxItem {
        item_id: Uuid7::now(),
        conversation_id: adapter.conversation_id.clone(),
        producer: ProducerRef {
            kind: ProducerKind::Adapter,
            id: adapter.id.clone(),
        },
        dedupe_key: adapter_dedupe_key(&adapter.id, message.target, message.message_id),
        attention,
        native: serde_json::to_value(native)?,
        prompt: message.text.to_string(),
        artifacts: Vec::new(),
        appended_at_ms: now_ms,
    })
}
