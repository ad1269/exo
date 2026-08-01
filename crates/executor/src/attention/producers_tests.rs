use serde_json::{Value, json};

use super::{
    AdapterInboundMessage, AdapterNative, Attention, ProducerKind, SchedulerNative,
    inbox_item_from_adapter_message, inbox_item_from_fire,
};
use crate::{AdapterConfig, AdapterRecord, AdapterSource, NewAdapter, ScheduledFireRecord};

const T0: u64 = 1_700_000_000_000;

fn fire(task_id: &str, slot_ms: u64) -> ScheduledFireRecord {
    ScheduledFireRecord {
        task_id: task_id.to_string(),
        task_name: "health check".to_string(),
        slot_ms,
        run_id: format!("run-{slot_ms}"),
        agent_id: "agent-1".to_string(),
        conversation_id: "conversation-1".to_string(),
        prompt: "health check output".to_string(),
        fired_at_ms: slot_ms + 12,
    }
}

fn adapter(id_source: &str) -> AdapterRecord {
    AdapterRecord::new(
        NewAdapter {
            agent_id: "agent-1".to_string(),
            conversation_id: "conversation-1".to_string(),
            name: id_source.to_string(),
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

#[test]
fn a_fire_carries_its_slot_identity_and_its_task_context() {
    let record = fire("task-health", 1_000);
    let item = inbox_item_from_fire(&record, Attention::Wake, T0).expect("item");

    assert_eq!(item.conversation_id, "conversation-1");
    assert_eq!(item.producer.kind, ProducerKind::Scheduler);
    assert_eq!(item.producer.id, "task-health");
    assert_eq!(item.dedupe_key, "task-health:1000");
    assert_eq!(item.prompt, "health check output");
    // The append time is the parameter, not the fire time: a fire redelivered
    // after a crash reaches the inbox long after it fired, and drain order is
    // append order.
    assert_eq!(item.appended_at_ms, T0);

    let native: SchedulerNative = serde_json::from_value(item.native).expect("native");
    assert_eq!(native.slot_ms, 1_000);
    assert_eq!(native.fired_at_ms, 1_012);
    assert_eq!(native.run_id, "run-1000");
}

#[test]
fn two_slots_of_one_task_are_two_occurrences() {
    // The failure this key shape exists to prevent: a key scoped to the task
    // alone would collapse every fire of a recurring task into one item and
    // silently drop the work.
    let first =
        inbox_item_from_fire(&fire("task-health", 1_000), Attention::Wake, T0).expect("first item");
    let second = inbox_item_from_fire(&fire("task-health", 2_000), Attention::Wake, T0)
        .expect("second item");
    assert_ne!(first.dedupe_key, second.dedupe_key);
}

#[test]
fn an_adapter_message_is_keyed_on_its_platform_id_within_its_adapter_and_target() {
    let adapter = adapter("chat");
    let metadata = json!({ "thread_ts": "1699999999.000100" });
    let item = inbox_item_from_adapter_message(
        &adapter,
        AdapterInboundMessage {
            target: "#room",
            sender: Some("ad"),
            message_id: "platform-42",
            text: "are we up?",
            metadata: &metadata,
            attachments: &[],
        },
        Attention::Wake,
        T0,
    )
    .expect("item");

    assert_eq!(item.conversation_id, "conversation-1");
    // A human arrives via an adapter, so the producer is the adapter.
    assert_eq!(item.producer.kind, ProducerKind::Adapter);
    assert_eq!(item.producer.id, adapter.id);
    assert_eq!(item.dedupe_key, format!("{}:#room:platform-42", adapter.id));
    assert_eq!(item.prompt, "are we up?");

    let native: AdapterNative = serde_json::from_value(item.native).expect("native");
    assert_eq!(native.sender.as_deref(), Some("ad"));
    assert_eq!(native.adapter_type, "exochat");
    // Platform metadata travels opaquely; nothing in the runtime reads it.
    assert_eq!(native.metadata, metadata);
}

#[test]
fn one_platform_message_id_on_two_targets_is_two_occurrences() {
    // Platform message ids are only unique within their own adapter and
    // target, which is why the key carries all three.
    let adapter = adapter("chat");
    let metadata = Value::Null;
    let keys: Vec<String> = ["#room", "#other"]
        .into_iter()
        .map(|target| {
            inbox_item_from_adapter_message(
                &adapter,
                AdapterInboundMessage {
                    target,
                    sender: None,
                    message_id: "platform-42",
                    text: "hello",
                    metadata: &metadata,
                    attachments: &[],
                },
                Attention::Wake,
                T0,
            )
            .expect("item")
            .dedupe_key
        })
        .collect();
    assert_ne!(keys[0], keys[1]);
}
