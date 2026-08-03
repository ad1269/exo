use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use exoharness::Uuid7;
use serde::Serialize;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::types::{
    AdapterAttachment, AdapterDeliveryStatsRecord, AdapterDeliveryStatus, AdapterEventRecord,
    AdapterEventType, AdapterInboundMessageRecord, AdapterLifecycleState,
    AdapterOutboundMessageRecord, AdapterRecord, AdapterTargetConversationRecord, NewAdapter,
    now_ms,
};

const MAX_QUEUED_MESSAGES_PER_ADAPTER: usize = 1_000;
const MAX_MESSAGES_PER_CLAIM: usize = 100;
pub(crate) const MAX_DELIVERY_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone)]
pub struct AdapterStore {
    root: PathBuf,
    /// One adapter runtime hosts two tasks that record deliveries — command
    /// dispatch (marker dedupe) and event handling (acks, nacks at the cap) —
    /// so the stats read-increment-write needs a lock to not lose bumps.
    /// Cross-process writers do not exist: the runner lock admits one adapter
    /// runtime and the CLI only reads.
    stats_lock: Arc<tokio::sync::Mutex<()>>,
}

impl AdapterStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            stats_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub async fn create_adapter(&self, request: NewAdapter) -> Result<AdapterRecord> {
        let adapter = AdapterRecord::new(request, now_ms())?;
        self.put_adapter(&adapter).await?;
        Ok(adapter)
    }

    pub async fn list_adapters(&self) -> Result<Vec<AdapterRecord>> {
        let adapter_dir = self.adapters_dir();
        match fs::metadata(&adapter_dir).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error.into()),
        }
        let mut entries = fs::read_dir(&adapter_dir)
            .await
            .with_context(|| format!("failed to read adapter directory {adapter_dir:?}"))?;
        let mut adapters = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(&path)
                .await
                .with_context(|| format!("failed to read adapter {}", path.display()))?;
            adapters.push(serde_json::from_slice::<AdapterRecord>(&bytes)?);
        }
        adapters.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
        Ok(adapters)
    }

    pub async fn list_adapters_for_conversation(
        &self,
        agent_id: &str,
        conversation_id: &str,
        include_disabled: bool,
    ) -> Result<Vec<AdapterRecord>> {
        Ok(self
            .list_adapters()
            .await?
            .into_iter()
            .filter(|adapter| {
                adapter.agent_id == agent_id && adapter.conversation_id == conversation_id
            })
            .filter(|adapter| include_disabled || adapter.enabled)
            .collect())
    }

    pub async fn enabled_adapters(&self) -> Result<Vec<AdapterRecord>> {
        Ok(self
            .list_adapters()
            .await?
            .into_iter()
            .filter(|adapter| adapter.enabled)
            .collect())
    }

    pub async fn get_adapter(&self, adapter_id: &str) -> Result<Option<AdapterRecord>> {
        let path = self.adapter_path(adapter_id);
        match fs::read(&path).await {
            Ok(bytes) => Ok(Some(serde_json::from_slice::<AdapterRecord>(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => {
                Err(error).with_context(|| format!("failed to read adapter {}", path.display()))
            }
        }
    }

    pub async fn put_adapter(&self, adapter: &AdapterRecord) -> Result<()> {
        fs::create_dir_all(self.adapters_dir()).await?;
        let path = self.adapter_path(&adapter.id);
        write_json_file(&path, adapter)
            .await
            .with_context(|| format!("failed to write adapter {}", path.display()))
    }

    pub async fn disable_adapter(&self, adapter_id: &str) -> Result<Option<AdapterRecord>> {
        let Some(mut adapter) = self.get_adapter(adapter_id).await? else {
            return Ok(None);
        };
        adapter.enabled = false;
        adapter.lifecycle_state = AdapterLifecycleState::Disabled;
        adapter.updated_at_ms = now_ms();
        self.put_adapter(&adapter).await?;
        Ok(Some(adapter))
    }

    pub async fn enable_adapter(&self, adapter_id: &str) -> Result<Option<AdapterRecord>> {
        let Some(mut adapter) = self.get_adapter(adapter_id).await? else {
            return Ok(None);
        };
        adapter.enabled = true;
        adapter.lifecycle_state = AdapterLifecycleState::Starting;
        adapter.last_error = None;
        adapter.updated_at_ms = now_ms();
        self.put_adapter(&adapter).await?;
        Ok(Some(adapter))
    }

    pub async fn delete_adapter(&self, adapter_id: &str) -> Result<Option<AdapterRecord>> {
        let Some(adapter) = self.get_adapter(adapter_id).await? else {
            return Ok(None);
        };
        remove_file_if_exists(self.adapter_path(adapter_id)).await?;
        remove_dir_if_exists(self.events_dir(adapter_id)).await?;
        remove_dir_if_exists(self.outbox_dir(adapter_id)).await?;
        remove_dir_if_exists(self.inflight_dir(adapter_id)).await?;
        remove_dir_if_exists(self.delivered_dir(adapter_id)).await?;
        remove_dir_if_exists(self.failed_dir(adapter_id)).await?;
        remove_dir_if_exists(self.inbound_seen_dir(adapter_id)).await?;
        remove_dir_if_exists(self.outbound_sent_dir(adapter_id)).await?;
        remove_dir_if_exists(self.target_conversations_dir(adapter_id)).await?;
        remove_file_if_exists(self.delivery_stats_path(adapter_id)).await?;
        Ok(Some(adapter))
    }

    pub async fn mark_connected(&self, adapter_id: &str) -> Result<Option<AdapterRecord>> {
        let Some(mut adapter) = self.get_adapter(adapter_id).await? else {
            return Ok(None);
        };
        adapter.last_connected_at_ms = Some(now_ms());
        adapter.last_error = None;
        adapter.lifecycle_state = AdapterLifecycleState::Running;
        adapter.updated_at_ms = now_ms();
        self.put_adapter(&adapter).await?;
        Ok(Some(adapter))
    }

    pub async fn mark_error(
        &self,
        adapter_id: &str,
        error: impl Into<String>,
    ) -> Result<Option<AdapterRecord>> {
        let Some(mut adapter) = self.get_adapter(adapter_id).await? else {
            return Ok(None);
        };
        adapter.last_error = Some(error.into());
        adapter.lifecycle_state = AdapterLifecycleState::Error;
        adapter.updated_at_ms = now_ms();
        self.put_adapter(&adapter).await?;
        Ok(Some(adapter))
    }

    pub async fn put_event(&self, event: &AdapterEventRecord) -> Result<()> {
        fs::create_dir_all(self.events_dir(&event.adapter_id)).await?;
        let path = self.event_path(&event.adapter_id, &event.id);
        write_json_file(&path, event)
            .await
            .with_context(|| format!("failed to write adapter event {}", path.display()))
    }

    pub async fn record_event(
        &self,
        adapter_id: String,
        event_type: AdapterEventType,
        summary: String,
    ) -> Result<AdapterEventRecord> {
        let event = AdapterEventRecord::new(adapter_id.clone(), event_type, summary, now_ms())?;
        self.put_event(&event).await?;
        if let Some(mut adapter) = self.get_adapter(&adapter_id).await? {
            adapter.updated_at_ms = now_ms();
            self.put_adapter(&adapter).await?;
        }
        Ok(event)
    }

    pub async fn list_events(
        &self,
        adapter_id: &str,
        event_type: Option<AdapterEventType>,
        since_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<AdapterEventRecord>> {
        let events_dir = self.events_dir(adapter_id);
        match fs::metadata(&events_dir).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error.into()),
        }
        let mut entries = fs::read_dir(&events_dir)
            .await
            .with_context(|| format!("failed to read adapter events directory {events_dir:?}"))?;
        let mut events = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(&path)
                .await
                .with_context(|| format!("failed to read adapter event {}", path.display()))?;
            let event = serde_json::from_slice::<AdapterEventRecord>(&bytes)?;
            if let Some(event_type) = event_type
                && event.event_type != event_type
            {
                continue;
            }
            if let Some(since_ms) = since_ms
                && event.created_at_ms < since_ms
            {
                continue;
            }
            events.push(event);
        }
        // Newest first; event ids are time-ordered UUIDv7s, so they break
        // same-millisecond ties deterministically.
        events.sort_by(|left, right| {
            right
                .created_at_ms
                .cmp(&left.created_at_ms)
                .then(right.id.cmp(&left.id))
        });
        events.truncate(limit);
        Ok(events)
    }

    pub async fn enqueue_outbound_message(
        &self,
        adapter_id: String,
        text: String,
        target: Option<String>,
        attachments: Vec<AdapterAttachment>,
    ) -> Result<AdapterOutboundMessageRecord> {
        let message =
            AdapterOutboundMessageRecord::new(adapter_id, text, target, attachments, now_ms())?;
        if self.count_pending_messages(&message.adapter_id).await?
            >= MAX_QUEUED_MESSAGES_PER_ADAPTER
        {
            anyhow::bail!(
                "adapter outbound queue is full (maximum {MAX_QUEUED_MESSAGES_PER_ADAPTER} messages)"
            );
        }
        fs::create_dir_all(self.outbox_dir(&message.adapter_id)).await?;
        let path = self.outbox_path(&message.adapter_id, &message.id);
        write_json_file(&path, &message).await.with_context(|| {
            format!(
                "failed to write adapter outbound message {}",
                path.display()
            )
        })?;
        Ok(message)
    }

    /// Number of outbound messages that are queued or in flight for an
    /// adapter. Best-effort: the two directories are scanned without a lock,
    /// so concurrent enqueues or claims can skew the count by a few. The
    /// queue cap that uses this is soft backpressure, not a hard invariant.
    async fn count_pending_messages(&self, adapter_id: &str) -> Result<usize> {
        Ok(count_json_files(&self.outbox_dir(adapter_id)).await?
            + count_json_files(&self.inflight_dir(adapter_id)).await?)
    }

    pub async fn claim_outbound_messages(
        &self,
        adapter_id: &str,
    ) -> Result<Vec<AdapterOutboundMessageRecord>> {
        let outbox_dir = self.outbox_dir(adapter_id);
        match fs::metadata(&outbox_dir).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error.into()),
        }
        let mut entries = fs::read_dir(&outbox_dir)
            .await
            .with_context(|| format!("failed to read adapter outbox directory {outbox_dir:?}"))?;
        let mut messages = Vec::new();
        while messages.len() < MAX_MESSAGES_PER_CLAIM
            && let Some(entry) = entries.next_entry().await?
        {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(&path).await.with_context(|| {
                format!("failed to read adapter outbound message {}", path.display())
            })?;
            let mut message = serde_json::from_slice::<AdapterOutboundMessageRecord>(&bytes)?;
            message.status = AdapterDeliveryStatus::InFlight;
            message.attempt = message.attempt.saturating_add(1);
            message.updated_at_ms = now_ms();
            fs::create_dir_all(self.inflight_dir(adapter_id)).await?;
            let inflight_path = self.inflight_path(adapter_id, &message.id);
            write_json_file(&inflight_path, &message).await?;
            remove_file_if_exists(path).await?;
            messages.push(message);
        }
        messages.sort_by_key(|message| message.created_at_ms);
        Ok(messages)
    }

    pub async fn acknowledge_outbound_message(
        &self,
        adapter_id: &str,
        message_id: &str,
    ) -> Result<Option<AdapterOutboundMessageRecord>> {
        let Some(mut message) = self
            .read_pending_outbound_message(adapter_id, message_id)
            .await?
        else {
            return Ok(None);
        };
        let completed_at_ms = now_ms();
        message.status = AdapterDeliveryStatus::Delivered;
        message.updated_at_ms = completed_at_ms;
        message.completed_at_ms = Some(completed_at_ms);
        message.last_error = None;
        fs::create_dir_all(self.delivered_dir(adapter_id)).await?;
        write_json_file(&self.delivered_path(adapter_id, message_id), &message).await?;
        remove_file_if_exists(self.inflight_path(adapter_id, message_id)).await?;
        remove_file_if_exists(self.outbox_path(adapter_id, message_id)).await?;
        Ok(Some(message))
    }

    pub async fn nack_outbound_message(
        &self,
        adapter_id: &str,
        message_id: &str,
        error: impl Into<String>,
    ) -> Result<Option<AdapterOutboundMessageRecord>> {
        let Some(mut message) = self
            .read_pending_outbound_message(adapter_id, message_id)
            .await?
        else {
            return Ok(None);
        };
        // A last attempt that fails on a message the worker already sent is a
        // crash after delivery, not a delivery failure: the marker says the
        // platform has it. `failed/` is terminal, so burying it there would be
        // a claim nothing ever revisits. Only reachable at the cap — below it
        // the message is requeued and the dispatch check catches the marker.
        if message.attempt >= MAX_DELIVERY_ATTEMPTS
            && self.has_outbound_sent(adapter_id, message_id).await?
        {
            return self
                .acknowledge_outbound_message(adapter_id, message_id)
                .await;
        }
        message.last_error = Some(error.into());
        message.updated_at_ms = now_ms();
        remove_file_if_exists(self.inflight_path(adapter_id, message_id)).await?;
        remove_file_if_exists(self.outbox_path(adapter_id, message_id)).await?;
        if message.attempt >= MAX_DELIVERY_ATTEMPTS {
            message.status = AdapterDeliveryStatus::Failed;
            message.completed_at_ms = Some(message.updated_at_ms);
            fs::create_dir_all(self.failed_dir(adapter_id)).await?;
            write_json_file(&self.failed_path(adapter_id, message_id), &message).await?;
        } else {
            message.status = AdapterDeliveryStatus::Queued;
            fs::create_dir_all(self.outbox_dir(adapter_id)).await?;
            write_json_file(&self.outbox_path(adapter_id, message_id), &message).await?;
        }
        Ok(Some(message))
    }

    pub async fn requeue_outbound_message(
        &self,
        adapter_id: &str,
        message_id: &str,
    ) -> Result<Option<AdapterOutboundMessageRecord>> {
        self.nack_outbound_message(
            adapter_id,
            message_id,
            "worker stopped before acknowledging command",
        )
        .await
    }

    /// Returns how each in-flight message settled. Almost always queued for
    /// another attempt, but one whose last attempt died after the worker sent
    /// it comes back delivered, which the caller has to report.
    pub async fn requeue_inflight_messages(
        &self,
        adapter_id: &str,
    ) -> Result<Vec<AdapterOutboundMessageRecord>> {
        let inflight_dir = self.inflight_dir(adapter_id);
        match fs::metadata(&inflight_dir).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        }
        let mut entries = fs::read_dir(&inflight_dir).await.with_context(|| {
            format!("failed to read adapter inflight directory {inflight_dir:?}")
        })?;
        let mut settled = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Some(message_id) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            if let Some(message) = self
                .requeue_outbound_message(adapter_id, message_id)
                .await?
            {
                settled.push(message);
            }
        }
        Ok(settled)
    }

    pub async fn get_target_conversation(
        &self,
        adapter_id: &str,
        target: &str,
    ) -> Result<Option<AdapterTargetConversationRecord>> {
        let path = self.target_conversation_path(adapter_id, target);
        match fs::read(&path).await {
            Ok(bytes) => Ok(Some(serde_json::from_slice::<
                AdapterTargetConversationRecord,
            >(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| {
                format!(
                    "failed to read adapter target conversation {}",
                    path.display()
                )
            }),
        }
    }

    pub async fn put_target_conversation(
        &self,
        record: &AdapterTargetConversationRecord,
    ) -> Result<()> {
        let dir = self.target_conversations_dir(&record.adapter_id);
        fs::create_dir_all(&dir).await?;
        let path = self.target_conversation_path(&record.adapter_id, &record.target);
        write_json_file(&path, record).await
    }

    pub async fn list_target_conversations(
        &self,
        adapter_id: &str,
    ) -> Result<Vec<AdapterTargetConversationRecord>> {
        let dir = self.target_conversations_dir(adapter_id);
        match fs::metadata(&dir).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to stat target conversation dir {}", dir.display())
                });
            }
        }
        let mut records = Vec::new();
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(&path).await.with_context(|| {
                format!("failed to read target conversation {}", path.display())
            })?;
            records.push(serde_json::from_slice::<AdapterTargetConversationRecord>(
                &bytes,
            )?);
        }
        records.sort_by_key(|record| record.updated_at_ms);
        Ok(records)
    }

    pub async fn record_inbound_message_once(
        &self,
        adapter_id: &str,
        target: &str,
        message_id: &str,
    ) -> Result<bool> {
        let record = AdapterInboundMessageRecord {
            adapter_id: adapter_id.to_string(),
            target: target.to_string(),
            message_id: message_id.to_string(),
            first_seen_at_ms: now_ms(),
        };
        let seen_dir = self.inbound_seen_dir(adapter_id);
        fs::create_dir_all(&seen_dir).await?;
        let path = seen_dir.join(format!("{}.json", stable_message_key(target, message_id)));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(mut file) => {
                let bytes = serde_json::to_vec_pretty(&record)?;
                file.write_all(&bytes).await.with_context(|| {
                    format!("failed to write inbound seen marker {}", path.display())
                })?;
                file.flush().await.with_context(|| {
                    format!("failed to flush inbound seen marker {}", path.display())
                })?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error).with_context(|| {
                format!("failed to create inbound seen marker {}", path.display())
            }),
        }
    }

    /// Where a worker records the outbound messages it has handed to its
    /// platform. Exported to the worker at spawn; the kernel only ever reads
    /// whether a marker exists.
    pub fn outbound_sent_dir(&self, adapter_id: &str) -> PathBuf {
        self.root.join("outbound-sent").join(adapter_id)
    }

    /// A stat that fails for any reason other than absence reads as no marker.
    /// The same broken disk that stops a worker writing markers would otherwise
    /// make every read of one an error, and an error on this path costs more
    /// than the duplicate send it is trying to prevent.
    pub async fn has_outbound_sent(&self, adapter_id: &str, message_id: &str) -> Result<bool> {
        let path = self.outbound_sent_path(adapter_id, message_id);
        match fs::metadata(&path).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => {
                tracing::warn!(
                    adapter_id,
                    message_id,
                    path = %path.display(),
                    %error,
                    "failed to stat an outbound sent marker; reading it as absent"
                );
                Ok(false)
            }
        }
    }

    fn outbound_sent_path(&self, adapter_id: &str, message_id: &str) -> PathBuf {
        self.outbound_sent_dir(adapter_id)
            .join(format!("{message_id}.json"))
    }

    pub async fn delivery_stats(&self, adapter_id: &str) -> Result<AdapterDeliveryStatsRecord> {
        let path = self.delivery_stats_path(adapter_id);
        match fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to parse delivery stats {}", path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(AdapterDeliveryStatsRecord::default())
            }
            Err(error) => Err(error)
                .with_context(|| format!("failed to read delivery stats {}", path.display())),
        }
    }

    pub async fn record_delivery_outcome(
        &self,
        adapter_id: &str,
        deduped: bool,
    ) -> Result<AdapterDeliveryStatsRecord> {
        let _guard = self.stats_lock.lock().await;
        // A stats file that stopped parsing would otherwise wedge the counter
        // for good; restarting the count costs history, not correctness, and
        // matches how an unreadable sent marker reads as absent.
        let mut stats = match self.delivery_stats(adapter_id).await {
            Ok(stats) => stats,
            Err(error) => {
                tracing::warn!(adapter_id, %error, "resetting unreadable delivery stats");
                AdapterDeliveryStatsRecord::default()
            }
        };
        stats.delivered += 1;
        if deduped {
            stats.deduped += 1;
        }
        fs::create_dir_all(self.delivery_stats_dir()).await?;
        write_json_file(&self.delivery_stats_path(adapter_id), &stats).await?;
        Ok(stats)
    }

    fn delivery_stats_dir(&self) -> PathBuf {
        self.root.join("delivery-stats")
    }

    fn delivery_stats_path(&self, adapter_id: &str) -> PathBuf {
        self.delivery_stats_dir().join(format!("{adapter_id}.json"))
    }

    fn adapters_dir(&self) -> PathBuf {
        self.root.join("adapters")
    }

    fn adapter_path(&self, adapter_id: &str) -> PathBuf {
        self.adapters_dir().join(format!("{adapter_id}.json"))
    }

    fn events_dir(&self, adapter_id: &str) -> PathBuf {
        self.root.join("events").join(adapter_id)
    }

    fn event_path(&self, adapter_id: &str, event_id: &str) -> PathBuf {
        self.events_dir(adapter_id).join(format!("{event_id}.json"))
    }

    fn outbox_dir(&self, adapter_id: &str) -> PathBuf {
        self.root.join("outbox").join(adapter_id)
    }

    fn outbox_path(&self, adapter_id: &str, message_id: &str) -> PathBuf {
        self.outbox_dir(adapter_id)
            .join(format!("{message_id}.json"))
    }

    fn inflight_dir(&self, adapter_id: &str) -> PathBuf {
        self.root.join("outbox-inflight").join(adapter_id)
    }

    fn inflight_path(&self, adapter_id: &str, message_id: &str) -> PathBuf {
        self.inflight_dir(adapter_id)
            .join(format!("{message_id}.json"))
    }

    fn delivered_dir(&self, adapter_id: &str) -> PathBuf {
        self.root.join("outbox-delivered").join(adapter_id)
    }

    fn delivered_path(&self, adapter_id: &str, message_id: &str) -> PathBuf {
        self.delivered_dir(adapter_id)
            .join(format!("{message_id}.json"))
    }

    fn failed_dir(&self, adapter_id: &str) -> PathBuf {
        self.root.join("outbox-failed").join(adapter_id)
    }

    fn failed_path(&self, adapter_id: &str, message_id: &str) -> PathBuf {
        self.failed_dir(adapter_id)
            .join(format!("{message_id}.json"))
    }

    async fn read_pending_outbound_message(
        &self,
        adapter_id: &str,
        message_id: &str,
    ) -> Result<Option<AdapterOutboundMessageRecord>> {
        for path in [
            self.inflight_path(adapter_id, message_id),
            self.outbox_path(adapter_id, message_id),
        ] {
            match fs::read(&path).await {
                Ok(bytes) => {
                    return Ok(Some(
                        serde_json::from_slice::<AdapterOutboundMessageRecord>(&bytes)?,
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(None)
    }

    fn target_conversations_dir(&self, adapter_id: &str) -> PathBuf {
        self.root.join("target-conversations").join(adapter_id)
    }

    fn target_conversation_path(&self, adapter_id: &str, target: &str) -> PathBuf {
        self.target_conversations_dir(adapter_id)
            .join(format!("{}.json", stable_target_key(target)))
    }

    fn inbound_seen_dir(&self, adapter_id: &str) -> PathBuf {
        self.root.join("inbound-seen").join(adapter_id)
    }
}

async fn remove_file_if_exists(path: PathBuf) -> Result<()> {
    match fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("failed to delete file {}", path.display()))
        }
    }
}

async fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let temp_path = path.with_extension(format!("json.{}.tmp", Uuid7::now()));
    fs::write(&temp_path, serde_json::to_vec_pretty(value)?)
        .await
        .with_context(|| format!("failed to write temp file {}", temp_path.display()))?;
    fs::rename(&temp_path, path).await.with_context(|| {
        format!(
            "failed to replace {} with temp file {}",
            path.display(),
            temp_path.display()
        )
    })
}

async fn remove_dir_if_exists(path: PathBuf) -> Result<()> {
    match fs::remove_dir_all(&path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("failed to delete directory {}", path.display()))
        }
    }
}

async fn count_json_files(path: &Path) -> Result<usize> {
    let mut entries = match fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let mut count = 0;
    while let Some(entry) = entries.next_entry().await? {
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("json") {
            count += 1;
        }
    }
    Ok(count)
}
/// FNV-1a of an adapter target, used for both the target-conversation mapping
/// filename and the derived conversation slug. `pub(crate)` so the runtime
/// derives the same slug the store keys by — they must agree.
pub(crate) fn stable_target_key(target: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in target.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn stable_message_key(target: &str, message_id: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in target.bytes().chain([0]).chain(message_id.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::super::types::{
        AdapterAttachment, AdapterAttachmentKind, AdapterConfig, AdapterEventType, AdapterSource,
    };

    use super::*;

    #[tokio::test]
    async fn creates_lists_disables_and_deletes_adapters() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        let adapter = store
            .create_adapter(NewAdapter {
                agent_id: "agent".to_string(),
                conversation_id: "conversation".to_string(),
                name: "irc".to_string(),
                source: AdapterSource::Library,
                config: AdapterConfig {
                    adapter_type: "irc".to_string(),
                    worker_command: vec!["node".to_string(), "irc.js".to_string()],
                    initialization: serde_json::json!({}),
                    state_dir: None,
                    secret_env: Vec::new(),
                },
            })
            .await
            .unwrap();

        assert_eq!(store.list_adapters().await.unwrap(), vec![adapter.clone()]);
        store
            .record_event(
                adapter.id.clone(),
                AdapterEventType::Connected,
                "connected".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .list_adapters_for_conversation("agent", "conversation", false)
                .await
                .unwrap()
                .len(),
            1
        );
        store.disable_adapter(&adapter.id).await.unwrap();
        assert!(
            store
                .list_adapters_for_conversation("agent", "conversation", false)
                .await
                .unwrap()
                .is_empty()
        );
        let enabled = store
            .enable_adapter(&adapter.id)
            .await
            .unwrap()
            .expect("adapter should exist");
        assert!(enabled.enabled);
        assert_eq!(enabled.lifecycle_state, AdapterLifecycleState::Starting);
        assert!(store.delete_adapter(&adapter.id).await.unwrap().is_some());
        assert!(store.get_adapter(&adapter.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn target_conversation_mapping_round_trips() {
        use super::super::types::AdapterTargetConversationRecord;

        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        // A real adapter record so delete_adapter reaches the mapping cleanup.
        let adapter = store
            .create_adapter(NewAdapter {
                agent_id: "agent".to_string(),
                conversation_id: "root".to_string(),
                name: "discord".to_string(),
                source: AdapterSource::Library,
                config: AdapterConfig {
                    adapter_type: "discord".to_string(),
                    worker_command: vec!["node".to_string()],
                    initialization: serde_json::json!({}),
                    state_dir: None,
                    secret_env: Vec::new(),
                },
            })
            .await
            .unwrap();

        // Missing mapping reads as None.
        assert!(
            store
                .get_target_conversation(&adapter.id, "channel-A")
                .await
                .unwrap()
                .is_none()
        );

        let rec_a = AdapterTargetConversationRecord::new(
            adapter.id.clone(),
            "channel-A".into(),
            "conv-A".into(),
            1,
        )
        .unwrap();
        let rec_b = AdapterTargetConversationRecord::new(
            adapter.id.clone(),
            "channel-B".into(),
            "conv-B".into(),
            2,
        )
        .unwrap();
        store.put_target_conversation(&rec_a).await.unwrap();
        store.put_target_conversation(&rec_b).await.unwrap();

        assert_eq!(
            store
                .get_target_conversation(&adapter.id, "channel-A")
                .await
                .unwrap(),
            Some(rec_a.clone())
        );
        let listed = store.list_target_conversations(&adapter.id).await.unwrap();
        assert_eq!(listed.len(), 2);
        // Distinct targets must not collide on the hashed filename.
        assert_ne!(rec_a.conversation_id, rec_b.conversation_id);

        // Deleting the adapter removes its target-conversation mappings.
        store.delete_adapter(&adapter.id).await.unwrap();
        assert!(
            store
                .list_target_conversations(&adapter.id)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn preserves_outbound_targets() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        let message = store
            .enqueue_outbound_message(
                "adapter".to_string(),
                "hello".to_string(),
                Some("123@s.whatsapp.net".to_string()),
                vec![AdapterAttachment {
                    kind: AdapterAttachmentKind::Image,
                    path: Some(".exo/generated/chart.png".to_string()),
                    url: None,
                    data: None,
                    sandbox_path: None,
                    mime_type: Some("image/png".to_string()),
                    file_name: None,
                }],
            )
            .await
            .unwrap();

        assert_eq!(message.target.as_deref(), Some("123@s.whatsapp.net"));
        assert_eq!(message.attachments.len(), 1);
        let messages = store.claim_outbound_messages("adapter").await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].target.as_deref(), Some("123@s.whatsapp.net"));
        assert_eq!(
            messages[0].attachments[0].path.as_deref(),
            Some(".exo/generated/chart.png")
        );
        assert!(
            store
                .claim_outbound_messages("adapter")
                .await
                .unwrap()
                .is_empty()
        );
        store
            .acknowledge_outbound_message("adapter", &message.id)
            .await
            .unwrap();
        let delivered: AdapterOutboundMessageRecord = serde_json::from_slice(
            &fs::read(store.delivered_path("adapter", &message.id))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(delivered.status, AdapterDeliveryStatus::Delivered);
        assert_eq!(delivered.attempt, 1);
        assert!(delivered.completed_at_ms.is_some());
        assert!(
            store
                .claim_outbound_messages("adapter")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn requeues_claimed_outbound_messages() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        let message = store
            .enqueue_outbound_message("adapter".to_string(), "hello".to_string(), None, Vec::new())
            .await
            .unwrap();

        let claimed = store.claim_outbound_messages("adapter").await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert!(
            store
                .claim_outbound_messages("adapter")
                .await
                .unwrap()
                .is_empty()
        );

        store
            .requeue_outbound_message("adapter", &message.id)
            .await
            .unwrap();
        let claimed_again = store.claim_outbound_messages("adapter").await.unwrap();
        assert_eq!(claimed_again.len(), 1);
        assert_eq!(claimed_again[0].id, message.id);
        assert_eq!(claimed_again[0].attempt, 2);
    }

    #[tokio::test]
    async fn nacks_retry_then_preserve_terminal_failure() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        let message = store
            .enqueue_outbound_message("adapter".to_string(), "hello".to_string(), None, Vec::new())
            .await
            .unwrap();

        for attempt in 1..=MAX_DELIVERY_ATTEMPTS {
            let claimed = store.claim_outbound_messages("adapter").await.unwrap();
            assert_eq!(claimed.len(), 1);
            assert_eq!(claimed[0].attempt, attempt);
            let nacked = store
                .nack_outbound_message("adapter", &message.id, format!("failure {attempt}"))
                .await
                .unwrap()
                .unwrap();
            if attempt < MAX_DELIVERY_ATTEMPTS {
                assert_eq!(nacked.status, AdapterDeliveryStatus::Queued);
            } else {
                assert_eq!(nacked.status, AdapterDeliveryStatus::Failed);
                assert!(nacked.completed_at_ms.is_some());
            }
        }

        assert!(
            store
                .claim_outbound_messages("adapter")
                .await
                .unwrap()
                .is_empty()
        );
        let failed: AdapterOutboundMessageRecord = serde_json::from_slice(
            &fs::read(store.failed_path("adapter", &message.id))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(failed.status, AdapterDeliveryStatus::Failed);
        assert_eq!(failed.attempt, MAX_DELIVERY_ATTEMPTS);
        assert_eq!(failed.last_error.as_deref(), Some("failure 3"));
    }

    #[tokio::test]
    async fn lists_events_newest_first_with_filters() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());

        let connected = store
            .record_event(
                "adapter".to_string(),
                AdapterEventType::Connected,
                "worker connected".to_string(),
            )
            .await
            .unwrap();
        let error = store
            .record_event(
                "adapter".to_string(),
                AdapterEventType::Error,
                "shard error".to_string(),
            )
            .await
            .unwrap();
        let inbound = store
            .record_event(
                "adapter".to_string(),
                AdapterEventType::Inbound,
                "message received".to_string(),
            )
            .await
            .unwrap();

        let all = store.list_events("adapter", None, None, 10).await.unwrap();
        assert_eq!(
            all.iter().map(|event| &event.id).collect::<Vec<_>>(),
            vec![&inbound.id, &error.id, &connected.id],
            "events must be newest first"
        );

        let errors = store
            .list_events("adapter", Some(AdapterEventType::Error), None, 10)
            .await
            .unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].id, error.id);

        let limited = store.list_events("adapter", None, None, 2).await.unwrap();
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].id, inbound.id);

        let since = store
            .list_events("adapter", None, Some(inbound.created_at_ms), 10)
            .await
            .unwrap();
        assert!(
            since
                .iter()
                .all(|event| event.created_at_ms >= inbound.created_at_ms)
        );

        assert!(
            store
                .list_events("missing-adapter", None, None, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn records_inbound_message_ids_once() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());

        assert!(
            store
                .record_inbound_message_once("adapter", "target", "message")
                .await
                .unwrap()
        );
        assert!(
            !store
                .record_inbound_message_once("adapter", "target", "message")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn reads_sent_markers_written_by_a_worker() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());

        assert!(!store.has_outbound_sent("adapter", "message").await.unwrap());
        write_sent_marker(&store, "adapter", "message").await;
        assert!(store.has_outbound_sent("adapter", "message").await.unwrap());
        assert!(!store.has_outbound_sent("adapter", "other").await.unwrap());
        assert!(!store.has_outbound_sent("other", "message").await.unwrap());
    }

    #[tokio::test]
    async fn an_unreadable_marker_path_reads_as_no_marker() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        // A file where the per-adapter marker directory belongs, so the stat
        // fails with something other than "not found".
        fs::write(tempdir.path().join("outbound-sent"), "not a directory")
            .await
            .unwrap();

        // Erroring here would fail every attempt until the message gave up
        // undelivered, which costs more than the duplicate send it prevents.
        assert!(!store.has_outbound_sent("adapter", "message").await.unwrap());
    }

    #[tokio::test]
    async fn a_marked_message_is_delivered_rather_than_failed_at_the_attempt_cap() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        let message = store
            .enqueue_outbound_message("adapter".to_string(), "hello".to_string(), None, Vec::new())
            .await
            .unwrap();
        for _ in 1..MAX_DELIVERY_ATTEMPTS {
            store.claim_outbound_messages("adapter").await.unwrap();
            store
                .nack_outbound_message("adapter", &message.id, "failed")
                .await
                .unwrap()
                .unwrap();
        }
        // The last attempt reaches the platform and then the worker dies
        // between writing the marker and acking.
        store.claim_outbound_messages("adapter").await.unwrap();
        write_sent_marker(&store, "adapter", &message.id).await;

        let settled = store
            .nack_outbound_message("adapter", &message.id, "worker exited")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(settled.status, AdapterDeliveryStatus::Delivered);
        assert_eq!(settled.attempt, MAX_DELIVERY_ATTEMPTS);
        assert!(settled.last_error.is_none());
        assert!(store.delivered_path("adapter", &message.id).exists());
        assert!(!store.failed_path("adapter", &message.id).exists());
    }

    #[tokio::test]
    async fn an_unmarked_message_still_fails_at_the_attempt_cap() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        let message = store
            .enqueue_outbound_message("adapter".to_string(), "hello".to_string(), None, Vec::new())
            .await
            .unwrap();
        for _ in 0..MAX_DELIVERY_ATTEMPTS {
            store.claim_outbound_messages("adapter").await.unwrap();
            store
                .nack_outbound_message("adapter", &message.id, "failed")
                .await
                .unwrap()
                .unwrap();
        }

        assert!(store.failed_path("adapter", &message.id).exists());
        assert!(!store.delivered_path("adapter", &message.id).exists());
    }

    #[tokio::test]
    async fn requeueing_reports_a_marked_message_at_the_cap_as_delivered() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        let message = store
            .enqueue_outbound_message("adapter".to_string(), "hello".to_string(), None, Vec::new())
            .await
            .unwrap();
        for _ in 1..MAX_DELIVERY_ATTEMPTS {
            store.claim_outbound_messages("adapter").await.unwrap();
            store
                .nack_outbound_message("adapter", &message.id, "failed")
                .await
                .unwrap()
                .unwrap();
        }
        // Claimed for the last time, sent, marked — then the runner restarts
        // and finds it still in flight.
        store.claim_outbound_messages("adapter").await.unwrap();
        write_sent_marker(&store, "adapter", &message.id).await;

        let settled = store.requeue_inflight_messages("adapter").await.unwrap();

        assert_eq!(settled.len(), 1);
        assert_eq!(settled[0].id, message.id);
        assert_eq!(settled[0].status, AdapterDeliveryStatus::Delivered);
        assert!(store.delivered_path("adapter", &message.id).exists());
    }

    #[tokio::test]
    async fn deleting_an_adapter_clears_its_sent_markers() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        let adapter = store
            .create_adapter(NewAdapter {
                agent_id: "agent".to_string(),
                conversation_id: "conversation".to_string(),
                name: "irc".to_string(),
                source: AdapterSource::Library,
                config: AdapterConfig {
                    adapter_type: "irc".to_string(),
                    worker_command: vec!["node".to_string(), "irc.js".to_string()],
                    initialization: serde_json::json!({}),
                    state_dir: None,
                    secret_env: Vec::new(),
                },
            })
            .await
            .unwrap();
        write_sent_marker(&store, &adapter.id, "message").await;

        store.delete_adapter(&adapter.id).await.unwrap();

        assert!(
            !store
                .has_outbound_sent(&adapter.id, "message")
                .await
                .unwrap()
        );
        assert!(!store.outbound_sent_dir(&adapter.id).exists());
    }

    #[tokio::test]
    async fn delivery_stats_accumulate_and_are_removed_with_the_adapter() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        let adapter = store
            .create_adapter(NewAdapter {
                agent_id: "agent".to_string(),
                conversation_id: "conversation".to_string(),
                name: "irc".to_string(),
                source: AdapterSource::Library,
                config: AdapterConfig {
                    adapter_type: "irc".to_string(),
                    worker_command: vec!["node".to_string(), "irc.js".to_string()],
                    initialization: serde_json::json!({}),
                    state_dir: None,
                    secret_env: Vec::new(),
                },
            })
            .await
            .unwrap();
        assert_eq!(
            store.delivery_stats(&adapter.id).await.unwrap(),
            AdapterDeliveryStatsRecord::default()
        );

        store
            .record_delivery_outcome(&adapter.id, false)
            .await
            .unwrap();
        let stats = store
            .record_delivery_outcome(&adapter.id, true)
            .await
            .unwrap();
        assert_eq!(stats.delivered, 2);
        assert_eq!(stats.deduped, 1);
        assert_eq!(store.delivery_stats(&adapter.id).await.unwrap(), stats);

        store.delete_adapter(&adapter.id).await.unwrap();
        assert_eq!(
            store.delivery_stats(&adapter.id).await.unwrap(),
            AdapterDeliveryStatsRecord::default()
        );
    }

    // The runtime's command task and event task both record deliveries, so
    // concurrent bumps must all land.
    #[tokio::test]
    async fn concurrent_delivery_outcomes_all_land() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());

        let bumps = (0..20).map(|index| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .record_delivery_outcome("adapter", index % 2 == 0)
                    .await
            })
        });
        for bump in bumps.collect::<Vec<_>>() {
            bump.await.unwrap().unwrap();
        }

        let stats = store.delivery_stats("adapter").await.unwrap();
        assert_eq!(stats.delivered, 20);
        assert_eq!(stats.deduped, 10);
    }

    #[tokio::test]
    async fn an_unreadable_stats_file_resets_instead_of_wedging_the_counter() {
        let tempdir = TempDir::new().unwrap();
        let store = AdapterStore::new(tempdir.path());
        store
            .record_delivery_outcome("adapter", false)
            .await
            .unwrap();
        fs::write(
            tempdir.path().join("delivery-stats").join("adapter.json"),
            "not json",
        )
        .await
        .unwrap();

        assert!(store.delivery_stats("adapter").await.is_err());
        let stats = store
            .record_delivery_outcome("adapter", true)
            .await
            .unwrap();
        assert_eq!(stats.delivered, 1);
        assert_eq!(stats.deduped, 1);
    }

    // Markers are written by the worker, never by the kernel.
    async fn write_sent_marker(store: &AdapterStore, adapter_id: &str, message_id: &str) {
        let dir = store.outbound_sent_dir(adapter_id);
        fs::create_dir_all(&dir).await.unwrap();
        fs::write(dir.join(format!("{message_id}.json")), "{}")
            .await
            .unwrap();
    }
}
