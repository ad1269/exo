use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use exoharness::Uuid7;
use serde::{Deserialize, Serialize};
use tokio::fs;

use super::backend::AttentionBackend;
use super::types::{AppendOutcome, DispatchLease, InboxItem, InboxItemStatus, ItemId};
use crate::now_ms;

/// How long a dispatch lease lives without renewal. A holder that dies takes
/// its renewal task with it, so this is the takeover latency for a
/// conversation orphaned by a crash.
pub(crate) const LEASE_TTL_MS: u64 = 60_000;

/// File-backed [`AttentionBackend`]: cross-process on one box, one directory
/// per conversation.
///
/// Interim and deletable by design: exists so the wiring can run before the
/// exoharness#113 coordinator lands; the coordinator replaces this file from
/// below with no change above the [`AttentionBackend`] line. Its known soft
/// spot is deliberate — [`renew_dispatch_lease`](Self::renew_dispatch_lease)
/// is read-then-write rather than compare-and-swap, precisely the class of
/// flaw the coordinator's conditional puts remove.
///
/// Layout under `<dir>/<conversation_id>/` (conversation ids are UUIDv7
/// strings, safe as path segments):
///
/// - `items/<item_id>.json` — the item and its drain status;
/// - `dedupe/<fnv16hex(dedupe_key)>.json` — which item claimed the key,
///   created with `O_EXCL` so exactly one append wins;
/// - `lease.json` — the live dispatch lease.
pub struct FileAttentionBackend {
    dir: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredItemRecord {
    item: InboxItem,
    status: InboxItemStatus,
}

/// Carries the full key, not just its hash: on an `O_EXCL` loss the incoming
/// key is checked against the stored one, so a 64-bit FNV collision surfaces
/// as an error instead of a silent mis-dedupe.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DedupeRecord {
    dedupe_key: String,
    item_id: ItemId,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct LeaseRecord {
    pub(super) conversation_id: String,
    pub(super) token: String,
    pub(super) holder_pid: u32,
    pub(super) expires_at_ms: u64,
}

impl FileAttentionBackend {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn items_dir(&self, conversation_id: &str) -> PathBuf {
        self.dir.join(conversation_id).join("items")
    }

    fn item_path(&self, conversation_id: &str, item_id: ItemId) -> PathBuf {
        self.items_dir(conversation_id)
            .join(format!("{item_id}.json"))
    }

    fn dedupe_path(&self, conversation_id: &str, dedupe_key: &str) -> PathBuf {
        self.dir
            .join(conversation_id)
            .join("dedupe")
            .join(format!("{}.json", fnv16hex(dedupe_key)))
    }

    fn lease_path(&self, conversation_id: &str) -> PathBuf {
        self.dir.join(conversation_id).join("lease.json")
    }

    /// Conversation directories currently on disk. The trait scopes the mark
    /// calls by item id alone, so finding an item's file means scanning them.
    async fn conversation_ids(&self) -> Result<Vec<String>> {
        let mut entries = match fs::read_dir(&self.dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut ids = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                ids.push(name.to_string());
            }
        }
        Ok(ids)
    }

    async fn find_item_path(&self, item_id: ItemId) -> Result<Option<PathBuf>> {
        for conversation_id in self.conversation_ids().await? {
            let path = self.item_path(&conversation_id, item_id);
            if fs::try_exists(&path).await? {
                return Ok(Some(path));
            }
        }
        Ok(None)
    }

    /// Acquiring means no live dispatcher holds the conversation, so a
    /// `Drained`-but-unacknowledged item belongs to a turn that died before
    /// committing; re-pending redelivers it into a new turn. Turn-level
    /// at-least-once; item-level exactly-once stays with the status
    /// transitions.
    async fn repend_drained(&self, conversation_id: &str) -> Result<()> {
        for path in self.item_paths(conversation_id).await? {
            let Some(mut record) = read_json_file::<StoredItemRecord>(&path).await? else {
                continue;
            };
            if matches!(record.status, InboxItemStatus::Drained { .. }) {
                record.status = InboxItemStatus::Pending;
                write_json_file(&path, &record).await?;
            }
        }
        Ok(())
    }

    async fn item_paths(&self, conversation_id: &str) -> Result<Vec<PathBuf>> {
        let mut entries = match fs::read_dir(self.items_dir(conversation_id)).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut paths = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension().and_then(|ext| ext.to_str()) == Some("json") {
                paths.push(entry.path());
            }
        }
        Ok(paths)
    }
}

#[async_trait]
impl AttentionBackend for FileAttentionBackend {
    /// The item file lands first, then the dedupe file claims the key with
    /// `O_EXCL`. A crash between the two leaves an orphan pending item that a
    /// redelivery could duplicate — a bounded duplicate, traded for never
    /// losing an item, the same at-least-once posture as the adapter outbox.
    async fn append_inbox_item(&self, item: InboxItem) -> Result<AppendOutcome> {
        let conversation_id = item.conversation_id.clone();
        let dedupe_key = item.dedupe_key.clone();
        let item_id = item.item_id;
        let item_path = self.item_path(&conversation_id, item_id);
        let dedupe_path = self.dedupe_path(&conversation_id, &dedupe_key);
        fs::create_dir_all(self.items_dir(&conversation_id)).await?;
        fs::create_dir_all(dedupe_path.parent().expect("dedupe path has a parent")).await?;

        write_json_file(
            &item_path,
            &StoredItemRecord {
                item,
                status: InboxItemStatus::Pending,
            },
        )
        .await?;
        if create_json_file_once(
            &dedupe_path,
            &DedupeRecord {
                dedupe_key: dedupe_key.clone(),
                item_id,
            },
        )
        .await?
        {
            Ok(AppendOutcome {
                item_id,
                deduplicated: false,
            })
        } else {
            let existing: DedupeRecord = read_json_file(&dedupe_path)
                .await?
                .context("dedupe file vanished between create and read")?;
            if existing.dedupe_key != dedupe_key {
                bail!(
                    "dedupe hash collision in conversation {conversation_id}: \
                     {:?} and {dedupe_key:?} share {}",
                    existing.dedupe_key,
                    fnv16hex(&dedupe_key),
                );
            }
            fs::remove_file(&item_path).await.with_context(|| {
                format!("failed to remove duplicate item {}", item_path.display())
            })?;
            Ok(AppendOutcome {
                item_id: existing.item_id,
                deduplicated: true,
            })
        }
    }

    async fn list_pending(&self, conversation_id: &str) -> Result<Vec<InboxItem>> {
        let mut pending = Vec::new();
        for path in self.item_paths(conversation_id).await? {
            let Some(record) = read_json_file::<StoredItemRecord>(&path).await? else {
                continue;
            };
            if matches!(record.status, InboxItemStatus::Pending) {
                pending.push(record.item);
            }
        }
        // UUIDv7 item ids are mint-ordered, so the tiebreak within a
        // millisecond is append order — the same FIFO the in-memory backend
        // gets from its vector.
        pending.sort_by_key(|item| (item.appended_at_ms, item.item_id));
        Ok(pending)
    }

    async fn mark_drained(&self, item_ids: &[ItemId], turn_ref: &str) -> Result<Vec<ItemId>> {
        let mut transitioned = Vec::new();
        for &item_id in item_ids {
            let Some(path) = self.find_item_path(item_id).await? else {
                continue;
            };
            let Some(mut record) = read_json_file::<StoredItemRecord>(&path).await? else {
                continue;
            };
            if !matches!(record.status, InboxItemStatus::Pending) {
                continue;
            }
            record.status = InboxItemStatus::Drained {
                turn_ref: turn_ref.to_string(),
            };
            write_json_file(&path, &record).await?;
            transitioned.push(item_id);
        }
        Ok(transitioned)
    }

    async fn mark_acknowledged(&self, item_ids: &[ItemId]) -> Result<Vec<ItemId>> {
        let mut transitioned = Vec::new();
        for &item_id in item_ids {
            let Some(path) = self.find_item_path(item_id).await? else {
                continue;
            };
            let Some(mut record) = read_json_file::<StoredItemRecord>(&path).await? else {
                continue;
            };
            let InboxItemStatus::Drained { turn_ref } = &record.status else {
                continue;
            };
            record.status = InboxItemStatus::Acknowledged {
                turn_ref: turn_ref.clone(),
            };
            write_json_file(&path, &record).await?;
            transitioned.push(item_id);
        }
        Ok(transitioned)
    }

    async fn acquire_dispatch_lease(&self, conversation_id: &str) -> Result<Option<DispatchLease>> {
        let lease_path = self.lease_path(conversation_id);
        fs::create_dir_all(lease_path.parent().expect("lease path has a parent")).await?;
        loop {
            let token = Uuid7::now().to_string();
            let record = LeaseRecord {
                conversation_id: conversation_id.to_string(),
                token: token.clone(),
                holder_pid: std::process::id(),
                expires_at_ms: now_ms() + LEASE_TTL_MS,
            };
            if create_json_file_once(&lease_path, &record).await? {
                self.repend_drained(conversation_id).await?;
                return Ok(Some(DispatchLease {
                    conversation_id: conversation_id.to_string(),
                    token,
                }));
            }
            let Some(held) = read_json_file::<LeaseRecord>(&lease_path).await? else {
                // Released or torn down between our create and read; retry.
                continue;
            };
            if !lease_expired(&held, now_ms()) {
                return Ok(None);
            }
            // Takeover: rename the lease away, then CHECK what was renamed.
            // The rename alone is not an arbiter — a slow loser can rename a
            // fresh lease a faster winner already re-created — so the
            // tombstone's token is compared against the expired token this
            // process observed, and a stolen fresh lease is restored. The
            // restore can itself race a third acquirer; that residual window
            // is the read-then-write class renew already documents, and one
            // of the reasons this backend is labeled deletable.
            let tombstone = lease_path.with_extension(format!("json.{}.tomb", Uuid7::now()));
            match fs::rename(&lease_path, &tombstone).await {
                Ok(()) => {
                    let renamed = read_json_file::<LeaseRecord>(&tombstone).await?;
                    let stole_fresh_lease =
                        renamed.is_some_and(|renamed| renamed.token != held.token);
                    if stole_fresh_lease {
                        // Restore with link-then-unlink rather than rename:
                        // link refuses an existing destination, so a third
                        // acquirer's even-fresher lease is never clobbered.
                        match fs::hard_link(&tombstone, &lease_path).await {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                            Err(error) => return Err(error.into()),
                        }
                        fs::remove_file(&tombstone).await.with_context(|| {
                            format!("failed to remove lease tombstone {}", tombstone.display())
                        })?;
                        continue;
                    }
                    fs::remove_file(&tombstone).await.with_context(|| {
                        format!("failed to remove lease tombstone {}", tombstone.display())
                    })?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Read-then-write, not compare-and-swap: between the token check and the
    /// refreshed write, a takeover could slip in and be overwritten. The
    /// window needs the holder to also have let the TTL lapse, but it exists
    /// — one of the reasons this backend is labeled deletable.
    async fn renew_dispatch_lease(&self, lease: &DispatchLease) -> Result<bool> {
        let lease_path = self.lease_path(&lease.conversation_id);
        let Some(mut record) = read_json_file::<LeaseRecord>(&lease_path).await? else {
            return Ok(false);
        };
        if record.token != lease.token {
            return Ok(false);
        }
        record.expires_at_ms = now_ms() + LEASE_TTL_MS;
        write_json_file(&lease_path, &record).await?;
        Ok(true)
    }

    async fn release_dispatch_lease(&self, lease: &DispatchLease) -> Result<bool> {
        let lease_path = self.lease_path(&lease.conversation_id);
        let Some(record) = read_json_file::<LeaseRecord>(&lease_path).await? else {
            return Ok(false);
        };
        if record.token != lease.token {
            return Ok(false);
        }
        fs::remove_file(&lease_path)
            .await
            .with_context(|| format!("failed to remove lease {}", lease_path.display()))?;
        Ok(true)
    }
}

/// Pure so lease expiry is unit-testable; the trait has no `now_ms`
/// parameter, so the callers above read the system clock.
pub(super) fn lease_expired(record: &LeaseRecord, now_ms: u64) -> bool {
    record.expires_at_ms < now_ms
}

/// FNV-1a to 16 hex chars, the same filename pattern as the adapter store's
/// `stable_message_key`, over the raw dedupe key.
fn fnv16hex(key: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in key.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

/// Atomic replace: temp file in the same directory, then rename.
pub(super) async fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
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

/// `O_EXCL` create; false when the file already existed.
/// Create-if-absent with the content appearing atomically: the record is
/// written to a temp file and hard-linked into place. A bare `O_EXCL` create
/// followed by a write has a birth window where a concurrent reader sees an
/// empty file; the link makes the file exist only in its full form.
async fn create_json_file_once<T: Serialize>(path: &Path, value: &T) -> Result<bool> {
    let temp_path = path.with_extension(format!("json.{}.tmp", Uuid7::now()));
    fs::write(&temp_path, serde_json::to_vec_pretty(value)?)
        .await
        .with_context(|| format!("failed to write temp file {}", temp_path.display()))?;
    let created = match fs::hard_link(&temp_path, path).await {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(error) => {
            return Err(error).with_context(|| format!("failed to create {}", path.display()));
        }
    };
    fs::remove_file(&temp_path)
        .await
        .with_context(|| format!("failed to remove temp file {}", temp_path.display()))?;
    Ok(created)
}

async fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };
    serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))
        .map(Some)
}
