use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::Context;
use exoharness::{AcquireLeaseRequest, ReleaseLeaseRequest, RenewLeaseRequest, Result, Uuid7};
use lingua::Message;
use lingua::universal::UserContent;
use tokio::sync::Mutex as AsyncMutex;

use crate::{HarnessConversation, SendRequest, SendResult};

/// TTL ~4x the renewal cadence: a healthy holder renews three times before
/// its lease could lapse.
const WAKEUP_LEASE_TTL_MS: u64 = 60_000;
const WAKEUP_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(15);
const WAKEUP_ACQUIRE_RETRY_INTERVAL: Duration = Duration::from_millis(500);

pub async fn send_conversation_wakeup(
    conversation: &dyn HarnessConversation,
    prompt: String,
) -> Result<SendResult> {
    send_conversation_wakeup_content(conversation, UserContent::String(prompt)).await
}

/// Wakeup variant for multimodal content, e.g. adapter messages that carry
/// inbound images for the model to analyze.
///
/// The send runs under a kernel turn lease: acquired before the turn, renewed
/// on a heartbeat, released after. A failed renewal means a newer activation
/// owns the thread — the send future is dropped on the spot, and anything it
/// might still have attempted is fenced kernel-side by the turn's epoch.
pub async fn send_conversation_wakeup_content(
    conversation: &dyn HarnessConversation,
    content: UserContent,
) -> Result<SendResult> {
    let handle = conversation.exoharness_handle();
    // One activation per wakeup call: a restarted process must never look
    // like the prior holder still being alive.
    let holder = format!("wakeup-{}", Uuid7::now());
    let lease = loop {
        let attempt = handle
            .acquire_lease(AcquireLeaseRequest {
                holder: holder.clone(),
                ttl_ms: WAKEUP_LEASE_TTL_MS,
            })
            .await?;
        if attempt.acquired {
            break attempt.lease;
        }
        tokio::time::sleep(WAKEUP_ACQUIRE_RETRY_INTERVAL).await;
    };

    let renew = async {
        loop {
            tokio::time::sleep(WAKEUP_LEASE_RENEW_INTERVAL).await;
            if let Err(error) = handle
                .renew_lease(RenewLeaseRequest {
                    lease_id: lease.lease_id,
                    epoch: lease.epoch,
                    ttl_ms: WAKEUP_LEASE_TTL_MS,
                })
                .await
            {
                break error;
            }
        }
    };
    let send_result = tokio::select! {
        result = conversation.send(SendRequest {
            input: vec![Message::User { content }],
            session_id: None,
            epoch: Some(lease.epoch),
        }) => result,
        error = renew => {
            // Fenced: the thread has a newer owner. No release — the lease
            // being renewed is no longer ours to end.
            return Err(error).context("wakeup fenced mid-send: a newer activation owns the thread");
        }
    };
    let release = ReleaseLeaseRequest {
        lease_id: lease.lease_id,
        epoch: lease.epoch,
    };
    match send_result {
        Ok(result) => {
            conversation.close_session(result.session_id).await?;
            handle.release_lease(release).await?;
            Ok(result)
        }
        Err(error) => {
            // Release anyway so the thread reopens now rather than at TTL;
            // the send error is the one worth returning.
            if let Err(release_error) = handle.release_lease(release).await {
                tracing::warn!(%release_error, "failed to release the wakeup lease after a send error");
            }
            Err(error)
        }
    }
}

pub(crate) fn conversation_send_lock(conversation_id: &str) -> Arc<AsyncMutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .expect("conversation wakeup lock registry poisoned");
    Arc::clone(
        locks
            .entry(conversation_id.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
    )
}
