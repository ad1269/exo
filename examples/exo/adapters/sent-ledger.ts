import fs from "node:fs";
import path from "node:path";

import { writeWorkerEvent } from "./protocol";

// The runtime redelivers any outbound message it claimed but never saw acked,
// so a send whose ack was lost in flight arrives at the worker a second time.
// The ledger records which command ids this adapter has already handed to the
// platform, which turns that redelivery into an ack instead of a second send.
//
// This is per-adapter-instance state. It lives in the adapter's own state dir
// beside that adapter's session and auth material, so two adapters — even two
// of the same type — never read each other's ledger. Command ids are unique
// only within the adapter whose outbox minted them, so sharing one ledger
// across adapters would be wrong, not merely wasteful.
//
// Honest limits:
// - The ledger is only as durable as the adapter state dir. Wiping worker
//   state re-opens redelivery for anything still inflight in the runtime.
// - The ledger says "this id was sent", not "the platform kept it". A send the
//   platform accepted and later discarded is not resent.
// - Durability here means process-crash durability, which is the threat model:
//   the worker dies between the platform send and the ack. Every append is
//   fsynced and compaction swaps atomically, both of which survive that.
//   Machine-level power loss is explicitly NOT covered on the very first
//   write, because POSIX requires fsyncing the PARENT DIRECTORY for a newly
//   created filename to survive and this does not do it. Accepted rather than
//   fixed: a lost file reads as an empty ledger, which degrades to pre-ledger
//   behavior instead of corrupting anything, and directory fsync in node is
//   platform-fussy enough to cost more than the edge is worth.
//
// `sendOnce` below is the only blessed way to perform a send; the ordering it
// guarantees is the whole point of the ledger.

const MAX_RETAINED_IDS = 1000;
// Named so the file describes itself next to session.json and auth/ in the
// same state dir: plain line-delimited text, not an opaque blob.
const LEDGER_FILE_NAME = "sent-ledger.txt";

export type SentLedger = {
  has(commandId: string): boolean;
  record(commandId: string): void;
};

// Mirrors the runtime's own default in
// crates/executor/src/adapter/worker.rs, which exports EXO_ADAPTER_STATE_DIR
// for every worker it spawns. The fallback only matters when a worker is run
// by hand outside the runtime.
export function sentLedgerPath(adapterType: string): string {
  const stateDir =
    process.env.EXO_ADAPTER_STATE_DIR ??
    path.join(
      ".exo",
      "adapters",
      adapterType,
      process.env.EXO_ADAPTER_ID ?? "default",
    );
  return path.join(stateDir, LEDGER_FILE_NAME);
}

// The one blessed way to perform an outbound send. Workers hand over a
// `deliver` callback and never emit the ack themselves, so the
// check → deliver → record → ack order has a single home instead of being
// re-derived correctly in seven worker loops.
//
// A known id acks without calling `deliver` at all: the runtime is redelivering
// something the platform already has, and the ack is what finally clears it
// from inflight. Every ack carries a `disposition`, which is the observability
// of this fix: without it a suppressed redelivery is indistinguishable from a
// fresh send in the event stream.
//
// This assumes serial command processing, and every worker provides it: each
// drives its command stream with `for await (const line of input)` and awaits
// this call before pulling the next line. That is load-bearing. `has` and
// `record` straddle an await, so a worker that dispatched commands
// concurrently could let two deliveries of the same id both pass the `has`
// check before either recorded — reintroducing the duplicate this exists to
// prevent.
//
// `deliver` throwing is a real send failure — the id is NOT recorded and no ack
// is emitted, so the error propagates to the caller's existing nack path and
// the runtime is free to retry. Each worker reports failures its own way, which
// is why the nack stays at the call site rather than moving in here.
//
// Honest limit: a crash between `deliver` returning and `record` returning can
// still duplicate the message once. That window is milliseconds wide; the
// window this closes is the whole worker/loop restart it sits inside.
//
// Which is why `deliver` receives the command id. The ledger is the universal
// floor: it works on any platform, closes the entire reconnect class of
// duplicates, and leaves that millisecond residue. A platform-provided
// idempotency key is the per-platform ceiling: hand the platform a key derived
// from this id and the residue closes too, because the platform recognizes the
// retry itself. Only the receiving platform can do that — no amount of
// sender-side bookkeeping closes a window that spans the network — which is the
// classic end-to-end argument, and why the key can only be plugged in where a
// platform offers one. Discord's `nonce` + `enforceNonce` is the worked
// example; adapters with no such key keep the floor and lose nothing.
export async function sendOnce(
  ledger: SentLedger,
  commandId: string,
  deliver: (commandId: string) => Promise<void> | void,
): Promise<void> {
  if (ledger.has(commandId)) {
    writeWorkerEvent({
      type: "command_ack",
      command_id: commandId,
      disposition: "deduped",
    });
    return;
  }
  await deliver(commandId);
  ledger.record(commandId);
  writeWorkerEvent({
    type: "command_ack",
    command_id: commandId,
    disposition: "sent",
  });
}

export function loadSentLedger(ledgerPath: string): SentLedger {
  const retained = readRetainedIds(ledgerPath);
  const seen = new Set(retained);
  let order = retained;

  return {
    has(commandId: string): boolean {
      return seen.has(commandId);
    },
    record(commandId: string): void {
      if (seen.has(commandId)) {
        return;
      }
      seen.add(commandId);
      order.push(commandId);
      // Compacting at twice the cap bounds the file and this process's memory
      // without rewriting the whole ledger on every send.
      if (order.length > MAX_RETAINED_IDS * 2) {
        order = order.slice(-MAX_RETAINED_IDS);
        seen.clear();
        for (const id of order) {
          seen.add(id);
        }
        rewriteLedger(ledgerPath, order);
        return;
      }
      appendId(ledgerPath, commandId);
    },
  };
}

// An unreadable or malformed ledger is treated as empty: losing dedupe state
// degrades to today's behavior, whereas throwing here would take down an
// adapter that is otherwise healthy.
function readRetainedIds(ledgerPath: string): string[] {
  let contents: string;
  try {
    contents = fs.readFileSync(ledgerPath, "utf8");
  } catch {
    return [];
  }
  const ids = contents
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (ids.length <= MAX_RETAINED_IDS) {
    return ids;
  }
  const retained = ids.slice(-MAX_RETAINED_IDS);
  rewriteLedger(ledgerPath, retained);
  return retained;
}

// Appends are flushed before returning so the caller can ack knowing the id
// survives a crash on the next line.
function appendId(ledgerPath: string, commandId: string): void {
  let handle: number | null = null;
  try {
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    handle = fs.openSync(ledgerPath, "a");
    fs.writeSync(handle, `${commandId}\n`);
    fs.fsyncSync(handle);
  } catch {
    // Keep the id in memory so this process still dedupes; a redelivery after
    // a restart is the pre-ledger behavior rather than a new failure.
  } finally {
    if (handle !== null) {
      try {
        fs.closeSync(handle);
      } catch {
        // Nothing useful to do with a close failure on an already-flushed append.
      }
    }
  }
}

// Compaction replaces the ledger rather than editing it: write the survivors to
// a temp file, flush them, then rename over the old one. Rename is atomic, so a
// crash mid-compaction leaves either the whole old ledger or the whole new one
// — never a half-written file. Truncating in place would have made the same
// crash lose the ledger outright, which is the one failure the ledger exists to
// prevent. The fsync is what makes the swap honest: without it the rename can
// reach disk before the bytes it points at.
function rewriteLedger(ledgerPath: string, ids: string[]): void {
  const tempPath = `${ledgerPath}.tmp`;
  let handle: number | null = null;
  try {
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    handle = fs.openSync(tempPath, "w");
    fs.writeSync(handle, ids.map((id) => `${id}\n`).join(""));
    fs.fsyncSync(handle);
    fs.closeSync(handle);
    handle = null;
    fs.renameSync(tempPath, ledgerPath);
  } catch {
    if (handle !== null) {
      try {
        fs.closeSync(handle);
      } catch {
        // Nothing useful to do with a close failure on a temp file we are
        // about to discard.
      }
    }
    try {
      fs.unlinkSync(tempPath);
    } catch {
      // The temp file may never have been created; nothing to clean up.
    }
  }
}
