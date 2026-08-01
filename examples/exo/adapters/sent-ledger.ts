import fs from "node:fs";
import path from "node:path";

// The runtime redelivers any outbound message it claimed but never saw acked,
// so a send whose ack was lost in flight arrives at the worker a second time.
// The ledger records which command ids this adapter has already handed to the
// platform, which turns that redelivery into an ack instead of a second send.
//
// Honest limits:
// - A crash between the platform send returning and `record` returning can
//   still duplicate the message once. That window is milliseconds wide; the
//   window this closes is the whole worker/loop restart it sits inside.
// - The ledger is only as durable as the adapter state dir. Wiping worker
//   state re-opens redelivery for anything still inflight in the runtime.
// - The ledger says "this id was sent", not "the platform kept it". A send the
//   platform accepted and later discarded is not resent.

const MAX_RETAINED_IDS = 1000;

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
  return path.join(stateDir, "sent-ledger");
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

function rewriteLedger(ledgerPath: string, ids: string[]): void {
  const tempPath = `${ledgerPath}.tmp`;
  try {
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    fs.writeFileSync(tempPath, ids.map((id) => `${id}\n`).join(""));
    fs.renameSync(tempPath, ledgerPath);
  } catch {
    try {
      fs.unlinkSync(tempPath);
    } catch {
      // The temp file may never have been created; nothing to clean up.
    }
  }
}
