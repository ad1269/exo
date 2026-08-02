import fs from "node:fs";
import path from "node:path";

import { writeWorkerEvent } from "./protocol";

export function sentMarkerDir(): string {
  const dir = process.env.EXO_ADAPTER_SENT_DIR;
  if (!dir) {
    throw new Error("EXO_ADAPTER_SENT_DIR must be set by the adapter runtime");
  }
  return dir;
}

// Deliver, record, ack. Workers never check anything — the kernel dedupes on
// the marker this writes. `deliver` throwing leaves no marker and no ack, so
// the error reaches the caller's nack path and the runtime retries.
export async function sendOnce(
  markerDir: string,
  commandId: string,
  deliver: (commandId: string) => Promise<void> | void,
): Promise<void> {
  await deliver(commandId);
  recordSentMarker(markerDir, commandId);
  writeWorkerEvent({ type: "command_ack", command_id: commandId });
}

// Flushed before the ack so a crash on the next line still leaves the marker.
// A failed write acks anyway: the platform has the message, and the kernel
// reports the missing marker.
function recordSentMarker(markerDir: string, commandId: string): void {
  let handle: number | null = null;
  try {
    fs.mkdirSync(markerDir, { recursive: true });
    handle = fs.openSync(path.join(markerDir, `${commandId}.json`), "w");
    fs.writeSync(
      handle,
      JSON.stringify({ message_id: commandId, sent_at_ms: Date.now() }),
    );
    fs.fsyncSync(handle);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(
      `[adapter] failed to record sent marker for ${commandId}: ${message}\n`,
    );
  } finally {
    if (handle !== null) {
      try {
        fs.closeSync(handle);
      } catch {
        // Nothing to do with a close failure on a flushed write.
      }
    }
  }
}
