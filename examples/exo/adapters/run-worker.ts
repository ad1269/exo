import { createHash } from "node:crypto";
import readline from "node:readline/promises";
import process from "node:process";

import {
  adapterConfig,
  parseWorkerCommand,
  writeWorkerEvent,
  type WorkerInboundEvent,
  type WorkerOutboundCommand,
} from "./protocol";
import { sendOnce, sentMarkerDir } from "./sent-marker";

// Events a worker may emit itself. Acks and nacks are absent by type: the SDK
// writes them from the one blessed path, so a worker cannot ack a send it did
// not make or nack one it did.
export type WorkerEmitEvent = Exclude<
  WorkerInboundEvent,
  { type: "command_ack" | "command_nack" }
>;

export type WorkerContext = {
  config: Record<string, unknown>;
  emit: (event: WorkerEmitEvent) => void;
};

export type RunWorkerHooks = {
  // Establish the platform connection and start translating inbound platform
  // traffic into `ctx.emit` calls. Runs once, before the command loop.
  connect: (ctx: WorkerContext) => Promise<void> | void;
  // Deliver one outbound command to the platform. Throwing nacks the command
  // and the runtime retries; returning records the sent marker and acks. The
  // worker never sees either.
  deliver: (
    command: WorkerOutboundCommand,
    ctx: WorkerContext,
  ) => Promise<void> | void;
};

// One part of a multi-part send: its position and its stable identity.
export type PartRef = {
  index: number;
  id: string;
};

// Deterministic identity for one part of a command, so a retried command
// presents the same key per part and a platform with an idempotency key
// re-posts only the parts it has not seen. Full hex; platforms truncate to
// their own limit — Discord's 25-char enforced nonce is a prefix of this.
export function partIdentity(commandId: string, index: number): string {
  return createHash("sha256")
    .update(commandId)
    .update("\0")
    .update(String(index))
    .digest("hex");
}

// Deliver a command as ordered parts, each with its identity. Sequential on
// purpose: parts are chunks of one message, and posting them out of order
// scrambles the reply. A throw stops at the failed part; the retry re-walks
// every part, and per-part identity is what keeps the walked-again prefix
// from double-posting where the platform can check it.
export async function deliverParts<T>(
  command: WorkerOutboundCommand,
  parts: readonly T[],
  send: (part: T, ref: PartRef) => Promise<void> | void,
): Promise<void> {
  if (parts.length === 0) {
    // Returning would let sendOnce mark the command sent with zero platform
    // posts — a phantom delivery. A throw makes it a nack instead.
    throw new Error(`no parts to deliver for command ${command.id}`);
  }
  for (const [index, part] of parts.entries()) {
    await send(part, { index, id: partIdentity(command.id, index) });
  }
}

export type RunWorkerOptions = {
  // Test seams; production workers take the defaults.
  input?: AsyncIterable<string>;
  markerDir?: string;
};

// The command loop every worker used to hand-roll: parse, deliver through
// sendOnce, nack on failure, never crash the loop on a bad line. A worker is
// its two hooks; delivery bookkeeping lives here.
export async function runWorker(
  hooks: RunWorkerHooks,
  options: RunWorkerOptions = {},
): Promise<void> {
  const markerDir = options.markerDir ?? sentMarkerDir();
  const ctx: WorkerContext = {
    config: adapterConfig(),
    emit: writeWorkerEvent,
  };
  await hooks.connect(ctx);
  const input =
    options.input ??
    readline.createInterface({
      input: process.stdin,
      crlfDelay: Number.POSITIVE_INFINITY,
    });
  try {
    for await (const line of input) {
      if (line.trim().length === 0) {
        continue;
      }
      let commandId: string | null = null;
      try {
        const command = parseWorkerCommand(JSON.parse(line));
        commandId = command.id;
        await sendOnce(markerDir, command.id, () =>
          hooks.deliver(command, ctx),
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        writeWorkerEvent({ type: "error", message });
        if (commandId !== null) {
          writeWorkerEvent({
            type: "command_nack",
            command_id: commandId,
            message,
          });
        }
      }
    }
  } catch (error) {
    // Emit, then rethrow so the process exits: a worker with live platform
    // handles would otherwise idle as a zombie the kernel reads as healthy,
    // while its claimed commands sit un-acked forever. Exiting engages the
    // kernel's restart-and-requeue.
    const message = error instanceof Error ? error.message : String(error);
    writeWorkerEvent({
      type: "error",
      message: `worker command stream closed with error: ${message}`,
    });
    throw error;
  }
}
