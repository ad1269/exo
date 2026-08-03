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
    // A command-stream failure ends the loop, not the process mid-write: the
    // runtime restarts the worker and requeues whatever was in flight.
    const message = error instanceof Error ? error.message : String(error);
    writeWorkerEvent({
      type: "error",
      message: `worker command stream closed with error: ${message}`,
    });
  }
}
