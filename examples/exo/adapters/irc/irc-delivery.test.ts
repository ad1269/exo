import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import fsPromises from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

// The runtime redelivers unacked inflight messages after a worker restart.
// These tests drive the real worker over its real stdio protocol against a
// fake IRC server, so they assert what actually reaches the wire.

const workerPath = fileURLToPath(new URL("./worker.ts", import.meta.url));
const repoRoot = fileURLToPath(new URL("../../../..", import.meta.url));
// Spawn tsx directly rather than through `pnpm tsx` as the runtime does: an
// intermediate pnpm process survives the kill and keeps the IRC socket open.
const tsxPath = path.join(repoRoot, "node_modules", ".bin", "tsx");

let server: net.Server;
let port: number;
let stateDir: string;
let received: string[];
let workers: ChildProcessWithoutNullStreams[];
let sockets: net.Socket[];

beforeEach(async () => {
  received = [];
  workers = [];
  sockets = [];
  stateDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "exo-irc-state-"));
  server = net.createServer((socket) => {
    sockets.push(socket);
    socket.setEncoding("utf8");
    let buffer = "";
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      const lines = buffer.split("\r\n");
      buffer = lines.pop() ?? "";
      received.push(...lines.filter((line) => line.length > 0));
    });
    socket.on("error", () => {
      // The worker is killed mid-connection between phases; a reset here is
      // the expected shape of that teardown.
    });
  });
  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("fake IRC server did not bind a TCP port");
  }
  port = address.port;
});

afterEach(async () => {
  for (const worker of workers) {
    worker.kill("SIGKILL");
  }
  for (const socket of sockets) {
    socket.destroy();
  }
  await new Promise<void>((resolve) => {
    server.close(() => resolve());
  });
  await fsPromises.rm(stateDir, { recursive: true, force: true });
});

function startWorker(): ChildProcessWithoutNullStreams {
  const worker = spawn(tsxPath, [workerPath], {
    cwd: repoRoot,
    env: {
      ...process.env,
      EXO_ADAPTER_ID: "delivery-test",
      EXO_ADAPTER_TYPE: "irc",
      EXO_ADAPTER_STATE_DIR: stateDir,
      EXO_ADAPTER_CONFIG: JSON.stringify({
        server: "127.0.0.1",
        port,
        tls: false,
        nick: "exo-test",
        username: "exo-test",
        realname: "Exo Test",
        channel: "#exo-test",
        trigger: "mention",
      }),
    },
    stdio: ["pipe", "pipe", "pipe"],
  });
  workers.push(worker);
  return worker;
}

// Resolves with the disposition on the worker's first `command_ack` for the
// given id.
function waitForAck(
  worker: ChildProcessWithoutNullStreams,
  commandId: string,
): Promise<string | undefined> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`timed out waiting for ack of ${commandId}`));
    }, 30_000);
    let buffer = "";
    worker.stdout.setEncoding("utf8");
    worker.stdout.on("data", (chunk: string) => {
      buffer += chunk;
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        if (line.trim().length === 0) {
          continue;
        }
        const event = JSON.parse(line) as {
          type: string;
          command_id?: string;
          message?: string;
          disposition?: string;
        };
        if (event.type === "command_ack" && event.command_id === commandId) {
          clearTimeout(timer);
          resolve(event.disposition);
          return;
        }
        if (event.type === "command_nack" && event.command_id === commandId) {
          clearTimeout(timer);
          reject(new Error(`worker nacked ${commandId}: ${event.message}`));
          return;
        }
      }
    });
  });
}

function waitForConnection(): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error("timed out waiting for the worker to connect"));
    }, 30_000);
    server.once("connection", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

function sendCommand(
  worker: ChildProcessWithoutNullStreams,
  commandId: string,
  text: string,
): void {
  worker.stdin.write(
    `${JSON.stringify({
      type: "send_message",
      id: commandId,
      target: null,
      text,
      attachments: [],
    })}\n`,
  );
}

function privmsgs(): string[] {
  return received.filter((line) => line.startsWith("PRIVMSG"));
}

describe("IRC worker outbound delivery", () => {
  it("sends once, then acks a redelivery of the same command without resending", async () => {
    const first = startWorker();
    await waitForConnection();
    sendCommand(first, "cmd-redelivered", "hello once");
    expect(await waitForAck(first, "cmd-redelivered")).toBe("sent");
    expect(privmsgs()).toEqual(["PRIVMSG #exo-test :hello once"]);

    // Drop the worker without letting the runtime record the ack, which is
    // exactly the state requeue_inflight_messages redelivers from.
    first.kill("SIGKILL");
    await new Promise<void>((resolve) => first.once("exit", () => resolve()));

    const second = startWorker();
    await waitForConnection();
    sendCommand(second, "cmd-redelivered", "hello once");
    // The ack the runtime records says the worker deduped rather than sent.
    expect(await waitForAck(second, "cmd-redelivered")).toBe("deduped");

    // The redelivery is acked from the ledger, so the platform sees one message.
    expect(privmsgs()).toEqual(["PRIVMSG #exo-test :hello once"]);
  }, 90_000);

  it("still sends a genuinely new command after a restart", async () => {
    const first = startWorker();
    await waitForConnection();
    sendCommand(first, "cmd-a", "first");
    await waitForAck(first, "cmd-a");

    first.kill("SIGKILL");
    await new Promise<void>((resolve) => first.once("exit", () => resolve()));

    const second = startWorker();
    await waitForConnection();
    sendCommand(second, "cmd-b", "second");
    await waitForAck(second, "cmd-b");

    expect(privmsgs()).toEqual([
      "PRIVMSG #exo-test :first",
      "PRIVMSG #exo-test :second",
    ]);
  }, 90_000);
});
