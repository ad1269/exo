import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

// Drives the real worker over its real stdio protocol against a fake IRC
// server, so what it asserts is what reaches the wire and the disk. The dedupe
// itself is the kernel's, and is tested at the dispatch seam in Rust.

const workerPath = fileURLToPath(new URL("./worker.ts", import.meta.url));
const repoRoot = fileURLToPath(new URL("../../../..", import.meta.url));
// Spawn tsx directly rather than through `pnpm tsx` as the runtime does: an
// intermediate pnpm process survives the kill and keeps the IRC socket open.
const tsxPath = path.join(repoRoot, "node_modules", ".bin", "tsx");

type WorkerEvent = {
  type: string;
  command_id?: string;
  message?: string;
};

let server: net.Server;
let port: number;
let stateDir: string;
let sentDir: string;
let received: string[];
let workers: ChildProcessWithoutNullStreams[];
let events: Map<ChildProcessWithoutNullStreams, WorkerEvent[]>;
let sockets: net.Socket[];

beforeEach(async () => {
  received = [];
  workers = [];
  sockets = [];
  events = new Map();
  stateDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "exo-irc-state-"));
  sentDir = path.join(stateDir, "outbound-sent");
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
      EXO_ADAPTER_SENT_DIR: sentDir,
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
  const workerEvents: WorkerEvent[] = [];
  events.set(worker, workerEvents);
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
      try {
        workerEvents.push(JSON.parse(line) as WorkerEvent);
      } catch {
        // The runtime treats a non-protocol line as fatal; here it is noise.
      }
    }
  });
  return worker;
}

async function waitFor(what: string, ready: () => boolean): Promise<void> {
  const deadline = Date.now() + 20_000;
  while (!ready()) {
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${what}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

async function waitForAck(
  worker: ChildProcessWithoutNullStreams,
  commandId: string,
): Promise<void> {
  await waitFor(`ack of ${commandId}`, () => {
    const workerEvents = events.get(worker) ?? [];
    const nack = workerEvents.find(
      (event) =>
        event.type === "command_nack" && event.command_id === commandId,
    );
    if (nack) {
      throw new Error(`worker nacked ${commandId}: ${nack.message}`);
    }
    return workerEvents.some(
      (event) => event.type === "command_ack" && event.command_id === commandId,
    );
  });
}

// Acks and PRIVMSGs travel on different channels, so the ack says nothing about
// whether the message has arrived at the server yet.
async function waitForPrivmsgs(count: number): Promise<void> {
  await waitFor(`${count} PRIVMSG(s)`, () => privmsgs().length >= count);
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

function markerExists(commandId: string): boolean {
  return fs.existsSync(path.join(sentDir, `${commandId}.json`));
}

describe("IRC worker outbound delivery", () => {
  it("records a sent marker for every message it acks", async () => {
    const worker = startWorker();
    await waitFor("the worker to connect", () => sockets.length >= 1);

    sendCommand(worker, "cmd-1", "hello once");
    await waitForAck(worker, "cmd-1");

    // The kernel's dedupe reads this marker, so it has to exist by the time
    // the ack does.
    expect(markerExists("cmd-1")).toBe(true);
    await waitForPrivmsgs(1);
    expect(privmsgs()).toEqual(["PRIVMSG #exo-test :hello once"]);
  }, 45_000);

  it("keeps sending after a restart, marking each command", async () => {
    const first = startWorker();
    await waitFor("the first worker to connect", () => sockets.length >= 1);
    sendCommand(first, "cmd-a", "first");
    await waitForAck(first, "cmd-a");

    first.kill("SIGKILL");
    await new Promise<void>((resolve) => first.once("exit", () => resolve()));

    const second = startWorker();
    await waitFor("the second worker to connect", () => sockets.length >= 2);
    sendCommand(second, "cmd-b", "second");
    await waitForAck(second, "cmd-b");

    await waitForPrivmsgs(2);
    expect(privmsgs()).toEqual([
      "PRIVMSG #exo-test :first",
      "PRIVMSG #exo-test :second",
    ]);
    expect(markerExists("cmd-a")).toBe(true);
    expect(markerExists("cmd-b")).toBe(true);
  }, 60_000);
});
