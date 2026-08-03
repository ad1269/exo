import fs from "node:fs";
import fsPromises from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { runWorker } from "./run-worker";
import type { WorkerOutboundCommand } from "./protocol";

let tempdir: string;
let markerDir: string;

beforeEach(async () => {
  tempdir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "exo-run-worker-"));
  markerDir = path.join(tempdir, "outbound-sent", "adapter-1");
});

afterEach(async () => {
  await fsPromises.rm(tempdir, { recursive: true, force: true });
});

function captureWorkerEvents() {
  const events: { type: string; command_id?: string; message?: string }[] = [];
  const spy = vi
    .spyOn(process.stdout, "write")
    .mockImplementation((chunk: unknown) => {
      for (const line of String(chunk).split("\n")) {
        if (line.trim().length > 0) {
          events.push(JSON.parse(line));
        }
      }
      return true;
    });
  return { events, restore: () => spy.mockRestore() };
}

function commandLine(id: string, text: string): string {
  return JSON.stringify({ type: "send_message", id, target: "#room", text });
}

async function* lines(...items: string[]): AsyncGenerator<string> {
  for (const item of items) {
    yield item;
  }
}

describe("runWorker", () => {
  it("connects once, then delivers each command through sendOnce", async () => {
    const delivered: string[] = [];
    let connects = 0;
    const captured = captureWorkerEvents();
    try {
      await runWorker(
        {
          connect: () => {
            connects += 1;
          },
          deliver: (command: WorkerOutboundCommand) => {
            delivered.push(command.text);
          },
        },
        {
          input: lines(
            commandLine("cmd-1", "one"),
            commandLine("cmd-2", "two"),
          ),
          markerDir,
        },
      );
    } finally {
      captured.restore();
    }

    expect(connects).toBe(1);
    expect(delivered).toEqual(["one", "two"]);
    expect(fs.existsSync(path.join(markerDir, "cmd-1.json"))).toBe(true);
    expect(fs.existsSync(path.join(markerDir, "cmd-2.json"))).toBe(true);
    expect(captured.events).toEqual([
      { type: "command_ack", command_id: "cmd-1" },
      { type: "command_ack", command_id: "cmd-2" },
    ]);
  });

  it("nacks a failed delivery and keeps the loop alive", async () => {
    const delivered: string[] = [];
    const captured = captureWorkerEvents();
    try {
      await runWorker(
        {
          connect: () => {},
          deliver: (command: WorkerOutboundCommand) => {
            if (command.id === "cmd-bad") {
              throw new Error("platform rejected the send");
            }
            delivered.push(command.text);
          },
        },
        {
          input: lines(
            commandLine("cmd-bad", "boom"),
            commandLine("cmd-good", "after"),
          ),
          markerDir,
        },
      );
    } finally {
      captured.restore();
    }

    expect(delivered).toEqual(["after"]);
    expect(fs.existsSync(path.join(markerDir, "cmd-bad.json"))).toBe(false);
    expect(captured.events).toEqual([
      { type: "error", message: "platform rejected the send" },
      {
        type: "command_nack",
        command_id: "cmd-bad",
        message: "platform rejected the send",
      },
      { type: "command_ack", command_id: "cmd-good" },
    ]);
  });

  it("reports an unparsable line without a nack and keeps going", async () => {
    const captured = captureWorkerEvents();
    try {
      await runWorker(
        {
          connect: () => {},
          deliver: () => {},
        },
        {
          input: lines("not json", "", commandLine("cmd-1", "one")),
          markerDir,
        },
      );
    } finally {
      captured.restore();
    }

    const types = captured.events.map((event) => event.type);
    expect(types).toEqual(["error", "command_ack"]);
  });
});
