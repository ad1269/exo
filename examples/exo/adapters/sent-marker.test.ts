import fs from "node:fs";
import fsPromises from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { sendOnce, sentMarkerDir } from "./sent-marker";

let tempdir: string;
let markerDir: string;

beforeEach(async () => {
  tempdir = await fsPromises.mkdtemp(
    path.join(os.tmpdir(), "exo-sent-marker-"),
  );
  markerDir = path.join(tempdir, "outbound-sent", "adapter-1");
});

afterEach(async () => {
  await fsPromises.rm(tempdir, { recursive: true, force: true });
});

function markerPath(commandId: string): string {
  return path.join(markerDir, `${commandId}.json`);
}

// Workers never emit the ack themselves, so what sendOnce writes to stdout is
// part of its contract.
function captureWorkerEvents() {
  const events: unknown[] = [];
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
  return { events, spy, restore: () => spy.mockRestore() };
}

describe("sendOnce", () => {
  it("delivers, records a marker, then acks", async () => {
    let delivered = 0;
    const captured = captureWorkerEvents();
    try {
      await sendOnce(markerDir, "cmd-1", () => {
        delivered += 1;
      });
    } finally {
      captured.restore();
    }

    expect(delivered).toBe(1);
    expect(JSON.parse(fs.readFileSync(markerPath("cmd-1"), "utf8"))).toEqual({
      message_id: "cmd-1",
      sent_at_ms: expect.any(Number),
    });
    expect(captured.events).toEqual([
      { type: "command_ack", command_id: "cmd-1" },
    ]);
  });

  it("hands the command id to deliver so it can seed a platform key", async () => {
    const seen: string[] = [];
    const captured = captureWorkerEvents();
    try {
      await sendOnce(markerDir, "cmd-1", (commandId) => {
        seen.push(commandId);
      });
    } finally {
      captured.restore();
    }

    expect(seen).toEqual(["cmd-1"]);
  });

  it("flushes the marker before the ack", async () => {
    const captured = captureWorkerEvents();
    const fsyncSpy = vi.spyOn(fs, "fsyncSync");
    let flushes: number[] = [];
    let acks: number[] = [];
    try {
      await sendOnce(markerDir, "cmd-1", () => {});
      flushes = fsyncSpy.mock.invocationCallOrder;
      acks = captured.spy.mock.invocationCallOrder;
    } finally {
      fsyncSpy.mockRestore();
      captured.restore();
    }

    // The kernel treats an ack without a marker as a violation, so the flush
    // has to land first.
    expect(flushes.length).toBeGreaterThan(0);
    expect(Math.max(...flushes)).toBeLessThan(Math.min(...acks));
  });

  it("does not record or ack when deliver throws", async () => {
    const captured = captureWorkerEvents();
    try {
      await expect(
        sendOnce(markerDir, "cmd-1", () => {
          throw new Error("platform rejected the send");
        }),
      ).rejects.toThrow("platform rejected the send");
    } finally {
      captured.restore();
    }

    // A failed send stays retryable, and the caller's nack path owns reporting.
    expect(fs.existsSync(markerPath("cmd-1"))).toBe(false);
    expect(captured.events).toEqual([]);
  });

  it("does not record or ack when an async deliver rejects", async () => {
    const captured = captureWorkerEvents();
    try {
      await expect(
        sendOnce(markerDir, "cmd-1", async () => {
          await Promise.resolve();
          throw new Error("timed out");
        }),
      ).rejects.toThrow("timed out");
    } finally {
      captured.restore();
    }

    expect(fs.existsSync(markerPath("cmd-1"))).toBe(false);
    expect(captured.events).toEqual([]);
  });

  it("still acks when the marker cannot be written", async () => {
    // The marker path is a directory, so the write fails. The message did
    // reach the platform; withholding the ack would send it again.
    fs.mkdirSync(markerPath("cmd-1"), { recursive: true });
    const stderr = vi
      .spyOn(process.stderr, "write")
      .mockImplementation(() => true);
    const captured = captureWorkerEvents();
    let warnings = 0;
    try {
      await sendOnce(markerDir, "cmd-1", () => {});
      warnings = stderr.mock.calls.length;
    } finally {
      captured.restore();
      stderr.mockRestore();
    }

    expect(captured.events).toEqual([
      { type: "command_ack", command_id: "cmd-1" },
    ]);
    expect(warnings).toBe(1);
  });
});

describe("sentMarkerDir", () => {
  const original = process.env.EXO_ADAPTER_SENT_DIR;

  afterEach(() => {
    if (original === undefined) {
      delete process.env.EXO_ADAPTER_SENT_DIR;
    } else {
      process.env.EXO_ADAPTER_SENT_DIR = original;
    }
  });

  it("uses the directory the runtime exports", () => {
    process.env.EXO_ADAPTER_SENT_DIR = "/var/exo/outbound-sent/adapter-1";
    expect(sentMarkerDir()).toBe("/var/exo/outbound-sent/adapter-1");
  });

  it("throws when the runtime did not export one", () => {
    delete process.env.EXO_ADAPTER_SENT_DIR;
    expect(() => sentMarkerDir()).toThrow("EXO_ADAPTER_SENT_DIR");
  });
});
