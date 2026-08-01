import fs from "node:fs";
import fsPromises from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { loadSentLedger, sentLedgerPath } from "./sent-ledger";

let tempdir: string;
let ledgerPath: string;

beforeEach(async () => {
  tempdir = await fsPromises.mkdtemp(
    path.join(os.tmpdir(), "exo-sent-ledger-"),
  );
  ledgerPath = path.join(tempdir, "state", "sent-ledger.txt");
});

afterEach(async () => {
  await fsPromises.rm(tempdir, { recursive: true, force: true });
});

function readIds(): string[] {
  return fs
    .readFileSync(ledgerPath, "utf8")
    .split("\n")
    .filter((line) => line.length > 0);
}

describe("loadSentLedger", () => {
  it("starts empty and records ids", () => {
    const ledger = loadSentLedger(ledgerPath);
    expect(ledger.has("cmd-1")).toBe(false);
    ledger.record("cmd-1");
    expect(ledger.has("cmd-1")).toBe(true);
    expect(ledger.has("cmd-2")).toBe(false);
  });

  it("creates the state directory on first record", () => {
    loadSentLedger(ledgerPath).record("cmd-1");
    expect(readIds()).toEqual(["cmd-1"]);
  });

  it("remembers ids across a restart from the same path", () => {
    const before = loadSentLedger(ledgerPath);
    before.record("cmd-1");
    before.record("cmd-2");

    const after = loadSentLedger(ledgerPath);
    expect(after.has("cmd-1")).toBe(true);
    expect(after.has("cmd-2")).toBe(true);
    expect(after.has("cmd-3")).toBe(false);
  });

  it("does not leak ids between different paths", () => {
    loadSentLedger(ledgerPath).record("cmd-1");
    const other = loadSentLedger(
      path.join(tempdir, "other", "sent-ledger.txt"),
    );
    expect(other.has("cmd-1")).toBe(false);
  });

  it("ignores a repeated record instead of appending it twice", () => {
    const ledger = loadSentLedger(ledgerPath);
    ledger.record("cmd-1");
    ledger.record("cmd-1");
    expect(readIds()).toEqual(["cmd-1"]);
  });

  it("prunes to the retention cap on load and keeps the newest ids", () => {
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    const ids = Array.from({ length: 2500 }, (_, index) => `cmd-${index}`);
    fs.writeFileSync(ledgerPath, ids.map((id) => `${id}\n`).join(""));

    const ledger = loadSentLedger(ledgerPath);
    expect(ledger.has("cmd-2499")).toBe(true);
    expect(ledger.has("cmd-1500")).toBe(true);
    expect(ledger.has("cmd-1499")).toBe(false);
    expect(ledger.has("cmd-0")).toBe(false);
    expect(readIds()).toHaveLength(1000);
  });

  // Every record fsyncs, so tripping compaction costs a few thousand real
  // disk flushes. That is the intended per-send price at chat message rates;
  // it is only this loop that feels it.
  it(
    "compacts the file once recording passes twice the cap",
    { timeout: 60_000 },
    () => {
      const ledger = loadSentLedger(ledgerPath);
      for (let index = 0; index < 2001; index += 1) {
        ledger.record(`cmd-${index}`);
      }
      expect(readIds()).toHaveLength(1000);
      expect(ledger.has("cmd-2000")).toBe(true);
      expect(ledger.has("cmd-1001")).toBe(true);
      expect(ledger.has("cmd-0")).toBe(false);
    },
  );

  it("tolerates blank and whitespace-only lines", () => {
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    fs.writeFileSync(ledgerPath, "cmd-1\n\n   \ncmd-2\n");

    const ledger = loadSentLedger(ledgerPath);
    expect(ledger.has("cmd-1")).toBe(true);
    expect(ledger.has("cmd-2")).toBe(true);
  });

  it("treats a partially written trailing line as its own id", () => {
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    fs.writeFileSync(ledgerPath, "cmd-1\ncmd-2-trunc");

    const ledger = loadSentLedger(ledgerPath);
    expect(ledger.has("cmd-1")).toBe(true);
    expect(ledger.has("cmd-2")).toBe(false);
  });

  it("treats an unreadable ledger as empty without throwing", () => {
    fs.mkdirSync(ledgerPath, { recursive: true });

    const ledger = loadSentLedger(ledgerPath);
    expect(ledger.has("cmd-1")).toBe(false);
    expect(() => ledger.record("cmd-1")).not.toThrow();
    // The append cannot land on a directory, but the id still dedupes for the
    // life of this process.
    expect(ledger.has("cmd-1")).toBe(true);
  });

  it("survives binary garbage in the ledger file", () => {
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    fs.writeFileSync(ledgerPath, Buffer.from([0x00, 0xff, 0xfe, 0x0a, 0x01]));

    const ledger = loadSentLedger(ledgerPath);
    expect(ledger.has("cmd-1")).toBe(false);
    expect(() => ledger.record("cmd-1")).not.toThrow();
    expect(ledger.has("cmd-1")).toBe(true);
  });
});

describe("sentLedgerPath", () => {
  const stateDir = process.env.EXO_ADAPTER_STATE_DIR;
  const adapterId = process.env.EXO_ADAPTER_ID;

  afterEach(() => {
    restore("EXO_ADAPTER_STATE_DIR", stateDir);
    restore("EXO_ADAPTER_ID", adapterId);
  });

  function restore(name: string, value: string | undefined): void {
    if (value === undefined) {
      delete process.env[name];
    } else {
      process.env[name] = value;
    }
  }

  it("uses the state dir the runtime exports", () => {
    process.env.EXO_ADAPTER_STATE_DIR = "/var/exo/adapters/irc/main";
    expect(sentLedgerPath("irc")).toBe(
      "/var/exo/adapters/irc/main/sent-ledger.txt",
    );
  });

  it("falls back to the runtime's own default layout", () => {
    delete process.env.EXO_ADAPTER_STATE_DIR;
    process.env.EXO_ADAPTER_ID = "main";
    expect(sentLedgerPath("slack")).toBe(
      path.join(".exo", "adapters", "slack", "main", "sent-ledger.txt"),
    );
  });

  it("falls back to the default adapter id", () => {
    delete process.env.EXO_ADAPTER_STATE_DIR;
    delete process.env.EXO_ADAPTER_ID;
    expect(sentLedgerPath("discord")).toBe(
      path.join(".exo", "adapters", "discord", "default", "sent-ledger.txt"),
    );
  });
});
