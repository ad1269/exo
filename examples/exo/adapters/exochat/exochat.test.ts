import { describe, expect, it } from "vitest";

import { flushSocket, type SocketEvent } from "./exochat";

// Enough of a WebSocket to drain a buffer and die while doing it.
class FakeSocket {
  bufferedAmount: number;
  readyState: number = WebSocket.OPEN;
  private listeners = new Map<string, ((event: SocketEvent) => void)[]>();

  constructor(bufferedAmount: number) {
    this.bufferedAmount = bufferedAmount;
  }

  addEventListener(type: string, listener: (event: SocketEvent) => void): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  removeEventListener(
    type: string,
    listener: (event: SocketEvent) => void,
  ): void {
    this.listeners.set(
      type,
      (this.listeners.get(type) ?? []).filter((each) => each !== listener),
    );
  }

  listenerCount(): number {
    return [...this.listeners.values()].reduce(
      (total, each) => total + each.length,
      0,
    );
  }

  // A write that never reaches the peer drains the buffer just like one that
  // does, which is the whole reason the flush cannot trust the count alone.
  fail(event: SocketEvent, type: "close" | "error" = "close"): void {
    this.bufferedAmount = 0;
    this.readyState = WebSocket.CLOSED;
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }

  drain(): void {
    this.bufferedAmount = 0;
  }
}

describe("flushSocket", () => {
  it("resolves once the buffer drains on a live socket", async () => {
    const socket = new FakeSocket(64);
    setTimeout(() => socket.drain(), 10);

    await expect(flushSocket(socket, 1_000)).resolves.toBeUndefined();
    expect(socket.listenerCount()).toBe(0);
  });

  it("throws with the close reason when the socket dies mid-flush", async () => {
    const socket = new FakeSocket(64);
    setTimeout(
      () => socket.fail({ code: 1006, reason: "relay went away" }),
      10,
    );

    await expect(flushSocket(socket, 1_000)).rejects.toThrow("relay went away");
    expect(socket.listenerCount()).toBe(0);
  });

  it("reports a close with no reason by its code", async () => {
    const socket = new FakeSocket(64);
    setTimeout(() => socket.fail({ code: 1006 }), 10);

    await expect(flushSocket(socket, 1_000)).rejects.toThrow("code 1006");
  });

  it("throws on an error event raised during the flush", async () => {
    const socket = new FakeSocket(64);
    setTimeout(() => socket.fail({}, "error"), 10);

    await expect(flushSocket(socket, 1_000)).rejects.toThrow("errored");
  });

  // The buffer is often already empty by the time the flush runs, which used
  // to skip every liveness check and report a dead socket as a clean send.
  it("checks the socket even when nothing was buffered", async () => {
    const socket = new FakeSocket(0);
    socket.readyState = WebSocket.CLOSED;

    await expect(flushSocket(socket, 1_000)).rejects.toThrow(
      "closed before the frame was flushed",
    );
    expect(socket.listenerCount()).toBe(0);
  });

  it("resolves when nothing was buffered and the socket is open", async () => {
    await expect(
      flushSocket(new FakeSocket(0), 1_000),
    ).resolves.toBeUndefined();
  });

  it("times out on a socket that never drains", async () => {
    const socket = new FakeSocket(64);

    await expect(flushSocket(socket, 20)).rejects.toThrow("timed out");
    expect(socket.listenerCount()).toBe(0);
  });
});
