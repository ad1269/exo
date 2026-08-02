// The subset of a WebSocket a flush needs, so the wait can be tested without
// a relay on the other end.
export type FlushableSocket = {
  readonly bufferedAmount: number;
  readonly readyState: number;
  addEventListener(type: string, listener: (event: SocketEvent) => void): void;
  removeEventListener(
    type: string,
    listener: (event: SocketEvent) => void,
  ): void;
};

export type SocketEvent = { code?: number; reason?: string };

const POLL_INTERVAL_MS = 5;

// `send` only queues, and the WHATWG WebSocket has no completion callback, so a
// drained `bufferedAmount` is the only signal that the frame left this process.
// It is not on its own a signal that the frame was accepted: undici decrements
// the count when a write fails exactly as it does when one succeeds, so a
// socket that died mid-flush drains like a success. The close and error events
// are latched for that, and re-checked once the buffer is empty — including
// when it was already empty on entry.
export async function flushSocket(
  ws: FlushableSocket,
  timeoutMs: number,
): Promise<void> {
  let death: string | null = null;
  const onClose = (event: SocketEvent) => {
    death ??= `ExoChat WebSocket closed before the frame was flushed: ${
      event.reason || `code ${event.code ?? "unknown"}`
    }`;
  };
  const onError = () => {
    death ??= "ExoChat WebSocket errored before the frame was flushed";
  };
  ws.addEventListener("close", onClose);
  ws.addEventListener("error", onError);
  try {
    const deadline = Date.now() + timeoutMs;
    while (ws.bufferedAmount > 0) {
      if (death !== null) {
        throw new Error(death);
      }
      if (Date.now() > deadline) {
        throw new Error(
          `ExoChat send timed out after ${timeoutMs}ms with the frame unflushed`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
    }
    if (death !== null) {
      throw new Error(death);
    }
    if (ws.readyState !== WebSocket.OPEN) {
      throw new Error("ExoChat WebSocket closed before the frame was flushed");
    }
  } finally {
    ws.removeEventListener("close", onClose);
    ws.removeEventListener("error", onError);
  }
}
