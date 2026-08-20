import { describe, expect, it } from "vitest";

import {
  collectPendingInputRequests,
  inputResolvedEvent,
  parseInputRequested,
  type AddEventsResult,
  type Event,
  type EventData,
  type EventQuery,
  type GetEventsResult,
  type JsonObject,
  type ToolInstance,
  type ToolRequest,
  type TurnContext,
} from "@exo/harness";

import { createInputToolInstances } from "./input-tools";

// Minimal in-memory event log honoring the cursor/types/direction subset the
// tool and the pending-set helpers use.
class FakeEventLog {
  private events: Event[] = [];
  private seq = 0;

  async addEvents(data: EventData[]): Promise<AddEventsResult> {
    const eventIds = data.map((eventData) => {
      this.seq += 1;
      const id = `event-${String(this.seq).padStart(4, "0")}`;
      this.events.push({
        id,
        conversationId: "conversation-1",
        createdAt: new Date(1700000000000 + this.seq * 1000).toISOString(),
        data: eventData,
      });
      return id;
    });
    return { eventIds, latestEventId: eventIds.at(-1) ?? `event-0` };
  }

  async getEvents(query?: EventQuery): Promise<GetEventsResult> {
    let selected = this.events.filter((event) => {
      if (!query?.types) {
        return true;
      }
      const kind =
        event.data.type === "custom"
          ? String((event.data as { event_type?: unknown }).event_type)
          : event.data.type;
      return query.types.includes(kind);
    });
    if (query?.cursor) {
      const cursor = query.cursor;
      selected = selected.filter((event) => event.id > cursor);
    }
    return {
      events: selected,
      cursor: selected.at(-1)?.id ?? null,
    };
  }

  requestedPayloads() {
    return this.events
      .map((event) => parseInputRequested(event.data))
      .filter((payload) => payload !== null);
  }
}

function fakeContext(log: FakeEventLog) {
  const executedTools: ToolRequest[] = [];
  const context = {
    executeTool: async (request: ToolRequest) => {
      executedTools.push(request);
      return { ok: true };
    },
    exoharness: {
      current: {
        conversation: {
          getEvents: (query?: EventQuery) => log.getEvents(query),
          addEvents: (request: { data: EventData[] }) =>
            log.addEvents(request.data),
        },
        turn: {
          addEvents: (data: EventData[]) => log.addEvents(data),
        },
      },
    },
  } as unknown as TurnContext;
  return { context, executedTools };
}

function requestInputTool(): ToolInstance {
  const [tool] = createInputToolInstances({ pollIntervalMs: 5 });
  return tool;
}

function execute(
  tool: ToolInstance,
  context: TurnContext,
  args: JsonObject,
): Promise<unknown> {
  return tool.handler.execute(
    {
      prompt: "Which region?",
      kind: null,
      answerSchema: null,
      adapterId: null,
      target: null,
      timeoutSeconds: null,
      ...args,
    },
    { context, toolCallId: "call-1" },
  );
}

describe("request_input", () => {
  it("emits input_requested, blocks, and returns the appended answer", async () => {
    const log = new FakeEventLog();
    const { context } = fakeContext(log);
    const tool = requestInputTool();

    const pending = execute(tool, context, {});
    // Wait until the request event is durable, as a resolver would.
    await new Promise((resolve) => setTimeout(resolve, 10));
    const [requested] = log.requestedPayloads();
    expect(requested.kind).toBe("question");
    expect(requested.tool_call_id).toBe("call-1");
    // The blocked query sees the conversation as blocked while unresolved.
    expect(await collectPendingInputRequests(log)).toHaveLength(1);

    await log.addEvents([
      inputResolvedEvent({
        request_id: requested.request_id,
        resolution: "answered",
        answer: "us-east-1",
        resolved_by: "martin",
      }),
    ]);

    expect(await pending).toEqual({
      ok: true,
      requestId: requested.request_id,
      resolution: "answered",
      answer: "us-east-1",
      resolvedBy: "martin",
    });
    expect(await collectPendingInputRequests(log)).toHaveLength(0);
  });

  it("relays the prompt through the adapter and records the routing context", async () => {
    const log = new FakeEventLog();
    const { context, executedTools } = fakeContext(log);
    const tool = requestInputTool();

    const pending = execute(tool, context, {
      adapterId: "adapter-1",
      target: "channel-9",
    });
    await new Promise((resolve) => setTimeout(resolve, 10));
    const [requested] = log.requestedPayloads();
    expect(requested.adapter_id).toBe("adapter-1");
    expect(requested.target).toBe("channel-9");
    expect(executedTools).toEqual([
      {
        functionName: "send_adapter_message",
        arguments: {
          adapterId: "adapter-1",
          text: "Which region?",
          target: "channel-9",
          attachments: null,
        },
      },
    ]);

    await log.addEvents([
      inputResolvedEvent({
        request_id: requested.request_id,
        resolution: "answered",
        answer: "yes",
      }),
    ]);
    await pending;
  });

  it("expires the request when nobody answers", async () => {
    const log = new FakeEventLog();
    const { context } = fakeContext(log);
    const tool = requestInputTool();

    const result = (await execute(tool, context, {
      timeoutSeconds: 0.02,
    })) as JsonObject;

    expect(result.ok).toBe(false);
    expect(result.resolution).toBe("expired");
    expect(await collectPendingInputRequests(log)).toHaveLength(0);
  });

  it("cancels the request when the adapter relay fails", async () => {
    const log = new FakeEventLog();
    const { context } = fakeContext(log);
    (context as { executeTool: unknown }).executeTool = async () => {
      throw new Error("adapter is disabled");
    };
    const tool = requestInputTool();

    const result = (await execute(tool, context, {
      adapterId: "adapter-1",
    })) as JsonObject;

    expect(result.ok).toBe(false);
    expect(String(result.error)).toContain("adapter is disabled");
    expect(await collectPendingInputRequests(log)).toHaveLength(0);
  });
});
