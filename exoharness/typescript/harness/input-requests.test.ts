import { describe, expect, it } from "vitest";

import {
  collectPendingInputRequests,
  findInputResolution,
  inputRequestedEvent,
  inputResolvedEvent,
  parseInputRequested,
  parseInputResolved,
  pendingInputRequests,
  type Event,
  type EventData,
  type EventQuery,
  type GetEventsResult,
} from "./index";

function event(id: string, data: EventData): Event {
  return {
    id,
    conversationId: "conversation-1",
    createdAt: `2026-08-20T12:00:0${id.length % 10}Z`,
    data,
  };
}

describe("input request events", () => {
  it("builds and parses input_requested payloads, dropping nullish fields", () => {
    const data = inputRequestedEvent({
      request_id: "req-1",
      kind: "question",
      prompt: "Which region?",
      answer_schema: null,
      tool_call_id: "call-1",
      adapter_id: null,
      target: null,
    });

    expect(data).toEqual({
      type: "custom",
      event_type: "input_requested",
      payload: {
        request_id: "req-1",
        kind: "question",
        prompt: "Which region?",
        tool_call_id: "call-1",
      },
    });
    expect(parseInputRequested(data)).toEqual({
      request_id: "req-1",
      kind: "question",
      prompt: "Which region?",
      tool_call_id: "call-1",
    });
  });

  it("builds and parses input_resolved payloads", () => {
    const data = inputResolvedEvent({
      request_id: "req-1",
      resolution: "answered",
      answer: "us-east-1",
      resolved_by: "martin",
    });

    expect(parseInputResolved(data)).toEqual({
      request_id: "req-1",
      resolution: "answered",
      answer: "us-east-1",
      resolved_by: "martin",
    });
  });

  it("preserves unrecognized extension fields when parsing", () => {
    const parsed = parseInputRequested({
      type: "custom",
      event_type: "input_requested",
      payload: {
        request_id: "req-1",
        kind: "auth",
        prompt: "Paste the token",
        adapter_id: "adapter-1",
        target: "channel-9",
      },
    });

    expect(parsed?.adapter_id).toBe("adapter-1");
    expect(parsed?.target).toBe("channel-9");
  });

  it("rejects other event types and malformed payloads", () => {
    expect(parseInputRequested({ type: "messages" })).toBeNull();
    expect(
      parseInputRequested({
        type: "custom",
        event_type: "input_requested",
        payload: { request_id: "req-1", kind: "riddle", prompt: "?" },
      }),
    ).toBeNull();
    expect(
      parseInputResolved({
        type: "custom",
        event_type: "input_resolved",
        payload: { request_id: "req-1", resolution: "maybe" },
      }),
    ).toBeNull();
  });
});

describe("pendingInputRequests", () => {
  const requested = (id: string, prompt = "?") =>
    inputRequestedEvent({ request_id: id, kind: "question", prompt });
  const resolved = (id: string) =>
    inputResolvedEvent({ request_id: id, resolution: "answered" });

  it("lists unresolved requests oldest first and drops resolved ones", () => {
    const pending = pendingInputRequests([
      event("e1", requested("req-1", "first")),
      event("e2", requested("req-2", "second")),
      event("e3", resolved("req-1")),
    ]);

    expect(pending).toHaveLength(1);
    expect(pending[0].eventId).toBe("e2");
    expect(pending[0].payload.prompt).toBe("second");
  });

  it("ignores resolutions without a matching request and duplicate requests", () => {
    const pending = pendingInputRequests([
      event("e1", resolved("req-unknown")),
      event("e2", requested("req-1", "original")),
      event("e3", requested("req-1", "duplicate")),
    ]);

    expect(pending).toHaveLength(1);
    expect(pending[0].payload.prompt).toBe("original");
  });

  it("finds the first resolution for a request", () => {
    const events = [
      event("e1", requested("req-1")),
      event("e2", resolved("req-1")),
      event(
        "e3",
        inputResolvedEvent({ request_id: "req-1", resolution: "expired" }),
      ),
    ];

    expect(findInputResolution(events, "req-1")?.resolution).toBe("answered");
    expect(findInputResolution(events, "req-2")).toBeNull();
  });
});

describe("collectPendingInputRequests", () => {
  it("pages through the event log with a cursor", async () => {
    const pages: GetEventsResult[] = [
      {
        events: [
          event(
            "e1",
            inputRequestedEvent({
              request_id: "req-1",
              kind: "question",
              prompt: "?",
            }),
          ),
        ],
        cursor: "e1",
      },
      {
        events: [
          event(
            "e2",
            inputResolvedEvent({ request_id: "req-1", resolution: "answered" }),
          ),
          event(
            "e3",
            inputRequestedEvent({
              request_id: "req-2",
              kind: "feedback",
              prompt: "How did that land?",
            }),
          ),
        ],
        cursor: "e3",
      },
      { events: [], cursor: null },
    ];
    const queries: EventQuery[] = [];
    const conversation = {
      getEvents: async (query?: EventQuery) => {
        queries.push(query ?? {});
        return pages[queries.length - 1];
      },
    };

    const pending = await collectPendingInputRequests(conversation);

    expect(pending).toHaveLength(1);
    expect(pending[0].payload.request_id).toBe("req-2");
    expect(queries).toHaveLength(3);
    expect(queries[0].types).toEqual(["input_requested", "input_resolved"]);
    expect(queries[0].cursor).toBeNull();
    expect(queries[1].cursor).toBe("e1");
    expect(queries[2].cursor).toBe("e3");
  });
});
