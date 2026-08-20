import type {
  Conversation,
  Event,
  EventData,
  JsonObject,
  JsonValue,
} from "./index";

// Shared helpers for the input-request event convention: a pair of custom
// events (`input_requested` / `input_resolved`) that mark a conversation as
// blocked on a human and record how the block was settled. See
// website/docs-src/concepts/input-requests.md for the specification. The
// event names are deliberately un-namespaced: they are candidates for the
// exoharness event vocabulary if the convention is adopted more widely, so
// promotion must be wire-compatible.

export const INPUT_REQUESTED_EVENT_TYPE = "input_requested";
export const INPUT_RESOLVED_EVENT_TYPE = "input_resolved";
export const INPUT_REQUEST_EVENT_TYPES = [
  INPUT_REQUESTED_EVENT_TYPE,
  INPUT_RESOLVED_EVENT_TYPE,
];

export const INPUT_REQUEST_KINDS = [
  "question",
  "tool_confirmation",
  "feedback",
  "auth",
] as const;

export type InputRequestKind = (typeof INPUT_REQUEST_KINDS)[number];

export const INPUT_RESOLUTIONS = ["answered", "cancelled", "expired"] as const;

export type InputResolution = (typeof INPUT_RESOLUTIONS)[number];

// Payload field names are snake_case: that is the wire format the spec
// defines, matching the exoharness event style these shapes may be promoted
// into. Both payloads are open — producers may attach additional fields, and
// consumers must ignore fields they do not recognize (the parsers below
// preserve extras).
export interface InputRequestedPayload {
  request_id: string;
  kind: InputRequestKind;
  prompt: string;
  answer_schema?: JsonValue | null;
  tool_call_id?: string | null;
  // Exo extension fields: set when the request was relayed to an external
  // channel, so the next reply from that channel can be routed back.
  adapter_id?: string | null;
  target?: string | null;
}

export interface InputResolvedPayload {
  request_id: string;
  resolution: InputResolution;
  answer?: JsonValue | null;
  resolved_by?: string | null;
}

export interface PendingInputRequest {
  eventId: string;
  requestedAt: string;
  payload: InputRequestedPayload;
}

export function inputRequestedEvent(payload: InputRequestedPayload): EventData {
  return {
    type: "custom",
    event_type: INPUT_REQUESTED_EVENT_TYPE,
    payload: withoutNullishFields(payload as unknown as JsonObject),
  };
}

export function inputResolvedEvent(payload: InputResolvedPayload): EventData {
  return {
    type: "custom",
    event_type: INPUT_RESOLVED_EVENT_TYPE,
    payload: withoutNullishFields(payload as unknown as JsonObject),
  };
}

export function parseInputRequested(
  data: EventData,
): InputRequestedPayload | null {
  const payload = customEventPayload(data, INPUT_REQUESTED_EVENT_TYPE);
  if (
    payload === null ||
    typeof payload.request_id !== "string" ||
    typeof payload.prompt !== "string" ||
    !(INPUT_REQUEST_KINDS as readonly string[]).includes(String(payload.kind))
  ) {
    return null;
  }
  return payload as unknown as InputRequestedPayload;
}

export function parseInputResolved(
  data: EventData,
): InputResolvedPayload | null {
  const payload = customEventPayload(data, INPUT_RESOLVED_EVENT_TYPE);
  if (
    payload === null ||
    typeof payload.request_id !== "string" ||
    !(INPUT_RESOLUTIONS as readonly string[]).includes(
      String(payload.resolution),
    )
  ) {
    return null;
  }
  return payload as unknown as InputResolvedPayload;
}

// The pending set over an event list: requests without a matching resolution,
// oldest first. Events must be in ascending order. The first resolution for a
// request_id wins; malformed payloads and resolutions for unknown requests
// are ignored.
export function pendingInputRequests(events: Event[]): PendingInputRequest[] {
  const pending = new Map<string, PendingInputRequest>();
  for (const event of events) {
    const requested = parseInputRequested(event.data);
    if (requested !== null && !pending.has(requested.request_id)) {
      pending.set(requested.request_id, {
        eventId: event.id,
        requestedAt: event.createdAt,
        payload: requested,
      });
      continue;
    }
    const resolved = parseInputResolved(event.data);
    if (resolved !== null) {
      pending.delete(resolved.request_id);
    }
  }
  return [...pending.values()];
}

// First resolution for a request in event order, or null while it is pending.
export function findInputResolution(
  events: Event[],
  requestId: string,
): InputResolvedPayload | null {
  for (const event of events) {
    const resolved = parseInputResolved(event.data);
    if (resolved !== null && resolved.request_id === requestId) {
      return resolved;
    }
  }
  return null;
}

const PENDING_SCAN_PAGE_LIMIT = 200;

// The pending set for a whole conversation, via a cursor scan filtered to the
// two convention kinds so per-turn traffic is never paged through.
export async function collectPendingInputRequests(
  conversation: Pick<Conversation, "getEvents">,
): Promise<PendingInputRequest[]> {
  const events: Event[] = [];
  let cursor: string | null = null;
  for (;;) {
    const result = await conversation.getEvents({
      cursor,
      direction: "asc",
      limit: PENDING_SCAN_PAGE_LIMIT,
      types: INPUT_REQUEST_EVENT_TYPES,
    });
    events.push(...result.events);
    if (result.events.length === 0 || !result.cursor) {
      break;
    }
    cursor = result.cursor;
  }
  return pendingInputRequests(events);
}

function customEventPayload(
  data: EventData,
  eventType: string,
): JsonObject | null {
  if (data.type !== "custom" || data.event_type !== eventType) {
    return null;
  }
  const payload = (data as { payload?: unknown }).payload;
  return isRecord(payload) ? (payload as JsonObject) : null;
}

function withoutNullishFields(payload: JsonObject): JsonObject {
  const compact: JsonObject = {};
  for (const [key, value] of Object.entries(payload)) {
    if (value !== null && value !== undefined) {
      compact[key] = value;
    }
  }
  return compact;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
