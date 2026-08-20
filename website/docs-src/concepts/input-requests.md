---
title: Input Requests
description: A shared event convention for turns that block on human input.
---

# Input Requests

A convention for one recurring situation: the agent needs something only a
human can provide — an answer, a confirmation, a credential — and the turn
blocks until it arrives. Today that state exists only as harness-private
custom events, so a client must know each harness's private vocabulary to
render the question, answer it, or even tell that a conversation is blocked.

This page specifies a shared *envelope* for that state as two custom event
types. The *mechanism* that produces and consumes them (how a harness blocks,
how an adapter relays the question, how a reply is routed back) stays
harness-side and is deliberately unspecified.

## The events

Both are ordinary `custom` events (`EventData::Custom` on the Rust side, a
`{ type: "custom", event_type, payload }` record on the wire). No substrate
changes are involved.

### `input_requested`

Emitted when the agent starts waiting on a human.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `request_id` | string | yes | Unique id for this request; the join key for its resolution. |
| `kind` | `"question" \| "tool_confirmation" \| "feedback" \| "auth"` | yes | What is being asked for. |
| `prompt` | string | yes | Human-readable text of the request. |
| `answer_schema` | JSON Schema | no | Expected shape of the answer, when the requester wants more than free text. |
| `tool_call_id` | string | no | The blocked tool call, when the request is implemented as one. |

### `input_resolved`

Emitted when the request is settled — by a human answer, a cancellation, or a
timeout.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `request_id` | string | yes | The request being resolved. |
| `resolution` | `"answered" \| "cancelled" \| "expired"` | yes | How it was settled. |
| `answer` | any | no | The answer, for `resolution: "answered"`. Should match `answer_schema` when one was given. |
| `resolved_by` | string | no | Who or what settled it (a user handle, an adapter, a timeout). |

## Matching rule

A resolution matches the request with the same `request_id`. A conversation's
**pending set** is its `input_requested` events without a matching
`input_resolved`; a conversation with a non-empty pending set is *blocked on a
human*. If multiple resolutions carry the same `request_id`, the first in
event order wins and later ones are ignored.

Both payloads are open: producers may attach additional harness-specific
fields, and consumers must ignore fields they do not recognize. Exo's
reference implementation attaches `adapter_id` and `target` to
`input_requested` when the question was relayed to an external channel, so
the next reply from that channel can be routed back as the resolution.

## Envelope vs. mechanism

The split matters once harnesses are pluggable. The runtime, CLI, schedulers,
and UI clients all need to answer "is this conversation waiting on a human,
and for what?" without knowing which harness is plugged in. That is the
envelope, and it is all this convention standardizes. How a given harness
blocks (Exo uses a blocked tool call: a `request_input` tool emits
`input_requested` and polls the event log for the resolution), how the
question reaches a person, and how their reply comes back are mechanism, and
each harness is free to do those differently.

Shared helpers live in `@exo/harness` (`input-requests.ts`): typed payloads,
event builders, and pending-set computation over an event list or via a
cursor scan, so consumers share one implementation instead of re-deriving the
shape.

## Relationship to the "namespace your custom events" guidance

The [data model](./data-model#event) says custom event types should be
namespaced. These two are deliberately not: the tag names are chosen as if
already standard, so that if multiple harnesses adopt the convention, the
payloads can be promoted into the exoharness `EventData` vocabulary without a
wire migration. Until then they are exactly what they look like — custom
events that any harness may emit, or ignore.

Harness-*private* bookkeeping around them stays namespaced as usual (for
example, Exo's attention scheduler records its reminder nudges as
`exo.input_request_nudged`).

## Reference implementation in Exo

This proposal ships the envelope plus one working mechanism end to end:

- a `request_input` tool that emits `input_requested`, blocks the turn, and
  returns the answer when `input_resolved` arrives (or expires it on
  timeout);
- a Discord path: a relayed request is posted to the bound channel, and the
  next reply from that channel is converted into `input_resolved`;
- `exo conversation blocked <agent>`, which lists conversations by their
  pending set — purely a client-side read over the event log;
- an attention pass in the scheduler that re-nudges aged requests and
  surfaces blocked conversations first.
