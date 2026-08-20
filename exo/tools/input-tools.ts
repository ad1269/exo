import { randomUUID } from "node:crypto";

import {
  findInputResolution,
  INPUT_REQUEST_KINDS,
  INPUT_RESOLVED_EVENT_TYPE,
  inputRequestedEvent,
  inputResolvedEvent,
  type Event,
  type HarnessToolRegistry,
  type InputRequestKind,
  type InputResolvedPayload,
  type JsonObject,
  type JsonValue,
  type ToolInstance,
  type ToolResult,
} from "@exo/harness";

// Reference mechanism for the input-request convention (see
// website/docs-src/concepts/input-requests.md): a blocked tool call. The
// request_input tool appends `input_requested` to the conversation event log,
// optionally relays the prompt through an adapter, then polls the log until a
// matching `input_resolved` arrives and returns the answer as the tool
// result. The envelope is the convention; this tool is just one way to
// produce and consume it.

const DEFAULT_TIMEOUT_SECONDS = 600;
const MAX_TIMEOUT_SECONDS = 3600;
const DEFAULT_POLL_INTERVAL_MS = 2_000;

export interface InputToolOptions {
  // Test seam: how often the blocked tool re-reads the event log.
  pollIntervalMs?: number;
}

function requestInputTool(options: InputToolOptions): ToolInstance {
  const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
  return {
    source: "built_in",
    definition: {
      name: "request_input",
      description:
        "Ask a human for input and block until they answer. Emits an input_requested event, waits for the matching input_resolved event, and returns the answer. Use this only when you are blocked on something a human must decide or provide; keep working yourself otherwise. When the need arose from an adapter message, pass the adapterId and target from the inbound wakeup so the question is posted to that channel and the next reply from it resolves the request. Without an adapter the request waits for a resolution appended by another client (CLI, REPL). Times out with resolution 'expired' if nobody answers.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          prompt: {
            type: "string",
            description: "The question or request to show the human.",
          },
          kind: {
            type: ["string", "null"],
            enum: [...INPUT_REQUEST_KINDS, null],
            description:
              "What is being asked for: question, tool_confirmation, feedback, or auth. Null defaults to question.",
          },
          answerSchema: {
            type: ["object", "null"],
            additionalProperties: true,
            properties: {},
            description:
              "Optional JSON Schema for the expected answer. Null for free text.",
          },
          adapterId: {
            type: ["string", "null"],
            description:
              "Adapter to relay the prompt through, from the inbound wakeup or list_adapters. Null to wait without relaying.",
          },
          target: {
            type: ["string", "null"],
            description:
              "External destination for the relayed prompt, e.g. the Discord channel id from the inbound wakeup. Null to use the adapter default; only replies are then matched by adapter, not by channel.",
          },
          timeoutSeconds: {
            type: ["number", "null"],
            description: `Seconds to wait before the request expires. Null for the default (${DEFAULT_TIMEOUT_SECONDS}).`,
          },
        },
        required: [
          "prompt",
          "kind",
          "answerSchema",
          "adapterId",
          "target",
          "timeoutSeconds",
        ],
      },
    },
    handler: {
      async execute(args: JsonObject, execution): Promise<ToolResult> {
        const prompt = args.prompt;
        if (typeof prompt !== "string" || prompt.trim().length === 0) {
          return { ok: false, error: "prompt must be a non-empty string" };
        }
        const kind = parseKind(args.kind);
        if (kind === null) {
          return {
            ok: false,
            error: `kind must be null or one of ${INPUT_REQUEST_KINDS.join(", ")}`,
          };
        }
        const adapterId =
          typeof args.adapterId === "string" ? args.adapterId : null;
        const target = typeof args.target === "string" ? args.target : null;
        const timeoutMs =
          1_000 *
          Math.min(
            typeof args.timeoutSeconds === "number" && args.timeoutSeconds > 0
              ? args.timeoutSeconds
              : DEFAULT_TIMEOUT_SECONDS,
            MAX_TIMEOUT_SECONDS,
          );

        const { conversation, turn } = execution.context.exoharness.current;
        const requestId = randomUUID();
        const appended = await turn.addEvents([
          inputRequestedEvent({
            request_id: requestId,
            kind,
            prompt,
            answer_schema: (args.answerSchema ?? null) as JsonValue | null,
            tool_call_id: execution.toolCallId ?? null,
            adapter_id: adapterId,
            target,
          }),
        ]);

        if (adapterId !== null) {
          try {
            await execution.context.executeTool({
              functionName: "send_adapter_message",
              arguments: {
                adapterId,
                text: prompt,
                target,
                attachments: null,
              },
            });
          } catch (error) {
            // The request cannot be answered if nobody saw it; settle it so
            // the conversation does not stay listed as blocked.
            await turn.addEvents([
              inputResolvedEvent({
                request_id: requestId,
                resolution: "cancelled",
                resolved_by: "request_input:relay_failed",
              }),
            ]);
            return {
              ok: false,
              requestId,
              error: `failed to relay prompt through adapter ${adapterId}: ${errorMessage(error)}`,
            };
          }
        }

        // Poll the event log for the resolution. Resolutions are appended by
        // the adapter runner (external replies) or any other client via
        // addEvents; appends are safe alongside this blocked turn.
        const deadline = Date.now() + timeoutMs;
        let cursor: string | null = appended.latestEventId;
        for (;;) {
          const result = await conversation.getEvents({
            cursor,
            direction: "asc",
            types: [INPUT_RESOLVED_EVENT_TYPE],
          });
          const resolution = findInputResolution(
            result.events as Event[],
            requestId,
          );
          if (resolution !== null) {
            return resolutionResult(requestId, resolution);
          }
          if (result.events.length > 0 && result.cursor) {
            cursor = result.cursor;
          }
          const remainingMs = deadline - Date.now();
          if (remainingMs <= 0) {
            break;
          }
          await sleep(Math.min(pollIntervalMs, remainingMs));
        }

        await turn.addEvents([
          inputResolvedEvent({
            request_id: requestId,
            resolution: "expired",
            resolved_by: "request_input:timeout",
          }),
        ]);
        return {
          ok: false,
          requestId,
          resolution: "expired",
          error: `no input_resolved arrived within ${Math.round(timeoutMs / 1_000)}s`,
        };
      },
    },
  };
}

function resolutionResult(
  requestId: string,
  resolution: InputResolvedPayload,
): ToolResult {
  return {
    ok: resolution.resolution === "answered",
    requestId,
    resolution: resolution.resolution,
    answer: resolution.answer ?? null,
    resolvedBy: resolution.resolved_by ?? null,
  };
}

function parseKind(value: unknown): InputRequestKind | null {
  if (value === null || value === undefined) {
    return "question";
  }
  return (INPUT_REQUEST_KINDS as readonly unknown[]).includes(value)
    ? (value as InputRequestKind)
    : null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function createInputToolInstances(
  options: InputToolOptions = {},
): ToolInstance[] {
  return [requestInputTool(options)];
}

export function registerInputTools(
  registry: HarnessToolRegistry,
  options: InputToolOptions = {},
): void {
  for (const tool of createInputToolInstances(options)) {
    registry.register(tool);
  }
}
