import { type } from "arktype";
import {
  PartialMessage,
  RedactedThinkingBlock,
  TokenUsage,
  LastCycleSource,
  AssistantTurn,
  InferenceError,
  CitationBlock,
  SafetyRatingBlock,
  CodeExecutionRequestBlock,
  CodeExecutionResultBlock,
  ImageBlock,
  ToolCall,
  ToolResult,
  GateType,
  ApprovalSnapshot,
  ForkMode,
  type InboundMessage,
} from "./runtime-core";

// ---------------------------------------------------------------------------
// Inference Event Protocol (INFERENCE.md § Event Protocol)
// ---------------------------------------------------------------------------

/**
 * Wire-safe representation of InboundMessage for use in InferenceEvent
 * variants. The runtime InboundMessage type contains Uint8Array fields
 * (MessageAttachment.data) that cannot survive JSON serialization, so the
 * wire validator uses `unknown` for attachment data and accepts whatever
 * JSON.parse produces.
 */
const WireInboundMessage = type({
  ref: { uid: "number", mailbox: "string" },
  headers: "Record<string, unknown>",
  flags: "string[]",
  "content?": "string",
  "payload?": "object",
  "attachments?": "unknown[]",
  signatureStatus: type.enumerated("valid", "invalid", "unknown", "missing"),
});

/**
 * A single event in the inference event protocol. Every event carries a
 * monotonic session-scoped sequence number.
 *
 * Event types are namespaced: `inference.*`, `tool.*`, `reactor.*`,
 * `fork.*`, `message.*`, `custom.*`.
 *
 * (INFERENCE.md § Event Protocol)
 */
export const InferenceEvent = type.or(
  {
    type: "'inference.start'",
    seq: "number",
    data: { model: "string" },
  },
  {
    type: "'inference.thinking.delta'",
    seq: "number",
    data: {
      token: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.block.signature'",
    seq: "number",
    data: { signature: "string", "index?": "number" },
  },
  {
    type: "'inference.thinking.redacted'",
    seq: "number",
    data: { redactedThinking: RedactedThinkingBlock, "index?": "number" },
  },
  {
    type: "'inference.text.delta'",
    seq: "number",
    data: {
      token: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.refusal.delta'",
    seq: "number",
    data: {
      token: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.tool_call.start'",
    seq: "number",
    data: {
      callId: "string",
      name: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.tool_call.delta'",
    seq: "number",
    data: {
      callId: "string",
      argumentFragment: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.tool_call.end'",
    seq: "number",
    data: {
      callId: "string",
      name: "string",
      arguments: "Record<string, unknown>",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.usage'",
    seq: "number",
    data: { usage: TokenUsage, source: LastCycleSource },
  },
  {
    type: "'inference.done'",
    seq: "number",
    data: {
      turn: AssistantTurn,
      usage: TokenUsage,
      source: LastCycleSource,
      "pacingDelayMs?": "number",
    },
  },
  {
    type: "'inference.error'",
    seq: "number",
    data: { error: InferenceError, partial: PartialMessage },
  },
  {
    type: "'inference.retry'",
    seq: "number",
    data: {
      attempt: "number",
      delayMs: "number",
      previousError: InferenceError,
    },
  },
  {
    type: "'inference.citation'",
    seq: "number",
    // `index`, when present, names the source content block (typically
    // a TextBlock) the citation annotates. The harness uses it to
    // interleave the citation into the finalized turn's `content[]`
    // immediately after the matching block. Adapters whose wire
    // protocol does not carry per-citation block indices omit the
    // field; the harness then appends those citations at the end of
    // `content[]` and consumers attribute them to the nearest
    // preceding TextBlock per the CitationBlock docstring.
    data: { citation: CitationBlock, "index?": "number" },
  },
  {
    type: "'inference.safety_rating'",
    seq: "number",
    // Prompt-level structured safety signal (observed Gemini
    // `promptFeedback.blockReason`). No candidate index: the first
    // capture has zero candidates. Harness appends the block to the
    // finalized turn's `content[]`.
    data: { safetyRating: SafetyRatingBlock },
  },
  {
    type: "'inference.code_execution.start'",
    seq: "number",
    data: { request: CodeExecutionRequestBlock, "index?": "number" },
  },
  {
    type: "'inference.code_execution.delta'",
    seq: "number",
    // requestId correlates fragments back to the originating
    // CodeExecutionRequestBlock; index is the positional hint into
    // the response's content-block stream. They are independent: a
    // single response may stream code execution for multiple
    // requests interleaved, distinguished by requestId; index lets
    // the harness's per-block accumulator route the fragment to
    // the correct block when the array isn't yet finalized.
    data: {
      requestId: "string",
      codeFragment: "string",
      "index?": "number",
    },
  },
  {
    type: "'inference.code_execution.result'",
    seq: "number",
    data: { result: CodeExecutionResultBlock, "index?": "number" },
  },
  {
    type: "'inference.image_output'",
    seq: "number",
    // Fires mid-stream when an adapter finalizes an image-output
    // block, signaling that the image is ready for downstream
    // handoff before the full inference.done lands. The wrapped
    // ImageBlock typically carries a base64 MediaSource — the
    // payload can be large (Gemini's image-output captures show
    // ~1MB inline blobs); consumers that subscribe to this event
    // should treat it as a non-trivial transport size.
    data: { image: ImageBlock, "index?": "number" },
  },
  {
    type: "'tool.start'",
    seq: "number",
    data: { call: ToolCall },
  },
  {
    type: "'tool.update'",
    seq: "number",
    data: { callId: "string", partial: "string" },
  },
  {
    type: "'tool.done'",
    seq: "number",
    data: { result: ToolResult },
  },
  {
    type: "'message.queued'",
    seq: "number",
    data: { message: WireInboundMessage },
  },
  {
    type: "'message.run.started'",
    seq: "number",
    data: {
      "messageId?": "string",
      messageRunId: "string",
      receivedAt: "number",
    },
  },
  {
    type: "'message.run.ended'",
    seq: "number",
    data: {
      messageRunId: "string",
      "messageId?": "string",
      status: type.enumerated("completed", "failed"),
      "error?": {
        message: "string",
        "kind?": "string",
      },
    },
  },
  {
    type: "'message.correlated'",
    seq: "number",
    data: { message: WireInboundMessage, correlationId: "string" },
  },
  {
    type: "'connector.reply'",
    seq: "number",
    data: { content: "string", "checkpointHash?": "string" },
  },
  {
    type: "'reactor.start'",
    seq: "number",
    data: "object",
  },
  {
    type: "'reactor.gate.blocked'",
    seq: "number",
    data: {
      reason: GateType,
      gateId: "string",
      "correlationId?": "string",
      "approvalSnapshot?": ApprovalSnapshot,
    },
  },
  {
    type: "'reactor.gate.cleared'",
    seq: "number",
    data: {
      gateId: "string",
      reason: type.enumerated("resolved", "timeout", "shutdown"),
    },
  },
  {
    type: "'reactor.done'",
    seq: "number",
    data: "object",
  },
  {
    type: "'reactor.error'",
    seq: "number",
    data: { error: "string", fatal: "boolean" },
  },
  {
    type: "'fork.created'",
    seq: "number",
    data: { forkId: "string", parentId: "string", mode: ForkMode },
  },
  {
    type: "'fork.done'",
    seq: "number",
    data: { forkId: "string", "result?": "unknown" },
  },
  {
    type: "'fork.error'",
    seq: "number",
    data: { forkId: "string", error: "string" },
  },
  {
    type: "'fork.aborted'",
    seq: "number",
    data: { forkId: "string" },
  },
  {
    type: /^custom\./,
    seq: "number",
    data: "Record<string, unknown>",
  },
);
// The TypeScript type is defined manually rather than inferred from the
// validator because the `custom.*` variant uses a regex pattern which
// arktype infers as `string`. A bare `string` in the discriminant position
// prevents TypeScript from narrowing the union in switch statements.
// The manually defined type uses a `custom.${string}` template literal
// for that variant, preserving the narrowing behavior downstream code
// relies on.
export type InferenceEvent =
  | { type: "inference.start"; seq: number; data: { model: string } }
  | {
      type: "inference.thinking.delta";
      seq: number;
      data: { token: string; partial: PartialMessage; index?: number };
    }
  | {
      type: "inference.block.signature";
      seq: number;
      data: { signature: string; index?: number };
    }
  | {
      type: "inference.thinking.redacted";
      seq: number;
      data: { redactedThinking: RedactedThinkingBlock; index?: number };
    }
  | {
      type: "inference.text.delta";
      seq: number;
      data: { token: string; partial: PartialMessage; index?: number };
    }
  | {
      type: "inference.refusal.delta";
      seq: number;
      data: { token: string; partial: PartialMessage; index?: number };
    }
  | {
      type: "inference.tool_call.start";
      seq: number;
      data: {
        callId: string;
        name: string;
        partial: PartialMessage;
        index?: number;
      };
    }
  | {
      type: "inference.tool_call.delta";
      seq: number;
      data: {
        callId: string;
        argumentFragment: string;
        partial: PartialMessage;
        index?: number;
      };
    }
  | {
      type: "inference.tool_call.end";
      seq: number;
      data: {
        callId: string;
        name: string;
        arguments: Record<string, unknown>;
        partial: PartialMessage;
        index?: number;
      };
    }
  | {
      type: "inference.usage";
      seq: number;
      data: { usage: TokenUsage; source: LastCycleSource };
    }
  | {
      type: "inference.done";
      seq: number;
      data: {
        turn: AssistantTurn;
        usage: TokenUsage;
        source: LastCycleSource;
        pacingDelayMs?: number;
      };
    }
  | {
      type: "inference.error";
      seq: number;
      data: { error: InferenceError; partial: PartialMessage };
    }
  | {
      /**
       * Emitted between attempts when the per-call retry policy decides
       * to retry after an error. `attempt` is the 1-indexed number of
       * the attempt that just **failed** — the same value the policy
       * saw on its `RetrySituation.attempt` reading. `delayMs` is the
       * delay the wrapper will apply before the next attempt starts;
       * `previousError` carries the classified error that triggered
       * the retry. The event is not emitted when the policy aborts.
       */
      type: "inference.retry";
      seq: number;
      data: {
        attempt: number;
        delayMs: number;
        previousError: InferenceError;
      };
    }
  | {
      type: "inference.citation";
      seq: number;
      data: { citation: CitationBlock; index?: number };
    }
  | {
      type: "inference.safety_rating";
      seq: number;
      data: { safetyRating: SafetyRatingBlock };
    }
  | {
      type: "inference.code_execution.start";
      seq: number;
      data: { request: CodeExecutionRequestBlock; index?: number };
    }
  | {
      type: "inference.code_execution.delta";
      seq: number;
      data: { requestId: string; codeFragment: string; index?: number };
    }
  | {
      type: "inference.code_execution.result";
      seq: number;
      data: { result: CodeExecutionResultBlock; index?: number };
    }
  | {
      type: "inference.image_output";
      seq: number;
      data: { image: ImageBlock; index?: number };
    }
  | { type: "tool.start"; seq: number; data: { call: ToolCall } }
  | {
      type: "tool.update";
      seq: number;
      data: { callId: string; partial: string };
    }
  | { type: "tool.done"; seq: number; data: { result: ToolResult } }
  | {
      type: "message.queued";
      seq: number;
      data: { message: InboundMessage };
    }
  | {
      /**
       * Per-message run-bracket open. Emitted by the reactor when it
       * dequeues an inbound mail message and begins per-message work.
       *
       * `messageRunId` is reactor-minted, unique per dequeue. It is
       * non-negotiable for crash-replay correlation: the reactor can
       * legitimately dequeue the same `messageId` more than once across
       * a crash + replay cycle, so two bracket-open events with the
       * same `messageId` and no run-id cannot be unambiguously paired
       * with their `message.run.ended` counterparts.
       */
      type: "message.run.started";
      seq: number;
      data: {
        messageId?: string;
        messageRunId: string;
        receivedAt: number;
      };
    }
  | {
      /**
       * Per-message run-bracket close. Pairs with `message.run.started`
       * by `messageRunId`. `messageId` is carried redundantly so log
       * readers can correlate without a join against the open event; it is
       * absent for a message that named no id of its own.
       *
       * The `status` enum is `"completed" | "failed"` only.
       * Cancellation lives in the workflow-runtime's
       * `CancelRequested` -> `RunFailed` vocabulary, not on the
       * reactor's bracket: the reactor does not run a state machine
       * and what it observes when cancellation arrives is a harness
       * abort, which is structurally `"failed"` with a specific
       * `error.kind`.
       *
       * `error.kind` is documented as one of
       * `"inference_error" | "tool_error" | "reactor_fatal" |
       * "harness_aborted" | "doom_loop"` initially, extensible as new
       * failure categories surface. `"doom_loop"` marks a protective
       * break the reactor took on the agent's behalf when the agent
       * repeated an identical tool batch past the configured threshold;
       * unlike `"reactor_fatal"` it is not an internal fault.
       */
      type: "message.run.ended";
      seq: number;
      data: {
        messageRunId: string;
        messageId?: string;
        status: "completed" | "failed";
        error?: {
          message: string;
          kind?: string;
        };
      };
    }
  | {
      type: "message.correlated";
      seq: number;
      data: { message: InboundMessage; correlationId: string };
    }
  | {
      type: "connector.reply";
      seq: number;
      data: { content: string; checkpointHash?: string };
    }
  | { type: "reactor.start"; seq: number; data: Record<string, never> }
  | {
      type: "reactor.gate.blocked";
      seq: number;
      data: {
        reason: GateType;
        gateId: string;
        correlationId?: string;
        approvalSnapshot?: ApprovalSnapshot;
      };
    }
  | {
      type: "reactor.gate.cleared";
      seq: number;
      data: {
        gateId: string;
        reason: "resolved" | "timeout" | "shutdown";
      };
    }
  | { type: "reactor.done"; seq: number; data: Record<string, never> }
  | {
      type: "reactor.error";
      seq: number;
      data: { error: string; fatal: boolean };
    }
  | {
      type: "fork.created";
      seq: number;
      data: { forkId: string; parentId: string; mode: ForkMode };
    }
  | {
      type: "fork.done";
      seq: number;
      data: { forkId: string; result?: unknown };
    }
  | {
      type: "fork.error";
      seq: number;
      data: { forkId: string; error: string };
    }
  | { type: "fork.aborted"; seq: number; data: { forkId: string } }
  | {
      type: `custom.${string}`;
      seq: number;
      data: Record<string, unknown>;
    };

// Load-bearing drift guards for the dual-maintained `reactor.gate.blocked`
// event. The arktype `InferenceEvent` validator and the hand-written
// `InferenceEvent` type are kept in lockstep by hand (the `custom.*` regex
// variant forces the manual mirror). arktype passes undeclared keys through at
// runtime, so a schema that dropped `approvalSnapshot` would not fail at
// runtime. Projecting the field off each inferred shape makes it load-bearing:
// `tsc` errors if either mirror stops carrying it, mirroring the
// `_persistedSuspendedCall` guard in storage-isogit.
const _arkGateBlockedApprovalSnapshot = (
  data: Extract<
    typeof InferenceEvent.infer,
    { type: "reactor.gate.blocked" }
  >["data"],
): ApprovalSnapshot | undefined => data.approvalSnapshot;
void _arkGateBlockedApprovalSnapshot;

const _tsGateBlockedApprovalSnapshot = (
  data: Extract<InferenceEvent, { type: "reactor.gate.blocked" }>["data"],
): ApprovalSnapshot | undefined => data.approvalSnapshot;
void _tsGateBlockedApprovalSnapshot;

/**
 * Validate unknown data as an InferenceEvent. ArkType's regex-based validator
 * infers `custom.*` event types as `string`, but the manual InferenceEvent type
 * uses a `custom.${string}` template literal for switch narrowing. This function
 * centralizes that single unavoidable cast.
 */
export function parseInferenceEvent(
  data: unknown,
): InferenceEvent | type.errors {
  const result = InferenceEvent(data);
  if (result instanceof type.errors) return result;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- arktype regex infers as string; manual type uses template literal
  return result as InferenceEvent;
}
