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
  "authenticatedSender?": "string",
});

/**
 * Why a turn ended `failed`:
 * - `reactor_fatal`: an internal fault stopped the reactor.
 * - `doom_loop`: the reactor broke a turn that repeated an identical tool
 *   batch past the configured threshold. It is not an internal fault.
 * - `aborted`: an external abort stopped the reactor while the turn ran.
 * - `reactor_done`: the delivery arrived after the reactor finished.
 */
export const TurnFailureKind = type.enumerated(
  "reactor_fatal",
  "doom_loop",
  "aborted",
  "reactor_done",
);
export type TurnFailureKind = typeof TurnFailureKind.infer;

/**
 * The data of `reactor.turn.ended`. `joined` names the turn the delivery
 * continued, and `failed` carries the error.
 */
export const TurnEndedData = type.or(
  {
    turnId: "string",
    outcome: type.enumerated("replied", "unanswered"),
  },
  { turnId: "string", outcome: "'joined'", joinedTurnId: "string" },
  {
    turnId: "string",
    outcome: "'failed'",
    error: { message: "string", kind: TurnFailureKind },
  },
);
export type TurnEndedData =
  | { turnId: string; outcome: "replied" | "unanswered" }
  | { turnId: string; outcome: "joined"; joinedTurnId: string }
  | {
      turnId: string;
      outcome: "failed";
      error: { message: string; kind: TurnFailureKind };
    };

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
    data: { model: "string", "turnId?": "string" },
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
      "turnId?": "string",
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
    type: "'message.correlated'",
    seq: "number",
    data: { message: WireInboundMessage, correlationId: "string" },
  },
  {
    type: "'connector.reply'",
    seq: "number",
    data: {
      content: "string",
      "checkpointHash?": "string",
      "turnId?": "string",
    },
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
      "turnId?": "string",
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
    type: "'reactor.turn.started'",
    seq: "number",
    data: { turnId: "string", resumed: "boolean", "messageId?": "string" },
  },
  {
    type: "'reactor.turn.ended'",
    seq: "number",
    data: TurnEndedData,
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
  | {
      type: "inference.start";
      seq: number;
      data: { model: string; turnId?: string };
    }
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
        turnId?: string;
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
      type: "message.correlated";
      seq: number;
      data: { message: InboundMessage; correlationId: string };
    }
  | {
      type: "connector.reply";
      seq: number;
      data: { content: string; checkpointHash?: string; turnId?: string };
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
        turnId?: string;
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
  | {
      /**
       * The reactor starts work for a turn in this process: on the dequeue
       * of a new message (`resumed: false`, with the message's id when it
       * has one), or on the first resume of a parked turn (`resumed: true`).
       */
      type: "reactor.turn.started";
      seq: number;
      data: { turnId: string; resumed: boolean; messageId?: string };
    }
  | {
      /**
       * The turn ended. The reactor emits at most one of these for each
       * turn in a process. A turn can end with no `reactor.turn.started`
       * (INFERENCE.md § Turn Lifecycle).
       */
      type: "reactor.turn.ended";
      seq: number;
      data: TurnEndedData;
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
// `_persistedSuspendedCall` guard in runtime-core.
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

// The same drift guards for the `turnId` the reactor stamps on these events.
// Each pair errors under `tsc` if either mirror stops carrying the field.
type ArkEventData<T extends string> = Extract<
  typeof InferenceEvent.infer,
  { type: T }
>["data"];
type TSEventData<T extends InferenceEvent["type"]> = Extract<
  InferenceEvent,
  { type: T }
>["data"];

const _arkInferenceStartTurnId = (
  data: ArkEventData<"inference.start">,
): string | undefined => data.turnId;
void _arkInferenceStartTurnId;
const _tsInferenceStartTurnId = (
  data: TSEventData<"inference.start">,
): string | undefined => data.turnId;
void _tsInferenceStartTurnId;

const _arkInferenceDoneTurnId = (
  data: ArkEventData<"inference.done">,
): string | undefined => data.turnId;
void _arkInferenceDoneTurnId;
const _tsInferenceDoneTurnId = (
  data: TSEventData<"inference.done">,
): string | undefined => data.turnId;
void _tsInferenceDoneTurnId;

const _arkConnectorReplyTurnId = (
  data: ArkEventData<"connector.reply">,
): string | undefined => data.turnId;
void _arkConnectorReplyTurnId;
const _tsConnectorReplyTurnId = (
  data: TSEventData<"connector.reply">,
): string | undefined => data.turnId;
void _tsConnectorReplyTurnId;

const _arkGateBlockedTurnId = (
  data: ArkEventData<"reactor.gate.blocked">,
): string | undefined => data.turnId;
void _arkGateBlockedTurnId;
const _tsGateBlockedTurnId = (
  data: TSEventData<"reactor.gate.blocked">,
): string | undefined => data.turnId;
void _tsGateBlockedTurnId;

// The same drift guards for the turn events. A turn event carries its turn id
// as a required field, and each outcome of `reactor.turn.ended` carries its
// own required fields.
const _arkTurnStarted = (
  data: ArkEventData<"reactor.turn.started">,
): [string, boolean, string | undefined] => [
  data.turnId,
  data.resumed,
  data.messageId,
];
void _arkTurnStarted;
const _tsTurnStarted = (
  data: TSEventData<"reactor.turn.started">,
): [string, boolean, string | undefined] => [
  data.turnId,
  data.resumed,
  data.messageId,
];
void _tsTurnStarted;

const _arkTurnEndedJoined = (
  data: Extract<ArkEventData<"reactor.turn.ended">, { outcome: "joined" }>,
): string => data.joinedTurnId;
void _arkTurnEndedJoined;
const _tsTurnEndedJoined = (
  data: Extract<TSEventData<"reactor.turn.ended">, { outcome: "joined" }>,
): string => data.joinedTurnId;
void _tsTurnEndedJoined;

const _arkTurnEndedFailed = (
  data: Extract<ArkEventData<"reactor.turn.ended">, { outcome: "failed" }>,
): [string, TurnFailureKind] => [data.error.message, data.error.kind];
void _arkTurnEndedFailed;
const _tsTurnEndedFailed = (
  data: Extract<TSEventData<"reactor.turn.ended">, { outcome: "failed" }>,
): [string, TurnFailureKind] => [data.error.message, data.error.kind];
void _tsTurnEndedFailed;

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
