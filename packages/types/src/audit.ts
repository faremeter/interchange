import { type } from "arktype";

import { MatchedGrant, grantEffects } from "./grants";

const Effect = type.enumerated(...grantEffects);

export const AuditAuthz = type({
  effect: Effect.or("null").describe(
    "The authorization outcome the runtime resolved for this tool call: `allow`, `deny`, or `ask`, or `null` when no grant matched.",
  ),
  "resolvedBy?": MatchedGrant.or("null").describe(
    "The single grant whose effect determined the outcome (the most specific match), or `null` when nothing matched.",
  ),
  matchingGrants: MatchedGrant.array(),
  blocked: type("boolean").describe(
    "True when the runtime prevented the tool call from executing because authorization did not resolve to `allow`.",
  ),
  "blockReason?": "string",
});
export type AuditAuthz = typeof AuditAuthz.infer;

export const AuditRecord = type({
  callId: "string",
  tool: "string",
  arguments: "Record<string, unknown>",
  authz: AuditAuthz.or("null"),
  result: type({
    content: "string | Record<string, unknown>",
    isError: "boolean",
  }),
  timestamp: "string",
  sessionId: "string",
  // Monotonic sequence number from the reactor's tool.done event.
  // Supplied by the caller; the reactor owns the sequence.
  seq: "number.integer >= 0",
});
export type AuditRecord = typeof AuditRecord.infer;

export const ErrorRecord = type({
  source: "'inference' | 'reactor'",
  category: "string",
  message: "string",
  "statusCode?": "number.integer",
  fatal: "boolean",
  timestamp: "string",
  sessionId: "string",
  seq: "number.integer >= 0",
});
export type ErrorRecord = typeof ErrorRecord.infer;
