// Tool-consumer condition evaluator: restricts a grant to a specific
// credential consumer via { tool: "tool:<handle>" }.
//
// An empty consumer fails closed -- checked before the equality test so a
// grant value of "" cannot match an empty consumer. Non-string values
// throw (fail-loud), like the other evaluators.

import type { ConditionEvaluator, ConditionRegistry } from "./types";

/**
 * Consumer identity for a tool package, stamped onto a `credential:{id}` /
 * `use` grant's `{ tool }` condition at launch and supplied as `ctx.consumer`
 * by the runtime gate. Launch and gate must derive it identically, so both
 * import this builder. Package-level: every tool in a package shares one
 * identity.
 */
export function toolConsumer(packageName: string): string {
  return `tool:${packageName}`;
}

/**
 * Condition evaluator for credential consumers. Register as `tool`.
 */
export const toolConsumerEvaluator: ConditionEvaluator = (
  value: unknown,
  ctx,
): boolean => {
  if (typeof value !== "string") {
    throw new Error(
      `tool: condition value must be a string consumer identity, got ${typeof value}`,
    );
  }
  if (ctx.consumer === "") return false;
  return ctx.consumer === value;
};

/**
 * Registry for credential-use grants. Only `tool` is meaningful on a
 * `credential:{id}` / `use` grant; any unrecognized key throws (fail-loud)
 * rather than silently widening the grant.
 */
export const CREDENTIAL_USE_CONDITIONS: ConditionRegistry = {
  tool: toolConsumerEvaluator,
};
