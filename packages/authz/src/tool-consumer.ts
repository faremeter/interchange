// Tool-consumer condition evaluator.
//
// Restricts a grant to a specific credential consumer. The grant's
// conditions object should contain:
//
//   { tool: "tool:<handle>" }   // or a reserved built-in consumer identity
//
// The evaluator returns true only when ctx.consumer is a non-empty identity
// that exactly matches the configured value. An absent (empty) consumer fails
// closed: a consumer-scoped grant never authorizes a caller that supplied no
// consumer identity, and it must be rejected before the equality check so a
// grant value of "" cannot match an empty consumer. A non-string condition
// value is a misconfiguration and throws, mirroring the fail-loud shape of the
// other evaluators rather than silently withholding.

import type { ConditionEvaluator, ConditionRegistry } from "./types";

/**
 * Build the consumer identity for a tool package. This is the value stamped
 * onto a `credential:{id}` / `use` grant's `{ tool }` condition at launch, and
 * the identical string the runtime gate supplies as `ctx.consumer` when a tool
 * in that package uses the credential. Launch and gate MUST derive it the same
 * way, so both import this one builder rather than inlining the format. The
 * granularity is package-level: every tool in a package shares one consumer
 * identity.
 */
export function toolConsumer(packageName: string): string {
  return `tool:${packageName}`;
}

/**
 * Condition evaluator for credential consumers.
 *
 * Register as `tool` in the condition registry:
 *   `{ tool: toolConsumerEvaluator }`
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
 * The condition registry for credential-use authorization. It carries only the
 * `tool` evaluator: a `credential:{id}` / `use` grant may narrow to a specific
 * consumer, and no other condition key is meaningful on that path. Any
 * unrecognized key throws (fail-loud) rather than silently widening the grant.
 */
export const CREDENTIAL_USE_CONDITIONS: ConditionRegistry = {
  tool: toolConsumerEvaluator,
};
