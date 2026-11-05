// Tool-consumer condition evaluator.
//
// Restricts a grant to a specific credential consumer. The grant's
// conditions object should contain:
//
//   { tool: "tool:<factory id>" } // a tool factory
//   { tool: "action:<handler>" } // a workflow action handler
//   { tool: "<built-in id>" }    // a reserved built-in consumer identity
//
// The condition key is `tool` for every consumer kind; the prefix of the value
// names the kind.
//
// The evaluator returns true only when ctx.consumer is a non-empty identity
// that exactly matches the configured value. An absent (empty) consumer fails
// closed: a consumer-scoped grant never authorizes a caller that supplied no
// consumer identity. A condition value that is not a non-empty string is a
// misconfiguration and throws, mirroring the fail-loud shape of the other
// evaluators rather than silently withholding; because "" never reaches the
// comparison, an empty grant value cannot match an empty consumer.

import { type } from "arktype";

import type { CredentialBinding } from "@intx/types";

import { createConditionEvaluator } from "./condition-evaluator";
import type { ConditionRegistry } from "./types";

/**
 * Build the consumer identity for a tool factory. This is the value stamped
 * onto a `credential:{id}` / `use` grant's `{ tool }` condition at launch, and
 * the identical string the runtime gate supplies as `ctx.consumer` when a tool
 * in that factory uses the credential. Launch and gate MUST derive a consumer
 * identity the same way, through the builders in this module, rather than
 * inline the format. The granularity is the factory: every tool in one
 * `defineTool` bundle shares one consumer identity.
 */
export function toolConsumer(factoryId: string): string {
  return `tool:${factoryId}`;
}

/**
 * Build the consumer identity for a workflow action handler, keyed by the
 * handler's export name. The `action:` prefix keeps it disjoint from every
 * tool consumer, so a tool factory and an action handler with the same name
 * never share a grant. Every action step that runs the handler shares one
 * consumer identity.
 */
export function actionConsumer(handler: string): string {
  return `action:${handler}`;
}

/**
 * The consumer identity a credential binding names: its tool factory or its
 * action handler.
 */
export function credentialBindingConsumer(binding: CredentialBinding): string {
  if (binding.package !== undefined) return toolConsumer(binding.package);
  return actionConsumer(binding.action);
}

/**
 * Condition evaluator for credential consumers.
 *
 * Register as `tool` in the condition registry:
 *   `{ tool: toolConsumerEvaluator }`
 */
export const toolConsumerEvaluator = createConditionEvaluator(
  "tool",
  type("string > 0").describe("a non-empty consumer identity"),
  (consumer, ctx) => {
    if (ctx.consumer === "") return false;
    return ctx.consumer === consumer;
  },
);

/**
 * The condition registry for credential-use authorization. It carries only the
 * `tool` evaluator: a `credential:{id}` / `use` grant may narrow to a specific
 * consumer, and no other condition key is meaningful on that path. Any
 * unrecognized key throws (fail-loud) rather than silently widening the grant.
 */
export const CREDENTIAL_USE_CONDITIONS: ConditionRegistry = {
  tool: toolConsumerEvaluator,
};
