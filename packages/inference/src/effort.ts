import { getLogger } from "@intx/log";
import type { InferenceEffort } from "@intx/types/runtime-core";

const logger = getLogger(["interchange", "inference", "adapter"]);

/**
 * The thinking budget an effort maps to on a model that takes a token
 * budget rather than an effort name (classic Anthropic, Gemini 2.5). The
 * low, medium, and high values follow Google's published mapping for its
 * OpenAI-compatible endpoint. `max` repeats `high`: 24576 is the largest
 * budget every Gemini 2.5 model accepts.
 */
export const EFFORT_THINKING_BUDGET: Readonly<Record<InferenceEffort, number>> =
  {
    low: 1024,
    medium: 8192,
    high: 24576,
    max: 24576,
  };

/** Name the setting a thinking budget came from, for an error message. */
export function budgetSource(
  effort: InferenceEffort,
  budgetTokens: number | undefined,
): string {
  return budgetTokens === undefined
    ? `effort "${effort}"`
    : "thinking.budgetTokens";
}

/**
 * Reject a thinking budget that leaves no room under the output cap. The
 * cap bounds thinking and answer together, and the caller owns the cap, so
 * the adapter fails the call with a fix rather than raise the cap itself or
 * let the provider reject the request. `source` names the setting the
 * budget came from (see `budgetSource`).
 */
export function rejectBudgetOverCap(
  model: string,
  source: string,
  budget: number,
  maxTokens: number,
): void {
  if (maxTokens > budget) return;
  throw new Error(
    `${source} on model ${model} needs a thinking budget of ` +
      `${budget} tokens, but maxTokens is ${maxTokens}; set maxTokens ` +
      `above ${budget}.`,
  );
}

// A step routing to a model that cannot carry an option re-issues the same
// drop on every turn and every retry, so each distinct drop is reported once
// for the life of the process.
const reported = new Set<string>();

/** Forget every reported drop so tests can assert on the next one. */
export function resetDroppedOptionReports(): void {
  reported.clear();
}

/**
 * A per-call inference option the adapter cannot put on the wire for this
 * model. The selector accepts the option with no model awareness, so the
 * drop is warn-logged rather than a silent no-op.
 */
export function warnDroppedOption(
  model: string,
  option: string,
  reason: string,
): void {
  const key = JSON.stringify([model, option, reason]);
  if (reported.has(key)) return;
  reported.add(key);
  logger.warn`Dropping ${option} for model ${model}: ${reason}`;
}
