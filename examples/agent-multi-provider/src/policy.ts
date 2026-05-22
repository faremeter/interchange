// Routing and failover policy for the multi-provider example.
//
// The @intx/agent surface intentionally does not bundle a
// failover or cost-routing engine — `setSource()` lets you swap the
// active inference source in place, and the example layers the policy
// on top. That keeps the agent package's surface small and lets
// users encode the policy that matches their billing/SLA story.

import type { InferenceSource } from "@intx/types/runtime";

export type SourceEntry = {
  /** Stable identifier for the entry (e.g. "primary", "fallback"). */
  name: string;
  source: InferenceSource;
};

/**
 * Heuristic that picks a "cheap" vs "smart" model based on prompt
 * length. The threshold is arbitrary — production callers should
 * substitute a heuristic that matches their data (e.g. classify
 * intent, pick based on tool-call expectations, etc.).
 */
export type ModelTier = "cheap" | "smart";

export function pickModelTier(prompt: string): ModelTier {
  return prompt.length < 80 ? "cheap" : "smart";
}

/**
 * Return the InferenceSource the agent should use for `prompt` given
 * the routing table. The `models` map selects which model is "cheap"
 * vs "smart"; the chosen entry's `source` is cloned with the chosen
 * model overlaid and a freshly synthesized id. The original source is
 * never mutated.
 */
export function routeSource(args: {
  prompt: string;
  primary: SourceEntry;
  models: { cheap: string; smart: string };
}): { source: InferenceSource; tier: ModelTier; model: string } {
  const tier = pickModelTier(args.prompt);
  const model = args.models[tier];
  return {
    source: {
      ...args.primary.source,
      id: `${args.primary.source.provider}:${model}`,
      model,
    },
    tier,
    model,
  };
}

/**
 * The shape `withFailover` returns. `primaryError` is present iff
 * failover engaged — the caller can log it or surface it to the user
 * even though the request itself ultimately succeeded.
 */
export type WithFailoverResult<T> = {
  result: T;
  served: SourceEntry;
  attempts: SourceEntry[];
  primaryError?: unknown;
};

/**
 * Wrap an inference attempt with a single-shot failover: if the call
 * rejects, swap to `fallback` and retry once. The returned object
 * carries which source ultimately served the request so the caller
 * can report it; `attempts` lists every source tried in order, and
 * `primaryError` carries the primary's failure when failover engaged
 * so it can be logged rather than silently discarded.
 *
 * When the fallback also rejects, the thrown wrapper Error carries
 * both failure messages in its `message` text and points its `cause`
 * at the fallback's error. Neither the caller's primary nor fallback
 * error object is mutated.
 */
export async function withFailover<T>(args: {
  primary: SourceEntry;
  fallback: SourceEntry;
  applySource: (source: InferenceSource) => void;
  invoke: () => Promise<T>;
}): Promise<WithFailoverResult<T>> {
  const attempts: SourceEntry[] = [args.primary];
  args.applySource(args.primary.source);
  let primaryError: unknown;
  try {
    const result = await args.invoke();
    return { result, served: args.primary, attempts };
  } catch (cause) {
    primaryError = cause;
  }

  attempts.push(args.fallback);
  args.applySource(args.fallback.source);
  try {
    const result = await args.invoke();
    return { result, served: args.fallback, attempts, primaryError };
  } catch (fallbackCause) {
    const fallbackError =
      fallbackCause instanceof Error
        ? fallbackCause
        : new Error(String(fallbackCause));
    const primaryMessage =
      primaryError instanceof Error
        ? primaryError.message
        : String(primaryError);
    throw new Error(
      `withFailover: both sources failed. ` +
        `Primary (${args.primary.name}): ${primaryMessage}. ` +
        `Fallback (${args.fallback.name}): ${fallbackError.message}`,
      { cause: fallbackError },
    );
  }
}
