// Errors the runtime body surfaces to its host.

/**
 * Thrown when `runtimeRun` is asked to resume from a seed log it
 * cannot honour with its in-process re-arming surface.
 *
 * The v1 runtime body supports resume from seed logs that are either
 * complete-or-cancelled or aligned on step boundaries: every
 * non-terminal entry in the seed log's reconstructed `state.steps`
 * must be schedulable via the DAG's `nextSchedulable` set. Seed logs
 * that stop while a step is `awaiting-signal`, `awaiting-timer`, or
 * mid-`map` have no schedulable primitive to advance them -- the
 * runtime cannot rehydrate the signal channel, re-arm the timer
 * scheduler entry, or rebuild the inner-map iteration state from the
 * log alone. The host (supervisor) owns the recovery decision: crash
 * the workflow process and let a fresh deploy pick up the live log,
 * re-inject the awaited signal, etc.
 *
 * Surfacing the limitation as a structured error keeps the contract
 * honest instead of stalling with an opaque "no schedulable
 * primitives" message.
 */
export class RuntimeResumeUnsupportedError extends Error {
  readonly stepId: string;
  readonly awaitedPrimitive: "awaiting-signal" | "awaiting-timer" | "in-flight";
  constructor(
    stepId: string,
    awaitedPrimitive: "awaiting-signal" | "awaiting-timer" | "in-flight",
    detail: string,
  ) {
    super(
      `resume against a seed log whose step ${stepId} is ${awaitedPrimitive} is not supported by the in-process runtime: ${detail}`,
    );
    this.name = "RuntimeResumeUnsupportedError";
    this.stepId = stepId;
    this.awaitedPrimitive = awaitedPrimitive;
  }
}
