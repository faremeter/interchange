// Errors the runtime body surfaces to its host.

/**
 * Thrown when `runtimeRun` is asked to resume from a durable log it
 * cannot honour with its in-process re-arming surface. The log may be a
 * supplied `resumeFromEvents` seed or one this call adopts from the
 * durable store (a supervisor re-fire that carries no seed).
 *
 * The v1 runtime body supports resume when every remaining non-terminal
 * entry in the reconstructed `state.steps` is either aligned on a step
 * boundary or left in one of the resumable carve-outs (an in-flight
 * `loop` container, or an `awaitSignal` step still `awaiting-signal`
 * [re-parked] or -- with no timeout -- left `in-flight` by an
 * already-logged `SignalReceived`), or is a crash-mid-invocation step
 * (an agent `step` or `action` left `in-flight`) which the runtime
 * settles as a terminal `StepFailed` rather than re-arming. A log that
 * stops while a step is `awaiting-timer`, mid-`map`, or otherwise
 * `in-flight` (a `childWorkflow`, or a timeout-bearing `awaitSignal`
 * left `in-flight`) has no schedulable primitive to advance it -- the
 * runtime cannot re-arm the timer scheduler entry or rebuild the
 * inner-map iteration state from the log alone. The host (supervisor)
 * owns the recovery decision: crash the workflow process and let a fresh
 * deploy re-drive the live log.
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
      `resume against a durable log whose step ${stepId} is ${awaitedPrimitive} is not supported by the in-process runtime: ${detail}`,
    );
    this.name = "RuntimeResumeUnsupportedError";
    this.stepId = stepId;
    this.awaitedPrimitive = awaitedPrimitive;
  }
}
