// Errors the runtime body surfaces to its host.

/**
 * Thrown when `runtimeRun` is asked to resume from a durable log it cannot
 * honour with its in-process re-arming surface. The v1 runtime supports
 * resume when every remaining non-terminal step is aligned on a step
 * boundary or in one of the resumable carve-outs (an in-flight `loop`
 * container, an `awaitSignal` step still `awaiting-signal` or -- with no
 * timeout -- left `in-flight` by an already-logged `SignalReceived`), or is
 * a crash-mid-invocation `step`/`action` which the runtime settles as a
 * terminal `StepFailed`. A log that stops while a step is `awaiting-timer`,
 * mid-`map`, or otherwise `in-flight` (a `childWorkflow`, or a
 * timeout-bearing `awaitSignal`) has no schedulable primitive to advance it;
 * the host (supervisor) owns the recovery decision. Surfacing the limitation
 * as a structured error keeps the contract honest instead of stalling with an
 * opaque "no schedulable primitives" message.
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
