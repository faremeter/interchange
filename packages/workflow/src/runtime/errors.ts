// Errors the runtime body and its host raise to each other.

/**
 * Thrown when `runtimeRun` is asked to resume from a durable log it
 * cannot honour with its in-process re-arming surface. The log may be a
 * supplied `resumeFromEvents` seed or one this call adopts from the
 * durable store (a seedless recovery call).
 *
 * The v1 runtime body supports resume when every remaining non-terminal
 * entry in the reconstructed `state.steps` is either aligned on a step
 * boundary or left in one of the resumable carve-outs (an in-flight
 * `loop` container, or an `awaitSignal` step still `awaiting-signal`
 * [re-parked] or left `in-flight` after a mover already logged, a
 * `SignalReceived` or a timed gate's `TimerFired`), or is a
 * crash-mid-invocation step (an agent `step` or `action` left
 * `in-flight`) which the runtime settles as a terminal `StepFailed`
 * rather than re-arming. A log that stops while a step is
 * `awaiting-timer`, mid-`map`, or otherwise `in-flight` with no
 * resumable carve-out has no schedulable primitive to advance it.
 * The runtime cannot re-arm the timer scheduler entry or rebuild the
 * inner-map iteration state from the log alone. The run records that
 * throw as `RunFailed`.
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

/**
 * Thrown when a seeded resume names blob refs and the env's blob
 * substrate is ephemeral. The seed is left as it was: writing a
 * terminal event would make a later resume on the substrate that
 * holds those blobs see a failed run.
 */
export class EphemeralResumeSeedError extends Error {
  constructor(blobOutputCount: number) {
    super(
      `resume requires the BlobSubstrate that recorded the seed log's blob refs (${String(blobOutputCount)} blob output(s) present); the runLocal in-memory substrate is ephemeral and starts empty. Pass the originating env, or use a durable substrate.`,
    );
    this.name = "EphemeralResumeSeedError";
  }
}

/**
 * Thrown by a step invoker whose step did its work but whose outcome the host
 * could not make durable: committing the turn failed after the agent's send
 * settled, so neither its output nor its park is recorded. The work already
 * happened, so the runtime lands the step as a failure with its retries
 * exhausted and neither retries nor routes it, the same as when recording a
 * completed step's output fails.
 */
export class StepOutcomeCommitError extends Error {
  constructor(cause: unknown) {
    super(
      `the step's outcome could not be committed: ${cause instanceof Error ? cause.message : String(cause)}`,
      { cause },
    );
    this.name = "StepOutcomeCommitError";
  }
}
