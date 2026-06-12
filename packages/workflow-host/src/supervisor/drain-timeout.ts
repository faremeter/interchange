// Supervisor-side drainTimeout accumulator and escalation path.
//
// When the supervisor's control loop sends `drain` to the workflow
// process, the child's `DrainController` flips its signal and the
// runtime body picks up the change on the next tick at each of the
// four observation points. Steps whose declared `drainBehavior` is
// `"cancel"` abort their local controller; steps whose behavior is
// `"wait"` continue running.
//
// The supervisor's accumulator tracks wall-clock time spent against
// `"cancel"`-behavior work after drain has been issued. Time spent
// while every in-flight step is `"wait"`-behavior does NOT tick the
// accumulator -- waiting for a human-in-the-loop pause should not
// burn the drain budget. On `drainTimeout` expiry, the accumulator
// invokes the supervisor's injected `signAsPrincipal("supervisor",
// ...)` callback to obtain a signed `CancelRequested{origin:
// "supervisor-drain"}` and commits it via the injected substrate
// handle. The runtime body's existing cancellation cascade handles
// teardown from there.
//
// The accumulator is started by the supervisor on receipt of the
// host's drain command; it is stopped when (a) drainTimeout expires
// and the CancelRequested commit lands, or (b) the run reaches a
// terminal phase before expiry. Pausing is driven by the supervisor's
// view of which step kinds are in flight -- it consults the same
// `behaviorFor` projection the workflow-host child uses.

import { getLogger } from "@intx/log";

import type { CancelOrigin } from "@intx/workflow";

import { commitCancelRequested } from "./cancel-signing";
import type { PrincipalSigner } from "./types";
import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions";

const logger = getLogger(["workflow-host", "supervisor", "drain-timeout"]);

/**
 * Default `drainTimeout` per deployment. Operators override this via
 * a per-deployment policy; this value is the spec's locked default.
 */
export const DEFAULT_DRAIN_TIMEOUT_MS = 60_000;

export type DrainTimeoutOpts = {
  /**
   * Substrate handle through which the escalation commit lands.
   * Reused from the supervisor's bindings -- the accumulator does
   * not own a substrate of its own.
   */
  substrate: SubstrateRepoStore;
  /** Workflow-run repo for this deployment. */
  repoId: RepoId;
  /** Workflow-run repo ref the supervisor commits events to. */
  ref: string;
  /** Deployment id baked into the supervisor's signing principal. */
  deploymentId: string;
  /** Run id the drain is being escalated against. */
  runId: string;
  /**
   * Host-supplied per-principal signing callback the supervisor uses
   * to mint the CancelRequested signature. The drain origin is
   * `supervisor-drain`.
   */
  signAsPrincipal: PrincipalSigner;
  /** ISO-8601 reason for the CancelRequested escalation. */
  reason?: string;
  /**
   * Operator-overridable per-deployment timeout. Defaults to
   * `DEFAULT_DRAIN_TIMEOUT_MS`.
   */
  drainTimeoutMs?: number;
  /**
   * Deterministic clock for the accumulator. Tests inject a
   * controllable clock; production wires `() => Date.now()`.
   */
  now: () => number;
  /**
   * Scheduling primitive the accumulator uses for its tick callback.
   * Tests inject a deterministic timer; production wires
   * `(cb, ms) => setTimeout(cb, ms)`.
   */
  setTimer: (cb: () => void, ms: number) => unknown;
  /**
   * Disposer paired with `setTimer`. Production wires
   * `(handle) => clearTimeout(handle as ReturnType<typeof setTimeout>)`.
   */
  clearTimer: (handle: unknown) => void;
};

/**
 * Handle the supervisor holds onto so it can pause/resume ticking
 * during step-kind transitions, and so it can stop the accumulator
 * cleanly when the run reaches a terminal phase before the
 * drainTimeout expires.
 */
export interface DrainTimeoutAccumulator {
  /**
   * Begin counting cancel-time. Idempotent; a second call after the
   * accumulator has already started is a no-op so the supervisor's
   * control loop can re-issue drain without breaking ordering.
   */
  start(): void;
  /**
   * Pause the accumulator. The supervisor calls this when its view
   * of the workflow's in-flight steps transitions from cancel-mode
   * to wait-mode (e.g., the last cancel-mode step completed and only
   * `awaitSignal` steps remain). Time spent paused does not count
   * toward the timeout.
   */
  pause(): void;
  /**
   * Resume the accumulator. Inverse of `pause`. Time elapsed since
   * the last `start` or `resume` carries forward -- pause/resume is
   * about wall-clock attribution, not zeroing the count.
   */
  resume(): void;
  /**
   * Stop the accumulator without escalating. Called when the run
   * reaches a terminal phase before drainTimeout expires.
   */
  stop(): void;
  /** Total accumulated cancel-time at the current moment, in ms. */
  accumulatedMs(): number;
  /** Whether the accumulator has already escalated. */
  readonly escalated: boolean;
}

type AccumulatorState =
  | { phase: "idle" }
  | { phase: "running"; startedAt: number; baseline: number; timer: unknown }
  | { phase: "paused"; baseline: number }
  | { phase: "escalated" }
  | { phase: "stopped" };

/**
 * Factory shape the supervisor binds against. Mirrors
 * `createDrainTimeoutAccumulator` exactly; surfaced so tests can
 * inject a mock factory through `WorkflowSupervisorBindings.
 * drainTimeoutAccumulatorFactory` and observe the supervisor's
 * drain-arming sequence without rigging a real timer host.
 */
export type DrainTimeoutAccumulatorFactory = (
  opts: DrainTimeoutOpts,
) => DrainTimeoutAccumulator;

/**
 * Construct the supervisor's drainTimeout accumulator. The returned
 * handle is single-use: once `escalated` flips true the accumulator
 * has committed `CancelRequested{origin: "supervisor-drain"}` and
 * cannot tick again.
 */
export function createDrainTimeoutAccumulator(
  opts: DrainTimeoutOpts,
): DrainTimeoutAccumulator {
  const drainTimeoutMs = opts.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
  const reason = opts.reason ?? "drainTimeout expired";
  let state: AccumulatorState = { phase: "idle" };
  let escalated = false;

  function remainingMs(): number {
    if (state.phase === "running") {
      const elapsed = opts.now() - state.startedAt + state.baseline;
      return Math.max(0, drainTimeoutMs - elapsed);
    }
    if (state.phase === "paused") {
      return Math.max(0, drainTimeoutMs - state.baseline);
    }
    return drainTimeoutMs;
  }

  function arm(baseline: number): void {
    const remaining = drainTimeoutMs - baseline;
    if (remaining <= 0) {
      void escalate();
      return;
    }
    const timer = opts.setTimer(() => {
      // The timer is the authority for escalation -- the accumulator
      // does not poll. When the timer fires we transition to
      // `escalated` and commit through the substrate.
      void escalate();
    }, remaining);
    state = {
      phase: "running",
      startedAt: opts.now(),
      baseline,
      timer,
    };
  }

  async function escalate(): Promise<void> {
    if (escalated) return;
    escalated = true;
    if (state.phase === "running") {
      opts.clearTimer(state.timer);
    }
    state = { phase: "escalated" };
    try {
      const origin: CancelOrigin = "supervisor-drain";
      await commitCancelRequested({
        substrate: opts.substrate,
        repoId: opts.repoId,
        ref: opts.ref,
        deploymentId: opts.deploymentId,
        runId: opts.runId,
        origin,
        reason,
        at: new Date().toISOString(),
        signAsPrincipal: opts.signAsPrincipal,
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`drainTimeout escalation commit failed for run ${opts.runId}: ${message}`;
      throw cause;
    }
  }

  return {
    start(): void {
      if (state.phase !== "idle") return;
      arm(0);
    },
    pause(): void {
      if (state.phase !== "running") return;
      const elapsed = opts.now() - state.startedAt + state.baseline;
      opts.clearTimer(state.timer);
      state = { phase: "paused", baseline: elapsed };
    },
    resume(): void {
      if (state.phase !== "paused") return;
      arm(state.baseline);
    },
    stop(): void {
      if (state.phase === "running") {
        opts.clearTimer(state.timer);
      }
      if (state.phase === "escalated") return;
      state = { phase: "stopped" };
    },
    accumulatedMs(): number {
      return drainTimeoutMs - remainingMs();
    },
    get escalated(): boolean {
      return escalated;
    },
  };
}
