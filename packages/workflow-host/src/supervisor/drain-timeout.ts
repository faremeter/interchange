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
import type { PrincipalSigner, TerminalEventSource } from "./types";
import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";

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
  /**
   * Optional per-runId terminal-event source the accumulator consumes
   * to settle early when the tracked run reaches a terminal phase
   * before the drainTimeout fires. When absent, the accumulator falls
   * back to timer-only settlement (the pre-binding behaviour). The
   * supervisor wires this against its `terminalEventSource` binding;
   * the iterator is finalised via `return()` whenever the accumulator
   * settles (terminal, timeout, or `stop()`).
   */
  terminalEventSource?: TerminalEventSource;
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
  /**
   * Resolves when every asynchronous resource the accumulator owns has
   * been disposed: the escalation commit (if armed) has finished, and
   * the per-runId terminal-event watcher (if armed) has had its
   * iterator finalised. The supervisor awaits this in `shutdown` so a
   * still-running watcher cannot fire a settle against a torn-down
   * supervisor.
   *
   * Idempotent: calling `disposed()` multiple times returns the same
   * promise. Resolves once `stop()` or the timer-driven escalation has
   * run.
   */
  disposed(): Promise<void>;
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
  /**
   * Active terminal-event iterator the accumulator is consuming for
   * its tracked runId. Held so the dispose path can finalise it via
   * `return()`. `null` when no source binding was supplied or the
   * iterator has already been finalised.
   */
  let terminalIterator: AsyncIterator<unknown> | null = null;
  /** Promise the per-runId terminal-watcher coroutine resolves into. */
  let terminalWatcherDone: Promise<void> | null = null;
  /** Latest pending escalation commit; awaited by `disposed()`. */
  let escalationPending: Promise<void> | null = null;
  let disposedPromise: Promise<void> | null = null;

  function armTerminalWatcher(): void {
    if (opts.terminalEventSource === undefined) return;
    if (terminalIterator !== null) return;
    const iterable = opts.terminalEventSource(opts.runId);
    const iterator = iterable[Symbol.asyncIterator]();
    terminalIterator = iterator;
    terminalWatcherDone = (async () => {
      try {
        // The first terminal event the source yields is the signal to
        // settle. The source pre-filters on runId, so any element it
        // produces applies to the tracked run.
        const next = await iterator.next();
        if (next.done === true) return;
        settleOnTerminal();
      } catch (cause) {
        // The iterator's failure does not need an explicit
        // escalation: the outer `setTimer`-based deadline keeps
        // ticking against the same `state.phase === "running"` slot
        // and fires `escalate()` if no terminal event arrives. The
        // warn here surfaces the iterator failure to operator logs
        // so a persistent broken event source is visible; the
        // accumulator's contract is preserved by the timer's
        // fall-through.
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`terminal-event watcher for run ${opts.runId} threw: ${message}`;
      }
    })();
  }

  function settleOnTerminal(): void {
    if (escalated) return;
    if (state.phase === "stopped" || state.phase === "escalated") return;
    if (state.phase === "running") {
      opts.clearTimer(state.timer);
    }
    state = { phase: "stopped" };
  }

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

  function escalate(): void {
    if (escalated) return;
    escalated = true;
    if (state.phase === "running") {
      opts.clearTimer(state.timer);
    }
    state = { phase: "escalated" };
    // The terminal watcher's role ends when the deadline fires: the
    // commit below races the on-disk CancelRequested against any
    // natural terminal arrival, and either way the accumulator settles
    // here. Finalising the iterator lets the producer free its
    // resources.
    finaliseTerminalWatcher();
    const origin: CancelOrigin = "supervisor-drain";
    escalationPending = commitCancelRequested({
      substrate: opts.substrate,
      repoId: opts.repoId,
      ref: opts.ref,
      deploymentId: opts.deploymentId,
      runId: opts.runId,
      origin,
      reason,
      at: new Date().toISOString(),
      signAsPrincipal: opts.signAsPrincipal,
    })
      .then(() => undefined)
      .catch((cause: unknown) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`drainTimeout escalation commit failed for run ${opts.runId}: ${message}`;
        throw cause instanceof Error ? cause : new Error(message);
      });
  }

  function finaliseTerminalWatcher(): void {
    const iterator = terminalIterator;
    if (iterator === null) return;
    terminalIterator = null;
    if (typeof iterator.return !== "function") return;
    // Fire the iterator's `return()` so the producer side observes the
    // cancel. The producer (`subscribeKind`-backed) wires its own
    // `AbortSignal`; finalising the iterator surfaces as an abort
    // through the substrate's `subscribe` primitive, which is the
    // contract `subscribeKind` documents.
    void iterator.return(undefined).catch((cause: unknown) => {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`terminal-event watcher return() for run ${opts.runId} threw: ${message}`;
    });
  }

  return {
    start(): void {
      if (state.phase !== "idle") return;
      armTerminalWatcher();
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
      finaliseTerminalWatcher();
      if (state.phase === "escalated") return;
      state = { phase: "stopped" };
    },
    accumulatedMs(): number {
      return drainTimeoutMs - remainingMs();
    },
    get escalated(): boolean {
      return escalated;
    },
    disposed(): Promise<void> {
      if (disposedPromise !== null) return disposedPromise;
      disposedPromise = (async () => {
        if (escalationPending !== null) {
          await escalationPending.catch(() => {
            /* error already logged in `escalate`. */
          });
        }
        if (terminalWatcherDone !== null) {
          await terminalWatcherDone.catch(() => {
            /* error already logged in the watcher coroutine. */
          });
        }
      })();
      return disposedPromise;
    },
  };
}
