// Shared park-aware drive for a suspendable child body.
//
// Runs a body sub-DAG through `runtimeRun` and returns a `SuspendableChildHandle`
// the caller drives across the body's parks (rather than awaiting a terminal).
// This is the SINGLE handle implementation behind the `SuspendableChildHandle`
// contract: an onTrigger section body, a loop iteration, and a `childWorkflow`
// child each build a host-shaped env and hand it here, so they share park,
// resume, and signal semantics by construction.
//
// The helper OWNS `onPark` / `onSignalPark` / `onSignalAwaiter`: it spreads
// the caller's env and installs its own sinks. Park sinks feed the FIFO the
// caller drains via `next()`. `onSignalAwaiter` records that a re-park has
// entered `awaitNext`, which `whenSignalAwaiter` waits for. The caller must
// pass an env that has not wired these sinks; a silent override would drop
// them, so this fails loud instead.

import { signalName } from "@intx/types/signals";

import type { WorkflowDefinition } from "../definition/index";
import type {
  SuspendableChildHandle,
  SuspendableChildPark,
  WorkflowRuntimeEnv,
} from "./env";
import { resumeFromLog, type WorkflowEvent } from "../state-machine/index";
import { bridgeAbort } from "./abort-bridge";
import { runtimeRun } from "./run";

/**
 * A handle that has already reached a terminal. `next` returns that same
 * terminal on every call, matching a live handle after `complete` settles.
 * `resume` and `deliverSignal` reject: this handle has no channel, and a
 * delivery after settle is a caller bug.
 */
export function createTerminalChildHandle(
  terminalStatus: "completed" | "failed" | "cancelled",
): SuspendableChildHandle {
  const terminal = {
    kind: "terminal" as const,
    terminalStatus,
  };
  const rejectSettled = (): Promise<never> =>
    Promise.reject(
      new Error(`child handle has already settled ${terminalStatus}`),
    );
  return {
    next: () => Promise.resolve(terminal),
    resume: () => rejectSettled(),
    deliverSignal: () => rejectSettled(),
  };
}

export function createSuspendableChildHandle(
  env: WorkflowRuntimeEnv,
  args: {
    definition: WorkflowDefinition;
    childRunId: string;
    input: unknown;
    /**
     * The depth the body run executes at (the container's own depth, passed
     * through unchanged) and the tree-wide ceiling, so a `childWorkflow`
     * spawned inside the body counts against the same bound as the top-level
     * run rather than resetting at the body boundary. Mirrors the options
     * `createSidecarRunChild` threads into the handle that drives the child.
     */
    depth: number;
    maxChildSpawnDepth: number;
    resumeFromEvents?: readonly WorkflowEvent[];
    signal: AbortSignal;
    /**
     * Channel teardown, run once the body settles. `signalChannel.stop()` is a
     * concrete-channel method (not on the `SignalChannel` interface), so the
     * caller that created the child's channel supplies its teardown here rather
     * than the helper reaching for it on `env.signalChannel`.
     */
    cleanup?: () => void | Promise<void>;
    /**
     * Whether an input park is returned to the caller. Required, with no
     * default: the safe direction is to reject, and a caller that forgot
     * the flag would otherwise climb a park the container cannot answer.
     * `childWorkflow` passes true. A loop iteration and an `onTrigger`
     * body pass false, and `next` aborts the body on the input park.
     */
    surfaceInputPark: boolean;
  },
): SuspendableChildHandle {
  const {
    definition,
    childRunId,
    input,
    depth,
    maxChildSpawnDepth,
    resumeFromEvents,
    signal,
    cleanup,
    surfaceInputPark,
  } = args;

  if (
    env.onPark !== undefined ||
    env.onSignalPark !== undefined ||
    env.onSignalAwaiter !== undefined
  ) {
    throw new Error(
      `createSuspendableChildHandle: the env for ${childRunId} already wired ` +
        `onPark/onSignalPark/onSignalAwaiter; this helper owns those sinks, ` +
        `so a caller-set sink would be silently overridden`,
    );
  }

  // FIFO the caller drains via `next()`: each entry is either an approval park
  // to proxy up or a fatal illegal-park error. A single waiter slot suffices
  // because `next()` has exactly one consumer driving it sequentially,
  // mirroring the signal channel's single-consumer shape.
  type BodyEvent =
    | { kind: "park"; park: SuspendableChildPark }
    | { kind: "signal-park"; name: string; timeout?: number }
    | { kind: "input-park"; correlationId: string }
    | { kind: "error"; error: Error };
  const events: BodyEvent[] = [];
  let wake: (() => void) | null = null;
  const notify = (): void => {
    if (wake !== null) {
      const resolve = wake;
      wake = null;
      resolve();
    }
  };

  // Names whose re-park has entered `awaitNext`. A host channel does not
  // replay a blob that landed before that subscription, and a re-park does
  // not re-fire `onSignalPark`, so a caller holding the decision waits here
  // and delivers after the name is recorded.
  const awaiterReady = new Set<string>();
  const awaiterWaiters = new Map<
    string,
    { resolve: () => void; reject: (error: Error) => void }[]
  >();
  const resolveAwaiter = (name: string): void => {
    awaiterReady.add(name);
    const waiters = awaiterWaiters.get(name);
    if (waiters === undefined) return;
    awaiterWaiters.delete(name);
    for (const waiter of waiters) waiter.resolve();
  };
  const rejectAwaiterWaiters = (error: Error): void => {
    for (const waiters of awaiterWaiters.values()) {
      for (const waiter of waiters) waiter.reject(error);
    }
    awaiterWaiters.clear();
  };

  const runEnv: WorkflowRuntimeEnv = {
    ...env,
    onPark: (park) => {
      if (park.parkKind === "approval") {
        events.push({
          kind: "park",
          park: {
            correlationId: park.correlationId,
            ...(park.approvalSnapshot !== undefined
              ? { approvalSnapshot: park.approvalSnapshot }
              : {}),
          },
        });
      } else if (surfaceInputPark && park.parkKind === "input") {
        events.push({
          kind: "input-park",
          correlationId: park.correlationId,
        });
      } else {
        events.push({
          kind: "error",
          error: new Error(
            `suspendable body ${childRunId} parked on a control-plane input ` +
              `channel (${park.correlationId}); a suspendable body may not ` +
              `re-arm an input park -- it has no upstream resolver`,
          ),
        });
      }
      notify();
    },
    // A body `awaitSignal` gate on an author name: surface it so the container
    // proxies it up as a signal-relay await and relays the resolved signal back
    // via `deliverSignal`. Without this the body would park on the signal
    // channel with nothing upstream to route a delivery to it.
    onSignalPark: (park) => {
      events.push({
        kind: "signal-park",
        name: park.name,
        ...(park.timeout !== undefined ? { timeout: park.timeout } : {}),
      });
      notify();
    },
    onSignalAwaiter: (park) => {
      resolveAwaiter(park.name);
    },
  };

  // A suspendable body runs IN-PROCESS under the workflow-process child's own
  // principal, which cannot sign the control-plane `CancelRequested` a durable
  // cancel writes -- that needs supervisor authority no in-process body holds
  // (an onTrigger section body and a loop iteration are identical here). So the
  // body never self-cancels. A teardown -- a parent-abort cascade (`signal`) or
  // an illegal input re-arm (in `next()` below) -- aborts the run's OWN cancel
  // controller through `runtimeRun`'s `localAbort`, which fails a parked step to
  // `StepFailed` and settles the body `RunFailed` under its own principal, with
  // no durable cancel to sign. A single controller unifies both teardown
  // triggers.
  const localTeardown = new AbortController();
  const onParentAbort = (): void => {
    localTeardown.abort();
  };
  const detachParentAbort = bridgeAbort(signal, onParentAbort);

  // On resume, drive the run from its durable log; the body step re-parks
  // silently (a re-park does not re-fire onPark), and the caller relays the
  // grant via resume on the correlation it recovered from its own log. On a
  // fresh spawn, seed the run with the event's trigger payload.
  const baseOptions =
    resumeFromEvents !== undefined
      ? { runId: childRunId, resumeFromEvents, depth, maxChildSpawnDepth }
      : { runId: childRunId, triggerPayload: input, depth, maxChildSpawnDepth };
  const handle = runtimeRun(definition, runEnv, {
    ...baseOptions,
    localAbort: localTeardown.signal,
  });

  let settled: {
    terminalStatus: "completed" | "failed" | "cancelled";
  } | null = null;
  let failure: Error | null = null;
  // Separate from `wake`. `next` has one consumer; a park-release watch is
  // the other, and it must not take the event-queue slot.
  const releaseWaiters = new Set<() => void>();
  const notifyReleaseWaiters = (): void => {
    for (const wakeRelease of [...releaseWaiters]) wakeRelease();
  };
  let openParkWatch: string | null = null;
  void handle.complete
    .then((result) => {
      settled = { terminalStatus: result.terminalStatus };
    })
    .catch((cause) => {
      failure = cause instanceof Error ? cause : new Error(String(cause));
    })
    .finally(() => {
      detachParentAbort();
      if (cleanup !== undefined) void cleanup();
      notify();
      notifyReleaseWaiters();
      rejectAwaiterWaiters(
        failure ??
          new Error(
            `child run ${childRunId} settled before a resumed gate re-armed`,
          ),
      );
    });

  // One read of the child log. `open` means a step is still awaiting
  // `name`, or that await is not in the log yet. `next` can still return
  // the park event after the gate has moved on, so a caller that already
  // holds a signal reads this before accepting it.
  const classifyPark = async (
    name: string,
  ): Promise<
    | { kind: "open"; lastSeq: number }
    | { kind: "released" }
    | {
        kind: "terminal";
        terminalStatus: "completed" | "failed" | "cancelled";
      }
  > => {
    if (failure !== null) throw failure;
    const log = await env.repoStore.read(childRunId);
    if (failure !== null) throw failure;
    if (settled !== null) {
      return { kind: "terminal", terminalStatus: settled.terminalStatus };
    }
    const state = resumeFromLog(childRunId, log);
    if (
      state.phase === "completed" ||
      state.phase === "failed" ||
      state.phase === "cancelled"
    ) {
      return { kind: "terminal", terminalStatus: state.phase };
    }
    for (const step of state.steps.values()) {
      if (
        step.phase === "awaiting-signal" &&
        step.awaitingSignal?.name === name
      ) {
        return { kind: "open", lastSeq: state.lastSeq };
      }
    }
    const opened = log.some(
      (event) => event.kind === "SignalAwaited" && event.signalName === name,
    );
    if (!opened) return { kind: "open", lastSeq: state.lastSeq };
    return { kind: "released" };
  };

  const readParkRelease = async (
    name: string,
  ): Promise<
    | { kind: "open" }
    | { kind: "released" }
    | {
        kind: "terminal";
        terminalStatus: "completed" | "failed" | "cancelled";
      }
  > => {
    const read = await classifyPark(name);
    if (read.kind === "open") return { kind: "open" };
    return read;
  };

  // Does not call `next`, so a sibling park stays queued. The park is
  // durable before `next` returns it; a read that has not yet seen this
  // name's `SignalAwaited` keeps waiting rather than reporting a release.
  // The subscription is only a wake. A store that does not replay `from`
  // still works. The host adapter's `subscribe` returns a generator that
  // attaches its tail on the first pull, not when `subscribe` returns, so
  // the pull happens before the second read. That read sees a release
  // that landed while the previous snapshot was taken. A release that
  // lands during that read wakes the tail.
  const waitForParkRelease = async (
    name: string,
    abort: AbortSignal,
  ): Promise<
    | { kind: "released" }
    | {
        kind: "terminal";
        terminalStatus: "completed" | "failed" | "cancelled";
      }
  > => {
    if (openParkWatch !== null) {
      throw new Error(
        `child ${childRunId} already watches ${openParkWatch} for release; ` +
          `a drive relays one park at a time`,
      );
    }
    openParkWatch = name;
    try {
      if (abort.aborted) throw new Error("aborted");
      for (;;) {
        if (abort.aborted) throw new Error("aborted");
        const read = await classifyPark(name);
        if (read.kind !== "open") return read;
        const fromSeq = read.lastSeq + 1;
        const poke = new AbortController();
        const detachPoke = bridgeAbort(abort, () => {
          poke.abort();
        });
        const onSettle = (): void => {
          poke.abort();
        };
        releaseWaiters.add(onSettle);
        let subscription:
          | ReturnType<WorkflowRuntimeEnv["repoStore"]["subscribe"]>
          | undefined;
        let pending:
          | ReturnType<
              ReturnType<WorkflowRuntimeEnv["repoStore"]["subscribe"]>["next"]
            >
          | undefined;
        try {
          if (settled !== null || failure !== null || abort.aborted) {
            continue;
          }
          subscription = env.repoStore.subscribe(childRunId, {
            signal: poke.signal,
            from: { seq: fromSeq },
          });
          // Pull before the second read. On the host store this is what
          // attaches the tail; a commit during the read is then in the
          // iterator even when that read's snapshot missed it.
          pending = subscription.next();
          void pending.catch(() => undefined);
          const again = await classifyPark(name);
          if (again.kind !== "open") return again;
          if (abort.aborted) throw new Error("aborted");
          // The log moved before this tail could see it. A store that
          // does not replay `from` would wait forever on the old cursor.
          if (again.lastSeq !== read.lastSeq) continue;
          let sawEvent = false;
          const first = await pending;
          pending = undefined;
          if (first.done !== true) sawEvent = true;
          if (abort.aborted) throw new Error("aborted");
          if (
            !sawEvent &&
            settled === null &&
            failure === null &&
            !abort.aborted
          ) {
            throw new Error(
              `child ${childRunId} park-release watch on ${name} ended before ` +
                `the park released`,
            );
          }
        } finally {
          releaseWaiters.delete(onSettle);
          detachPoke();
          poke.abort();
          const abandoned = pending;
          pending = undefined;
          await subscription?.return?.();
          if (abandoned !== undefined) {
            await abandoned.catch(() => undefined);
          }
        }
      }
    } finally {
      openParkWatch = null;
    }
  };

  return {
    next: async () => {
      for (;;) {
        const event = events.shift();
        if (event !== undefined) {
          if (event.kind === "error") {
            // The body re-armed an input park nothing will resolve. Tear the
            // child down locally so its terminal (and the channel teardown tied
            // to it) fires, then surface the error: the throw lands the
            // container run's terminal via `runPrimitiveSafe`. Local teardown,
            // not `handle.cancel`, because an in-process body cannot sign a
            // control-plane cancel (see the `localTeardown` note above).
            localTeardown.abort();
            throw event.error;
          }
          if (event.kind === "signal-park") {
            return {
              kind: "signal-park",
              name: event.name,
              ...(event.timeout !== undefined
                ? { timeout: event.timeout }
                : {}),
            };
          }
          if (event.kind === "input-park") {
            return { kind: "input-park", correlationId: event.correlationId };
          }
          return { kind: "park", park: event.park };
        }
        if (failure !== null) throw failure;
        if (settled !== null) {
          return { kind: "terminal", terminalStatus: settled.terminalStatus };
        }
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
    },
    resume: async (correlationId, decision) => {
      await env.signalChannel.deliver(signalName(correlationId), decision);
    },
    deliverSignal: async (name, payload, signalId) => {
      await env.signalChannel.deliver(name, payload, signalId);
    },
    readParkRelease,
    waitForParkRelease,
    whenSignalAwaiter: (name) => {
      if (awaiterReady.has(name)) return Promise.resolve();
      if (failure !== null) return Promise.reject(failure);
      if (settled !== null) {
        return Promise.reject(
          new Error(`child run ${childRunId} settled before ${name} re-armed`),
        );
      }
      return new Promise<void>((resolve, reject) => {
        const waiters = awaiterWaiters.get(name);
        if (waiters !== undefined) {
          waiters.push({ resolve, reject });
          return;
        }
        awaiterWaiters.set(name, [{ resolve, reject }]);
      });
    },
  };
}
