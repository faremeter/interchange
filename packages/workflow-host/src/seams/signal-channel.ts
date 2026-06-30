// Production workflow-host signal channel (Seam 3, log-tail wait).
//
// The signal channel is constructed per run by the host's runtime
// wiring. Its `deliver` commits a `SignalReceived` blob to the run's
// event log; commit-success is the dedup gate (the state machine's
// `observedSignalIds` rejects a duplicate `signalId` on the next
// transition). `awaitNext` consults the state-machine `RunState` for
// signals that were committed and reduced into `unconsumedSignals`
// before the awaiter subscribed, then falls back to a per-name
// `subscribeKind` tail of the run's events ref.
//
// Per-name FIFO across concurrent awaiters: the channel maintains an
// awaiter queue per signal name. A single `subscribeKind` loop per
// name pulls events from the substrate; each matching event resolves
// the head of that queue. The loop starts on first `awaitNext` for a
// name and tears down when the queue empties or `stop()` is called.
//
// Commit-ordering invariant: `deliver` commits the `SignalReceived`
// event blob first, then returns. Resolution of an awaiter happens
// from the `subscribeKind` loop only after the substrate has surfaced
// the commit. The awaiter is never resolved from inside `deliver`'s
// call site, so resume-after-crash sees a coherent log: an awaiter
// that resolved must have a corresponding committed `SignalReceived`.

import { type } from "arktype";

import type {
  Principal,
  RepoId,
  RepoStore,
  SubscribeKindEntry,
} from "@intx/hub-sessions/substrate";
import { subscribeKind } from "@intx/hub-sessions/substrate";
import type { RunState, SignalChannel } from "@intx/workflow";

/**
 * Substrate-shape envelope for the `SignalReceived` event blob
 * committed to `runs/<runId>/events/<seq>.json`. The validator covers
 * the single event type the signal channel both reads (live tail) and
 * writes (deliver). Fields ride at the top level so the shape is
 * symmetric with the runtime body's append shape -- a downstream
 * reader that hydrates the envelope as a state-machine `WorkflowEvent`
 * sees `signalName`/`signalId`/`payload` regardless of whether the
 * commit came from the signal channel's `deliver` or the runtime
 * body's `commit` of a SignalReceived after `awaitNext`. Non-signal
 * blobs at the same path prefix do not match the kinds filter inside
 * `subscribeKind`.
 */
export const SignalReceivedEnvelope = type({
  type: "'SignalReceived'",
  signalName: "string",
  signalId: "string",
  payload: "unknown",
});
export type SignalReceivedEnvelope = typeof SignalReceivedEnvelope.infer;

export type SignalChannelOpts = {
  /**
   * Substrate handle the channel reads from and writes to. The caller
   * wires this against the workflow-run kind handler's registered
   * substrate -- the channel's writes land under
   * `runs/<runId>/events/<seq>.json` and the handler's `validatePush`
   * must accept that path layout.
   */
  repoStore: RepoStore;
  /**
   * Principal the channel presents to the substrate. The substrate
   * gates every operation behind `authorize`; the principal must be
   * granted `writeTree` (for `deliver`) and `subscribe` (for
   * `awaitNext`'s log tail) against the workflow-run repo.
   */
  principal: Principal;
  /** Workflow-run repo this channel operates against. */
  repoId: RepoId;
  /**
   * Events ref the channel tails and writes to. The workflow-run
   * repo layout pins all `runs/<runId>/events/` blobs under a single
   * moving ref. Callers typically supply `"refs/heads/main"`.
   */
  ref: string;
  /**
   * The run this channel belongs to. The channel filters
   * `subscribeKind` entries on this runId so a host-wide events ref
   * carrying multiple runs does not cross-resolve awaiters.
   */
  runId: string;
  /**
   * Reader for the in-memory `RunState`. The runtime body owns the
   * state; the channel reads `unconsumedSignals` (pre-await
   * delivery / resume rehydration) and `observedSignalIds` (dedup)
   * on every `awaitNext`. A reader rather than a snapshot keeps the
   * channel coherent with the runtime body's latest reduction.
   */
  readState: () => RunState;
  /** Generator for synthesized `signalId`s when `deliver`'s caller omits one. */
  newId: () => string;
  /** Clock used to stamp the committed `SignalReceived` blob's `at`. */
  clock: () => Date;
};

type Awaiter = {
  resolve: (value: { payload: unknown; signalId: string }) => void;
  reject: (cause: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
};

type NameSubscription = {
  abort: AbortController;
  done: Promise<void>;
};

export type SignalChannelHandle = SignalChannel & {
  /**
   * Tear down every per-name subscription and reject every pending
   * awaiter. Idempotent. After `stop()` the channel holds no
   * substrate watcher handles.
   */
  stop(): Promise<void>;
};

export function createWorkflowHostSignalChannel(
  opts: SignalChannelOpts,
): SignalChannelHandle {
  const awaiters = new Map<string, Awaiter[]>();
  const subscriptions = new Map<string, NameSubscription>();
  let stopped = false;

  function peekState(
    name: string,
  ): { payload: unknown; signalId: string } | null {
    const state = opts.readState();
    const queue = state.unconsumedSignals.get(name);
    if (queue === undefined || queue.length === 0) return null;
    const head = queue[0];
    if (head === undefined) return null;
    // The state-machine `unconsumedSignals` is drained by the
    // runtime body's next `SignalAwaited` reduction, not by the
    // channel. The channel reads the head; the caller commits the
    // SignalAwaited that consumes it. This separation keeps the
    // channel free of state-machine mutation responsibilities.
    return { payload: head.payload, signalId: head.id };
  }

  function matchesAwaiter(
    entry: SubscribeKindEntry<SignalReceivedEnvelope>,
    name: string,
  ): boolean {
    if (entry.runId !== opts.runId) return false;
    if (entry.event.type !== "SignalReceived") return false;
    return entry.event.signalName === name;
  }

  function shiftAwaiter(name: string): Awaiter | null {
    const queue = awaiters.get(name);
    if (queue === undefined || queue.length === 0) return null;
    const head = queue.shift();
    if (head === undefined) return null;
    if (head.signal !== undefined && head.onAbort !== undefined) {
      head.signal.removeEventListener("abort", head.onAbort);
    }
    if (queue.length === 0) awaiters.delete(name);
    return head;
  }

  function awaiterCount(name: string): number {
    const queue = awaiters.get(name);
    if (queue === undefined) return 0;
    return queue.length;
  }

  function startNameSubscription(name: string): void {
    if (subscriptions.has(name)) return;
    const abort = new AbortController();
    // The teardown must run synchronously *before* the awaiter wakes
    // when this loop drains, otherwise the awaiter's continuation
    // (which often calls awaitNext again on the same name) sees the
    // leaked subscription entry and short-circuits in
    // startNameSubscription, stranding the new awaiter on a dead
    // subscribeKind loop. Removing the entry from `subscriptions` and
    // aborting *before* resolving the awaiter keeps the per-name
    // invariant intact across multi-round resolution cycles.
    const teardown = (): void => {
      if (subscriptions.get(name)?.abort === abort) {
        subscriptions.delete(name);
      }
      abort.abort();
    };
    const done = (async () => {
      try {
        const iter = subscribeKind(
          opts.repoStore,
          opts.principal,
          opts.repoId,
          opts.ref,
          SignalReceivedEnvelope,
          {
            signal: abort.signal,
            from: "head",
            kinds: ["SignalReceived"],
          },
        );
        for await (const entry of iter) {
          if (stopped) break;
          if (!matchesAwaiter(entry, name)) continue;
          const observed = opts.readState().observedSignalIds;
          if (observed.has(entry.event.signalId)) continue;
          const next = shiftAwaiter(name);
          if (next === null) {
            // No awaiter left -- the queue was drained between the
            // event landing and this loop reaching it. The reduced
            // state machine queues the event into
            // `unconsumedSignals`; the next `awaitNext` for the same
            // name picks it up via the state-reader path.
            break;
          }
          if (awaiterCount(name) === 0) {
            // Resolving the last awaiter on this subscription. Tear
            // down before the resolve so any awaitNext call inside
            // the awaiter's continuation installs a fresh
            // subscription instead of joining a dead one.
            teardown();
            next.resolve({
              payload: entry.event.payload,
              signalId: entry.event.signalId,
            });
            return;
          }
          next.resolve({
            payload: entry.event.payload,
            signalId: entry.event.signalId,
          });
        }
      } finally {
        // Catches the queue-drained natural break, the `stopped`
        // bail, and any error escaping the loop. The abort path
        // through `stopSubscription` already deleted the Map entry;
        // the get(...)?.abort guard makes this idempotent.
        teardown();
      }
    })();
    subscriptions.set(name, { abort, done });
  }

  async function stopSubscription(name: string): Promise<void> {
    const sub = subscriptions.get(name);
    if (sub === undefined) return;
    subscriptions.delete(name);
    sub.abort.abort();
    await sub.done.catch(() => {
      /* swallow aborted-iterator surface */
    });
  }

  return {
    async deliver(name, payload, signalId) {
      if (stopped) {
        throw new Error("signal channel: deliver after stop");
      }
      const id = signalId ?? opts.newId();
      const at = opts.clock().toISOString();
      const prefix = `runs/${opts.runId}/events/`;
      await opts.repoStore.writeTreePreservingPrefix(
        opts.principal,
        opts.repoId,
        opts.ref,
        {
          preservePrefix: prefix,
          merge: async (existing) => {
            let maxSeq = -1;
            let duplicate = false;
            for (const [filepath, contents] of existing) {
              const fname = filepath.slice(prefix.length);
              const match = /^(0|[1-9][0-9]*)\.json$/.exec(fname);
              if (match === null) continue;
              const seqStr = match[1];
              if (seqStr === undefined) continue;
              const seq = Number.parseInt(seqStr, 10);
              if (seq > maxSeq) maxSeq = seq;
              try {
                const parsed: unknown = JSON.parse(
                  new TextDecoder().decode(contents),
                );
                if (isMatchingSignalId(parsed, id)) {
                  duplicate = true;
                }
              } catch {
                // A corrupt blob is rejected by validatePush at write
                // time. Treat as non-matching here.
              }
            }
            const out: Record<string, string> = {};
            for (const [filepath, contents] of existing) {
              out[filepath] = new TextDecoder().decode(contents);
            }
            if (duplicate) return out;
            const nextSeq = maxSeq + 1;
            // The workflow-run kind handler's `EventEnvelope`
            // validator requires `seq: number` on every event blob;
            // the same `nextSeq` we use to mint the filename also
            // carries into the body so a reader that hydrates the
            // envelope (state-machine resume, audit reads) sees a
            // self-describing event without consulting the filename.
            out[`${prefix}${String(nextSeq)}.json`] = JSON.stringify({
              type: "SignalReceived",
              seq: nextSeq,
              signalName: name,
              signalId: id,
              payload,
              at,
            });
            return out;
          },
          message: `SignalReceived ${id} (${name}) for run ${opts.runId}`,
        },
      );
    },
    async awaitNext(name, signal) {
      if (stopped) {
        throw new Error("signal channel: awaitNext after stop");
      }
      if (signal !== undefined && signal.aborted) {
        throw new Error("aborted");
      }
      // Drain the state-machine queue first. A signal that was
      // committed before the awaiter subscribed lives in
      // `unconsumedSignals` after log replay.
      const queued = peekState(name);
      if (queued !== null) return queued;

      return new Promise<{ payload: unknown; signalId: string }>(
        (resolve, reject) => {
          const awaiter: Awaiter = {
            resolve,
            reject,
            ...(signal !== undefined ? { signal } : {}),
          };
          if (signal !== undefined) {
            const onAbort = (): void => {
              const list = awaiters.get(name);
              if (list !== undefined) {
                const idx = list.indexOf(awaiter);
                if (idx >= 0) list.splice(idx, 1);
                if (list.length === 0) {
                  awaiters.delete(name);
                  void stopSubscription(name);
                }
              }
              reject(new Error("aborted"));
            };
            awaiter.onAbort = onAbort;
            signal.addEventListener("abort", onAbort, { once: true });
          }
          let list = awaiters.get(name);
          if (list === undefined) {
            list = [];
            awaiters.set(name, list);
          }
          list.push(awaiter);
          startNameSubscription(name);
        },
      );
    },
    async stop() {
      if (stopped) return;
      stopped = true;
      for (const list of awaiters.values()) {
        for (const awaiter of list) {
          if (awaiter.signal !== undefined && awaiter.onAbort !== undefined) {
            awaiter.signal.removeEventListener("abort", awaiter.onAbort);
          }
          awaiter.reject(new Error("signal channel stopped"));
        }
      }
      awaiters.clear();
      const names = [...subscriptions.keys()];
      for (const name of names) {
        await stopSubscription(name);
      }
    },
  };
}

function isMatchingSignalId(parsed: unknown, signalId: string): boolean {
  if (typeof parsed !== "object" || parsed === null) return false;
  const obj = parsed as {
    type?: unknown;
    signalId?: unknown;
  };
  if (obj.type !== "SignalReceived") return false;
  return obj.signalId === signalId;
}
