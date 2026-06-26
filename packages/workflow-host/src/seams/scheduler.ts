// Production workflow-host scheduler (Seam 1, event-sourced wait).
//
// The scheduler is a singleton per host process. It is the single
// writer of `TimerFired` to every active deployment's workflow-run
// log. The runtime body's `waitForTimer` helper subscribes to the
// run's log via `subscribeKind` and resolves when this scheduler
// commits the matching `TimerFired` event. No other component in
// the host -- workflow-process child, supervisor, sidecar handler --
// commits `TimerFired`. This single-writer invariant is what makes
// the recovery story work: at startup the scheduler walks the
// persisted log for unfired `TimerSet` events and re-queues them,
// confident that any `TimerFired` it does not find was never
// committed and is its responsibility to commit now.
//
// Recovery semantics:
//   - One-shot timers (`TimerSet` without a `cron` discriminator) on
//     resume are queued with their stored `fireAt`. If `fireAt` is
//     already in the past the scheduler fires immediately so the
//     paired `TimerFired` lands and the awaiting runtime body
//     unblocks. The runtime is responsible for its own jitter
//     tolerance.
//   - Cron-style timers whose `fireAt` is in the past on resume are
//     SKIPPED -- per the spec, missed cron ticks do not replay. The
//     scheduler waits for the next future tick to be committed and
//     queues that one.
//
// The scheduler reads workflow-event blobs at the canonical layout
// `runs/<runId>/events/<seq>.json` and writes a fresh blob at the
// next-seq slot for each `TimerFired` commit. The blob envelope
// carries `{ seq, type, data }` at the top level: `seq` is the
// integer that also appears in the filename, `type` is the
// `subscribeKind` discriminator the scheduler narrows on, and `data`
// carries the timer payload. The workflow-run kind handler's
// `validatePush` enforces that every event blob's body `seq` matches
// the filename's seq, so the scheduler mints the next seq inside the
// `writeTreePreservingPrefix` merge step and writes both into the
// envelope and the filename.

import { type } from "arktype";

import type { Principal, RepoId, RepoStore } from "@intx/hub-sessions";
import { subscribeKind } from "@intx/hub-sessions";

/**
 * Substrate-shape envelope for the workflow-event blob committed to
 * `runs/<runId>/events/<seq>.json`. The validator covers the two
 * event types the scheduler reads (TimerSet, TimerFired) and the
 * single type it writes (TimerFired). Non-timer blobs at the same
 * path prefix do not match this validator and are skipped silently
 * by the recovery walk. `seq` mirrors the integer in the filename,
 * matching the workflow-run kind handler's `EventEnvelope` contract.
 */
export const TimerEventEnvelope = type({
  seq: "number >= 0",
  type: "'TimerSet' | 'TimerFired'",
  data: {
    timerId: "string",
    "fireAt?": "string",
    "stepId?": "string | null",
    "cron?": "string | null",
  },
});
export type TimerEventEnvelope = typeof TimerEventEnvelope.infer;

export type SchedulerOpts = {
  /**
   * Substrate handle the scheduler reads from and writes to. The
   * caller wires this against the workflow-run kind handler's
   * registered substrate -- the scheduler does not care about the
   * kind discriminator, but the handler's `validatePush` must accept
   * the `runs/<runId>/events/<seq>.json` writes the scheduler emits.
   */
  repoStore: RepoStore;
  /**
   * Principal the scheduler presents to the substrate. The substrate
   * gates every operation behind `authorize`; the scheduler's
   * principal must be granted `writeTree` and `resolveRef` against
   * the workflow-run repos it services.
   */
  principal: Principal;
  /**
   * Callback that enumerates the active deployment workflow-run
   * repos the scheduler is responsible for. The scheduler invokes
   * this at `start()` time to seed its recovery walk and again on
   * every `TimerFired` commit to attribute the runId to its owning
   * deployment.
   */
  listActiveDeployments: () => Promise<readonly RepoId[]> | readonly RepoId[];
  /**
   * Events ref to tail. The workflow-run repo layout pins all
   * `runs/<runId>/events/` blobs under a single moving ref. Callers
   * typically supply `"refs/heads/main"`.
   */
  ref: string;
  /**
   * Clock used to compute delay-from-fireAt for queueing setTimeout
   * callbacks and to skip past-due cron entries on recovery.
   */
  clock: () => Date;
};

type QueuedTimer = {
  runId: string;
  timerId: string;
  fireAtMs: number;
  timeout: ReturnType<typeof setTimeout>;
  cron: boolean;
};

export type SchedulerHandle = {
  /**
   * Run start-time recovery against every active deployment. After
   * the recovery walk completes, every unfired one-shot timer is
   * queued and every missed cron tick has been skipped per the spec.
   * Idempotent: a second `start()` is a no-op while the first is
   * still pending.
   */
  start(): Promise<void>;
  /**
   * Tear down every queued timer. Idempotent. Outstanding
   * `setTimeout` handles are cancelled. After `stop()` the scheduler
   * holds no host resources.
   */
  stop(): Promise<void>;
  /**
   * Cancel any queued timer matching `(runId, timerId)`. The
   * runtime-shaped `Scheduler.scheduleIn` returns a disposer the
   * runtime body invokes when the awaiting site settles on a sibling
   * event before the timer's deadline; the adapter routes that
   * disposer here so the host scheduler does not commit a
   * `TimerFired` after the runtime has moved on. Idempotent: a call
   * for an unknown key is a no-op.
   */
  cancelQueued(runId: string, timerId: string): void;
  /**
   * Test-visible view of currently-queued timers. The shape is
   * intentionally narrow: the runtime body never inspects the
   * scheduler's queue; tests assert on it directly.
   */
  queuedTimers(): readonly {
    runId: string;
    timerId: string;
    fireAtMs: number;
  }[];
};

export function createWorkflowHostScheduler(
  opts: SchedulerOpts,
): SchedulerHandle {
  const queues = new Map<string, QueuedTimer>();
  const liveSubscriptions: {
    abort: AbortController;
    done: Promise<void>;
  }[] = [];
  let started = false;
  let stopped = false;

  function queueKey(runId: string, timerId: string): string {
    return `${runId} ${timerId}`;
  }

  async function fireTimer(runId: string, timerId: string): Promise<void> {
    const key = queueKey(runId, timerId);
    const entry = queues.get(key);
    if (entry === undefined) return;
    queues.delete(key);
    if (stopped) return;
    await commitTimerFired(opts, runId, timerId);
  }

  function enqueue(
    repoId: RepoId,
    runId: string,
    timerId: string,
    fireAtMs: number,
    cron: boolean,
  ): void {
    if (stopped) return;
    const key = queueKey(runId, timerId);
    if (queues.has(key)) return; // idempotent
    const delayMs = Math.max(0, fireAtMs - opts.clock().getTime());
    const timeout = setTimeout(() => {
      void fireTimer(runId, timerId).catch((cause) => {
        // The scheduler's commit failed. Surface as unhandled so
        // operators see it; the runtime body's awaiter will hang
        // until restart triggers recovery.
        throw cause instanceof Error
          ? cause
          : new Error(
              `scheduler ${String(repoId.id)}/${runId}/${timerId} commit failed: ${String(cause)}`,
            );
      });
    }, delayMs);
    queues.set(key, { runId, timerId, fireAtMs, timeout, cron });
  }

  function startLiveSubscription(repoId: RepoId): void {
    const abort = new AbortController();
    const done = (async () => {
      const iter = subscribeKind(
        opts.repoStore,
        opts.principal,
        repoId,
        opts.ref,
        TimerEventEnvelope,
        {
          signal: abort.signal,
          from: "head",
          kinds: ["TimerSet"],
        },
      );
      for await (const entry of iter) {
        if (stopped) break;
        if (entry.event.type !== "TimerSet") continue;
        const fireAt = entry.event.data.fireAt;
        if (fireAt === undefined) {
          throw new Error(
            `scheduler live ingest: TimerSet in ${String(repoId.id)} run ${entry.runId} timer ${entry.event.data.timerId} missing fireAt`,
          );
        }
        const fireAtMs = Date.parse(fireAt);
        if (Number.isNaN(fireAtMs)) {
          throw new Error(
            `scheduler live ingest: TimerSet in ${String(repoId.id)} run ${entry.runId} timer ${entry.event.data.timerId} fireAt unparseable: ${fireAt}`,
          );
        }
        const cron =
          entry.event.data.cron !== undefined && entry.event.data.cron !== null;
        if (cron && fireAtMs < opts.clock().getTime()) {
          // Same missed-cron-tick spec as recovery: a cron TimerSet
          // whose fireAt is in the past on arrival is dropped.
          continue;
        }
        enqueue(repoId, entry.runId, entry.event.data.timerId, fireAtMs, cron);
      }
    })();
    liveSubscriptions.push({ abort, done });
  }

  async function recoverDeployment(repoId: RepoId): Promise<void> {
    const events = await readAllEvents(opts, repoId);
    // Build per-(runId, timerId) ledger: a TimerSet without a
    // matching TimerFired is unfired. The walk is order-insensitive
    // because the second pass deletes matched entries.
    const unfired = new Map<
      string,
      { runId: string; timerId: string; fireAtMs: number; cron: boolean }
    >();
    for (const e of events) {
      if (e.envelope.type === "TimerSet") {
        const fireAt = e.envelope.data.fireAt;
        if (fireAt === undefined) {
          throw new Error(
            `scheduler recovery: TimerSet in ${String(repoId.id)} run ${e.runId} timer ${e.envelope.data.timerId} missing fireAt`,
          );
        }
        const fireAtMs = Date.parse(fireAt);
        if (Number.isNaN(fireAtMs)) {
          throw new Error(
            `scheduler recovery: TimerSet in ${String(repoId.id)} run ${e.runId} timer ${e.envelope.data.timerId} fireAt unparseable: ${fireAt}`,
          );
        }
        const cron =
          e.envelope.data.cron !== undefined && e.envelope.data.cron !== null;
        unfired.set(`${e.runId} ${e.envelope.data.timerId}`, {
          runId: e.runId,
          timerId: e.envelope.data.timerId,
          fireAtMs,
          cron,
        });
      } else {
        unfired.delete(`${e.runId} ${e.envelope.data.timerId}`);
      }
    }
    const now = opts.clock().getTime();
    for (const entry of unfired.values()) {
      if (entry.cron && entry.fireAtMs < now) {
        // Spec: missed cron ticks are skipped on resume. The next
        // cron tick will be committed by whoever owns the cron
        // emitter; the scheduler simply does not replay this one.
        continue;
      }
      enqueue(repoId, entry.runId, entry.timerId, entry.fireAtMs, entry.cron);
    }
  }

  return {
    async start() {
      if (started) return;
      started = true;
      const repoIds = await opts.listActiveDeployments();
      for (const repoId of repoIds) {
        await recoverDeployment(repoId);
      }
      // Live `TimerSet` ingestion. After the recovery walk, open a
      // per-deployment `subscribeKind` loop against the workflow-run
      // events ref with `from: "head"` and `kinds: ["TimerSet"]`. Each
      // yielded entry carries its owning runId (the substrate's
      // `SubscribeKindEntry` surfaces the path-derived runId
      // alongside the workflow-event seq), and the TimerSet payload
      // carries the wall-clock `fireAt` plus the optional `cron`
      // discriminator. `enqueue` is idempotent on `(runId, timerId)`,
      // so a TimerSet that the recovery walk already queued (i.e.
      // committed before subscribe could install its watcher) is
      // safely re-yielded without duplicating the queue entry.
      // `TimerFired` is not in the `kinds` filter: the scheduler
      // commits `TimerFired` itself and does not need to ingest its
      // own writes.
      for (const repoId of repoIds) {
        startLiveSubscription(repoId);
      }
    },
    async stop() {
      if (stopped) return;
      stopped = true;
      for (const t of queues.values()) clearTimeout(t.timeout);
      queues.clear();
      for (const sub of liveSubscriptions.splice(0)) {
        sub.abort.abort();
        await sub.done.catch(() => {
          /* swallow aborted-iterator surface */
        });
      }
    },
    cancelQueued(runId, timerId) {
      const key = queueKey(runId, timerId);
      const entry = queues.get(key);
      if (entry === undefined) return;
      clearTimeout(entry.timeout);
      queues.delete(key);
    },
    queuedTimers() {
      return [...queues.values()].map((t) => ({
        runId: t.runId,
        timerId: t.timerId,
        fireAtMs: t.fireAtMs,
      }));
    },
  };
}

type EventReadEntry = {
  runId: string;
  envelope: TimerEventEnvelope;
};

/**
 * Read every timer-event blob across every run under the given
 * workflow-run repo. The recovery walk reads blobs from the
 * substrate's on-disk working tree directly via `enumerateEventBlobs`:
 * `subscribeKind` is a diff-shaped iterator over new commits, not a
 * "list everything at HEAD" primitive, so the startup ledger needs a
 * path-aware enumeration of the current ref tip rather than a tail
 * subscription. The substrate writes commit-then-checkout for every
 * ref-update, so the working tree is a coherent snapshot of the
 * current ref tip.
 */
async function readAllEvents(
  opts: SchedulerOpts,
  repoId: RepoId,
): Promise<readonly EventReadEntry[]> {
  const entries: EventReadEntry[] = [];
  const records = await enumerateEventBlobs(opts, repoId);
  for (const r of records) {
    const env = TimerEventEnvelope(r.payload);
    if (env instanceof type.errors) {
      // A non-timer event blob (StepStarted, RunStarted, ...) at the
      // same path prefix is expected -- the scheduler skips it
      // silently. A timer-shaped blob whose narrow fails is a
      // substrate-level invariant violation; the kind handler's
      // validatePush is the layer that should catch it. Here we skip
      // to avoid crashing the scheduler over a single bad blob; a
      // separate audit pass surfaces the integrity problem.
      continue;
    }
    entries.push({ runId: r.runId, envelope: env });
  }
  return entries;
}

type RawEventRecord = {
  runId: string;
  payload: unknown;
};

/**
 * Walk the workflow-run repo's `runs/<runId>/events/<seq>.json`
 * subtree at the current ref tip and return every event blob's
 * payload, attributed to its run. A terminated run whose events have
 * been compacted into a combined `events.jsonl` (no `events/` subtree)
 * is skipped: this walk recovers pending timers, and a terminated run
 * has none.
 */
async function enumerateEventBlobs(
  opts: SchedulerOpts,
  repoId: RepoId,
): Promise<readonly RawEventRecord[]> {
  const dir = opts.repoStore.getRepoDir(repoId);
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const runsDir = path.join(dir, "runs");
  const out: RawEventRecord[] = [];
  let runEntries: string[];
  try {
    runEntries = await fs.readdir(runsDir);
  } catch (cause) {
    if (isErrnoNotFound(cause)) return out;
    throw cause;
  }
  for (const runId of runEntries) {
    const eventsDir = path.join(runsDir, runId, "events");
    let blobs: string[];
    try {
      blobs = await fs.readdir(eventsDir);
    } catch (cause) {
      if (isErrnoNotFound(cause)) continue;
      throw cause;
    }
    for (const blob of blobs) {
      if (!/^(0|[1-9][0-9]*)\.json$/.test(blob)) continue;
      const raw = await fs.readFile(path.join(eventsDir, blob), "utf8");
      let parsed: unknown;
      try {
        parsed = JSON.parse(raw);
      } catch (cause) {
        throw new Error(
          `scheduler recovery: cannot parse ${String(repoId.id)}/${runId}/events/${blob}: ${String(cause)}`,
        );
      }
      out.push({ runId, payload: parsed });
    }
  }
  return out;
}

function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  const code = (cause as { code?: unknown }).code;
  return code === "ENOENT";
}

/**
 * Commit a `TimerFired` event blob to the workflow-run repo. The
 * commit goes through `writeTreePreservingPrefix` so concurrent
 * commits on the same runId's events subtree serialize at the
 * substrate's per-repo lock. Idempotent: if a TimerFired for the
 * same `(runId, timerId)` already exists at the prefix, the merge
 * returns the existing tree unchanged.
 */
async function commitTimerFired(
  opts: SchedulerOpts,
  runId: string,
  timerId: string,
): Promise<void> {
  const owningRepoId = await findOwningDeployment(opts, runId);
  if (owningRepoId === undefined) {
    throw new Error(
      `scheduler commit: cannot find deployment owning run ${runId}`,
    );
  }
  const prefix = `runs/${runId}/events/`;
  await opts.repoStore.writeTreePreservingPrefix(
    opts.principal,
    owningRepoId,
    opts.ref,
    {
      preservePrefix: prefix,
      merge: async (existing) => {
        let maxSeq = -1;
        let alreadyFired = false;
        for (const [filepath, contents] of existing) {
          const name = filepath.slice(prefix.length);
          const match = /^(0|[1-9][0-9]*)\.json$/.exec(name);
          if (match === null) continue;
          const seqStr = match[1];
          if (seqStr === undefined) continue;
          const seq = Number.parseInt(seqStr, 10);
          if (seq > maxSeq) maxSeq = seq;
          try {
            const parsed: unknown = JSON.parse(
              new TextDecoder().decode(contents),
            );
            if (isMatchingTimerFired(parsed, timerId)) {
              alreadyFired = true;
            }
          } catch {
            // Skip on parse failure -- a corrupt blob would have
            // been rejected by validatePush at write time; treat as
            // a non-matching entry.
          }
        }
        const out: Record<string, string> = {};
        for (const [filepath, contents] of existing) {
          out[filepath] = new TextDecoder().decode(contents);
        }
        if (alreadyFired) return out;
        const nextSeq = maxSeq + 1;
        out[`${prefix}${String(nextSeq)}.json`] = JSON.stringify({
          seq: nextSeq,
          type: "TimerFired",
          data: { timerId },
        });
        return out;
      },
      message: `TimerFired ${timerId} for run ${runId}`,
    },
  );
}

function isMatchingTimerFired(parsed: unknown, timerId: string): boolean {
  if (typeof parsed !== "object" || parsed === null) return false;
  const obj = parsed as { type?: unknown; data?: { timerId?: unknown } };
  if (obj.type !== "TimerFired") return false;
  if (obj.data === undefined) return false;
  return obj.data.timerId === timerId;
}

async function findOwningDeployment(
  opts: SchedulerOpts,
  runId: string,
): Promise<RepoId | undefined> {
  const repoIds = await opts.listActiveDeployments();
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  for (const repoId of repoIds) {
    const dir = opts.repoStore.getRepoDir(repoId);
    try {
      await fs.access(path.join(dir, "runs", runId, "events"));
      return repoId;
    } catch (cause) {
      if (isErrnoNotFound(cause)) continue;
      throw cause;
    }
  }
  return undefined;
}
