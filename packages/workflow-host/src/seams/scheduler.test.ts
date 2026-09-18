import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { createRepoStore, workflowRunKindHandler } from "@intx/hub-sessions";
import type {
  AuthorizeFn,
  KindHandler,
  Principal,
  RepoId,
  RepoStore,
  ValidatePushResult,
} from "@intx/hub-sessions";

import { createWorkflowHostScheduler } from "./scheduler";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

function permissiveHandler(directoryPrefix: string): KindHandler {
  return {
    // The scheduler test uses an `agent-state`-shaped repo because
    // the workflow-run kind handler is not yet registered. The
    // scheduler does not care about the kind discriminator -- only
    // that the substrate accepts writes under `runs/<runId>/events/`.
    kind: "agent-state",
    directoryPrefix,
    validatePush(): ValidatePushResult {
      return { ok: true };
    },
    onRefUpdated() {
      /* no-op */
    },
  };
}

const allowAll: AuthorizeFn = () => ({ allowed: true });
const principal: Principal = { kind: "test" };
const REF = "refs/heads/main";

/**
 * A timer the test arms and fires. The scheduler decides WHEN from its clock;
 * this decides what actually fires, so a test can observe a queued timer
 * firing without waiting out its delay, and can assert a cancelled one is
 * disarmed rather than arguing from silence after a longer pause.
 */
function createManualTimeouts(): {
  scheduleTimeout: (handler: () => void, ms: number) => () => void;
  armed: () => readonly { ms: number; cancelled: boolean }[];
  fireAll: () => void;
} {
  const entries: { ms: number; handler: () => void; cancelled: boolean }[] = [];
  return {
    scheduleTimeout(handler, ms) {
      const entry = { ms, handler, cancelled: false };
      entries.push(entry);
      return () => {
        entry.cancelled = true;
      };
    },
    armed: () => entries.map((e) => ({ ms: e.ms, cancelled: e.cancelled })),
    fireAll() {
      for (const entry of entries) {
        if (entry.cancelled) continue;
        entry.cancelled = true;
        entry.handler();
      }
    },
  };
}

/**
 * The store the scheduler writes through, wrapped so a test can await the
 * scheduler's TimerFired commit instead of waiting for the blob to appear.
 *
 * `commitTimerFired` is the scheduler's only write and it goes through
 * `writeTreePreservingPrefix`. The substrate materializes the working tree
 * inside that call, before it resolves, so a resolved commit proves the blob
 * `readTimerFiredBlobs` reads off disk is already there.
 *
 * A rejected commit is re-surfaced to the waiter. The scheduler fires a timer
 * from a callback and can only rethrow into an unhandled rejection, so a
 * waiter on the commit alone would sit out the runner's budget over a failure
 * that had already been decided.
 */
function observeSchedulerCommits(store: RepoStore): {
  repoStore: RepoStore;
  whenCommitted: () => Promise<void>;
} {
  type Outcome = { ok: true } | { ok: false; cause: unknown };
  // The first commit is the one every caller here waits on, so a later one
  // does not overwrite an outcome a waiter has not read yet.
  let settled: Outcome | undefined;
  const waiters: ((outcome: Outcome) => void)[] = [];
  function record(outcome: Outcome): void {
    if (settled !== undefined) return;
    settled = outcome;
    for (const resolve of waiters.splice(0)) resolve(outcome);
  }
  return {
    repoStore: {
      ...store,
      async writeTreePreservingPrefix(
        ...args: Parameters<RepoStore["writeTreePreservingPrefix"]>
      ) {
        try {
          const result = await store.writeTreePreservingPrefix(...args);
          record({ ok: true });
          return result;
        } catch (cause) {
          record({ ok: false, cause });
          throw cause;
        }
      },
    },
    async whenCommitted() {
      const outcome =
        settled ??
        (await new Promise<Outcome>((resolve) => {
          waiters.push(resolve);
        }));
      if (!outcome.ok) {
        throw new Error(`scheduler commit rejected: ${String(outcome.cause)}`, {
          cause: outcome.cause,
        });
      }
    },
  };
}

async function seedTimerSet(
  store: ReturnType<typeof createRepoStore>,
  repoId: RepoId,
  runId: string,
  seq: number,
  timerId: string,
  fireAtMs: number,
  extras: { cron?: string } = {},
): Promise<void> {
  const fireAt = new Date(fireAtMs).toISOString();
  const payload: Record<string, unknown> = { timerId, fireAt };
  if (extras.cron !== undefined) payload.cron = extras.cron;
  await store.writeTree(principal, repoId, REF, {
    files: {
      [`runs/${runId}/events/${String(seq)}.json`]: JSON.stringify({
        seq,
        type: "TimerSet",
        ...payload,
      }),
    },
    message: `seed TimerSet ${timerId}`,
  });
}

async function readTimerFiredBlobs(
  dir: string,
  runId: string,
): Promise<{ seq: number; bodySeq: number | undefined; timerId: string }[]> {
  const eventsDir = path.join(dir, "runs", runId, "events");
  const entries = await fs.promises.readdir(eventsDir);
  const out: { seq: number; bodySeq: number | undefined; timerId: string }[] =
    [];
  for (const name of entries) {
    const match = /^(0|[1-9][0-9]*)\.json$/.exec(name);
    if (match === null) continue;
    const seqStr = match[1];
    if (seqStr === undefined) continue;
    const raw = await fs.promises.readFile(path.join(eventsDir, name), "utf8");
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null) continue;
    const obj = parsed as {
      seq?: unknown;
      type?: unknown;
      timerId?: unknown;
    };
    if (obj.type !== "TimerFired") continue;
    const timerId = obj.timerId;
    if (typeof timerId !== "string") continue;
    const bodySeq = typeof obj.seq === "number" ? obj.seq : undefined;
    out.push({ seq: Number.parseInt(seqStr, 10), bodySeq, timerId });
  }
  return out;
}

describe("workflow-host scheduler", () => {
  test(
    "fires a queued one-shot timer and commits a TimerFired blob after the TimerSet",
    async () => {
      const dataDir = await makeTempDir("scheduler-oneshot-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          "agent-state": permissiveHandler("workflow-runs-oneshot"),
        },
        authorize: allowAll,
      });
      const repoId: RepoId = { kind: "agent-state", id: "deployment-a" };
      const runId = "r1";

      const fireAtMs = Date.now() + 40;
      await seedTimerSet(store, repoId, runId, 0, "t-oneshot", fireAtMs);

      const timeouts = createManualTimeouts();
      const commits = observeSchedulerCommits(store);
      const scheduler = createWorkflowHostScheduler({
        repoStore: commits.repoStore,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
        scheduleTimeout: timeouts.scheduleTimeout,
      });
      try {
        await scheduler.start();
        // The recovery walk queued the timer, which armed one timeout.
        const queued = scheduler.queuedTimers();
        expect(queued).toHaveLength(1);
        expect(queued[0]?.timerId).toBe("t-oneshot");
        expect(timeouts.armed()).toHaveLength(1);

        // Fire the armed timer and await the commit it makes, instead of
        // waiting out its delay plus however long the commit takes.
        timeouts.fireAll();
        await commits.whenCommitted();

        const fired = await readTimerFiredBlobs(
          store.getRepoDir(repoId),
          runId,
        );
        expect(fired).toHaveLength(1);
        expect(fired[0]?.timerId).toBe("t-oneshot");
        // TimerFired must land at a strictly later seq than TimerSet.
        expect(fired[0]?.seq).toBeGreaterThan(0);
        // The envelope's body `seq` must match the filename's seq so
        // the workflow-run kind handler's validatePush accepts the
        // commit.
        expect(fired[0]?.bodySeq).toBe(fired[0]?.seq);
      } finally {
        await scheduler.stop();
      }
    },
    { timeout: 5000 },
  );

  test(
    "recovery enumerates unfired timers; matched TimerFired skips re-queue",
    async () => {
      const dataDir = await makeTempDir("scheduler-recovery-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          "agent-state": permissiveHandler("workflow-runs-recovery"),
        },
        authorize: allowAll,
      });
      const repoId: RepoId = { kind: "agent-state", id: "deployment-b" };
      const runId = "r1";

      const farFuture = Date.now() + 60_000;
      // Two TimerSets; the second is matched by a TimerFired and
      // should NOT be re-queued.
      await seedTimerSet(store, repoId, runId, 0, "t-unfired", farFuture);
      await seedTimerSet(store, repoId, runId, 1, "t-fired", farFuture);
      await store.writeTree(principal, repoId, REF, {
        files: {
          [`runs/${runId}/events/2.json`]: JSON.stringify({
            seq: 2,
            type: "TimerFired",
            timerId: "t-fired",
          }),
        },
        message: "TimerFired t-fired",
      });

      const scheduler = createWorkflowHostScheduler({
        repoStore: store,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
      });
      try {
        await scheduler.start();
        const queued = scheduler.queuedTimers();
        expect(queued).toHaveLength(1);
        expect(queued[0]?.timerId).toBe("t-unfired");
      } finally {
        await scheduler.stop();
      }
    },
    { timeout: 5000 },
  );

  test(
    "recovery reads committed state, not the working tree",
    async () => {
      // The invariant: recovery reconstructs its ledger from the git
      // object store, so a committed TimerFired excludes its timer even
      // when the materialized working tree does not reflect it. We force
      // the hardest form of that divergence -- the working tree is gone
      // entirely while the object store is intact -- so a working-tree
      // read would recover nothing, and a committed read must recover the
      // full ledger.
      const dataDir = await makeTempDir("scheduler-recovery-committed-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          "agent-state": permissiveHandler("workflow-runs-committed"),
        },
        authorize: allowAll,
      });
      const repoId: RepoId = { kind: "agent-state", id: "deployment-d" };
      const runId = "r1";

      const farFuture = Date.now() + 60_000;
      await seedTimerSet(store, repoId, runId, 0, "t-unfired", farFuture);
      await seedTimerSet(store, repoId, runId, 1, "t-fired", farFuture);
      await store.writeTree(principal, repoId, REF, {
        files: {
          [`runs/${runId}/events/2.json`]: JSON.stringify({
            seq: 2,
            type: "TimerFired",
            timerId: "t-fired",
          }),
        },
        message: "TimerFired t-fired",
      });

      // Diverge the working tree from the committed object store: remove
      // the materialized checkout while leaving `.git` intact. Only a
      // committed read survives this.
      await fs.promises.rm(path.join(store.getRepoDir(repoId), "runs"), {
        recursive: true,
        force: true,
      });

      const scheduler = createWorkflowHostScheduler({
        repoStore: store,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
      });
      try {
        await scheduler.start();
        const queued = scheduler.queuedTimers();
        const ids = queued.map((q) => q.timerId);
        // The fired timer's committed TimerFired excludes it...
        expect(ids).not.toContain("t-fired");
        // ...and the unfired timer is still recovered from committed
        // state, proving the read walked real events rather than the
        // wiped working tree.
        expect(ids).toEqual(["t-unfired"]);
      } finally {
        await scheduler.stop();
      }
    },
    { timeout: 5000 },
  );

  test(
    "recovery skips a compacted run with no events subtree",
    async () => {
      const dataDir = await makeTempDir("scheduler-recovery-compacted-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          "agent-state": permissiveHandler("workflow-runs-compacted"),
        },
        authorize: allowAll,
      });
      const repoId: RepoId = { kind: "agent-state", id: "deployment-e" };

      const farFuture = Date.now() + 60_000;
      await seedTimerSet(store, repoId, "r1", 0, "t-live", farFuture);
      // A terminated run is sealed into a combined `events.jsonl` with no
      // `events/` subtree. Recovery walks per-event blobs only, so it must
      // skip such a run rather than choke on the missing subtree.
      await store.writeTree(principal, repoId, REF, {
        files: {
          "runs/r2/events.jsonl": JSON.stringify({
            seq: 0,
            type: "TimerSet",
            timerId: "t-sealed",
            fireAt: new Date(farFuture).toISOString(),
          }),
        },
        message: "seal r2",
      });

      const scheduler = createWorkflowHostScheduler({
        repoStore: store,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
      });
      try {
        await scheduler.start();
        const ids = scheduler.queuedTimers().map((q) => q.timerId);
        expect(ids).toEqual(["t-live"]);
      } finally {
        await scheduler.stop();
      }
    },
    { timeout: 5000 },
  );

  test(
    "recovery throws on an unparseable event blob",
    async () => {
      const dataDir = await makeTempDir("scheduler-recovery-badjson-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          "agent-state": permissiveHandler("workflow-runs-badjson"),
        },
        authorize: allowAll,
      });
      const repoId: RepoId = { kind: "agent-state", id: "deployment-f" };

      await store.writeTree(principal, repoId, REF, {
        files: { "runs/r1/events/0.json": "{ not json" },
        message: "corrupt blob",
      });

      const scheduler = createWorkflowHostScheduler({
        repoStore: store,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
      });
      try {
        await expect(scheduler.start()).rejects.toThrow(/cannot parse/);
      } finally {
        await scheduler.stop();
      }
    },
    { timeout: 5000 },
  );

  test(
    "recovery throws on an illegal event filename",
    async () => {
      const dataDir = await makeTempDir("scheduler-recovery-badname-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          "agent-state": permissiveHandler("workflow-runs-badname"),
        },
        authorize: allowAll,
      });
      const repoId: RepoId = { kind: "agent-state", id: "deployment-g" };

      // A foreign name under events/ is corruption; recovery surfaces it
      // rather than silently dropping the blob.
      await store.writeTree(principal, repoId, REF, {
        files: {
          "runs/r1/events/not-a-seq.json": JSON.stringify({
            type: "TimerSet",
            timerId: "x",
            fireAt: new Date().toISOString(),
          }),
        },
        message: "bad filename",
      });

      const scheduler = createWorkflowHostScheduler({
        repoStore: store,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
      });
      try {
        await expect(scheduler.start()).rejects.toThrow(
          /event_filename_invalid/,
        );
      } finally {
        await scheduler.stop();
      }
    },
    { timeout: 5000 },
  );

  test(
    "cron-style TimerSet whose fireAt is in the past is skipped on recovery",
    async () => {
      const dataDir = await makeTempDir("scheduler-cron-skip-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          "agent-state": permissiveHandler("workflow-runs-cron"),
        },
        authorize: allowAll,
      });
      const repoId: RepoId = { kind: "agent-state", id: "deployment-c" };
      const runId = "r1";

      const inThePast = Date.now() - 60_000;
      await seedTimerSet(store, repoId, runId, 0, "t-cron-missed", inThePast, {
        cron: "*/5 * * * *",
      });
      // A non-cron timer in the past is queued (and fires immediately
      // -- recovery for one-shots replays even past fireAt).
      const oneShotPast = Date.now() - 100;
      await seedTimerSet(
        store,
        repoId,
        runId,
        1,
        "t-oneshot-past",
        oneShotPast,
      );

      const scheduler = createWorkflowHostScheduler({
        repoStore: store,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
      });
      try {
        await scheduler.start();
        const queued = scheduler.queuedTimers();
        const ids = queued.map((q) => q.timerId);
        expect(ids).not.toContain("t-cron-missed");
        expect(ids).toContain("t-oneshot-past");
      } finally {
        await scheduler.stop();
      }
    },
    { timeout: 5000 },
  );

  test(
    "stop() cancels every queued timer and prevents pending TimerFired commits",
    async () => {
      const dataDir = await makeTempDir("scheduler-stop-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          "agent-state": permissiveHandler("workflow-runs-stop"),
        },
        authorize: allowAll,
      });
      const repoId: RepoId = { kind: "agent-state", id: "deployment-d" };
      const runId = "r1";

      const fireAtMs = Date.now() + 1_000;
      await seedTimerSet(store, repoId, runId, 0, "t-stopped", fireAtMs);

      const timeouts = createManualTimeouts();
      const scheduler = createWorkflowHostScheduler({
        repoStore: store,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
        scheduleTimeout: timeouts.scheduleTimeout,
      });
      await scheduler.start();
      expect(scheduler.queuedTimers()).toHaveLength(1);
      await scheduler.stop();
      expect(scheduler.queuedTimers()).toHaveLength(0);

      // The cancel is the observable fact: stop() disarms the timer it
      // queued. Waiting longer than the delay and finding no blob argued the
      // same thing from silence, and would have argued it just as
      // convincingly had the timer been armed but slow.
      // Not the delay: that is `fireAt` minus the clock at arming time, so
      // asserting it would be asserting how long the lines above took.
      expect(timeouts.armed()).toHaveLength(1);
      expect(timeouts.armed()[0]?.cancelled).toBe(true);

      // Firing anyway must still commit nothing.
      timeouts.fireAll();
      const fired = await readTimerFiredBlobs(
        store.getRepoDir(repoId),
        runId,
      ).catch(() => []);
      expect(fired).toHaveLength(0);
    },
    { timeout: 5000 },
  );

  test(
    "live ingest: a TimerSet committed after start() fires without restart",
    async () => {
      const dataDir = await makeTempDir("scheduler-live-ingest-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          "agent-state": permissiveHandler("workflow-runs-live"),
        },
        authorize: allowAll,
      });
      const repoId: RepoId = { kind: "agent-state", id: "deployment-live" };
      const runId = "r-live";

      // Seed the run's events tree so it exists at start-time. The
      // scheduler's recovery walk picks up nothing (no TimerSet yet);
      // the live subscription must catch the post-start commit.
      await store.writeTree(principal, repoId, REF, {
        files: {
          [`runs/${runId}/events/0.json`]: JSON.stringify({
            seq: 0,
            type: "RunStarted",
          }),
        },
        message: "RunStarted",
      });

      // This test keeps the production `scheduleTimeout` -- the global timer
      // `createWorkflowHostScheduler` falls back to -- so the arming path the
      // host actually runs stays covered; its siblings drive the seam. The
      // delay still decides nothing, because the wait below is on the commit.
      const commits = observeSchedulerCommits(store);
      const scheduler = createWorkflowHostScheduler({
        repoStore: commits.repoStore,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
      });
      try {
        await scheduler.start();
        // No TimerSet has been committed yet.
        expect(scheduler.queuedTimers()).toHaveLength(0);

        // Commit a TimerSet against the running scheduler. The live
        // subscribeKind loop must pick it up and enqueue it.
        const fireAtMs = Date.now() + 40;
        await store.writeTree(principal, repoId, REF, {
          files: {
            [`runs/${runId}/events/1.json`]: JSON.stringify({
              seq: 1,
              type: "TimerSet",
              timerId: "t-live",
              fireAt: new Date(fireAtMs).toISOString(),
            }),
          },
          message: "TimerSet t-live",
        });

        // The TimerFired commit is the proof the live subscription ingested
        // the TimerSet and the timer fired, all without a restart. Awaiting
        // it covers the subscribe-notify latency, the timer's delay, and the
        // commit itself, with no number standing in for any of them.
        await commits.whenCommitted();
        const fired = await readTimerFiredBlobs(
          store.getRepoDir(repoId),
          runId,
        );
        expect(fired).toHaveLength(1);
        expect(fired[0]?.timerId).toBe("t-live");
        expect(fired[0]?.bodySeq).toBe(fired[0]?.seq);
      } finally {
        await scheduler.stop();
      }
    },
    { timeout: 5000 },
  );
});

describe("workflow-host scheduler against workflowRunKindHandler", () => {
  test(
    "TimerFired commit carries top-level seq and is accepted by validatePush",
    async () => {
      const dataDir = await makeTempDir("scheduler-workflow-run-");
      const hubPrincipal: Principal = { kind: "hub" };
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          "workflow-run": workflowRunKindHandler,
        },
        authorize: allowAll,
      });
      const repoId: RepoId = { kind: "workflow-run", id: "deployment-real" };
      const runId = "r1";

      // The seed lands at events/0.json. The body's `seq` matches the
      // filename's seq -- this is the contract the workflow-run kind
      // handler enforces and the contract the scheduler's TimerFired
      // write must also honour.
      const fireAtMs = Date.now() + 40;
      await store.writeTree(hubPrincipal, repoId, REF, {
        files: {
          ".gitignore": "",
          [`runs/${runId}/events/0.json`]: JSON.stringify({
            seq: 0,
            type: "TimerSet",
            timerId: "t-real",
            fireAt: new Date(fireAtMs).toISOString(),
          }),
        },
        message: "seed TimerSet against real handler",
      });

      // The scheduler's commit runs from a timer callback, so a validatePush
      // refusal reaches the runtime only as an unhandled rejection. Capture
      // it: `whenCommitted` below reports the refusal as the test's failure,
      // and this keeps the same refusal from crashing the runner as well.
      const captured: unknown[] = [];
      const handler = (reason: unknown) => {
        captured.push(reason);
      };
      process.on("unhandledRejection", handler);

      const timeouts = createManualTimeouts();
      const commits = observeSchedulerCommits(store);
      const scheduler = createWorkflowHostScheduler({
        repoStore: commits.repoStore,
        principal: hubPrincipal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
        scheduleTimeout: timeouts.scheduleTimeout,
      });
      try {
        await scheduler.start();
        // The recovery walk queued the seeded TimerSet, which armed one
        // timeout. Fire it and await the commit, rather than waiting out its
        // delay plus however long the real handler's validatePush takes.
        expect(timeouts.armed()).toHaveLength(1);
        timeouts.fireAll();
        await commits.whenCommitted();

        const fired = await readTimerFiredBlobs(
          store.getRepoDir(repoId),
          runId,
        );
        expect(fired).toHaveLength(1);
        expect(fired[0]?.timerId).toBe("t-real");
        // TimerFired must land at a seq strictly greater than the
        // TimerSet that triggered it.
        expect(fired[0]?.seq).toBeGreaterThan(0);
        expect(fired[0]?.bodySeq).toBe(fired[0]?.seq);

        // The blob's body must carry the top-level `seq` matching the
        // filename's seq, per the workflow-run kind handler's
        // EventEnvelope contract.
        const eventsDir = path.join(
          store.getRepoDir(repoId),
          "runs",
          runId,
          "events",
        );
        const firedName = `${String(fired[0]?.seq)}.json`;
        const raw = await fs.promises.readFile(
          path.join(eventsDir, firedName),
          "utf8",
        );
        const parsed: { seq?: unknown; type?: unknown } = JSON.parse(raw);
        expect(parsed.type).toBe("TimerFired");
        expect(parsed.seq).toBe(fired[0]?.seq);
      } finally {
        await scheduler.stop();
        process.off("unhandledRejection", handler);
      }
    },
    { timeout: 5000 },
  );
});
