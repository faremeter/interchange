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
        data: payload,
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
      data?: { timerId?: unknown };
    };
    if (obj.type !== "TimerFired") continue;
    if (obj.data === undefined) continue;
    const timerId = obj.data.timerId;
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

      const scheduler = createWorkflowHostScheduler({
        repoStore: store,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
      });
      try {
        await scheduler.start();
        // The recovery walk queued the timer.
        const queued = scheduler.queuedTimers();
        expect(queued).toHaveLength(1);
        expect(queued[0]?.timerId).toBe("t-oneshot");

        // Wait past the fireAt + commit latency.
        await new Promise<void>((resolve) => {
          setTimeout(resolve, 250);
        });

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
            data: { timerId: "t-fired" },
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

      const scheduler = createWorkflowHostScheduler({
        repoStore: store,
        principal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
      });
      await scheduler.start();
      expect(scheduler.queuedTimers()).toHaveLength(1);
      await scheduler.stop();
      expect(scheduler.queuedTimers()).toHaveLength(0);

      // Give the previously-queued setTimeout time to fire if not
      // properly cancelled, then verify no TimerFired blob landed.
      await new Promise<void>((resolve) => {
        setTimeout(resolve, 1200);
      });
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
            data: {},
          }),
        },
        message: "RunStarted",
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
              data: {
                timerId: "t-live",
                fireAt: new Date(fireAtMs).toISOString(),
              },
            }),
          },
          message: "TimerSet t-live",
        });

        // Poll for the TimerFired blob landing without restarting the
        // scheduler. The total budget covers the subscribe-notify
        // latency, the wall-clock delay, and the TimerFired commit.
        const deadline = Date.now() + 2000;
        let fired: {
          seq: number;
          bodySeq: number | undefined;
          timerId: string;
        }[] = [];
        while (Date.now() < deadline) {
          fired = await readTimerFiredBlobs(
            store.getRepoDir(repoId),
            runId,
          ).catch(() => []);
          if (fired.length > 0) break;
          await new Promise<void>((resolve) => {
            setTimeout(resolve, 25);
          });
        }
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
            data: {
              timerId: "t-real",
              fireAt: new Date(fireAtMs).toISOString(),
            },
          }),
        },
        message: "seed TimerSet against real handler",
      });

      // Capture any unhandled rejection raised by the scheduler's
      // fire-and-forget commit path so the pre-fix run reports a clean
      // assertion failure (no TimerFired landed) rather than crashing
      // the test runner on the path_violation that surfaces inside
      // setTimeout.
      const captured: unknown[] = [];
      const handler = (reason: unknown) => {
        captured.push(reason);
      };
      process.on("unhandledRejection", handler);

      const scheduler = createWorkflowHostScheduler({
        repoStore: store,
        principal: hubPrincipal,
        listActiveDeployments: () => [repoId],
        ref: REF,
        clock: () => new Date(),
      });
      try {
        await scheduler.start();

        const deadline = Date.now() + 2000;
        let fired: {
          seq: number;
          bodySeq: number | undefined;
          timerId: string;
        }[] = [];
        while (Date.now() < deadline) {
          fired = await readTimerFiredBlobs(
            store.getRepoDir(repoId),
            runId,
          ).catch(() => []);
          if (fired.length > 0) break;
          await new Promise<void>((resolve) => {
            setTimeout(resolve, 25);
          });
        }
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
