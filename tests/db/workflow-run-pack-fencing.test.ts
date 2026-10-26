import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import git from "isomorphic-git";
import { eq, inArray, sql } from "drizzle-orm";

import { generateKeyPair } from "@intx/crypto";
import {
  sidecarAllocation,
  workflowPendingProjection,
  workflowRun,
} from "@intx/db/schema";
import {
  createWorkflowHistoryReceiveTracker,
  createAgentRepoStore,
  createHubSessionLookups,
  RepoRemovedDuringReceiveError,
  type AgentRepoStore,
  type NewlyTerminalRun,
  type PackCommitResult,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants, seedWorkflowRun } from "@intx/test-harness/seed";
import type { KeyPair } from "@intx/types/runtime";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import { applyWorkflowRunPack } from "../hub-agent/lib/deploy-flow-env";
import {
  buildWorkflowRunLine,
  type WorkflowRunLine,
} from "./workflow-run-pack-test-helpers";

const TENANT_ID = "tnt-pack-fence";
const ANCHOR_RUN_ID = "dep-pack-fence";
const ANCHOR_ADDRESS = "run_pack_fence@tenant.example";
const ALLOCATION_ID = "alloc-pack-fence";
const WORKFLOW_RUN_REPO_ID = deriveWorkflowRunRepoId(ANCHOR_ADDRESS);
const WORKFLOW_RUN_REPO = {
  kind: "workflow-run",
  id: WORKFLOW_RUN_REPO_ID,
} as const;
const WORKFLOW_RUN_REF = "refs/heads/events";
// The run-event ref, for the tests that push real packs of run history.
const RUN_EVENTS_REF = "refs/heads/main";
const MANIFEST_DIGEST = "1".repeat(64);

type PrepareWorkflowRunPack = AgentRepoStore["prepareWorkflowRunPack"];

// A prepared receive whose `commit()` is the test's, for the tests that
// exercise only the locking around it.
function preparedWith(
  commit: () => Promise<PackCommitResult>,
): PrepareWorkflowRunPack {
  return async () => ({
    kind: "prepared",
    commit,
    discard: () => Promise.resolve(),
  });
}

const appliedNothing: PackCommitResult = {
  kind: "applied",
  newlyTerminalRuns: [],
};

describe.skipIf(!harnessDbEnvAvailable())(
  "workflow-run pack allocation fencing (real DB)",
  () => {
    let h: TestDb;
    let signingKey: KeyPair;
    const tempDirs: string[] = [];

    beforeAll(async () => {
      h = await createTestDb();
      signingKey = await generateKeyPair();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedWorkflowRun(h.db, {
        id: ANCHOR_RUN_ID,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        address: ANCHOR_ADDRESS,
      });
      await h.db.insert(sidecarAllocation).values({
        id: ALLOCATION_ID,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "test-provisioner",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test-provisioner:pack-fence",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
        status: "allocated",
        generation: 1,
        ensureAcceptedGeneration: 1,
      });
    });

    afterEach(async () => {
      for (const dir of tempDirs.splice(0)) {
        await fs.promises
          .rm(dir, { recursive: true, force: true })
          .catch((_error) => {
            // Best-effort test cleanup.
          });
      }
    });

    async function makeTempDir(prefix: string): Promise<string> {
      const dir = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
      tempDirs.push(dir);
      return dir;
    }

    // A real repo store whose `prepareWorkflowRunPack` is replaced when the
    // test passes `prepare`. `prepare` gets the real store, so it can wrap
    // the real call.
    async function createRepoStore(
      prepare?: (real: AgentRepoStore) => PrepareWorkflowRunPack,
    ): Promise<AgentRepoStore> {
      const real = createAgentRepoStore({
        dataDir: await makeTempDir("workflow-run-pack-fence-"),
        signingKey,
      });
      return prepare === undefined
        ? real
        : { ...real, prepareWorkflowRunPack: prepare(real) };
    }

    function createLookups(
      agentRepoStore: AgentRepoStore,
      historyReceives = createWorkflowHistoryReceiveTracker(),
    ) {
      return createHubSessionLookups({
        db: h.db,
        agentRepoStore,
        historyReceives,
      });
    }

    async function receivePack(
      lookups: ReturnType<typeof createLookups>,
      push: {
        pack: Uint8Array;
        ref: string;
        commitSha: string;
        generation?: number;
      } = {
        pack: new Uint8Array(),
        ref: WORKFLOW_RUN_REF,
        commitSha: "pack-tip",
      },
    ) {
      return lookups.receiveWorkflowRunPack(
        WORKFLOW_RUN_REPO,
        push.pack,
        push.ref,
        push.commitSha,
        {
          kind: "allocated",
          agentAddress: ANCHOR_ADDRESS,
          allocationId: ALLOCATION_ID,
          anchorRunId: ANCHOR_RUN_ID,
          generation: push.generation ?? 1,
        },
      );
    }

    async function runLine(
      steps: Parameters<typeof buildWorkflowRunLine>[2],
    ): Promise<WorkflowRunLine> {
      return buildWorkflowRunLine(
        await makeTempDir("workflow-run-pack-fence-src-"),
        RUN_EVENTS_REF,
        steps,
      );
    }

    function commitAt(line: WorkflowRunLine, index: number): string {
      const sha = line.commits[index];
      if (sha === undefined) throw new Error(`no commit ${String(index)}`);
      return sha;
    }

    async function pushOf(line: WorkflowRunLine, index: number) {
      const commitSha = commitAt(line, index);
      return {
        pack: await line.packAt(commitSha),
        ref: RUN_EVENTS_REF,
        commitSha,
      };
    }

    function repoDirOf(agentRepoStore: AgentRepoStore): string {
      return agentRepoStore.repoStore.getRepoDir(WORKFLOW_RUN_REPO);
    }

    async function refTip(agentRepoStore: AgentRepoStore): Promise<string> {
      return git.resolveRef({
        fs,
        dir: repoDirOf(agentRepoStore),
        ref: RUN_EVENTS_REF,
      });
    }

    async function publishedPacks(
      agentRepoStore: AgentRepoStore,
    ): Promise<string[]> {
      const entries = await fs.promises.readdir(
        path.join(repoDirOf(agentRepoStore), ".git", "objects", "pack"),
      );
      return entries.filter((entry) => entry.endsWith(".pack")).sort();
    }

    async function stagedEntries(
      agentRepoStore: AgentRepoStore,
    ): Promise<string[]> {
      return fs.promises
        .readdir(
          path.join(
            repoDirOf(agentRepoStore),
            ".git",
            "objects",
            "pack-staging",
          ),
        )
        .catch(() => []);
    }

    async function pendingRows() {
      return h.db
        .select({ anchorRunId: workflowPendingProjection.anchorRunId })
        .from(workflowPendingProjection);
    }

    // Lock the allocation row from another connection and release it at
    // once. The await returns only once the lock is granted, so a receive
    // that holds the row makes this hang until the runner's budget fails
    // the test.
    async function lockAllocationRow(): Promise<void> {
      await h.db.transaction(async (tx) => {
        await tx
          .select({ id: sidecarAllocation.id })
          .from(sidecarAllocation)
          .where(eq(sidecarAllocation.id, ALLOCATION_ID))
          .for("update");
      });
    }

    async function releaseAllocation(): Promise<void> {
      await h.db
        .update(sidecarAllocation)
        .set({
          status: "releasing",
          generation: 2,
          ensureAcceptedGeneration: null,
        })
        .where(eq(sidecarAllocation.id, ALLOCATION_ID));
    }

    // A wrapper of the real prepare that stops each call it gates after the
    // pack is staged and validated, before the hub sees the result. `gate`
    // maps a pack tip to the promise that call waits on; `ready` resolves
    // per tip once that call is paused. Every commit result is recorded.
    function pausingPrepare(
      gates: Map<string, Promise<unknown>>,
      ready: Map<string, PromiseWithResolvers<boolean>>,
      commits: PackCommitResult[],
    ): (real: AgentRepoStore) => PrepareWorkflowRunPack {
      return (real) =>
        async (...args) => {
          const prepared = await real.prepareWorkflowRunPack(...args);
          const tip = args[3];
          const gate = gates.get(tip);
          if (gate !== undefined) {
            ready.get(tip)?.resolve(true);
            await gate;
          }
          if (prepared.kind !== "prepared") return prepared;
          return {
            kind: "prepared",
            commit: async () => {
              const result = await prepared.commit();
              commits.push(result);
              return result;
            },
            discard: prepared.discard,
          };
        };
    }
    // The concrete postgres transaction the harness hands to a `transaction`
    // callback, so raw `tx.execute` keeps its typed rows.
    type HarnessTx = Parameters<Parameters<TestDb["db"]["transaction"]>[0]>[0];

    async function getBackendPid(tx: HarnessTx): Promise<number> {
      const rows = await tx.execute(
        sql`select pg_backend_pid()::integer as pid`,
      );
      const pid = rows[0]?.["pid"];
      if (typeof pid !== "number") {
        throw new Error(`Expected numeric PostgreSQL backend pid, got ${pid}`);
      }
      return pid;
    }

    // Report whether `promise` has settled, without consuming it. The lock
    // waits below watch for a state that only exists while the other side is
    // still in flight, so that side settling is the signal that the state will
    // never appear -- which is exactly the fencing regression each test pins.
    // Both outcomes are handled here, so this second handler cannot turn a
    // rejection the test still awaits into an unhandled one.
    function settleReporter(promise: Promise<unknown>): () => boolean {
      let settled = false;
      const mark = (): void => {
        settled = true;
      };
      void promise.then(mark, mark);
      return () => settled;
    }

    // Wait until backend `pid` is blocked by another backend. The lock state
    // lives in PostgreSQL, not in this process, so a query is the only way to
    // read it; the 10ms is the interval between reads and decides nothing,
    // because the loop ends on observed state rather than on a budget. The
    // block persists until the caller releases the holder, so a slow poll
    // cannot sample past it.
    async function waitForBlockedBackend(
      pid: number,
      blockedSideSettled: () => boolean,
    ): Promise<void> {
      for (;;) {
        const rows = await h.db.execute(
          sql`select cardinality(pg_blocking_pids(${pid})) > 0 as blocked`,
        );
        if (rows[0]?.["blocked"] === true) return;
        if (blockedSideSettled()) {
          throw new Error(
            `PostgreSQL backend ${pid} finished without ever blocking`,
          );
        }
        await Bun.sleep(10);
      }
    }

    // The mirror of the above: wait until SOME backend is blocked by `pid`.
    // Same deadline-free loop, same release ordering.
    async function waitForBackendBlockedBy(
      pid: number,
      blockedSideSettled: () => boolean,
    ): Promise<void> {
      for (;;) {
        const rows = await h.db.execute(sql`
          select exists (
            select 1
            from pg_stat_activity activity
            where ${pid} = any(pg_blocking_pids(activity.pid))
          ) as blocked
        `);
        if (rows[0]?.["blocked"] === true) return;
        if (blockedSideSettled()) {
          throw new Error(
            `No PostgreSQL backend became blocked by backend ${pid}`,
          );
        }
        await Bun.sleep(10);
      }
    }

    test("release waits until an accepted pack finishes advancing the ref", async () => {
      const packEntered = Promise.withResolvers<boolean>();
      const releasePack = Promise.withResolvers<boolean>();
      const agentRepoStore = await createRepoStore(() =>
        preparedWith(async () => {
          packEntered.resolve(true);
          await releasePack.promise;
          return appliedNothing;
        }),
      );

      const receivePromise = receivePack(createLookups(agentRepoStore));
      await packEntered.promise;

      const releaseStarted = Promise.withResolvers<number>();
      const releasePromise = h.db.transaction(async (tx) => {
        releaseStarted.resolve(await getBackendPid(tx));
        const [updated] = await tx
          .update(sidecarAllocation)
          .set({
            status: "releasing",
            generation: 2,
            ensureAcceptedGeneration: null,
          })
          .where(eq(sidecarAllocation.id, ALLOCATION_ID))
          .returning();
        return updated;
      });

      try {
        await waitForBlockedBackend(
          await releaseStarted.promise,
          settleReporter(releasePromise),
        );
      } finally {
        releasePack.resolve(true);
      }

      expect(await receivePromise).toEqual({ accepted: true });
      expect((await releasePromise)?.generation).toBe(2);
    });

    test("an old pack waits for release and is rejected at the new generation", async () => {
      let commitCalls = 0;
      const agentRepoStore = await createRepoStore(() =>
        preparedWith(async () => {
          commitCalls += 1;
          return appliedNothing;
        }),
      );
      const releaseHolding = Promise.withResolvers<number>();
      const finishRelease = Promise.withResolvers<boolean>();
      const releasePromise = h.db.transaction(async (tx) => {
        await tx
          .select({ id: sidecarAllocation.id })
          .from(sidecarAllocation)
          .where(eq(sidecarAllocation.id, ALLOCATION_ID))
          .limit(1)
          .for("update");
        await tx
          .update(sidecarAllocation)
          .set({
            status: "releasing",
            generation: 2,
            ensureAcceptedGeneration: null,
          })
          .where(eq(sidecarAllocation.id, ALLOCATION_ID));
        releaseHolding.resolve(await getBackendPid(tx));
        await finishRelease.promise;
      });

      const releasePid = await releaseHolding.promise;
      const receivePromise = receivePack(createLookups(agentRepoStore));
      try {
        await waitForBackendBlockedBy(
          releasePid,
          settleReporter(receivePromise),
        );
      } finally {
        finishRelease.resolve(true);
      }

      await releasePromise;
      expect(await receivePromise).toEqual({
        accepted: false,
        reason: "path_violation",
      });
      expect(commitCalls).toBe(0);
      expect(await pendingRows()).toEqual([]);
    });

    test("a receive that fails after it may have advanced Git keeps its pending projection", async () => {
      const agentRepoStore = await createRepoStore(() =>
        preparedWith(async () => {
          throw new Error("ref-update listener failed");
        }),
      );
      expect(await receivePack(createLookups(agentRepoStore))).toEqual({
        accepted: false,
        reason: "corrupt",
      });
      expect(await pendingRows()).toEqual([{ anchorRunId: ANCHOR_RUN_ID }]);
      // The failure preceded any Git write, so the repository must already
      // exist for recovery to read it as never advanced, not as lost.
      expect(fs.existsSync(path.join(repoDirOf(agentRepoStore), ".git"))).toBe(
        true,
      );
    });

    test("a pack waiting behind a recorded stop is rejected", async () => {
      let commitCalls = 0;
      const agentRepoStore = await createRepoStore(() =>
        preparedWith(async () => {
          commitCalls += 1;
          return appliedNothing;
        }),
      );
      const stopHolding = Promise.withResolvers<number>();
      const releaseStop = Promise.withResolvers<boolean>();
      const stopPromise = h.db.transaction(async (tx) => {
        await tx
          .select({ id: sidecarAllocation.id })
          .from(sidecarAllocation)
          .where(eq(sidecarAllocation.id, ALLOCATION_ID))
          .limit(1)
          .for("update");
        await tx
          .update(workflowRun)
          .set({ status: "cancelled", endedAt: new Date() })
          .where(eq(workflowRun.id, ANCHOR_RUN_ID));
        stopHolding.resolve(await getBackendPid(tx));
        await releaseStop.promise;
      });

      const stopPid = await stopHolding.promise;
      const receivePromise = receivePack(createLookups(agentRepoStore));
      try {
        await waitForBackendBlockedBy(stopPid, settleReporter(receivePromise));
      } finally {
        releaseStop.resolve(true);
      }

      await stopPromise;
      expect(await receivePromise).toEqual({
        accepted: false,
        reason: "path_violation",
      });
      expect(commitCalls).toBe(0);
      expect(await pendingRows()).toEqual([]);
    });

    test("a receive paused in validation holds no allocation lock and does not hold off another receive", async () => {
      const line = await runLine([[{ runId: "run-a" }], [{ runId: "run-b" }]]);
      const tipA = commitAt(line, 0);
      const tipB = commitAt(line, 1);
      const gateA = Promise.withResolvers<boolean>();
      const readyA = Promise.withResolvers<boolean>();
      const commits: PackCommitResult[] = [];
      const agentRepoStore = await createRepoStore(
        pausingPrepare(
          new Map([[tipA, gateA.promise]]),
          new Map([[tipA, readyA]]),
          commits,
        ),
      );
      const lookups = createLookups(agentRepoStore);

      const receiveA = receivePack(lookups, await pushOf(line, 0));
      try {
        await readyA.promise;
        await lockAllocationRow();
        expect(await receivePack(lookups, await pushOf(line, 1))).toEqual({
          accepted: true,
        });
        expect(await refTip(agentRepoStore)).toBe(tipB);
        await lockAllocationRow();
      } finally {
        gateA.resolve(true);
      }

      // B moved the ref past A's tip while A waited, so A changes nothing.
      expect(await receiveA).toEqual({ accepted: true });
      expect(await refTip(agentRepoStore)).toBe(tipB);
      expect(commits.map(({ kind }) => kind)).toEqual([
        "applied",
        "already_applied",
      ]);
      expect(await stagedEntries(agentRepoStore)).toEqual([]);
      expect(await pendingRows()).toEqual([]);
    });

    test("a release while a pack validates rejects the pack and leaves nothing staged or published", async () => {
      const line = await runLine([[{ runId: "run-a" }], [{ runId: "run-b" }]]);
      const seeded = commitAt(line, 0);
      const tip = commitAt(line, 1);
      const gate = Promise.withResolvers<boolean>();
      const ready = Promise.withResolvers<boolean>();
      const commits: PackCommitResult[] = [];
      const agentRepoStore = await createRepoStore(
        pausingPrepare(
          new Map([[tip, gate.promise]]),
          new Map([[tip, ready]]),
          commits,
        ),
      );
      await agentRepoStore.repoStore.initRepo(WORKFLOW_RUN_REPO);
      await applyWorkflowRunPack(
        agentRepoStore,
        WORKFLOW_RUN_REPO,
        await line.packAt(seeded),
        RUN_EVENTS_REF,
        seeded,
      );
      const packsBefore = await publishedPacks(agentRepoStore);
      // The seed went through the same wrapper; only the receive's own
      // commit counts below.
      commits.splice(0);

      const receive = receivePack(
        createLookups(agentRepoStore),
        await pushOf(line, 1),
      );
      try {
        await ready.promise;
        await releaseAllocation();
      } finally {
        gate.resolve(true);
      }

      expect(await receive).toEqual({
        accepted: false,
        reason: "path_violation",
      });
      expect(commits).toEqual([]);
      expect(await refTip(agentRepoStore)).toBe(seeded);
      expect(await publishedPacks(agentRepoStore)).toEqual(packsBefore);
      expect(await stagedEntries(agentRepoStore)).toEqual([]);
      expect(await pendingRows()).toEqual([]);
    });

    test("a stale-generation source is rejected before any Git work", async () => {
      let prepareCalls = 0;
      const agentRepoStore = await createRepoStore(
        (real) =>
          async (...args) => {
            prepareCalls += 1;
            return real.prepareWorkflowRunPack(...args);
          },
      );
      const line = await runLine([[{ runId: "run-a" }]]);

      expect(
        await receivePack(createLookups(agentRepoStore), {
          ...(await pushOf(line, 0)),
          generation: 0,
        }),
      ).toEqual({ accepted: false, reason: "path_violation" });
      expect(prepareCalls).toBe(0);
      expect(fs.existsSync(repoDirOf(agentRepoStore))).toBe(false);
      expect(await pendingRows()).toEqual([]);
    });

    test("a re-push of the tip the Hub holds is accepted with no pending row, receive or commit", async () => {
      const kinds: string[] = [];
      let commitCalls = 0;
      const agentRepoStore = await createRepoStore(
        (real) =>
          async (...args) => {
            const prepared = await real.prepareWorkflowRunPack(...args);
            kinds.push(prepared.kind);
            if (prepared.kind !== "prepared") return prepared;
            return {
              ...prepared,
              commit: () => {
                commitCalls += 1;
                return prepared.commit();
              },
            };
          },
      );
      const tracker = createWorkflowHistoryReceiveTracker();
      const begun: string[] = [];
      const lookups = createLookups(agentRepoStore, {
        ...tracker,
        begin: (id) => {
          begun.push(id);
          tracker.begin(id);
        },
      });
      const line = await runLine([[{ runId: "run-a" }]]);
      const push = await pushOf(line, 0);

      expect(await receivePack(lookups, push)).toEqual({ accepted: true });
      expect(await receivePack(lookups, push)).toEqual({ accepted: true });
      expect(kinds).toEqual(["prepared", "already_applied"]);
      expect(commitCalls).toBe(1);
      expect(begun).toHaveLength(1);
      expect(await refTip(agentRepoStore)).toBe(push.commitSha);
      expect(await pendingRows()).toEqual([]);
    });

    test("history is final while a receive validates, and the receive is then rejected", async () => {
      const line = await runLine([[{ runId: "run-a" }]]);
      const tip = commitAt(line, 0);
      const gate = Promise.withResolvers<boolean>();
      const ready = Promise.withResolvers<boolean>();
      const commits: PackCommitResult[] = [];
      const agentRepoStore = await createRepoStore(
        pausingPrepare(
          new Map([[tip, gate.promise]]),
          new Map([[tip, ready]]),
          commits,
        ),
      );
      const lookups = createLookups(agentRepoStore);
      const isFinal = lookups.isWorkflowRunHistoryFinal;
      if (isFinal === undefined)
        throw new Error("no isWorkflowRunHistoryFinal");

      const receive = receivePack(lookups, await pushOf(line, 0));
      let final: boolean;
      try {
        await ready.promise;
        await h.db
          .update(workflowRun)
          .set({ status: "cancelled", endedAt: new Date() })
          .where(eq(workflowRun.id, ANCHOR_RUN_ID));
        final = await isFinal({
          kind: "allocated",
          agentAddress: ANCHOR_ADDRESS,
          allocationId: ALLOCATION_ID,
          anchorRunId: ANCHOR_RUN_ID,
          generation: 1,
        });
      } finally {
        gate.resolve(true);
      }

      // The validating receive is not in flight yet, so history reads as
      // final. Its locked recheck then sees the terminal anchor.
      expect(final).toBe(true);
      expect(await receive).toEqual({
        accepted: false,
        reason: "path_violation",
      });
      expect(commits).toEqual([]);
      expect(await stagedEntries(agentRepoStore)).toEqual([]);
      expect(await pendingRows()).toEqual([]);
    });

    for (const order of [
      ["c", "a", "b"],
      ["a", "c", "b"],
    ] as const) {
      test(`overlapping receives on one line that finish in the order ${order.join(", ")} project each terminal run once`, async () => {
        for (const runId of ["run-a", "run-b", "run-c"]) {
          await seedWorkflowRun(h.db, {
            id: runId,
            anchorRunId: ANCHOR_RUN_ID,
            tenantId: TENANT_ID,
          });
        }
        const line = await runLine([
          [{ runId: "run-a", terminalType: "RunCompleted" }],
          [{ runId: "run-b", terminalType: "RunFailed" }],
          [{ runId: "run-c", terminalType: "RunCompleted" }],
        ]);
        const indexOf = { a: 0, b: 1, c: 2 } as const;
        const gates = new Map<string, PromiseWithResolvers<boolean>>();
        const ready = new Map<string, PromiseWithResolvers<boolean>>();
        for (const index of [0, 1, 2]) {
          gates.set(commitAt(line, index), Promise.withResolvers<boolean>());
          ready.set(commitAt(line, index), Promise.withResolvers<boolean>());
        }
        const commits: PackCommitResult[] = [];
        const agentRepoStore = await createRepoStore(
          pausingPrepare(
            new Map([...gates].map(([tip, gate]) => [tip, gate.promise])),
            ready,
            commits,
          ),
        );
        await agentRepoStore.repoStore.initRepo(WORKFLOW_RUN_REPO);
        const lookups = createLookups(agentRepoStore);

        const receives = new Map<string, ReturnType<typeof receivePack>>();
        for (const index of [0, 1, 2]) {
          receives.set(
            commitAt(line, index),
            receivePack(lookups, await pushOf(line, index)),
          );
        }
        const verdicts: unknown[] = [];
        try {
          for (const signal of ready.values()) await signal.promise;
          for (const name of order) {
            const tip = commitAt(line, indexOf[name]);
            gates.get(tip)?.resolve(true);
            verdicts.push(await receives.get(tip));
          }
        } finally {
          for (const gate of gates.values()) gate.resolve(true);
          await Promise.all(receives.values());
        }

        expect(verdicts).toEqual([
          { accepted: true },
          { accepted: true },
          { accepted: true },
        ]);
        expect(await refTip(agentRepoStore)).toBe(commitAt(line, 2));
        const projected: NewlyTerminalRun[] = commits.flatMap((result) =>
          result.kind === "applied" ? result.newlyTerminalRuns : [],
        );
        expect(projected.map(({ runId }) => runId).sort()).toEqual([
          "run-a",
          "run-b",
          "run-c",
        ]);
        expect(
          await h.db
            .select({ id: workflowRun.id, status: workflowRun.status })
            .from(workflowRun)
            .where(inArray(workflowRun.id, ["run-a", "run-b", "run-c"]))
            .orderBy(workflowRun.id),
        ).toEqual([
          { id: "run-a", status: "completed" },
          { id: "run-b", status: "failed" },
          { id: "run-c", status: "completed" },
        ]);
        expect(await pendingRows()).toEqual([]);
      });
    }

    test("a DB commit failure after the ref moved keeps the pack and the pending projection", async () => {
      const line = await runLine([[{ runId: "run-a" }]]);
      const tip = commitAt(line, 0);
      const agentRepoStore = await createRepoStore(
        (real) =>
          async (...args) => {
            const prepared = await real.prepareWorkflowRunPack(...args);
            if (prepared.kind !== "prepared") return prepared;
            return {
              kind: "prepared",
              commit: async () => {
                await prepared.commit();
                throw new Error("database commit failed");
              },
              discard: prepared.discard,
            };
          },
      );

      expect(
        await receivePack(createLookups(agentRepoStore), await pushOf(line, 0)),
      ).toEqual({ accepted: false, reason: "corrupt" });
      expect(await refTip(agentRepoStore)).toBe(tip);
      expect(await publishedPacks(agentRepoStore)).toHaveLength(1);
      expect(await stagedEntries(agentRepoStore)).toEqual([]);
      expect(await pendingRows()).toEqual([{ anchorRunId: ANCHOR_RUN_ID }]);
    });

    test("a pack that fails validation leaves no pending projection", async () => {
      const agentRepoStore = await createRepoStore(() => async () => {
        throw new Error("path_violation: the pack rewrites a run's history");
      });
      expect(await receivePack(createLookups(agentRepoStore))).toEqual({
        accepted: false,
        reason: "path_violation",
      });
      expect(await pendingRows()).toEqual([]);
    });

    test("a commit that finds its repository removed clears its pending projection", async () => {
      const agentRepoStore = await createRepoStore(() =>
        preparedWith(async () => {
          throw new RepoRemovedDuringReceiveError(WORKFLOW_RUN_REPO);
        }),
      );
      expect(await receivePack(createLookups(agentRepoStore))).toEqual({
        accepted: false,
        reason: "path_violation",
      });
      expect(await pendingRows()).toEqual([]);
    });

    test("a failed pending-projection insert ends the tracked receive and discards the staged pack", async () => {
      const line = await runLine([[{ runId: "run-a" }]]);
      let discards = 0;
      const agentRepoStore = await createRepoStore(
        (real) =>
          async (...args) => {
            const prepared = await real.prepareWorkflowRunPack(...args);
            if (prepared.kind !== "prepared") return prepared;
            return {
              kind: "prepared",
              commit: prepared.commit,
              discard: async () => {
                discards += 1;
                await prepared.discard();
              },
            };
          },
      );
      const tracker = createWorkflowHistoryReceiveTracker();
      const begun: string[] = [];
      const ended: string[] = [];
      const lookups = createLookups(agentRepoStore, {
        begin: (id) => {
          begun.push(id);
          tracker.begin(id);
        },
        end: (id) => {
          ended.push(id);
          tracker.end(id);
        },
        isInFlight: (id) => tracker.isInFlight(id),
      });

      await h.db.execute(sql`
        create function fail_pending_projection_insert() returns trigger
        language plpgsql as $$
        begin
          raise exception 'injected pending projection failure';
        end $$
      `);
      try {
        await h.db.execute(sql`
          create trigger fail_pending_projection_insert
          before insert on workflow_pending_projection
          for each row execute function fail_pending_projection_insert()
        `);
        expect(await receivePack(lookups, await pushOf(line, 0))).toEqual({
          accepted: false,
          reason: "corrupt",
        });
      } finally {
        await h.db.execute(sql`
          drop trigger if exists fail_pending_projection_insert
          on workflow_pending_projection
        `);
        await h.db.execute(sql`drop function fail_pending_projection_insert()`);
      }
      expect(begun).toHaveLength(1);
      expect(ended).toEqual(begun);
      expect(discards).toBe(1);
      expect(await stagedEntries(agentRepoStore)).toEqual([]);
      expect(await pendingRows()).toEqual([]);
    });
  },
);
