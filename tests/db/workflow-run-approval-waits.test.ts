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

import { eq, sql } from "drizzle-orm";

import { generateKeyPair } from "@intx/crypto";
import { signalName } from "@intx/types";
import type { KeyPair } from "@intx/types/runtime";
import type { DB } from "@intx/db";
import {
  createApprovalStore,
  createDB,
  createSignalCorrelationStore,
  expireApprovals,
  reopenApprovals,
} from "@intx/db";
import {
  approval,
  sidecarAllocation,
  workflowPendingProjection,
  workflowRun,
} from "@intx/db/schema";
import {
  createAgentRepoStore,
  createHubSessionLookups,
  createWorkflowHistoryReceiveTracker,
  createWorkflowLifecycleService,
  createWorkflowRunReader,
} from "@intx/hub-sessions";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  loadHarnessDbConfig,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPendingApproval,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

import {
  NO_STANDBY_ROUTER,
  NO_DISPATCH_RECOVERY,
} from "./sidecar-test-helpers";
import {
  buildWorkflowRunLine,
  type WorkflowRunPackRun,
} from "./workflow-run-pack-test-helpers";

const TENANT = "tnt";
const DEPLOYMENT = "dep";
const DEPLOYMENT_ADDRESS = "run_dep@tnt.example";
const REPO = {
  kind: "workflow-run",
  id: deriveWorkflowRunRepoId(DEPLOYMENT_ADDRESS),
} as const;
const OTHER_DEPLOYMENT = "dep-other";
const ALLOCATION = "allocation-approval-waits";
const WFR_REF = "refs/heads/main";
const RUN = "run-parked";

// A handle on which a pack receive runs `onUpdate` in place of its approval
// update: the one savepoint it takes inside the transaction that holds the
// allocation lock. `settle` runs the update itself.
function interceptApprovalUpdate(
  db: DB["db"],
  onUpdate: (settle: () => Promise<unknown>) => Promise<unknown>,
): DB["db"] {
  return new Proxy(db, {
    get(target, prop, receiver) {
      if (prop !== "transaction") return Reflect.get(target, prop, receiver);
      return (
        ...[run, config]: Parameters<DB["db"]["transaction"]>
      ): ReturnType<DB["db"]["transaction"]> =>
        target.transaction(
          (tx) =>
            run(
              new Proxy(tx, {
                get(locked, key, lockedReceiver) {
                  if (key !== "transaction")
                    return Reflect.get(locked, key, lockedReceiver);
                  return (...args: Parameters<typeof locked.transaction>) =>
                    onUpdate(() => locked.transaction(...args));
                },
              }),
            ),
          config,
        );
    },
  });
}

// A handle that holds one transaction until `before` has settled: the first
// to start once `when` is true. Picking it by a state, not by its position,
// keeps the pick right when the code under test opens another transaction.
function holdTransactionOnce(
  db: DB["db"],
  when: () => Promise<boolean>,
  before: () => Promise<unknown>,
): DB["db"] {
  let held = false;
  return new Proxy(db, {
    get(target, prop, receiver) {
      if (prop !== "transaction") return Reflect.get(target, prop, receiver);
      return async (
        ...args: Parameters<DB["db"]["transaction"]>
      ): Promise<unknown> => {
        if (!held && (await when())) {
          held = true;
          await before();
        }
        return target.transaction(...args);
      };
    },
  });
}

const awaited = (correlationId: string) => ({
  signalName: signalName(correlationId),
});
const abandoned = (correlationId: string) => ({
  signalName: signalName(correlationId),
  abandoned: true,
});

describe.skipIf(!harnessDbEnvAvailable())(
  "approval waits on workflow-run pack receive (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedAsset(h.db, {
        id: "ast",
        tenantId: TENANT,
        kind: "workflow",
        name: "ast",
      });
      await seedWorkflowRun(h.db, {
        id: DEPLOYMENT,
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        address: DEPLOYMENT_ADDRESS,
      });
      await seedWorkflowRun(h.db, {
        id: RUN,
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
      });
      await h.db.insert(sidecarAllocation).values({
        id: ALLOCATION,
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
        provisionerId: "test",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:v1",
        adapterManifestDigest: "1".repeat(64),
        maxDisconnectedMs: 900_000,
        status: "allocated",
        generation: 1,
        ensureAcceptedGeneration: 1,
      });
    });

    afterEach(async () => {
      for (const dir of tempDirs.splice(0)) {
        await fs.promises.rm(dir, { recursive: true, force: true });
      }
    });

    async function makeTempDir(prefix: string): Promise<string> {
      const dir = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
      tempDirs.push(dir);
      return dir;
    }

    async function park(
      correlationId: string,
      on: { runId: string; anchorRunId: string } = {
        runId: RUN,
        anchorRunId: DEPLOYMENT,
      },
    ): Promise<void> {
      await seedPendingApproval(h.db, {
        correlationId,
        tenantId: TENANT,
        ...on,
      });
    }

    async function statuses(): Promise<Record<string, string>> {
      const rows = await h.db
        .select({ id: approval.correlationId, status: approval.status })
        .from(approval);
      return Object.fromEntries(rows.map((row) => [row.id, row.status]));
    }

    // One sidecar's history and the Hub store that receives it. Each step is
    // one commit; `push` sends the history up to a step in one pack.
    async function deployment(
      steps: readonly (readonly WorkflowRunPackRun[])[],
    ) {
      const line = await buildWorkflowRunLine(
        await makeTempDir("wfr-waits-src-"),
        WFR_REF,
        steps,
      );
      const repoStore = createAgentRepoStore({
        dataDir: await makeTempDir("wfr-waits-data-"),
        signingKey,
      });
      await repoStore.repoStore.initRepo(REPO);
      return {
        repoStore,
        push: async (step: number, db: DB["db"] = h.db) => {
          const tip = line.commits[step];
          if (tip === undefined) throw new Error(`no step ${String(step)}`);
          const verdict = await createHubSessionLookups({
            db,
            agentRepoStore: repoStore,
            historyReceives: createWorkflowHistoryReceiveTracker(),
          }).receiveWorkflowRunPack(
            REPO,
            await line.packAt(tip),
            WFR_REF,
            tip,
            {
              kind: "allocated",
              agentAddress: DEPLOYMENT_ADDRESS,
              allocationId: ALLOCATION,
              anchorRunId: DEPLOYMENT,
              generation: 1,
            },
          );
          expect(verdict).toEqual({ accepted: true });
        },
      };
    }

    function lifecycleOver(repoStore: ReturnType<typeof createAgentRepoStore>) {
      return createWorkflowLifecycleService({
        standbyRouter: NO_STANDBY_ROUTER,
        retentionRouter: {
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          getRetentionCandidates: () => [],
          holdsAllocatedBinding: () => true,
          getAllocationRetention: () => "kept",
          retainAllocation: async () => "kept",
        },
        db: h.db,
        historyReceives: createWorkflowHistoryReceiveTracker(),
        dispatchProjection: NO_DISPATCH_RECOVERY,
        runReader: createWorkflowRunReader(repoStore.repoStore),
      });
    }

    test("a run that retires its wait on an approval expires that approval only", async () => {
      await park("c-left");
      await park("c-held");
      const { push } = await deployment([
        [{ runId: RUN, waits: [awaited("c-left"), awaited("c-held")] }],
        [
          {
            runId: RUN,
            waits: [awaited("c-left"), awaited("c-held"), abandoned("c-left")],
          },
        ],
      ]);

      await push(0);
      expect(await statuses()).toEqual({
        "c-left": "pending",
        "c-held": "pending",
      });

      await push(1);
      expect(await statuses()).toEqual({
        "c-left": "expired",
        "c-held": "pending",
      });
      const correlations = createSignalCorrelationStore(h.db);
      expect(
        (await correlations.resolveRoute("c-left"))?.resolvedAt,
      ).not.toBeNull();
      expect(
        (await correlations.resolveRoute("c-held"))?.resolvedAt,
      ).toBeNull();
      expect(await h.db.select().from(workflowPendingProjection)).toEqual([]);
    });

    test("a run that waits again on an approval it retired gets it back", async () => {
      await park("c-again");
      const retired = [awaited("c-again"), abandoned("c-again")];
      const { push } = await deployment([
        [{ runId: RUN, waits: retired }],
        [{ runId: RUN, waits: [...retired, awaited("c-again")] }],
      ]);

      await push(0);
      expect(await statuses()).toEqual({ "c-again": "expired" });

      await push(1);
      expect(await statuses()).toEqual({ "c-again": "pending" });
      const reopened = await createSignalCorrelationStore(h.db).resolveRoute(
        "c-again",
      );
      expect(reopened?.resolvedAt).toBeNull();
    });

    test("a pack whose commits retire an approval and then wait on it again leaves it pending", async () => {
      // Each commit reports one change for the approval, and the pack carries
      // both, so the later commit's has to be the one that stands.
      await park("c-order");
      const retired = [awaited("c-order"), abandoned("c-order")];
      const { push } = await deployment([
        [{ runId: RUN, waits: retired }],
        [{ runId: RUN, waits: [...retired, awaited("c-order")] }],
      ]);

      await push(1);

      expect(await statuses()).toEqual({ "c-order": "pending" });
    });

    test("a pack whose commits wait on an approval and then retire it leaves it expired", async () => {
      await park("c-order");
      const { push } = await deployment([
        [{ runId: RUN, waits: [awaited("c-order")] }],
        [{ runId: RUN, waits: [awaited("c-order"), abandoned("c-order")] }],
      ]);

      await push(1);

      expect(await statuses()).toEqual({ "c-order": "expired" });
    });

    test("a pack that leaves an approval where it was does not write its row", async () => {
      await park("c-kept");
      const updatedAt = async () =>
        (await h.db.select({ at: approval.updatedAt }).from(approval))[0]?.at;
      const before = await updatedAt();
      const { push } = await deployment([
        [
          {
            runId: RUN,
            waits: [awaited("c-kept"), abandoned("c-kept"), awaited("c-kept")],
          },
        ],
      ]);

      await push(0);

      expect(await statuses()).toEqual({ "c-kept": "pending" });
      expect(await updatedAt()).toEqual(before);
    });

    test("a wait in the same pack as the run's end leaves the approval expired", async () => {
      await park("c-ended");
      await h.db.transaction((tx) =>
        expireApprovals(tx, ["c-ended"], new Date()),
      );
      const { push } = await deployment([
        [{ runId: RUN, waits: [awaited("c-ended")] }],
        [
          {
            runId: RUN,
            waits: [awaited("c-ended")],
            terminalType: "RunCancelled",
          },
        ],
      ]);

      await push(1);

      expect(await statuses()).toEqual({ "c-ended": "expired" });
      const [run] = await h.db
        .select({ id: workflowRun.id, status: workflowRun.status })
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN));
      expect(run?.status).toBe("cancelled");
    });

    test("a retired wait leaves a decided approval as it was decided", async () => {
      await park("c-decided");
      const decidedAt = new Date("2025-02-01T00:00:00Z");
      await createSignalCorrelationStore(h.db).claimTerminal(
        "c-decided",
        decidedAt,
        "sig-decided",
      );
      await createApprovalStore(h.db).resolve("c-decided", {
        status: "approved",
        scope: "once",
        resolvedAt: decidedAt,
      });
      const { push } = await deployment([
        [{ runId: RUN, waits: [awaited("c-decided"), abandoned("c-decided")] }],
        [
          {
            runId: RUN,
            waits: [
              awaited("c-decided"),
              abandoned("c-decided"),
              awaited("c-decided"),
            ],
          },
        ],
      ]);

      await push(0);
      expect(await statuses()).toEqual({ "c-decided": "approved" });
      await push(1);
      expect(await statuses()).toEqual({ "c-decided": "approved" });
    });

    test("history cannot retire the approval of another run or another deployment", async () => {
      await seedWorkflowRun(h.db, {
        id: OTHER_DEPLOYMENT,
        anchorRunId: OTHER_DEPLOYMENT,
        tenantId: TENANT,
      });
      await seedWorkflowRun(h.db, {
        id: "run-sibling",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
      });
      await park("c-sibling", {
        runId: "run-sibling",
        anchorRunId: DEPLOYMENT,
      });
      await park("c-foreign", {
        runId: OTHER_DEPLOYMENT,
        anchorRunId: OTHER_DEPLOYMENT,
      });
      const { push } = await deployment([
        [
          { runId: RUN, waits: [abandoned("c-sibling")] },
          { runId: OTHER_DEPLOYMENT, waits: [abandoned("c-foreign")] },
        ],
      ]);

      await push(0);

      expect(await statuses()).toEqual({
        "c-sibling": "pending",
        "c-foreign": "pending",
      });
    });

    test("a wait in one run's log does not reopen another run's expired approval", async () => {
      await seedWorkflowRun(h.db, {
        id: "run-sibling",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
      });
      await park("c-sibling", {
        runId: "run-sibling",
        anchorRunId: DEPLOYMENT,
      });
      await h.db.transaction((tx) =>
        expireApprovals(tx, ["c-sibling"], new Date()),
      );
      const { push } = await deployment([
        [{ runId: RUN, waits: [awaited("c-sibling")] }],
      ]);

      await push(0);

      expect(await statuses()).toEqual({ "c-sibling": "expired" });
    });

    test("packs on two connections settle an approval in the order their commits reached Git", async () => {
      // The first pack retires the wait and the second waits on it again. A
      // pack settles its approvals under the allocation lock it moved Git
      // under, so the second cannot settle ahead of a slow first.
      await park("c-again");
      const retired = [awaited("c-again"), abandoned("c-again")];
      const { push } = await deployment([
        [{ runId: RUN, waits: retired }],
        [{ runId: RUN, waits: [...retired, awaited("c-again")] }],
      ]);
      const config = loadHarnessDbConfig();
      const first = createDB({ ...config, schema: h.schema, max: 1 });
      const second = createDB({ ...config, schema: h.schema, max: 1 });
      try {
        const [backend] = await first.db.execute(
          sql`select pg_backend_pid() as pid`,
        );
        const firstPid = Number(backend?.["pid"]);
        const atUpdate = Promise.withResolvers<undefined>();
        const release = Promise.withResolvers<undefined>();
        const firstPush = push(
          0,
          interceptApprovalUpdate(first.db, async (settle) => {
            atUpdate.resolve(undefined);
            await release.promise;
            return settle();
          }),
        );
        await Promise.race([
          atUpdate.promise,
          firstPush.then(() => {
            throw new Error(
              "the first pack finished without settling approvals under its lock",
            );
          }),
        ]);

        let secondSettled = false;
        const secondPush = push(1, second.db).finally(() => {
          secondSettled = true;
        });
        // The lock state lives in PostgreSQL, and the block lasts until the
        // first pack is released below, so the read interval decides nothing.
        let sawBlock = false;
        while (!secondSettled) {
          const waiting = await h.db.execute(
            sql`select pid from pg_stat_activity where ${firstPid} = any(pg_blocking_pids(pid))`,
          );
          if (waiting.length > 0) {
            sawBlock = true;
            break;
          }
          await new Promise((res) => setTimeout(res, 12));
        }
        release.resolve(undefined);
        await firstPush;
        await secondPush;

        expect(sawBlock).toBe(true);
        expect(await statuses()).toEqual({ "c-again": "pending" });
      } finally {
        await first.close();
        await second.close();
      }
    });

    test("a run's end on a pack waits for the holder of the allocation row, and the anchor's end expires what it opened", async () => {
      // A run's end on a pack is written after the pack let the allocation
      // row go, so it takes the row again. Otherwise the anchor could end
      // beside a write that opens an approval, and leave it pending under an
      // ended anchor. The writer here has touched no approval when the end
      // starts, so only the allocation row can make the end wait.
      await park("c-child");
      await h.db.transaction((tx) =>
        expireApprovals(tx, ["c-child"], new Date()),
      );
      const { push, repoStore } = await deployment([
        [{ runId: DEPLOYMENT, terminalType: "RunFailed" }],
      ]);
      const refTip = () =>
        repoStore.repoStore.resolveRef({ kind: "hub" }, REPO, WFR_REF);
      const tipBeforePack = await refTip();
      const config = loadHarnessDbConfig();
      const receiving = createDB({ ...config, schema: h.schema, max: 1 });
      const writing = createDB({ ...config, schema: h.schema, max: 1 });
      try {
        const [backend] = await writing.db.execute(
          sql`select pg_backend_pid() as pid`,
        );
        const writerPid = Number(backend?.["pid"]);
        const atRunEnd = Promise.withResolvers<undefined>();
        const writerHoldsRow = Promise.withResolvers<undefined>();
        // The pack moves Git in the transaction that holds the allocation
        // row, so a transaction that starts once Git holds the pack's commit
        // is not that one: the first is the anchor's end.
        let pushSettled = false;
        const pushed = push(
          0,
          holdTransactionOnce(
            receiving.db,
            async () => (await refTip()) !== tipBeforePack,
            async () => {
              atRunEnd.resolve(undefined);
              await writerHoldsRow.promise;
            },
          ),
        ).finally(() => {
          pushSettled = true;
        });
        await Promise.race([
          atRunEnd.promise,
          pushed.then(() => {
            throw new Error(
              "the pack was received without writing a run's end",
            );
          }),
        ]);

        let sawBlock = false;
        await writing.db.transaction(async (tx) => {
          await tx
            .select({ id: sidecarAllocation.id })
            .from(sidecarAllocation)
            .where(eq(sidecarAllocation.anchorRunId, DEPLOYMENT))
            .for("update");
          writerHoldsRow.resolve(undefined);
          // The lock state lives in PostgreSQL, and the block lasts until
          // this transaction commits, so the read interval decides nothing.
          while (!pushSettled) {
            const waiting = await h.db.execute(
              sql`select pid from pg_stat_activity where ${writerPid} = any(pg_blocking_pids(pid))`,
            );
            if (waiting.length > 0) {
              sawBlock = true;
              break;
            }
            await new Promise((res) => setTimeout(res, 12));
          }
          await reopenApprovals(tx, DEPLOYMENT, ["c-child"], new Date());
        });
        await pushed;

        expect(sawBlock).toBe(true);
        expect(await statuses()).toEqual({ "c-child": "expired" });
        const [anchor] = await h.db
          .select({ status: workflowRun.status })
          .from(workflowRun)
          .where(eq(workflowRun.id, DEPLOYMENT));
        expect(anchor?.status).toBe("failed");
      } finally {
        await receiving.close();
        await writing.close();
      }
    });

    test("recovery settles an approval whose update failed on receive", async () => {
      await park("c-left");
      await park("c-held");
      const { push, repoStore } = await deployment([
        [
          {
            runId: RUN,
            waits: [awaited("c-left"), awaited("c-held"), abandoned("c-left")],
          },
        ],
      ]);
      const failingDb = interceptApprovalUpdate(h.db, () =>
        Promise.reject(new Error("injected update failure")),
      );

      await push(0, failingDb);

      expect(await statuses()).toEqual({
        "c-left": "pending",
        "c-held": "pending",
      });
      expect(await h.db.select().from(workflowPendingProjection)).toHaveLength(
        1,
      );

      expect(
        await lifecycleOver(repoStore).releaseCapacity(TENANT, DEPLOYMENT),
      ).toBe("live");

      expect(await statuses()).toEqual({
        "c-left": "expired",
        "c-held": "pending",
      });
      expect(await h.db.select().from(workflowPendingProjection)).toEqual([]);
    });

    test("recovery reopens an approval whose run waits on it again", async () => {
      await park("c-again");
      await h.db.transaction((tx) =>
        expireApprovals(tx, ["c-again"], new Date()),
      );
      const { push, repoStore } = await deployment([
        [{ runId: RUN, waits: [awaited("c-again")] }],
      ]);
      await push(
        0,
        interceptApprovalUpdate(h.db, () =>
          Promise.reject(new Error("injected update failure")),
        ),
      );
      expect(await statuses()).toEqual({ "c-again": "expired" });

      expect(
        await lifecycleOver(repoStore).releaseCapacity(TENANT, DEPLOYMENT),
      ).toBe("live");

      expect(await statuses()).toEqual({ "c-again": "pending" });
      expect(
        (await createSignalCorrelationStore(h.db).resolveRoute("c-again"))
          ?.resolvedAt,
      ).toBeNull();
      expect(await h.db.select().from(workflowPendingProjection)).toEqual([]);
    });

    test("recovery leaves a decided approval decided when the log retires it", async () => {
      await park("c-decided");
      const decidedAt = new Date("2025-02-01T00:00:00Z");
      await createSignalCorrelationStore(h.db).claimTerminal(
        "c-decided",
        decidedAt,
        "sig-decided",
      );
      await createApprovalStore(h.db).resolve("c-decided", {
        status: "approved",
        scope: "once",
        resolvedAt: decidedAt,
      });
      const { push, repoStore } = await deployment([
        [{ runId: RUN, waits: [awaited("c-decided"), abandoned("c-decided")] }],
      ]);
      await push(
        0,
        interceptApprovalUpdate(h.db, () =>
          Promise.reject(new Error("injected update failure")),
        ),
      );

      await lifecycleOver(repoStore).releaseCapacity(TENANT, DEPLOYMENT);

      expect(await statuses()).toEqual({ "c-decided": "approved" });
      expect(await h.db.select().from(workflowPendingProjection)).toEqual([]);
    });

    test("recovery expires the approvals of a run whose end failed to write on receive", async () => {
      await park("c-ended");
      await park("c-live", { runId: DEPLOYMENT, anchorRunId: DEPLOYMENT });
      const { push, repoStore } = await deployment([
        [{ runId: RUN, terminalType: "RunFailed" }],
      ]);
      const refTip = () =>
        repoStore.repoStore.resolveRef({ kind: "hub" }, REPO, WFR_REF);
      const tipBeforePack = await refTip();
      // The first transaction to start once Git holds the pack's commit
      // writes the run's end.
      await push(
        0,
        holdTransactionOnce(
          h.db,
          async () => (await refTip()) !== tipBeforePack,
          () => Promise.reject(new Error("injected run end failure")),
        ),
      );
      expect(await statuses()).toEqual({
        "c-ended": "pending",
        "c-live": "pending",
      });
      expect(await h.db.select().from(workflowPendingProjection)).toHaveLength(
        1,
      );

      expect(
        await lifecycleOver(repoStore).releaseCapacity(TENANT, DEPLOYMENT),
      ).toBe("live");

      expect(await statuses()).toEqual({
        "c-ended": "expired",
        "c-live": "pending",
      });
      const [run] = await h.db
        .select({ status: workflowRun.status })
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN));
      expect(run?.status).toBe("failed");
      expect(await h.db.select().from(workflowPendingProjection)).toEqual([]);
    });

    test("recovery leaves an approval expired once its deployment has ended", async () => {
      // The anchor ends and leaves a run live whose log still waits. The
      // anchor's end expired that run's approval, and recovery must not take
      // the log's word and open it again.
      await park("c-child");
      const { push, repoStore } = await deployment([
        [
          { runId: RUN, waits: [awaited("c-child")] },
          { runId: DEPLOYMENT, terminalType: "RunFailed" },
        ],
      ]);
      // A failed approval update keeps the pack's projection pending, so
      // recovery has something to reconcile after the anchor has ended.
      await push(
        0,
        interceptApprovalUpdate(h.db, () =>
          Promise.reject(new Error("injected update failure")),
        ),
      );
      expect(await statuses()).toEqual({ "c-child": "expired" });
      expect(await h.db.select().from(workflowPendingProjection)).toHaveLength(
        1,
      );

      await lifecycleOver(repoStore).releaseCapacity(TENANT, DEPLOYMENT);

      expect(await h.db.select().from(workflowPendingProjection)).toEqual([]);
      expect(await statuses()).toEqual({ "c-child": "expired" });
    });
  },
);
