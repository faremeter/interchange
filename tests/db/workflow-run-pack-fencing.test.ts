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
import { sidecarAllocation, workflowPendingProjection } from "@intx/db/schema";
import {
  createAgentRepoStore,
  createHubSessionLookups,
  type AgentRepoStore,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants, seedWorkflowRun } from "@intx/test-harness/seed";
import type { KeyPair } from "@intx/types/runtime";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

const TENANT_ID = "tnt-pack-fence";
const ANCHOR_RUN_ID = "dep-pack-fence";
const ANCHOR_ADDRESS = "run_pack_fence@tenant.example";
const ALLOCATION_ID = "alloc-pack-fence";
const WORKFLOW_RUN_REPO_ID = deriveWorkflowRunRepoId(ANCHOR_ADDRESS);
const WORKFLOW_RUN_REF = "refs/heads/events";

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
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "test-provisioner:pack-fence",
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

    async function createRepoStore(
      receiveWorkflowRunPack: AgentRepoStore["receiveWorkflowRunPack"],
    ): Promise<AgentRepoStore> {
      const dataDir = await fs.promises.mkdtemp(
        path.join(os.tmpdir(), "workflow-run-pack-fence-"),
      );
      tempDirs.push(dataDir);
      return {
        ...createAgentRepoStore({ dataDir, signingKey }),
        receiveWorkflowRunPack,
      };
    }

    async function receivePack(agentRepoStore: AgentRepoStore) {
      return createHubSessionLookups({
        db: h.db,
        agentRepoStore,
      }).receiveWorkflowRunPack(
        { kind: "workflow-run", id: WORKFLOW_RUN_REPO_ID },
        new Uint8Array(),
        WORKFLOW_RUN_REF,
        "pack-tip",
        {
          kind: "allocated",
          agentAddress: ANCHOR_ADDRESS,
          allocationId: ALLOCATION_ID,
          anchorRunId: ANCHOR_RUN_ID,
          generation: 1,
        },
      );
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

    test("replacement waits until an accepted pack finishes advancing the ref", async () => {
      const packEntered = Promise.withResolvers<boolean>();
      const releasePack = Promise.withResolvers<boolean>();
      const agentRepoStore = await createRepoStore(async () => {
        packEntered.resolve(true);
        await releasePack.promise;
        return [];
      });

      const receivePromise = receivePack(agentRepoStore);
      await packEntered.promise;

      const replacementStarted = Promise.withResolvers<number>();
      const replacementPromise = h.db.transaction(async (tx) => {
        replacementStarted.resolve(await getBackendPid(tx));
        const [updated] = await tx
          .update(sidecarAllocation)
          .set({
            status: "replacing",
            generation: 2,
            ensureAcceptedGeneration: null,
          })
          .where(eq(sidecarAllocation.id, ALLOCATION_ID))
          .returning();
        return updated;
      });

      try {
        await waitForBlockedBackend(
          await replacementStarted.promise,
          settleReporter(replacementPromise),
        );
      } finally {
        releasePack.resolve(true);
      }

      expect(await receivePromise).toEqual({ accepted: true });
      expect((await replacementPromise)?.generation).toBe(2);
    });

    test("an old pack waits for replacement and is rejected at the new generation", async () => {
      let receiveCalls = 0;
      const agentRepoStore = await createRepoStore(async () => {
        receiveCalls += 1;
        return [];
      });
      const replacementHolding = Promise.withResolvers<number>();
      const releaseReplacement = Promise.withResolvers<boolean>();
      const replacementPromise = h.db.transaction(async (tx) => {
        await tx
          .select({ id: sidecarAllocation.id })
          .from(sidecarAllocation)
          .where(eq(sidecarAllocation.id, ALLOCATION_ID))
          .limit(1)
          .for("update");
        await tx
          .update(sidecarAllocation)
          .set({
            status: "replacing",
            generation: 2,
            ensureAcceptedGeneration: null,
          })
          .where(eq(sidecarAllocation.id, ALLOCATION_ID));
        replacementHolding.resolve(await getBackendPid(tx));
        await releaseReplacement.promise;
      });

      const replacementPid = await replacementHolding.promise;
      const receivePromise = receivePack(agentRepoStore);
      try {
        await waitForBackendBlockedBy(
          replacementPid,
          settleReporter(receivePromise),
        );
      } finally {
        releaseReplacement.resolve(true);
      }

      await replacementPromise;
      expect(await receivePromise).toEqual({
        accepted: false,
        reason: "path_violation",
      });
      expect(receiveCalls).toBe(0);
      expect(await h.db.select().from(workflowPendingProjection)).toEqual([]);
    });

    test("a receive that fails after it may have advanced Git keeps its pending projection", async () => {
      const agentRepoStore = await createRepoStore(async () => {
        throw new Error("ref-update listener failed");
      });
      expect(await receivePack(agentRepoStore)).toEqual({
        accepted: false,
        reason: "corrupt",
      });
      expect(
        await h.db
          .select({ anchorRunId: workflowPendingProjection.anchorRunId })
          .from(workflowPendingProjection),
      ).toEqual([{ anchorRunId: ANCHOR_RUN_ID }]);
    });
  },
);
