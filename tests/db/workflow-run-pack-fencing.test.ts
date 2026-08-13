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
import type { DBExecutor } from "@intx/db";
import { sidecarAllocation } from "@intx/db/schema";
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
        placementSharing: "exclusive",
        sidecarReuse: "never",
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

    async function getBackendPid(tx: DBExecutor): Promise<number> {
      const rows = await tx.execute(
        sql`select pg_backend_pid()::integer as pid`,
      );
      const pid = rows[0]?.["pid"];
      if (typeof pid !== "number") {
        throw new Error(`Expected numeric PostgreSQL backend pid, got ${pid}`);
      }
      return pid;
    }

    async function waitForBlockedBackend(pid: number): Promise<void> {
      for (let attempt = 0; attempt < 200; attempt += 1) {
        const rows = await h.db.execute(
          sql`select cardinality(pg_blocking_pids(${pid})) > 0 as blocked`,
        );
        if (rows[0]?.["blocked"] === true) return;
        await Bun.sleep(10);
      }
      throw new Error(`PostgreSQL backend ${pid} did not become blocked`);
    }

    async function waitForBackendBlockedBy(pid: number): Promise<void> {
      for (let attempt = 0; attempt < 200; attempt += 1) {
        const rows = await h.db.execute(sql`
          select exists (
            select 1
            from pg_stat_activity activity
            where ${pid} = any(pg_blocking_pids(activity.pid))
          ) as blocked
        `);
        if (rows[0]?.["blocked"] === true) return;
        await Bun.sleep(10);
      }
      throw new Error(`No PostgreSQL backend became blocked by backend ${pid}`);
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
        await waitForBlockedBackend(await replacementStarted.promise);
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
        await waitForBackendBlockedBy(replacementPid);
      } finally {
        releaseReplacement.resolve(true);
      }

      await replacementPromise;
      expect(await receivePromise).toEqual({
        accepted: false,
        reason: "path_violation",
      });
      expect(receiveCalls).toBe(0);
    });
  },
);
