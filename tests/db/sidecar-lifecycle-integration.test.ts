import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { eq } from "drizzle-orm";

import { generateKeyPair, sha256 } from "@intx/crypto";
import {
  createSidecarAllocationStore,
  createWorkflowPendingProjectionStore,
  withExecutableWorkflowRun,
} from "@intx/db";
import { sidecar, sidecarAllocation, workflowRun } from "@intx/db/schema";
import {
  createAgentRepoStore,
  createHubSessionLookups,
  createSidecarCredentialResolver,
  createSidecarRouter,
  createSidecarAllocationReconciler,
  createSidecarPluginRegistry,
  createWorkflowHistoryReceiveTracker,
  createWorkflowLifecycleService,
  createWorkflowRunReader,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants, seedWorkflowRun } from "@intx/test-harness/seed";
import {
  WorkflowControlFrame,
  type HostedIncarnation,
} from "@intx/types/sidecar";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import { createMockWs } from "./sidecar-test-helpers";

const TENANT = "tenant-lifecycle-integration";
const SIDECAR = "sidecar-lifecycle-integration";
const TOKEN = "lifecycle-integration-token";

describe.skipIf(!harnessDbEnvAvailable())(
  "sidecar lifecycle with real admission and allocation stores",
  () => {
    let h: TestDb;
    let dataDir: string;
    let now: Date;
    const sockets: ReturnType<typeof createMockWs>[] = [];
    const routers: ReturnType<typeof createSidecarRouter>[] = [];

    beforeAll(async () => {
      h = await createTestDb();
    });
    afterAll(async () => {
      await h.close();
    });
    beforeEach(async () => {
      await h.reset();
      dataDir = await fs.mkdtemp(
        path.join(os.tmpdir(), "sidecar-lifecycle-integration-"),
      );
      now = new Date();
      await seedTenants(h.db, [{ id: TENANT }]);
      await h.db
        .insert(sidecar)
        .values({ id: SIDECAR, tokenHashSha256: await sha256(TOKEN) });
    });
    afterEach(async () => {
      for (const router of routers.splice(0))
        for (const ws of sockets) router.handleClose(ws);
      sockets.length = 0;
      await fs.rm(dataDir, { recursive: true, force: true });
    });

    async function seedCopy(id: string, due = false) {
      const address = `${id}@example.test`;
      await seedWorkflowRun(h.db, {
        id,
        anchorRunId: id,
        tenantId: TENANT,
        address,
        status: "failed",
      });
      await h.db
        .update(workflowRun)
        .set({
          publicKey: "ab".repeat(32),
          endedAt: now,
          failureCode: "original_failure",
          failureMessage: "Original workflow failure",
          lifecyclePolicy: { capacityRetention: { failed: "1h" } },
          capacityReleaseAt: due ? now : new Date(now.getTime() + 3_600_000),
        })
        .where(eq(workflowRun.id, id));
      await h.db.insert(sidecarAllocation).values({
        id: `allocation-${id}`,
        anchorRunId: id,
        tenantId: TENANT,
        sidecarId: SIDECAR,
        status: "allocated",
        generation: 1,
        ensureAcceptedGeneration: 1,
        maxDisconnectedMs: 900_000,
        provisionerId: "test",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "test:v1",
      });
      return {
        id,
        address,
        target: { allocationId: `allocation-${id}`, generation: 1 },
        incarnation: {
          address,
          generation: 1,
          state: "stopped",
          retention: "kept",
        } satisfies HostedIncarnation,
      };
    }

    async function fixture(
      copies: Awaited<ReturnType<typeof seedCopy>>[],
      initialize?: (
        router: ReturnType<typeof createSidecarRouter>,
      ) => Promise<void>,
    ) {
      const signingKey = await generateKeyPair();
      const agentRepoStore = createAgentRepoStore({ dataDir, signingKey });
      const historyReceives = createWorkflowHistoryReceiveTracker();
      const credentials = createSidecarCredentialResolver({
        db: h.db,
        now: () => now,
      });
      const lookups = createHubSessionLookups({
        db: h.db,
        agentRepoStore,
        historyReceives,
      });
      const router = createSidecarRouter({
        hubPublicKey: "a".repeat(64),
        authenticateSidecar: async ({ token }) => credentials.resolve(token),
        resolveSidecarBindings: credentials.resolveBindings,
        validateSidecarIdentity: credentials.isCurrent,
        withExecutableWorkflowRun: (target, send, signal) =>
          withExecutableWorkflowRun(h.db, target, send, signal),
        lookups,
      });
      routers.push(router);
      if (initialize !== undefined) await initialize(router);
      else
        for (const copy of copies)
          router.fenceAllocation(copy.target.allocationId, 1);
      const ws = createMockWs();
      sockets.push(ws);
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "hello",
          sidecarId: SIDECAR,
          token: TOKEN,
          incarnations: copies.map((copy) => copy.incarnation),
        }),
      );
      await ws.awaitSent((sent) =>
        sent.some((frame) => frame.includes('"welcome"')),
      );
      const retentionRequests: ReturnType<typeof router.retainAllocation>[] =
        [];
      const lifecycle = createWorkflowLifecycleService({
        db: h.db,
        retentionRouter: {
          ...router,
          retainAllocation: (...args) => {
            const pending = router.retainAllocation(...args);
            retentionRequests.push(pending);
            return pending;
          },
        },
        historyReceives,
        runReader: createWorkflowRunReader(agentRepoStore.repoStore),
        now: () => now,
      });
      return {
        router,
        ws,
        lookups,
        lifecycle,
        historyReceives,
        retentionRequests,
      };
    }

    async function answerRetain(f: Awaited<ReturnType<typeof fixture>>) {
      const before = f.ws.sent.length;
      const priorRequests = f.retentionRequests.length;
      const running = f.lifecycle.reconcileNext();
      await f.ws.awaitSent((sent) =>
        sent.slice(before).some((raw) => raw.includes('"workflow.control"')),
      );
      const raw = f.ws.sent
        .slice(before)
        .find((frame) => frame.includes('"workflow.control"'));
      if (raw === undefined) throw new Error("Retain request missing");
      const frame = WorkflowControlFrame.assert(JSON.parse(raw));
      expect(frame.action).toBe("retain");
      f.router.handleMessage(
        f.ws,
        JSON.stringify({
          type: "workflow.control.ack",
          requestId: frame.requestId,
          retention: "kept",
          refTips: {
            "refs/heads/main": "unaccepted-local-tip",
            "refs/heads/events": "unaccepted-events-tip",
          },
        }),
      );
      await running;
      // Retention now finishes independently of the lifecycle worker. Await its
      // actual completion (including history-finality failures), not the scan.
      await Promise.allSettled(f.retentionRequests.slice(priorRequests));
    }

    test("a terminal anchor closes retention at its real pack-admission cutoff", async () => {
      const copy = await seedCopy("run_terminal_cutoff");
      const f = await fixture([copy]);
      expect(
        await f.lookups.receiveWorkflowRunPack(
          { kind: "workflow-run", id: deriveWorkflowRunRepoId(copy.address) },
          new Uint8Array(),
          "refs/heads/main",
          "unaccepted-local-tip",
          {
            kind: "allocated",
            agentAddress: copy.address,
            anchorRunId: copy.id,
            ...copy.target,
          },
        ),
      ).toEqual({ accepted: false, reason: "path_violation" });
      await answerRetain(f);
      expect(f.router.getRetentionCandidates()).toEqual([]);
      now = new Date(now.getTime() + 1_001);
      expect(await f.lifecycle.reconcileNext()).toBe(false);
      const run = await h.db.query.workflowRun.findFirst({
        where: eq(workflowRun.id, copy.id),
      });
      expect(run).toMatchObject({
        status: "failed",
        failureCode: "original_failure",
      });
      expect(f.router.getRetainedIncarnations(SIDECAR)).toEqual([copy.target]);
    });

    test("an earlier receive keeps retention pending with backoff until it settles", async () => {
      const copy = await seedCopy("run_pending_receive");
      const f = await fixture([copy]);
      const pending = createWorkflowPendingProjectionStore(h.db);
      await pending.open("pending-receive", copy.id);
      f.historyReceives.begin("pending-receive");
      await answerRetain(f);
      expect(f.router.getRetentionCandidates()).toEqual([
        { ...copy.target, sidecarId: SIDECAR },
      ]);
      now = new Date(now.getTime() + 1_001);
      expect(await f.lifecycle.reconcileNext()).toBe(false);
      f.historyReceives.end("pending-receive");
      await pending.close("pending-receive");
      now = new Date(now.getTime() + 5_000);
      await answerRetain(f);
      expect(f.router.getRetentionCandidates()).toEqual([]);
    });

    test("lifecycle fencing and a concurrent cleanup sync preserve the final active slot", async () => {
      const first = await seedCopy("run_a_release", true);
      const second = await seedCopy("run_b_release", true);
      const f = await fixture([first, second]);
      const anchor = await h.db.query.workflowRun.findFirst({
        where: eq(workflowRun.id, first.id),
      });
      if (anchor === undefined) throw new Error("Missing anchor");
      const runs = Array.from(
        { length: 128 },
        (_, i) => `run_capacity_${String(i)}`,
      );
      await h.db.insert(workflowRun).values(
        runs.map((id) => ({
          id,
          tenantId: TENANT,
          anchorRunId: id,
          definitionId: anchor.definitionId,
          status: "deployed" as const,
        })),
      );
      await h.db.insert(sidecar).values({
        id: "candidate-sidecar",
        tokenHashSha256: await sha256("candidate"),
      });
      await h.db.insert(sidecarAllocation).values(
        runs.map((anchorRunId, i) => ({
          id: `allocation-capacity-${String(i)}`,
          anchorRunId,
          tenantId: TENANT,
          sidecarId: i === 127 ? "candidate-sidecar" : SIDECAR,
          status:
            i === 127 ? ("provisioning" as const) : ("allocated" as const),
          generation: 1,
          ensureAcceptedGeneration: i === 127 ? null : 1,
          provisionerId: "test",
          provisionerApiVersion: 1 as const,
          provisionerBindingFingerprint: "test:v1",
          maxDisconnectedMs: 900_000,
        })),
      );
      expect(await f.lifecycle.reconcileNext()).toBe(true);
      expect(
        f.router.getCleanupConnection({ ...first.target, generation: 2 }),
      ).toBe(f.ws);
      const store = createSidecarAllocationStore(h.db, {
        getRetainedIncarnations: f.router.getRetainedIncarnations,
      });
      // Observe the second release before its caller gets to publish the fence.
      await store.beginRelease({
        allocationId: second.target.allocationId,
        expectedStatus: "allocated",
        expectedGeneration: 1,
      });
      await f.router.syncSidecar(SIDECAR);
      expect(
        f.router.getCleanupConnection({ ...second.target, generation: 2 }),
      ).toBe(f.ws);
      expect(f.router.getRetainedIncarnations(SIDECAR)).toEqual([
        first.target,
        second.target,
      ]);
      expect(
        f.ws.sent
          .map((raw): unknown => JSON.parse(raw))
          .filter(
            (frame) =>
              typeof frame === "object" &&
              frame !== null &&
              "type" in frame &&
              frame.type === "agent.undeploy",
          ),
      ).toEqual([]);
      expect(
        await store.markAllocated({
          allocationId: "allocation-capacity-127",
          generation: 1,
          sidecarId: SIDECAR,
        }),
      ).toMatchObject({ status: "allocated" });
    });
    test("a permanent provider rejection keeps a cleanup binding through Hub restart and hello without reopening work", async () => {
      const copy = await seedCopy("run_permanent_cleanup", true);
      const before = await fixture([copy]);
      const store = createSidecarAllocationStore(h.db, {
        getRetainedIncarnations: before.router.getRetainedIncarnations,
      });
      await store.beginRelease({
        allocationId: copy.target.allocationId,
        expectedStatus: "allocated",
        expectedGeneration: 1,
      });
      let destroys = 0;
      const plugins = createSidecarPluginRegistry({
        provisioners: [
          {
            id: "test",
            apiVersion: 1,
            bindingFingerprint: "test:v1",
            capabilities: [],
            ensure: async () => {
              throw new Error("Must not restart a terminal workflow");
            },
            destroy: async () => {
              destroys++;
              return {
                kind: "rejected",
                code: "provider_permission_denied",
                message: "Operator recovery required",
                retryable: false,
              };
            },
          },
        ],
      });
      before.router.undeployAllocation = async () => {
        throw new Error("Teardown not confirmed");
      };
      const reconciler = createSidecarAllocationReconciler({
        allocationStore: store,
        router: before.router,
        plugins,
        hubWebSocketUrl: "ws://localhost",
      });
      await reconciler.reconcileNext();
      const target = { ...copy.target, generation: 2 };
      expect(await store.findById(target.allocationId)).toMatchObject({
        status: "destroy_failed",
        failureCode: "provider_permission_denied",
        deploymentCleanupConfirmed: false,
      });
      expect(before.router.getCleanupConnection(target)).toBe(before.ws);
      before.router.handleClose(before.ws);

      let restarted:
        | ReturnType<typeof createSidecarAllocationReconciler>
        | undefined;
      const after = await fixture([copy], async (router) => {
        restarted = createSidecarAllocationReconciler({
          allocationStore: createSidecarAllocationStore(h.db, {
            getRetainedIncarnations: router.getRetainedIncarnations,
          }),
          router,
          plugins,
          hubWebSocketUrl: "ws://localhost",
        });
        await restarted.initialize();
      });
      if (restarted === undefined)
        throw new Error("Missing restarted reconciler");
      await restarted.handleConnected(target);
      await restarted.repairUnscheduledConnections();
      expect(await restarted.reconcileNext()).toBe(false);
      expect(destroys).toBe(1);
      expect(after.router.getCleanupConnection(target)).toBe(after.ws);
      expect(after.router.getRetainedIncarnations(SIDECAR)).toEqual([
        copy.target,
      ]);
      expect(await after.router.isAllocatedSidecarReady(target)).toBe(false);
      expect(
        after.ws.sent.some((frame) =>
          frame.includes('"type":"agent.undeploy"'),
        ),
      ).toBe(false);
      expect(await store.findById(target.allocationId)).toMatchObject({
        status: "destroy_failed",
        destroyAttempts: 1,
      });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, copy.id),
        }),
      ).toMatchObject({ status: "failed", failureCode: "original_failure" });
    });

    test("missing inventory fails at two minutes and releases the provider hold without another ensure", async () => {
      const holder = await seedCopy("run_inventory_holder");
      const f = await fixture([holder]);
      f.router.handleClose(f.ws);
      await h.db
        .update(sidecarAllocation)
        .set({ nextAttemptAt: null })
        .where(eq(sidecarAllocation.id, holder.target.allocationId));
      // Drive Hub time independently of either machine's wall clock.
      now = new Date(0);
      const start = now.getTime();
      await seedWorkflowRun(h.db, {
        id: "run_inventory",
        anchorRunId: "run_inventory",
        tenantId: TENANT,
        status: "deployed",
      });
      const store = createSidecarAllocationStore(h.db, {
        getRetainedIncarnations: f.router.getRetainedIncarnations,
      });
      await h.db.insert(sidecarAllocation).values({
        id: "allocation-inventory",
        anchorRunId: "run_inventory",
        tenantId: TENANT,
        provisionerId: "test",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "test:v1",
        maxDisconnectedMs: 900_000,
        status: "pending",
        generation: 0,
        nextAttemptAt: now,
        createdAt: now,
        updatedAt: now,
      });
      let ensures = 0;
      let destroys = 0;
      const reconciler = createSidecarAllocationReconciler({
        allocationStore: store,
        router: f.router,
        plugins: createSidecarPluginRegistry({
          provisioners: [
            {
              id: "test",
              apiVersion: 1,
              bindingFingerprint: "test:v1",
              capabilities: [],
              ensure: async () => {
                ensures++;
                return { kind: "accepted", sidecarId: SIDECAR };
              },
              destroy: async () => {
                destroys++;
                return { kind: "destroyed", cleanup: "confirmed" };
              },
            },
          ],
        }),
        hubWebSocketUrl: "wss://hub.example/ws",
        now: () => now,
        createSidecarId: () => "inventory-minted",
        createToken: () => "inventory-token",
      });
      await reconciler.reconcileNext();
      expect((await store.findById("allocation-inventory"))?.status).toBe(
        "provisioning",
      );
      now = new Date(start + 119_999);
      await reconciler.reconcileNext();
      expect((await store.findById("allocation-inventory"))?.status).toBe(
        "provisioning",
      );
      now = new Date(start + 120_000);
      // Advance the database schedule with the injected Hub clock. Its minimum
      // delay deliberately prevents the old wall-clock deadline from being due.
      await h.db
        .update(sidecarAllocation)
        .set({ nextAttemptAt: new Date(0) })
        .where(eq(sidecarAllocation.id, "allocation-inventory"));
      await reconciler.reconcileNext();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, "run_inventory"),
        }),
      ).toMatchObject({
        status: "failed",
        failureCode: "sidecar_inventory_unavailable",
      });
      expect((await store.findById("allocation-inventory"))?.status).toBe(
        "releasing",
      );
      await reconciler.reconcileNext();
      expect((await store.findById("allocation-inventory"))?.status).toBe(
        "released",
      );
      expect(ensures).toBe(1);
      expect(destroys).toBe(1);
      expect(await reconciler.reconcileNext()).toBe(false);
    });
  },
);
