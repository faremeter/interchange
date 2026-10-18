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
import { eq, inArray } from "drizzle-orm";

import { generateKeyPair, sha256 } from "@intx/crypto";
import {
  createSidecarAllocationStore,
  createWorkflowPendingProjectionStore,
  withExecutableWorkflowRun,
} from "@intx/db";
import { sidecar, sidecarAllocation, workflowRun } from "@intx/db/schema";
import {
  adapterManifestDigest,
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
  DEFAULT_SIDECAR_CAPACITY,
  AgentUndeployFrame,
  WorkflowControlFrame,
  type HostedIncarnation,
} from "@intx/types/sidecar";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import { createMockWs } from "./sidecar-test-helpers";

const NO_MANIFEST_DIGEST = await adapterManifestDigest(undefined);
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
      await h.db.insert(sidecar).values({
        id: SIDECAR,
        tokenHashSha256: await sha256(TOKEN),
        provisionerId: "test",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:v1",
        adapterManifestDigest: NO_MANIFEST_DIGEST,
      });
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
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:v1",
        adapterManifestDigest: NO_MANIFEST_DIGEST,
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
          capacity: DEFAULT_SIDECAR_CAPACITY,
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
      expect(
        f.router.getSidecarInventory(SIDECAR)?.retainedIncarnations,
      ).toEqual([copy.target]);
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
        provisionerId: "test",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:v1",
        adapterManifestDigest: NO_MANIFEST_DIGEST,
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
          provisionerApiVersion: 2 as const,
          provisionerBindingFingerprint: "test:v1",
          adapterManifestDigest: NO_MANIFEST_DIGEST,
          maxDisconnectedMs: 900_000,
        })),
      );
      expect(await f.lifecycle.reconcileNext()).toBe(true);
      expect(
        f.router.getCleanupConnection({ ...first.target, generation: 2 }),
      ).toBe(f.ws);
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: f.router.getSidecarInventory,
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
      expect(
        f.router.getSidecarInventory(SIDECAR)?.retainedIncarnations,
      ).toEqual([first.target, second.target]);
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
        getSidecarInventory: before.router.getSidecarInventory,
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
            apiVersion: 2,
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
            getSidecarInventory: router.getSidecarInventory,
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
      expect(
        after.router.getSidecarInventory(SIDECAR)?.retainedIncarnations,
      ).toEqual([copy.target]);
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

    test("a parked replacement removes its previous copy before ensuring new capacity", async () => {
      const copy = await seedCopy("run_parked_replacement");
      await h.db
        .update(workflowRun)
        .set({
          status: "running",
          endedAt: null,
          capacityReleaseAt: null,
          failureCode: null,
          failureMessage: null,
        })
        .where(eq(workflowRun.id, copy.id));
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      await store.wakeReconciliation(copy.target.allocationId, 1);
      await store.claimNextReconcilable({
        leaseId: "parked-replacement",
        leaseDurationMs: 60_000,
      });
      expect(
        await store.beginReplacement({
          allocationId: copy.target.allocationId,
          expectedStatus: "allocated",
          expectedGeneration: 1,
          expectedLeaseId: "parked-replacement",
          firstAttemptAt: new Date(0),
          nextAttemptAt: new Date(0),
          failureCode: "sidecar_connect_failed",
          failureMessage: "Parked worker disconnected",
        }),
      ).toMatchObject({
        status: "replacing",
        generation: 2,
        deploymentCleanupConfirmed: false,
        providerHoldReleased: false,
      });
      const credentials = createSidecarCredentialResolver({ db: h.db });
      expect(await credentials.resolveBindings(SIDECAR)).toMatchObject([
        {
          kind: "cleanup",
          allocationId: copy.target.allocationId,
          generation: 2,
        },
      ]);
      let ensures = 0;
      let reconciler:
        | ReturnType<typeof createSidecarAllocationReconciler>
        | undefined;
      const f = await fixture([copy], async (router) => {
        reconciler = createSidecarAllocationReconciler({
          allocationStore: store,
          router,
          plugins: createSidecarPluginRegistry({
            provisioners: [
              {
                id: "test",
                apiVersion: 2,
                bindingFingerprint: "test:v1",
                capabilities: [],
                destroy: async () => ({
                  kind: "destroyed",
                  cleanup: "required",
                }),
                ensure: async () => {
                  ensures++;
                  return { kind: "accepted" };
                },
              },
            ],
          }),
          hubWebSocketUrl: "wss://hub.example/ws",
          createSidecarId: () => "parked-replacement-sidecar",
        });
        await reconciler.initialize();
      });
      if (reconciler === undefined) throw new Error("Missing reconciler");
      const target = { ...copy.target, generation: 2 };
      expect(f.router.getCleanupConnection(target)).toBe(f.ws);
      expect(f.router.getRoutableAddresses()).toEqual([]);
      const running = reconciler.reconcileNext();
      await f.ws.awaitSent((sent) =>
        sent.some((raw) => raw.includes('"type":"agent.undeploy"')),
      );
      expect(ensures).toBe(0);
      const request = f.ws.sent.find((raw) =>
        raw.includes('"type":"agent.undeploy"'),
      );
      if (request === undefined) throw new Error("Missing undeploy request");
      const frame = AgentUndeployFrame.assert(JSON.parse(request));
      f.router.handleMessage(
        f.ws,
        JSON.stringify({
          type: "agent.undeploy.ack",
          requestId: frame.requestId,
          agentAddress: frame.agentAddress,
          generation: frame.generation,
        }),
      );
      await running;
      expect(ensures).toBe(1);
      expect(await store.findById(copy.target.allocationId)).toMatchObject({
        status: "allocated",
        sidecarId: "parked-replacement-sidecar",
        generation: 3,
        deploymentCleanupConfirmed: false,
        providerHoldReleased: false,
      });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, copy.id),
        }),
      ).toMatchObject({
        status: "running",
        publicKey: null,
        credentialRefs: null,
      });
    });

    test("a delayed cleanup hello cannot displace its running replacement", async () => {
      const copy = await seedCopy("run_replaced");
      const peer = await seedCopy("run_peer");
      await h.db
        .update(workflowRun)
        .set({
          status: "running",
          endedAt: null,
          capacityReleaseAt: null,
          failureCode: null,
          failureMessage: null,
        })
        .where(inArray(workflowRun.id, [copy.id, peer.id]));
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      await store.wakeReconciliation(copy.target.allocationId, 1);
      await store.claimNextReconcilable({
        leaseId: "begin-replacement",
        leaseDurationMs: 60_000,
      });
      const cleanup = await store.beginReplacement({
        allocationId: copy.target.allocationId,
        expectedStatus: "allocated",
        expectedGeneration: 1,
        expectedLeaseId: "begin-replacement",
        firstAttemptAt: new Date(0),
        nextAttemptAt: new Date(0),
        failureCode: "sidecar_connect_failed",
        failureMessage: "Parked worker disconnected",
      });
      if (cleanup === null) throw new Error("Missing replacement allocation");
      const cleanupTarget = {
        allocationId: cleanup.id,
        generation: cleanup.generation,
      };
      await store.claimNextReconcilable({
        leaseId: "replacement-capacity",
        leaseDurationMs: 60_000,
      });
      expect(
        await store.confirmDeploymentCleanup({
          ...cleanupTarget,
          expectedLeaseId: "replacement-capacity",
        }),
      ).toBe(true);

      const readingPeer = Promise.withResolvers<undefined>();
      const finishHello = Promise.withResolvers<undefined>();
      const credentials = createSidecarCredentialResolver({ db: h.db });
      const router = createSidecarRouter({
        authenticateSidecar: async ({ token }) => credentials.resolve(token),
        resolveSidecarBindings: credentials.resolveBindings,
        validateSidecarIdentity: async (identity, use) => {
          const current = await credentials.isCurrent(identity, use);
          if (
            identity.allocationId === peer.target.allocationId &&
            use === "reclaim"
          ) {
            readingPeer.resolve(undefined);
            await finishHello.promise;
          }
          return current;
        },
        withExecutableWorkflowRun: (target, send, signal) =>
          withExecutableWorkflowRun(h.db, target, send, signal),
      });
      routers.push(router);
      router.fenceAllocation(cleanup.id, cleanup.generation, {
        cleanup: { sidecarId: SIDECAR },
      });
      router.fenceAllocation(peer.target.allocationId, peer.target.generation);
      const connect = (
        sidecarId: string,
        token: string,
        incarnations: HostedIncarnation[],
      ) => {
        const ws = createMockWs();
        sockets.push(ws);
        router.handleOpen(ws);
        router.handleMessage(
          ws,
          JSON.stringify({
            type: "hello",
            sidecarId,
            token,
            capacity: DEFAULT_SIDECAR_CAPACITY,
            incarnations,
          }),
        );
        return ws;
      };
      // Removal is already confirmed. The reconnect snapshots the cleanup
      // binding while the provider's release is still pending.
      const old = connect(SIDECAR, TOKEN, [
        { address: peer.address, generation: 1, state: "live" },
      ]);
      try {
        await readingPeer.promise;
        expect(
          await store.confirmProviderRelease({
            ...cleanupTarget,
            expectedLeaseId: "replacement-capacity",
          }),
        ).toBe(true);
        const bound = await store.bindReplacementSidecar({
          ...cleanupTarget,
          sidecarId: "replacement-sidecar",
          tokenHashSha256: await sha256("replacement-token"),
          connectDeadline: new Date(Date.now() + 120_000),
          expectedLeaseId: "replacement-capacity",
          adapterManifestDigest: NO_MANIFEST_DIGEST,
        });
        if (bound === null) throw new Error("Missing replacement binding");
        const target = { allocationId: bound.id, generation: bound.generation };
        router.fenceAllocation(bound.id, bound.generation);
        await store.markAllocated({
          ...target,
          expectedLeaseId: "replacement-capacity",
        });
        const initialization = {
          ...target,
          anchorRunId: copy.id,
          tenantId: TENANT,
          leaseId: "replacement-capacity",
          signal: new AbortController().signal,
        };
        expect(await store.beginInitialization(initialization)).toBe(true);
        expect(
          await store.completeInitialization({
            ...initialization,
            publicKey: "cd".repeat(32),
          }),
        ).toBe(true);
        const replacement = connect(
          "replacement-sidecar",
          "replacement-token",
          [
            {
              address: copy.address,
              generation: bound.generation,
              state: "live",
            },
          ],
        );
        await replacement.awaitSent((sent) =>
          sent.some((frame) => frame.includes('"welcome"')),
        );
        expect(router.getRoutableAddresses()).toContain(copy.address);

        finishHello.resolve(undefined);
        await old.awaitSent((sent) =>
          sent.some((frame) => frame.includes('"welcome"')),
        );
        await router.syncSidecar(SIDECAR);
        expect(replacement.closed).toBe(false);
        expect(
          replacement.sent.some((frame) => frame.includes('"agent.undeploy"')),
        ).toBe(false);
        expect(router.getRoutableAddresses()).toContain(copy.address);
        expect(await router.isAllocatedSidecarReady(target)).toBe(true);
        expect(router.getCleanupConnection(cleanupTarget)).toBeUndefined();
      } finally {
        finishHello.resolve(undefined);
        await router.syncSidecar(SIDECAR);
      }
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
        getSidecarInventory: f.router.getSidecarInventory,
      });
      await h.db.insert(sidecarAllocation).values({
        id: "allocation-inventory",
        anchorRunId: "run_inventory",
        tenantId: TENANT,
        provisionerId: "test",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:v1",
        adapterManifestDigest: NO_MANIFEST_DIGEST,
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
              apiVersion: 2,
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
      await h.db
        .update(sidecarAllocation)
        .set({ nextAttemptAt: new Date(0) })
        .where(eq(sidecarAllocation.id, "allocation-inventory"));
      expect(await reconciler.reconcileNext()).toBe(true);
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
