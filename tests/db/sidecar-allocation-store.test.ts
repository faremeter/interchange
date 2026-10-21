import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq, sql } from "drizzle-orm";
import {
  DEFAULT_SIDECAR_CAPACITY,
  DEFAULT_SIDECAR_ACTIVE_DEPLOYMENTS,
} from "@intx/types/sidecar";
import {
  createSidecarAllocationReconciler,
  createSidecarPluginRegistry,
  createSidecarRouter,
  recoverSenderDeploy,
} from "@intx/hub-sessions";

import {
  createSidecarAllocationStore,
  createWorkflowProbeStore,
  createWorkflowRunDispatchStore,
  createWorkflowRunLaunchSpecStore,
  SIDECAR_DEPLOYMENT_STOPPED_FAILURE_CODE,
  SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
  SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
  SIDECAR_CLEANUP_PREPARATION_TIMEOUT_FAILURE_CODE,
  SIDECAR_CLEANUP_OPERATOR_RELEASED_FAILURE_CODE,
  SidecarReuseRejectedError,
  type RetainedSidecarIncarnation,
} from "@intx/db";
import {
  sidecar,
  sidecarAllocation,
  workflowDefinition,
  workflowPendingProjection,
  workflowRun,
  runToken,
} from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
  seedRunToken,
} from "@intx/test-harness/seed";

const TENANT_ID = "tnt-allocation";
const DEFINITION_ID = "wfd-allocation";
const ANCHOR_RUN_ID = "dep-allocation";
const PRINCIPAL_ID = "prn-allocation";
const MANIFEST_DIGEST = "1".repeat(64);
const OTHER_MANIFEST_DIGEST = "2".repeat(64);

describe.skipIf(!harnessDbEnvAvailable())(
  "sidecarAllocationStore (real DB)",
  () => {
    let h: TestDb;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedPrincipal(h.db, {
        id: PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: "user-allocation",
        status: "active",
      });
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION_ID,
        tenantId: TENANT_ID,
        name: DEFINITION_ID,
      });
      await seedWorkflowRun(h.db, {
        id: ANCHOR_RUN_ID,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
      await createWorkflowRunLaunchSpecStore(h.db).create({
        anchorRunId: ANCHOR_RUN_ID,
        sessionId: "ses-allocation",
        deploymentDomain: "tenant.example",
        sourceAuthorityPrincipalId: PRINCIPAL_ID,
        frozenApprovalBundle: {
          source: {
            kind: "asset",
            assetId: "ast-allocation",
            package: { format: "source", commitSha: "c0ffee".padEnd(40, "0") },
          },
          entry: "./workflow.mjs",
          projection: {
            id: "workflow",
            triggers: [],
            stepOrder: [],
            steps: {},
          },
          closure: { schemaVersion: "1", topLevel: [], entries: [] },
          approvedWireHash: "a".repeat(64),
          approvedGrants: [],
        },
        sourceOfferingIds: ["offering-primary"],
        defaultSourceOfferingId: "offering-primary",
        deployContent: { systemPrompt: "" },
      });
    });

    async function seedAnchor(anchorRunId: string): Promise<void> {
      await seedWorkflowRun(h.db, {
        id: anchorRunId,
        anchorRunId,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
      const launchSpecStore = createWorkflowRunLaunchSpecStore(h.db);
      const launchSpec = await launchSpecStore.get(ANCHOR_RUN_ID);
      if (launchSpec === null) throw new Error("Expected launch specification");
      await launchSpecStore.create({
        ...launchSpec,
        anchorRunId,
        sessionId: `ses-${anchorRunId}`,
      });
    }

    // Binds a fresh identity for the allocation's first generation, as the
    // reconciler does before calling ensure.
    async function bindFirstGeneration(
      allocationId: string,
      anchorRunId: string,
      provisionerId = "ec2-spot",
      adapterManifestDigest = MANIFEST_DIGEST,
    ) {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      await store.createPending({
        id: allocationId,
        anchorRunId,
        tenantId: TENANT_ID,
        provisionerId,
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: `${provisionerId}:test`,
        adapterManifestDigest,
        maxDisconnectedMs: 900_000,
      });
      await store.bindInitialSidecar({
        allocationId,
        expectedGeneration: 0,
        sidecarId: `${allocationId}-minted`,
        tokenHashSha256: new TextEncoder().encode(allocationId),
        adapterManifestDigest,
        connectDeadline: new Date(Date.now() + 60_000),
      });
      return store;
    }

    // Puts a probing probe of the allocations' provisioner binding on its own
    // sidecar, which then hosts no allocation.
    async function seedProbeHost(
      probeId: string,
      adapterManifestDigest: string,
    ) {
      await seedAsset(h.db, {
        id: "ast-allocation",
        tenantId: TENANT_ID,
        kind: "workflow",
        name: "allocation-workflow",
      });
      const probeStore = createWorkflowProbeStore(h.db);
      await probeStore.create({
        id: probeId,
        tenantId: TENANT_ID,
        definitionAssetId: "ast-allocation",
        source: { kind: "registry", registry: "npmjs" },
        entry: "./workflow.mjs",
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest,
      });
      await probeStore.bindSidecar({
        probeId,
        sidecarId: `${probeId}-minted`,
        tokenHashSha256: new TextEncoder().encode(probeId),
      });
      await probeStore.markProbing({ probeId });
    }

    test.each([false, true])(
      "an undeploy acknowledgement survives provider failure and restart (connected: %s)",
      async (connectedAfterRestart) => {
        const id = "alloc-durable-cleanup";
        const store = await bindFirstGeneration(id, ANCHOR_RUN_ID);
        await store.markAllocated({ allocationId: id, generation: 1 });
        expect(
          (
            await store.beginRelease({
              allocationId: id,
              expectedStatus: "allocated",
              expectedGeneration: 1,
            })
          )?.deploymentCleanupConfirmed,
        ).toBe(false);
        let connected = true;
        const connection = {};
        const undeploys: boolean[] = [];
        const confirmedAtDestroy: (boolean | undefined)[] = [];
        let destroys = 0;
        const dependencies = {
          plugins: createSidecarPluginRegistry({
            provisioners: [
              {
                id: "ec2-spot",
                apiVersion: 2,
                bindingFingerprint: "ec2-spot:test",
                capabilities: [],
                ensure: async () => ({ kind: "accepted" }),
                destroy: async () => {
                  destroys++;
                  // The acknowledgement must be durable before provider work can
                  // fail or disappear with the Hub process.
                  confirmedAtDestroy.push(
                    (await store.findById(id))?.deploymentCleanupConfirmed,
                  );
                  if (destroys === 1) throw new Error("Provider unavailable");
                  return { kind: "destroyed", cleanup: "required" };
                },
              },
            ],
          }),
          router: {
            getCleanupConnection: () => (connected ? connection : undefined),
            undeployAllocation: async () => {
              undeploys.push(connected);
            },
            fenceAllocation: () => undefined,
            retireAllocation: () => undefined,
            syncSidecar: async () => undefined,
            holdsAllocatedBinding: () => false,
            isAllocatedSidecarReady: async () => false,
            reportedDeploymentFailure: () => undefined,
            waitForAllocatedSidecar: async () => undefined,
            waitForSidecarInventory: async () => undefined,
          },
          hubWebSocketUrl: "ws://localhost",
        };
        await createSidecarAllocationReconciler({
          ...dependencies,
          allocationStore: store,
        }).reconcileNext();
        expect(await store.findById(id)).toMatchObject({
          status: "releasing",
          deploymentCleanupConfirmed: true,
          destroyAttempts: 1,
        });
        expect(confirmedAtDestroy).toEqual([true]);
        connected = connectedAfterRestart;
        const restartedStore = createSidecarAllocationStore(h.db, {
          getSidecarInventory: () => ({
            capacity: DEFAULT_SIDECAR_CAPACITY,
            retainedIncarnations: [],
          }),
        });
        const restarted = createSidecarAllocationReconciler({
          ...dependencies,
          allocationStore: restartedStore,
        });
        await restarted.initialize();
        // Make the existing backoff due without a wall-clock wait.
        await restartedStore.wakeReconciliation(id, 2);
        expect(await restarted.reconcileNext()).toBe(true);
        expect(await restartedStore.findById(id)).toMatchObject({
          status: "released",
          deploymentCleanupConfirmed: true,
        });
        expect(
          (await restartedStore.findById(id))?.connectDeadline,
        ).toBeUndefined();
        expect(undeploys).toEqual([true]);
        expect(confirmedAtDestroy).toEqual([true, true]);
        expect(destroys).toBe(2);
      },
    );

    test("a provider release survives restart and its outage cannot exhaust disconnected cleanup", async () => {
      const id = "alloc-provider-release";
      const store = await bindFirstGeneration(id, ANCHOR_RUN_ID);
      await store.markAllocated({ allocationId: id, generation: 1 });
      await store.beginRelease({
        allocationId: id,
        expectedStatus: "allocated",
        expectedGeneration: 1,
      });
      let connected = false;
      const connection = {};
      let destroys = 0;
      let undeploys = 0;
      const dependencies = {
        plugins: createSidecarPluginRegistry({
          provisioners: [
            {
              id: "ec2-spot",
              apiVersion: 2,
              bindingFingerprint: "ec2-spot:test",
              capabilities: [],
              ensure: async () => ({ kind: "accepted" }),
              destroy: async () => {
                if (++destroys > 1) throw new Error("Provider API unavailable");
                return { kind: "destroyed", cleanup: "required" };
              },
            },
          ],
        }),
        router: {
          getCleanupConnection: () => (connected ? connection : undefined),
          undeployAllocation: async () => {
            undeploys++;
          },
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          syncSidecar: async () => undefined,
          holdsAllocatedBinding: () => false,
          isAllocatedSidecarReady: async () => false,
          reportedDeploymentFailure: () => undefined,
          waitForAllocatedSidecar: async () => undefined,
          waitForSidecarInventory: async () => undefined,
        },
        hubWebSocketUrl: "ws://localhost",
        maxCleanupAttempts: 1,
      };
      await createSidecarAllocationReconciler({
        ...dependencies,
        allocationStore: store,
      }).reconcileNext();
      const restartedStore = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      expect(await restartedStore.findById(id)).toMatchObject({
        status: "releasing",
        providerHoldReleased: true,
        deploymentCleanupConfirmed: false,
        destroyAttempts: 0,
      });
      await h.db
        .update(sidecarAllocation)
        .set({ connectDeadline: new Date(0), nextAttemptAt: new Date(0) })
        .where(eq(sidecarAllocation.id, id));
      const restarted = createSidecarAllocationReconciler({
        ...dependencies,
        allocationStore: restartedStore,
      });
      await restarted.initialize();
      expect(await restarted.reconcileNext()).toBe(true);
      expect(await restartedStore.findById(id)).toMatchObject({
        status: "destroy_failed",
        failureCode: SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
        providerHoldReleased: true,
        destroyAttempts: 0,
      });
      connected = true;
      await restarted.handleConnected({ allocationId: id, generation: 2 });
      expect(await restarted.reconcileNext()).toBe(true);
      expect(await restartedStore.findById(id)).toMatchObject({
        status: "released",
        providerHoldReleased: true,
        deploymentCleanupConfirmed: true,
      });
      expect(destroys).toBe(2);
      expect(undeploys).toBe(1);
    });

    test.each([
      { transition: "release", missing: "both" },
      { transition: "release", missing: "provider" },
      { transition: "release", missing: "deployment" },
      { transition: "replacement", missing: "both" },
      { transition: "replacement", missing: "provider" },
      { transition: "replacement", missing: "deployment" },
    ] as const)(
      "cleanup proofs gate the store transition: %j",
      async ({ transition, missing }) => {
        const { store, allocation, leaseId } =
          await createClaimedAllocation("alloc-proof-gate");
        const previous = {
          allocationId: allocation.id,
          expectedStatus: "allocated" as const,
          expectedGeneration: allocation.generation,
          expectedLeaseId: leaseId,
        };
        const cleanup =
          transition === "release"
            ? await store.beginRelease(previous)
            : await store.beginReplacement({
                ...previous,
                firstAttemptAt: new Date(0),
                nextAttemptAt: new Date(0),
                failureCode: "sidecar_connect_failed",
                failureMessage: "Parked worker disconnected",
              });
        if (cleanup === null) throw new Error("Missing cleanup allocation");
        const confirmation = {
          allocationId: cleanup.id,
          generation: cleanup.generation,
          expectedLeaseId: "proof-gate-cleanup",
        };
        expect(
          await store.claimNextReconcilable({
            leaseId: confirmation.expectedLeaseId,
            leaseDurationMs: 60_000,
          }),
        ).toMatchObject({ id: cleanup.id });
        if (missing === "provider")
          expect(await store.confirmDeploymentCleanup(confirmation)).toBe(true);
        if (missing === "deployment")
          expect(await store.confirmProviderRelease(confirmation)).toBe(true);
        const finish = () =>
          transition === "release"
            ? store.markReleased(confirmation)
            : store.bindReplacementSidecar({
                ...confirmation,
                sidecarId: "proof-gate-replacement",
                tokenHashSha256: new Uint8Array([4, 5, 6]),
                connectDeadline: new Date(Date.now() + 60_000),
                adapterManifestDigest: MANIFEST_DIGEST,
              });
        const before = await store.findById(cleanup.id);
        expect(await finish()).toBeNull();
        expect(await store.findById(cleanup.id)).toEqual(before);
        expect(
          await h.db.query.sidecar.findFirst({
            where: eq(sidecar.id, "proof-gate-replacement"),
          }),
        ).toBeUndefined();

        expect(await store.confirmDeploymentCleanup(confirmation)).toBe(true);
        expect(await store.confirmProviderRelease(confirmation)).toBe(true);
        expect(await finish()).toMatchObject({
          status: transition === "release" ? "released" : "provisioning",
          deploymentCleanupConfirmed: transition === "release",
          providerHoldReleased: transition === "release",
        });
      },
    );

    test("an allocation that never acquired a sidecar can be released without provider proof", async () => {
      const store = createSidecarAllocationStore(h.db);
      const pending = await store.createPending({
        id: "alloc-never-bound",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      const releasing = await store.beginRelease({
        allocationId: pending.id,
        expectedStatus: "pending",
        expectedGeneration: pending.generation,
      });
      if (releasing === null) throw new Error("Missing release");
      expect(releasing.sidecarId).toBeUndefined();
      expect(releasing.providerHoldReleased).toBe(false);
      expect(
        await store.markReleased({
          allocationId: releasing.id,
          generation: releasing.generation,
        }),
      ).toMatchObject({
        status: "released",
        deploymentCleanupConfirmed: true,
        providerHoldReleased: true,
      });
    });

    test.each([
      {
        method: "confirmDeploymentCleanup",
        field: "deploymentCleanupConfirmed",
        unaccepted: true,
      },
      {
        method: "confirmProviderRelease",
        field: "providerHoldReleased",
        unaccepted: false,
      },
    ] as const)(
      "$field belongs to a current lease and cleanup generation, and replacement work resets it",
      async ({ method, field, unaccepted }) => {
        const { store, allocation, initialization } =
          await createClaimedAllocation("alloc-cleanup-guards");
        const confirm = store[method];
        expect(
          await confirm({
            allocationId: allocation.id,
            generation: 1,
            expectedLeaseId: initialization.leaseId,
          }),
        ).toBe(false);
        // Model replacement before any deployment was accepted.
        await h.db
          .update(sidecarAllocation)
          .set({
            status: "provisioning",
            ensureAcceptedGeneration: null,
            providerHoldReleased: true,
          })
          .where(eq(sidecarAllocation.id, allocation.id));
        await store.beginReplacement({
          allocationId: allocation.id,
          expectedStatus: "provisioning",
          expectedGeneration: 1,
          expectedLeaseId: initialization.leaseId,
          firstAttemptAt: new Date(0),
          nextAttemptAt: new Date(0),
          failureCode: "ensure_outcome_unknown",
          failureMessage: "Ensure interrupted",
        });
        expect((await store.findById(allocation.id))?.[field]).toBe(unaccepted);
        await store.claimNextReconcilable({
          leaseId: "cleanup-1",
          leaseDurationMs: 60_000,
        });
        const confirmation = {
          allocationId: allocation.id,
          generation: 2,
          expectedLeaseId: "cleanup-1",
        };
        expect(
          await confirm({
            ...confirmation,
            generation: 1,
          }),
        ).toBe(false);
        expect(
          await confirm({
            ...confirmation,
            expectedLeaseId: "old-owner",
          }),
        ).toBe(false);
        await h.db
          .update(sidecarAllocation)
          .set({ reconciliationLeaseExpiresAt: new Date(0) })
          .where(eq(sidecarAllocation.id, allocation.id));
        expect(await confirm(confirmation)).toBe(false);
        await store.claimNextReconcilable({
          leaseId: "cleanup-2",
          leaseDurationMs: 60_000,
        });
        expect(await confirm(confirmation)).toBe(false);
        const current = { ...confirmation, expectedLeaseId: "cleanup-2" };
        expect(await confirm(current)).toBe(true);
        expect(await confirm(current)).toBe(true);
        expect(
          (await createSidecarAllocationStore(h.db).findById(allocation.id))?.[
            field
          ],
        ).toBe(true);

        expect(await store.confirmProviderRelease(current)).toBe(true);
        expect(
          (
            await store.bindReplacementSidecar({
              allocationId: allocation.id,
              generation: 2,
              sidecarId: "replacement-worker",
              tokenHashSha256: new Uint8Array([8, 9, 10]),
              connectDeadline: new Date(Date.now() + 60_000),
              expectedLeaseId: "cleanup-2",
              adapterManifestDigest: MANIFEST_DIGEST,
            })
          )?.[field],
        ).toBe(false);
        expect(await confirm(current)).toBe(false);
        await store.markAllocated({
          allocationId: allocation.id,
          generation: 3,
          expectedLeaseId: "cleanup-2",
        });
        // A new release must never inherit a previous cleanup's confirmation.
        await h.db
          .update(sidecarAllocation)
          .set({ deploymentCleanupConfirmed: true, providerHoldReleased: true })
          .where(eq(sidecarAllocation.id, allocation.id));
        expect(
          (
            await store.beginRelease({
              allocationId: allocation.id,
              expectedGeneration: 3,
              expectedStatus: "allocated",
              expectedLeaseId: "cleanup-2",
            })
          )?.[field],
        ).toBe(false);
        await store.claimNextReconcilable({
          leaseId: "cleanup-3",
          leaseDurationMs: 60_000,
        });
        expect(
          await confirm({
            ...current,
            expectedLeaseId: "cleanup-3",
          }),
        ).toBe(false);
        const releasedProof = {
          ...current,
          generation: 4,
          expectedLeaseId: "cleanup-3",
        };
        expect(await confirm(releasedProof)).toBe(true);
        expect(await store.confirmDeploymentCleanup(releasedProof)).toBe(true);
        expect(await store.confirmProviderRelease(releasedProof)).toBe(true);
        await store.markReleased(releasedProof);
        expect(
          await confirm({
            ...current,
            generation: 4,
            expectedLeaseId: "cleanup-3",
          }),
        ).toBe(false);
      },
    );

    test("release preserves an existing disconnect deadline and a timeout keeps its cleanup fence without spending attempts", async () => {
      const id = "alloc-cleanup-disconnected";
      const store = await bindFirstGeneration(id, ANCHOR_RUN_ID);
      await store.markAllocated({ allocationId: id, generation: 1 });
      await h.db
        .update(workflowRun)
        .set({ publicKey: "committed-key" })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));
      const at = new Date();
      const disconnected = await store.markConnectionLost({
        allocationId: id,
        generation: 1,
        now: new Date(at.getTime() - 180_000),
        firstConnectDeadline: at,
      });
      const released = await store.beginRelease({
        allocationId: id,
        expectedStatus: "allocated",
        expectedGeneration: 1,
      });
      expect(released?.connectDeadline).toEqual(disconnected?.connectDeadline);
      const leaseId = "cleanup-wait";
      expect(
        await store.claimNextReconcilable({ leaseId, leaseDurationMs: 60_000 }),
      ).not.toBeNull();
      const args = {
        allocationId: id,
        generation: 2,
        expectedLeaseId: leaseId,
        connected: false,
        now: at,
      };
      expect(
        (await store.recordCleanupConnection(args))?.connectDeadline,
      ).toEqual(disconnected?.connectDeadline);
      const restarted = createSidecarAllocationStore(h.db);
      expect((await restarted.findById(id))?.connectDeadline).toEqual(
        disconnected?.connectDeadline,
      );
      expect(
        await restarted.recordCleanupConnection({
          ...args,
          expectedLeaseId: "stale",
          now: new Date(at.getTime() + 60_000),
        }),
      ).toBeNull();
      await h.db
        .update(sidecarAllocation)
        .set({ destroyAttempts: 3 })
        .where(eq(sidecarAllocation.id, id));
      const failed = await restarted.markDestroyFailed({
        allocationId: id,
        expectedGeneration: 2,
        expectedLeaseId: leaseId,
        code: SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
        message: "Sidecar did not reconnect before its cleanup deadline",
        countAttempt: false,
      });
      expect(failed).toMatchObject({
        status: "destroy_failed",
        destroyAttempts: 3,
      });
      expect(
        (await restarted.listActive()).map((allocation) => allocation.id),
      ).toContain(id);
      expect(await restarted.wakeReconciliation(id, 2)).toBe(false);
      const failedRun = await h.db.query.workflowRun.findFirst({
        where: eq(workflowRun.id, ANCHOR_RUN_ID),
      });
      expect(failedRun?.status).toBe("failed");
      expect(await restarted.resumeCleanupAfterDisconnect(id, 1)).toBe(false);
      expect(await restarted.resumeCleanupAfterDisconnect(id, 2)).toBe(true);
      expect(await restarted.resumeCleanupAfterDisconnect(id, 2)).toBe(false);
      expect(await restarted.findById(id)).toMatchObject({
        status: "releasing",
        generation: 2,
        destroyAttempts: 3,
      });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, ANCHOR_RUN_ID),
        }),
      ).toEqual(failedRun);
    });

    test.each([
      "parked",
      "expired lease",
      "active lease",
      "backoff",
      "attempt changed",
      "generation changed",
      "released",
      "exhausted",
      "permanent refusal",
    ] as const)("parked cleanup repair respects %s state", async (scenario) => {
      const id = "alloc-parked-cleanup";
      const store = await bindFirstGeneration(id, ANCHOR_RUN_ID);
      await store.markAllocated({ allocationId: id, generation: 1 });
      await store.beginRelease({
        allocationId: id,
        expectedStatus: "allocated",
        expectedGeneration: 1,
      });
      const deadline = new Date(Date.now() + 900_000);
      await h.db
        .update(sidecarAllocation)
        .set({
          connectDeadline: deadline,
          nextAttemptAt:
            scenario === "backoff" ? new Date(Date.now() + 30_000) : deadline,
          destroyAttempts: scenario === "attempt changed" ? 3 : 2,
          generation: scenario === "generation changed" ? 3 : 2,
          ...(scenario === "active lease" || scenario === "expired lease"
            ? {
                reconciliationLeaseId: "another-worker",
                reconciliationLeaseExpiresAt: new Date(
                  Date.now() + (scenario === "active lease" ? 60_000 : -1_000),
                ),
              }
            : {}),
          ...(scenario === "released" ? { status: "released" } : {}),
          ...(scenario === "exhausted" || scenario === "permanent refusal"
            ? {
                status: "destroy_failed",
                failureCode:
                  scenario === "exhausted"
                    ? SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE
                    : "provider_permission_denied",
              }
            : {}),
        })
        .where(eq(sidecarAllocation.id, id));
      const before = await store.findById(id);
      const shouldWake = scenario === "parked" || scenario === "expired lease";
      expect(
        await store.resumeCleanupAfterDisconnect(id, 2, {
          deadline,
          destroyAttempts: 2,
        }),
      ).toBe(shouldWake);
      const after = await store.findById(id);
      if (shouldWake) {
        expect(after).toMatchObject({
          status: "releasing",
          destroyAttempts: 2,
        });
        expect(after?.connectDeadline).toBeUndefined();
        expect(after?.nextAttemptAt?.getTime()).toBeLessThan(
          deadline.getTime(),
        );
        expect(
          await store.resumeCleanupAfterDisconnect(id, 2, {
            deadline,
            destroyAttempts: 2,
          }),
        ).toBe(false);
      } else expect(after).toEqual(before);
    });

    test("cleanup starts one durable disconnect window and a used connection resets it", async () => {
      const id = "alloc-cleanup-window";
      const store = await bindFirstGeneration(id, ANCHOR_RUN_ID);
      await store.markAllocated({ allocationId: id, generation: 1 });
      const released = await store.beginRelease({
        allocationId: id,
        expectedStatus: "allocated",
        expectedGeneration: 1,
      });
      // A first-connect deadline is not an established run's disconnect window.
      expect(released?.connectDeadline).toBeUndefined();
      const leaseId = "cleanup-window";
      await store.claimNextReconcilable({ leaseId, leaseDurationMs: 60_000 });
      const at = new Date();
      const args = {
        allocationId: id,
        generation: 2,
        expectedLeaseId: leaseId,
        connected: false,
      };
      const first = await store.recordCleanupConnection({ ...args, now: at });
      const deadline = new Date(at.getTime() + 900_000);
      expect(first).toMatchObject({
        connectDeadline: deadline,
        destroyAttempts: 0,
      });
      expect(
        (
          await store.recordCleanupConnection({
            ...args,
            now: new Date(at.getTime() + 180_000),
          })
        )?.connectDeadline,
      ).toEqual(deadline);
      expect(
        (await store.recordCleanupConnection({ ...args, connected: true }))
          ?.connectDeadline,
      ).toBeUndefined();
      const later = new Date(at.getTime() + 240_000);
      expect(
        (await store.recordCleanupConnection({ ...args, now: later }))
          ?.connectDeadline,
      ).toEqual(new Date(later.getTime() + 900_000));
      expect(
        await store.recordCleanupConnection({ ...args, generation: 1 }),
      ).toBeNull();
    });

    describe("replacement cleanup limits", () => {
      async function replacingCleanup(
        failure:
          | "none"
          | "undeploy"
          | "provider error"
          | "provider rejection"
          | "ensure error"
          | "ensure rejection" = "none",
      ) {
        const { store, allocation, leaseId, initialization } =
          await createClaimedAllocation("alloc-replacement-cleanup");
        await store.beginInitialization(initialization);
        await store.completeInitialization({
          ...initialization,
          publicKey: "key",
        });
        await h.db
          .update(workflowRun)
          .set({ status: "running" })
          .where(eq(workflowRun.id, ANCHOR_RUN_ID));
        await h.db
          .update(sidecarAllocation)
          .set({ destroyAttempts: 12 })
          .where(eq(sidecarAllocation.id, allocation.id));
        const target = { allocationId: allocation.id, generation: 2 };
        expect(
          await store.beginReplacement({
            allocationId: allocation.id,
            expectedStatus: "allocated",
            expectedGeneration: 1,
            expectedLeaseId: leaseId,
            firstAttemptAt: new Date(0),
            nextAttemptAt: new Date(0),
            failureCode: "sidecar_connect_failed",
            failureMessage: "Parked worker disconnected",
          }),
        ).toMatchObject({
          status: "replacing",
          destroyAttempts: 0,
          ensureAttempts: 0,
          deploymentCleanupConfirmed: false,
        });
        const state = {
          now: new Date(),
          connected: failure === "undeploy",
          providerAvailable: true,
          ensureSucceeds: false,
        };
        const calls: string[] = [];
        const connection = {};
        const provisioner = {
          id: allocation.provisionerId,
          apiVersion: 2 as const,
          bindingFingerprint: allocation.provisionerBindingFingerprint,
          capabilities: [],
          ensure: async () => {
            calls.push("ensure");
            if (!state.ensureSucceeds) {
              if (failure === "ensure error")
                throw new Error("Provisioning unavailable");
              if (failure === "ensure rejection")
                return {
                  kind: "rejected" as const,
                  retryable: true,
                  code: "unavailable",
                  message: "Try again",
                };
            }
            return { kind: "accepted" as const };
          },
          destroy: async () => {
            calls.push("destroy");
            if (failure === "provider error")
              throw new Error("Provider unavailable");
            if (failure === "provider rejection")
              return {
                kind: "rejected" as const,
                retryable: true,
                code: "unavailable",
                message: "Try again",
              };
            return { kind: "destroyed" as const, cleanup: "required" as const };
          },
        };
        const options: Parameters<typeof createSidecarAllocationReconciler>[0] =
          {
            allocationStore: store,
            plugins: {
              getProvisioner: () =>
                state.providerAvailable ? provisioner : null,
              selectProvisioner: async () => ({ ok: true, provisioner }),
            },
            router: {
              fenceAllocation: () => undefined,
              retireAllocation: () => undefined,
              getCleanupConnection: () =>
                state.connected ? connection : undefined,
              holdsAllocatedBinding: () => true,
              isAllocatedSidecarReady: async () => true,
              reportedDeploymentFailure: () => undefined,
              syncSidecar: async () => undefined,
              waitForAllocatedSidecar: async () => undefined,
              waitForSidecarInventory: async () => undefined,
              undeployAllocation: async (
                _target,
                _timeout,
                _signal,
                _connection,
                onSent,
              ) => {
                onSent();
                calls.push("undeploy");
                if (failure === "undeploy")
                  throw new Error("Could not remove old copy");
              },
            },
            hubWebSocketUrl: "wss://hub.example/ws",
            maxCleanupAttempts: 3,
            now: () => state.now,
          };
        return { store, target, state, calls, options };
      }

      test.each([
        { status: "releasing", phase: "sync" },
        { status: "releasing", phase: "validation" },
        { status: "replacing", phase: "sync" },
        { status: "replacing", phase: "validation" },
      ] as const)(
        "bounds cleanup preparation failures: %j",
        async ({ status, phase }) => {
          const { store, target, state, calls, options } =
            await replacingCleanup();
          const allocation =
            status === "releasing"
              ? await store.beginRelease({
                  allocationId: target.allocationId,
                  expectedStatus: "replacing",
                  expectedGeneration: target.generation,
                  now: state.now,
                })
              : await store.findById(target.allocationId);
          if (allocation === null)
            throw new Error("Missing cleanup allocation");
          const cleanupTarget = {
            allocationId: allocation.id,
            generation: allocation.generation,
          };
          const startedAt = state.now.getTime();
          state.connected = phase === "validation";
          let preparations = 0;
          const failPreparation = async () => {
            preparations++;
            throw new Error("Cleanup identity lookup unavailable");
          };
          if (phase === "sync") options.router.syncSidecar = failPreparation;
          else options.router.undeployAllocation = failPreparation;
          const reconciler = createSidecarAllocationReconciler({
            ...options,
            maxCleanupAttempts: 1,
          });
          for (let pass = 0; pass < 10; pass++) {
            state.now = new Date(startedAt + pass * 30_000);
            await reconciler.handleConnected(cleanupTarget);
            expect(await reconciler.reconcileNext()).toBe(true);
            expect(await store.findById(allocation.id)).toMatchObject({
              status,
              providerHoldReleased: true,
              deploymentCleanupConfirmed: false,
              destroyAttempts: 0,
            });
          }
          state.now = new Date(startedAt + 300_000);
          await reconciler.handleConnected(cleanupTarget);
          expect(await reconciler.reconcileNext()).toBe(true);
          expect(await store.findById(allocation.id)).toMatchObject({
            status: "destroy_failed",
            failureCode: SIDECAR_CLEANUP_PREPARATION_TIMEOUT_FAILURE_CODE,
            failureMessage: expect.stringContaining(
              "Cleanup identity lookup unavailable",
            ),
            providerHoldReleased: true,
            deploymentCleanupConfirmed: false,
            destroyAttempts: 0,
          });
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, ANCHOR_RUN_ID),
            }),
          ).toMatchObject({ status: "failed" });
          expect(preparations).toBe(11);
          expect(calls).not.toContain("ensure");

          state.connected = true;
          options.router.syncSidecar = async () => undefined;
          options.router.undeployAllocation = async () => {
            throw new Error("Terminal cleanup must not run");
          };
          const restarted = createSidecarAllocationReconciler(options);
          await restarted.initialize();
          await restarted.handleConnected(cleanupTarget);
          await restarted.repairUnscheduledConnections();
          expect(await restarted.reconcileNext()).toBe(false);
          expect((await store.findById(allocation.id))?.status).toBe(
            "destroy_failed",
          );
        },
      );

      test("release during replacement preserves the running cleanup disconnect deadline", async () => {
        const { store, target, state, calls, options } =
          await replacingCleanup();
        const reconciler = createSidecarAllocationReconciler(options);
        await reconciler.reconcileNext();
        const parked = await store.findById(target.allocationId);
        const deadline = parked?.connectDeadline;
        if (deadline === undefined) throw new Error("Missing cleanup deadline");
        expect(parked?.status).toBe("replacing");
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, ANCHOR_RUN_ID),
          }),
        ).toMatchObject({ publicKey: null });

        state.now = new Date(deadline.getTime() - 1);
        const releasing = await store.beginRelease({
          allocationId: target.allocationId,
          expectedStatus: "replacing",
          expectedGeneration: target.generation,
          now: state.now,
        });
        if (releasing === null) throw new Error("Missing releasing allocation");
        expect(releasing.connectDeadline).toEqual(deadline);
        expect(releasing.providerHoldReleased).toBe(true);

        state.providerAvailable = false;
        state.now = deadline;
        const restarted = createSidecarAllocationReconciler(options);
        await restarted.initialize();
        await store.wakeReconciliation(releasing.id, releasing.generation);
        await restarted.reconcileNext();
        expect(await store.findById(releasing.id)).toMatchObject({
          status: "destroy_failed",
          failureCode: SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
          destroyAttempts: 0,
        });
        state.connected = true;
        await restarted.handleConnected({
          allocationId: releasing.id,
          generation: releasing.generation,
        });
        expect(await restarted.reconcileNext()).toBe(true);
        expect(await store.findById(releasing.id)).toMatchObject({
          status: "released",
          providerHoldReleased: true,
          deploymentCleanupConfirmed: true,
        });
        expect(calls).toEqual(["destroy", "undeploy"]);
      });

      test.each(["ensure error", "ensure rejection"] as const)(
        "old-sidecar reconnects preserve provisioning backoff after %s",
        async (failure) => {
          const { store, target, state, calls, options } =
            await replacingCleanup(failure);
          state.connected = true;
          // Keep the retry in the future on the database clock without sleeping.
          state.now = new Date(Date.now() + 60_000);
          const reconciler = createSidecarAllocationReconciler(options);
          expect(await reconciler.reconcileNext()).toBe(true);
          const replacing = await store.findById(target.allocationId);
          if (replacing?.nextAttemptAt === undefined)
            throw new Error("Missing replacement retry");
          expect(replacing).toMatchObject({
            status: "replacing",
            ensureAttempts: 1,
            destroyAttempts: 0,
            providerHoldReleased: false,
          });
          const reconnect = {
            allocationId: replacing.id,
            generation: replacing.generation,
          };
          for (let flap = 0; flap < 10; flap++) {
            await reconciler.handleConnected(reconnect);
            expect((await store.findById(replacing.id))?.nextAttemptAt).toEqual(
              replacing.nextAttemptAt,
            );
            expect(await reconciler.reconcileNext()).toBe(false);
          }
          expect(calls.filter((call) => call === "ensure")).toHaveLength(1);
          // Drive the schedule due; a healthy retry must still be able to run.
          state.ensureSucceeds = true;
          await store.wakeReconciliation(replacing.id, replacing.generation);
          expect(await reconciler.reconcileNext()).toBe(true);
          expect(await store.findById(replacing.id)).toMatchObject({
            status: "allocated",
            ensureAttempts: 2,
          });
          expect(calls.filter((call) => call === "ensure")).toHaveLength(2);
        },
      );

      test.each([
        "ensure error",
        "ensure rejection",
        "last attempt succeeds",
      ] as const)(
        "bounds provisioning across cleanup and restart: %s",
        async (outcome) => {
          const { store, target, state, calls, options } =
            await replacingCleanup(
              outcome === "ensure rejection" ? outcome : "ensure error",
            );
          state.connected = true;
          for (let attempt = 1; attempt <= 10; attempt++) {
            // Failed cleanup attempts must not accelerate or delay provisioning's backoff.
            await h.db
              .update(sidecarAllocation)
              .set({ destroyAttempts: 2 })
              .where(eq(sidecarAllocation.id, target.allocationId));
            const current = await store.findById(target.allocationId);
            if (current === null) throw new Error("Missing allocation");
            state.ensureSucceeds =
              outcome === "last attempt succeeds" && attempt === 10;
            const reconciler = createSidecarAllocationReconciler(options);
            await reconciler.initialize();
            await store.wakeReconciliation(current.id, current.generation);
            expect(await reconciler.reconcileNext()).toBe(true);
            const next = await store.findById(current.id);
            expect(next?.ensureAttempts).toBe(attempt);
            expect(calls.filter((call) => call === "ensure")).toHaveLength(
              attempt,
            );
            if (attempt < 10) {
              expect(next?.status).toBe("replacing");
              expect(next?.nextAttemptAt?.getTime()).toBe(
                state.now.getTime() +
                  Math.min(1_000 * 2 ** (attempt - 1), 30_000),
              );
            } else {
              expect(next?.status).toBe(
                state.ensureSucceeds ? "allocated" : "releasing",
              );
            }
          }
          if (state.ensureSucceeds) return;
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, ANCHOR_RUN_ID),
            }),
          ).toMatchObject({
            status: "failed",
            failureCode: "sidecar_provisioning_retry_exhausted",
          });
          const final = createSidecarAllocationReconciler(options);
          await final.initialize();
          await final.reconcileNext();
          expect((await store.findById(target.allocationId))?.status).toBe(
            "released",
          );
          expect(await final.reconcileNext()).toBe(false);
          expect(calls.filter((call) => call === "ensure")).toHaveLength(10);
        },
      );

      test.each([false, true])(
        "bounds accepted workers that never connect and resets only after initialization (last connects: %s)",
        async (lastConnects) => {
          const { store, target, state, calls, options } =
            await replacingCleanup();
          const provisioner = options.plugins.getProvisioner("ec2-spot");
          if (provisioner === null) throw new Error("Missing test provisioner");
          let connected = false;
          const restoring: Parameters<
            typeof createSidecarAllocationReconciler
          >[0] = {
            ...options,
            plugins: {
              ...options.plugins,
              getProvisioner: () => ({
                ...provisioner,
                destroy: async () => ({
                  kind: "destroyed",
                  cleanup: "confirmed",
                }),
              }),
            },
            router: {
              ...options.router,
              getCleanupConnection: () => undefined,
              holdsAllocatedBinding: () => connected,
              isAllocatedSidecarReady: async () => connected,
              waitForAllocatedSidecar: async (_target, timeoutMs) => {
                expect(timeoutMs).toBe(0);
                throw new Error("Sidecar never connected before its deadline");
              },
            },
            classifyParkedRun: async () => ({
              verdict: "parked",
              reason: "Committed run remains parked",
            }),
            onReady: async (allocation, context) => {
              const initializing = {
                allocationId: allocation.id,
                generation: allocation.generation,
                anchorRunId: allocation.anchorRunId,
                tenantId: allocation.tenantId,
                ...context,
              };
              expect(await store.beginInitialization(initializing)).toBe(true);
              expect(
                await store.completeInitialization({
                  ...initializing,
                  publicKey: "restored-key",
                }),
              ).toBe(true);
            },
          };
          for (let attempt = 1; attempt <= 10; attempt++) {
            const current = await store.findById(target.allocationId);
            if (current === null) throw new Error("Missing allocation");
            connected = lastConnects && attempt === 10;
            const reconciler = createSidecarAllocationReconciler(restoring);
            await reconciler.initialize();
            await store.wakeReconciliation(current.id, current.generation);
            expect(await reconciler.reconcileNext()).toBe(true);
            const accepted = await store.findById(current.id);
            if (accepted === null)
              throw new Error("Missing accepted allocation");
            expect(accepted).toMatchObject({
              status: "allocated",
              ensureAttempts: attempt,
            });
            expect(calls.filter((call) => call === "ensure")).toHaveLength(
              attempt,
            );
            if (connected) break;
            if (accepted.connectDeadline === undefined)
              throw new Error("Missing connect deadline");
            state.now = accepted.connectDeadline;
            await store.wakeReconciliation(accepted.id, accepted.generation);
            expect(await reconciler.reconcileNext()).toBe(true);
            const replacing = await store.findById(current.id);
            expect(replacing).toMatchObject({
              status: "replacing",
              ensureAttempts: attempt,
            });
            expect(replacing?.nextAttemptAt?.getTime()).toBe(
              state.now.getTime() +
                Math.min(1_000 * 2 ** (attempt - 1), 30_000),
            );
          }

          const reconciler = createSidecarAllocationReconciler(restoring);
          const current = await store.findById(target.allocationId);
          if (current === null) throw new Error("Missing allocation");
          if (lastConnects) {
            expect(
              await h.db.query.workflowRun.findFirst({
                where: eq(workflowRun.id, ANCHOR_RUN_ID),
              }),
            ).toMatchObject({ status: "running", publicKey: "restored-key" });
            connected = false;
            const disconnected = await store.markConnectionLost({
              allocationId: current.id,
              generation: current.generation,
              now: state.now,
              firstConnectDeadline: new Date(state.now.getTime() + 120_000),
            });
            if (disconnected?.connectDeadline === undefined)
              throw new Error("Missing disconnect deadline");
            state.now = disconnected.connectDeadline;
            await store.wakeReconciliation(current.id, current.generation);
            await reconciler.reconcileNext();
            const replacing = await store.findById(current.id);
            expect(replacing).toMatchObject({
              status: "replacing",
              ensureAttempts: 0,
            });
            expect(replacing?.nextAttemptAt?.getTime()).toBe(
              state.now.getTime() + 1_000,
            );
            expect(
              await h.db.query.workflowRun.findFirst({
                where: eq(workflowRun.id, ANCHOR_RUN_ID),
              }),
            ).toMatchObject({ publicKey: null });
            connected = true;
          }
          const replacing = await store.findById(target.allocationId);
          if (replacing === null) throw new Error("Missing replacement");
          await store.wakeReconciliation(replacing.id, replacing.generation);
          await reconciler.reconcileNext();
          expect(await store.findById(replacing.id)).toMatchObject(
            lastConnects
              ? { status: "allocated", ensureAttempts: 1 }
              : {
                  status: "releasing",
                  ensureAttempts: 10,
                  failureCode: "sidecar_provisioning_retry_exhausted",
                  failureMessage:
                    "Provisioning failed after 10 attempts: Sidecar never connected before its deadline",
                },
          );
          expect(calls.filter((call) => call === "ensure")).toHaveLength(
            lastConnects ? 11 : 10,
          );
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, ANCHOR_RUN_ID),
            }),
          ).toMatchObject(
            lastConnects
              ? { status: "running" }
              : {
                  status: "failed",
                  failureCode: "sidecar_provisioning_retry_exhausted",
                  failureMessage:
                    "Provisioning failed after 10 attempts: Sidecar never connected before its deadline",
                },
          );
          if (!lastConnects) {
            await store.wakeReconciliation(replacing.id, replacing.generation);
            expect(await reconciler.reconcileNext()).toBe(true);
            expect(await store.findById(replacing.id)).toMatchObject({
              status: "released",
              failureCode: "sidecar_provisioning_retry_exhausted",
            });
          }
        },
      );

      test("failing a replacement rejects stale ownership without failing its workflow", async () => {
        const { store, target, state } = await replacingCleanup();
        const leaseId = "exhausted-replacement";
        const claimed = await store.claimNextReconcilable({
          leaseId,
          leaseDurationMs: 60_000,
        });
        expect(claimed?.id).toBe(target.allocationId);
        const failure = {
          ...target,
          expectedStatus: "replacing" as const,
          expectedGeneration: target.generation,
          expectedLeaseId: leaseId,
          failureCode: "sidecar_provisioning_retry_exhausted",
          failureMessage: "Provisioning attempts exhausted",
          now: state.now,
        };
        for (const stale of [
          { expectedLeaseId: "stale-lease" },
          { expectedGeneration: target.generation - 1 },
          { expectedStatus: "allocated" as const },
        ])
          expect(
            await store.beginUnrecoverableRelease({ ...failure, ...stale }),
          ).toBeNull();
        expect(await store.findById(target.allocationId)).toEqual(claimed);
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, ANCHOR_RUN_ID),
          }),
        ).toMatchObject({ status: "running", failureCode: null });
        expect(await store.beginUnrecoverableRelease(failure)).toMatchObject({
          status: "releasing",
          generation: target.generation,
        });
      });

      test("exhausted provisioning leaves a pending cancellation to lifecycle after confirmed removal", async () => {
        const { store, target, state, calls, options } =
          await replacingCleanup();
        await h.db
          .update(sidecarAllocation)
          .set({
            ensureAttempts: 10,
            deploymentCleanupConfirmed: true,
            providerHoldReleased: true,
          })
          .where(eq(sidecarAllocation.id, target.allocationId));
        await h.db
          .update(workflowRun)
          .set({ cancellationRequestedAt: state.now })
          .where(eq(workflowRun.id, ANCHOR_RUN_ID));
        expect(
          await createSidecarAllocationReconciler(options).reconcileNext(),
        ).toBe(true);
        expect(await store.findById(target.allocationId)).toMatchObject({
          status: "releasing",
          providerHoldReleased: true,
          deploymentCleanupConfirmed: true,
        });
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, ANCHOR_RUN_ID),
          }),
        ).toMatchObject({
          status: "running",
          failureCode: null,
          cancellationRequestedAt: state.now,
        });
        expect(calls).toEqual([]);
        state.providerAvailable = false;
        const restarted = createSidecarAllocationReconciler(options);
        await restarted.initialize();
        expect(await restarted.reconcileNext()).toBe(true);
        expect(await store.findById(target.allocationId)).toMatchObject({
          status: "released",
        });
        expect(calls).toEqual([]);
      });

      test.each(["none", "provider error", "undeploy"] as const)(
        "records provisioning exhaustion before replacement cleanup can fail: %s",
        async (failure) => {
          const { store, target, state, calls, options } =
            await replacingCleanup(failure);
          const deadline = new Date(state.now.getTime() + 5_000);
          const progress = {
            ensureAttempts: 10,
            destroyAttempts: 2,
            providerHoldReleased: failure !== "provider error",
            deploymentCleanupConfirmed: failure === "provider error",
            connectDeadline: deadline,
          };
          await h.db
            .update(sidecarAllocation)
            .set({
              ...progress,
              failureMessage: "Replacement never connected",
            })
            .where(eq(sidecarAllocation.id, target.allocationId));
          const reconciler = createSidecarAllocationReconciler(options);
          await reconciler.initialize();
          expect(await reconciler.reconcileNext()).toBe(true);
          const message =
            "Provisioning failed after 10 attempts: Replacement never connected";
          expect(await store.findById(target.allocationId)).toMatchObject({
            ...progress,
            generation: target.generation,
            status: "releasing",
            failureCode: "sidecar_provisioning_retry_exhausted",
            failureMessage: message,
          });
          expect(calls).toEqual([]);
          const failedRun = await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, ANCHOR_RUN_ID),
          });
          expect(failedRun).toMatchObject({
            status: "failed",
            failureCode: "sidecar_provisioning_retry_exhausted",
            failureMessage: message,
            endedAt: state.now,
          });
          expect(await reconciler.reconcileNext()).toBe(true);
          if (failure === "none") {
            expect(await store.findById(target.allocationId)).toMatchObject({
              ...progress,
              status: "releasing",
              nextAttemptAt: deadline,
            });
            state.now = deadline;
            await store.wakeReconciliation(
              target.allocationId,
              target.generation,
            );
            expect(await reconciler.reconcileNext()).toBe(true);
          }
          expect(await store.findById(target.allocationId)).toMatchObject({
            status: "destroy_failed",
            destroyAttempts: failure === "none" ? 2 : 3,
            failureCode:
              failure === "none"
                ? SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE
                : SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
          });
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, ANCHOR_RUN_ID),
            }),
          ).toEqual(failedRun);
          expect(calls).not.toContain("ensure");
        },
      );

      test.each([
        { removed: false, released: false },
        { removed: false, released: true },
        { removed: true, released: false },
        { removed: true, released: true },
      ])(
        "an expired provider grace stops replacement with cleanup proof %j",
        async ({ removed, released }) => {
          const { store, target, state, calls, options } =
            await replacingCleanup();
          await h.db
            .update(sidecarAllocation)
            .set({
              deploymentCleanupConfirmed: removed,
              providerHoldReleased: released,
            })
            .where(eq(sidecarAllocation.id, target.allocationId));
          if (!removed || !released)
            expect(
              await store.failWithoutInfrastructure({
                allocationId: target.allocationId,
                expectedStatus: "replacing",
                expectedGeneration: target.generation,
                code: "provisioner_unavailable",
                message: "Must preserve unfinished cleanup",
              }),
            ).toBeNull();
          state.providerAvailable = false;
          const reconciler = createSidecarAllocationReconciler(options);
          await reconciler.reconcileNext();
          const deadline = new Date(state.now.getTime() + 300_000);
          const waitingForProvider = await store.findById(target.allocationId);
          expect(waitingForProvider).toMatchObject({
            status: "replacing",
            ensureAttempts: 0,
            destroyAttempts: 0,
          });
          expect(
            waitingForProvider?.nextAttemptAt?.getTime(),
          ).toBeGreaterThanOrEqual(state.now.getTime() + 30_000);
          expect(
            waitingForProvider?.nextAttemptAt?.getTime(),
          ).toBeLessThanOrEqual(deadline.getTime());
          state.now = new Date(state.now.getTime() + 120_000);
          await store.wakeReconciliation(
            target.allocationId,
            target.generation,
          );
          await reconciler.reconcileNext();
          expect((await store.findById(target.allocationId))?.status).toBe(
            "replacing",
          );
          state.now = deadline;
          await store.wakeReconciliation(
            target.allocationId,
            target.generation,
          );
          await reconciler.reconcileNext();
          const failed = await store.findById(target.allocationId);
          expect(failed).toMatchObject({
            status: removed && released ? "failed" : "destroy_failed",
            failureCode: "provisioner_unavailable",
            destroyAttempts: 0,
            deploymentCleanupConfirmed: removed,
            providerHoldReleased: released,
          });
          expect(failed?.nextAttemptAt).toBeUndefined();
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, ANCHOR_RUN_ID),
            }),
          ).toMatchObject({
            status: "failed",
            failureCode: "provisioner_unavailable",
          });
          state.providerAvailable = true;
          state.connected = true;
          const restarted = createSidecarAllocationReconciler(options);
          await restarted.initialize();
          await restarted.handleConnected(target);
          await restarted.repairUnscheduledConnections();
          expect(await restarted.reconcileNext()).toBe(false);
          expect(calls).toEqual([]);
        },
      );

      test.each(["missing", "mismatched"])(
        "recovers a %s provider and only needs it again when replacing capacity",
        async (failure) => {
          const { store, target, state, calls, options } =
            await replacingCleanup();
          const matching = options.plugins.getProvisioner("ec2-spot");
          if (matching === null) throw new Error("Missing test provisioner");
          let restored = false;
          options.plugins.getProvisioner = () =>
            restored
              ? matching
              : failure === "missing"
                ? null
                : { ...matching, bindingFingerprint: "other-backend" };
          const reconciler = createSidecarAllocationReconciler(options);
          await reconciler.reconcileNext();
          const firstDeadline = new Date(state.now.getTime() + 300_000);
          expect(await store.findById(target.allocationId)).toMatchObject({
            destroyAttempts: 0,
            ensureAttempts: 0,
          });
          restored = true;
          state.connected = true;
          state.now = new Date(state.now.getTime() + 240_000);
          await store.wakeReconciliation(
            target.allocationId,
            target.generation,
          );
          await reconciler.reconcileNext();
          const recovered = await store.findById(target.allocationId);
          if (recovered === null)
            throw new Error("Missing recovered allocation");
          expect(recovered.status).toBe("allocated");
          expect(calls.filter((call) => call === "ensure")).toHaveLength(1);
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, ANCHOR_RUN_ID),
            }),
          ).toMatchObject({ status: "running" });

          await h.db
            .update(workflowRun)
            .set({ publicKey: "restored-key" })
            .where(eq(workflowRun.id, ANCHOR_RUN_ID));
          restored = false;
          const healthy = createSidecarAllocationReconciler(options);
          await healthy.initialize();
          await healthy.handleConnected({
            allocationId: recovered.id,
            generation: recovered.generation,
          });
          await healthy.reconcileNext();
          const deadline = new Date(state.now.getTime() + 300_000);
          expect((await store.findById(recovered.id))?.status).toBe(
            "allocated",
          );
          expect(deadline.getTime()).toBeGreaterThan(firstDeadline.getTime());
          state.now = deadline;
          await store.wakeReconciliation(recovered.id, recovered.generation);
          expect(await healthy.reconcileNext()).toBe(true);
          expect((await store.findById(recovered.id))?.status).toBe(
            "allocated",
          );
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, ANCHOR_RUN_ID),
            }),
          ).toMatchObject({ status: "running" });

          await store.wakeReconciliation(recovered.id, recovered.generation);
          await store.claimNextReconcilable({
            leaseId: "later-replacement",
            leaseDurationMs: 60_000,
          });
          const replacing = await store.beginReplacement({
            allocationId: recovered.id,
            expectedStatus: "allocated",
            expectedGeneration: recovered.generation,
            expectedLeaseId: "later-replacement",
            firstAttemptAt: new Date(0),
            nextAttemptAt: new Date(0),
            failureCode: "sidecar_connect_failed",
            failureMessage: "Parked worker disconnected again",
          });
          if (replacing === null) throw new Error("Missing replacement");
          await healthy.reconcileNext();
          expect((await store.findById(recovered.id))?.status).toBe(
            "replacing",
          );
          state.now = new Date(state.now.getTime() + 300_000);
          await store.wakeReconciliation(replacing.id, replacing.generation);
          await healthy.reconcileNext();
          expect(await store.findById(recovered.id)).toMatchObject({
            status: "destroy_failed",
            failureCode: "provisioner_unavailable",
          });
          expect(calls.filter((call) => call === "ensure")).toHaveLength(1);
        },
      );

      test("restarting the Hub grants a fresh provider grace period", async () => {
        const { store, target, state, options } = await replacingCleanup();
        state.providerAvailable = false;
        const firstDeadline = new Date(state.now.getTime() + 300_000);
        await createSidecarAllocationReconciler(options).reconcileNext();
        state.now = new Date(state.now.getTime() + 240_000);
        const restarted = createSidecarAllocationReconciler(options);
        await restarted.initialize();
        await store.wakeReconciliation(target.allocationId, target.generation);
        await restarted.reconcileNext();
        const newDeadline = new Date(state.now.getTime() + 300_000);
        state.now = firstDeadline;
        await store.wakeReconciliation(target.allocationId, target.generation);
        await restarted.reconcileNext();
        expect(await store.findById(target.allocationId)).toMatchObject({
          status: "replacing",
          destroyAttempts: 0,
          ensureAttempts: 0,
        });
        state.now = newDeadline;
        await store.wakeReconciliation(target.allocationId, target.generation);
        await restarted.reconcileNext();
        expect(await store.findById(target.allocationId)).toMatchObject({
          status: "destroy_failed",
          failureCode: "provisioner_unavailable",
        });
      });

      test.each(["provisioning", "replacing"] as const)(
        "does not start an eleventh provisioning attempt after recovering %s",
        async (status) => {
          const { store, target, calls, options } = await replacingCleanup();
          await h.db
            .update(sidecarAllocation)
            .set({
              status,
              ensureAttempts: 10,
              deploymentCleanupConfirmed: status === "replacing",
              providerHoldReleased: status === "replacing",
            })
            .where(eq(sidecarAllocation.id, target.allocationId));
          const restarted = createSidecarAllocationReconciler(options);
          await restarted.initialize();
          await restarted.reconcileNext();
          expect(await store.findById(target.allocationId)).toMatchObject({
            status: status === "replacing" ? "failed" : "releasing",
            failureCode: "sidecar_provisioning_retry_exhausted",
            failureMessage:
              status === "replacing"
                ? "Provisioning failed after 10 attempts: Parked worker disconnected"
                : "Provisioning failed after 10 attempts: Hub restarted before sidecar provisioning acceptance was recorded",
            ensureAttempts: 10,
          });
          expect(calls).not.toContain("ensure");
          if (status === "provisioning") {
            await restarted.reconcileNext();
            expect((await store.findById(target.allocationId))?.status).toBe(
              "released",
            );
          }
          expect(await restarted.reconcileNext()).toBe(false);
        },
      );

      test.each(["undeploy", "provider error", "provider rejection"] as const)(
        "exhausts failed %s attempts and fails the parked workflow",
        async (failure) => {
          const { store, target, calls, options } =
            await replacingCleanup(failure);
          const reconciler = createSidecarAllocationReconciler(options);
          for (let attempt = 1; attempt <= 3; attempt++) {
            await store.wakeReconciliation(
              target.allocationId,
              target.generation,
            );
            expect(await reconciler.reconcileNext()).toBe(true);
            expect(await store.findById(target.allocationId)).toMatchObject({
              status: attempt < 3 ? "replacing" : "destroy_failed",
              destroyAttempts: attempt,
            });
          }
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, ANCHOR_RUN_ID),
            }),
          ).toMatchObject({
            status: "failed",
            failureCode: SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
          });
          const restarted = createSidecarAllocationReconciler(options);
          await restarted.initialize();
          await restarted.handleConnected(target);
          await restarted.repairUnscheduledConnections();
          expect(await restarted.reconcileNext()).toBe(false);
          expect(calls).not.toContain("ensure");
          expect(await store.findById(target.allocationId)).toMatchObject({
            deploymentCleanupConfirmed: false,
            sidecarId: "alloc-replacement-cleanup-sidecar",
          });
        },
      );

      test.each(["reconnect", "repair", "timeout"])(
        "bounds disconnected cleanup and handles %s without reviving a failed workflow",
        async (outcome) => {
          const { store, target, state, calls, options } =
            await replacingCleanup();
          await createSidecarAllocationReconciler(options).reconcileNext();
          const deadline = new Date(state.now.getTime() + 900_000);
          expect(await store.findById(target.allocationId)).toMatchObject({
            status: "replacing",
            connectDeadline: deadline,
            nextAttemptAt: deadline,
            destroyAttempts: 0,
          });
          state.now = new Date(state.now.getTime() + 180_000);
          const restarted = createSidecarAllocationReconciler({
            ...options,
            allocationStore: createSidecarAllocationStore(h.db, {
              getSidecarInventory: () => ({
                capacity: DEFAULT_SIDECAR_CAPACITY,
                retainedIncarnations: [],
              }),
            }),
          });
          await restarted.initialize();
          expect(await restarted.reconcileNext()).toBe(false);
          await store.wakeReconciliation(
            target.allocationId,
            target.generation,
          );
          await restarted.reconcileNext();
          expect(
            (await store.findById(target.allocationId))?.connectDeadline,
          ).toEqual(deadline);
          const timedOut = outcome === "timeout";
          if (timedOut) {
            state.now = deadline;
            await store.wakeReconciliation(
              target.allocationId,
              target.generation,
            );
            await restarted.reconcileNext();
            expect(await store.findById(target.allocationId)).toMatchObject({
              status: "destroy_failed",
              failureCode: SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
              destroyAttempts: 0,
            });
          }
          state.connected = true;
          if (outcome === "repair")
            await restarted.repairUnscheduledConnections();
          else await restarted.handleConnected(target);
          expect(await store.findById(target.allocationId)).toMatchObject({
            status: timedOut ? "releasing" : "replacing",
            destroyAttempts: 0,
          });
          expect(
            (await store.findById(target.allocationId))?.connectDeadline,
          ).toBeUndefined();
          await restarted.reconcileNext();
          expect(calls.filter((call) => call === "undeploy")).toHaveLength(1);
          expect(calls.filter((call) => call === "ensure")).toHaveLength(
            timedOut ? 0 : 1,
          );
          expect(await store.findById(target.allocationId)).toMatchObject({
            status: timedOut ? "released" : "allocated",
            generation: timedOut ? 2 : 3,
          });
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, ANCHOR_RUN_ID),
            }),
          ).toMatchObject(
            timedOut
              ? {
                  status: "failed",
                  failureCode: SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
                }
              : { status: "running" },
          );
        },
      );

      test("observing a cleanup connection resets the replacement's disconnect window", async () => {
        const { store, target, state, options } =
          await replacingCleanup("undeploy");
        state.connected = false;
        const reconciler = createSidecarAllocationReconciler(options);
        await reconciler.reconcileNext();
        const firstDeadline = (await store.findById(target.allocationId))
          ?.connectDeadline;
        state.now = new Date(state.now.getTime() + 60_000);
        state.connected = true;
        // The connection is visible even though its reconnect notification was lost.
        await store.wakeReconciliation(target.allocationId, target.generation);
        await reconciler.reconcileNext();
        expect(await store.findById(target.allocationId)).toMatchObject({
          status: "replacing",
          destroyAttempts: 1,
        });
        expect(
          (await store.findById(target.allocationId))?.connectDeadline,
        ).toBeUndefined();
        state.connected = false;
        await store.wakeReconciliation(target.allocationId, target.generation);
        await reconciler.reconcileNext();
        const waiting = await store.findById(target.allocationId);
        expect(waiting?.connectDeadline).toEqual(
          new Date(state.now.getTime() + 900_000),
        );
        expect(waiting?.connectDeadline).not.toEqual(firstDeadline);
        expect(waiting?.destroyAttempts).toBe(1);
      });

      test.each(["parking", "timeout"])(
        "serializes a replacement reconnect with the %s write",
        async (phase) => {
          const { store, target, state, calls, options } =
            await replacingCleanup();
          const reconciler = createSidecarAllocationReconciler(options);
          if (phase === "timeout") {
            await reconciler.reconcileNext();
            state.now = new Date(state.now.getTime() + 900_000);
          }
          const entered = Promise.withResolvers<undefined>();
          const finish = Promise.withResolvers<undefined>();
          const record = store.recordCleanupConnection;
          const fail = store.markDestroyFailed;
          store.recordCleanupConnection = async (args) => {
            if (phase === "parking" && !args.connected) {
              entered.resolve(undefined);
              await finish.promise;
            }
            return record(args);
          };
          store.markDestroyFailed = async (args) => {
            entered.resolve(undefined);
            await finish.promise;
            return fail(args);
          };
          await store.wakeReconciliation(
            target.allocationId,
            target.generation,
          );
          const running = reconciler.reconcileNext();
          try {
            await entered.promise;
            state.connected = true;
            const connected = reconciler.handleConnected(target);
            finish.resolve(undefined);
            await Promise.all([running, connected]);
            expect(await store.findById(target.allocationId)).toMatchObject({
              status: phase === "timeout" ? "releasing" : "replacing",
              destroyAttempts: 0,
            });
            expect(
              (await store.findById(target.allocationId))?.connectDeadline,
            ).toBeUndefined();
            await reconciler.reconcileNext();
            expect(calls.filter((call) => call === "ensure")).toHaveLength(
              phase === "timeout" ? 0 : 1,
            );
            expect(await store.findById(target.allocationId)).toMatchObject({
              status: phase === "timeout" ? "released" : "allocated",
            });
          } finally {
            finish.resolve(undefined);
            await running;
          }
        },
      );
    });

    test("a release has a fresh retry budget and exhausted cleanup remains fenced across reconnect", async () => {
      const store = await bindFirstGeneration(
        "alloc-cleanup-budget",
        ANCHOR_RUN_ID,
      );
      await store.markAllocated({
        allocationId: "alloc-cleanup-budget",
        generation: 1,
      });
      await h.db
        .update(sidecarAllocation)
        .set({ destroyAttempts: 20 })
        .where(eq(sidecarAllocation.id, "alloc-cleanup-budget"));
      const releasing = await store.beginRelease({
        allocationId: "alloc-cleanup-budget",
        expectedStatus: "allocated",
        expectedGeneration: 1,
      });
      expect(releasing).toMatchObject({
        generation: 2,
        destroyAttempts: 0,
        ensureAcceptedGeneration: 1,
      });
      await store.markDestroyFailed({
        allocationId: "alloc-cleanup-budget",
        expectedGeneration: 2,
        code: SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
        message: "reply never arrived",
      });
      expect(
        await store.claimNextReconcilable({
          leaseId: "must-not-poll",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
      expect((await store.listActive()).map((row) => row.id)).toContain(
        "alloc-cleanup-budget",
      );
      expect(await store.wakeReconciliation("alloc-cleanup-budget", 1)).toBe(
        false,
      );
      expect(await store.wakeReconciliation("alloc-cleanup-budget", 2)).toBe(
        false,
      );
      expect(
        await store.resumeCleanupAfterDisconnect("alloc-cleanup-budget", 2),
      ).toBe(false);
      expect(
        await store.claimNextReconcilable({
          leaseId: "cleanup-reconnect",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
      expect(await store.listActive()).toEqual([
        expect.objectContaining({
          status: "destroy_failed",
          generation: 2,
          destroyAttempts: 1,
          failureCode: SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
        }),
      ]);
      expect(
        await store.markReleased({
          allocationId: "alloc-cleanup-budget",
          generation: 2,
        }),
      ).toBeNull();
    });

    test.each([
      ["pending", null, true],
      ["provisioning", null, true],
      ["allocated", null, true],
      ["replacing", null, true],
      ["releasing", null, true],
      ["destroy_failed", SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE, true],
      ["destroy_failed", SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE, false],
      [
        "destroy_failed",
        SIDECAR_CLEANUP_PREPARATION_TIMEOUT_FAILURE_CODE,
        false,
      ],
      ["destroy_failed", "provisioner_unavailable", false],
      ["destroy_failed", "provider_permission_denied", false],
      ["destroy_failed", null, false],
      ["released", null, false],
      ["failed", null, false],
    ] as const)(
      "connection repair selects recoverable work while startup preserves fences: %s, %s",
      async (status, failureCode, repairable) => {
        const id = "alloc-repair-selection";
        const store = await bindFirstGeneration(id, ANCHOR_RUN_ID);
        await h.db
          .update(sidecarAllocation)
          .set({ status, failureCode })
          .where(eq(sidecarAllocation.id, id));
        expect(
          (await store.listForConnectionRepair()).map((row) => row.id),
        ).toEqual(repairable ? [id] : []);
        expect((await store.listActive()).map((row) => row.id)).toEqual(
          status === "released" || status === "failed" ? [] : [id],
        );
      },
    );

    test("a permanent provider refusal is not reopened by a connection wake", async () => {
      const store = await bindFirstGeneration("alloc-permanent", ANCHOR_RUN_ID);
      await store.beginRelease({
        allocationId: "alloc-permanent",
        expectedStatus: "provisioning",
        expectedGeneration: 1,
      });
      await store.markDestroyFailed({
        allocationId: "alloc-permanent",
        expectedGeneration: 2,
        code: "provider_permission_denied",
        message: "operator recovery needed",
      });
      expect(await store.wakeReconciliation("alloc-permanent", 2)).toBe(false);
      expect(
        await store.resumeCleanupAfterDisconnect("alloc-permanent", 2),
      ).toBe(false);
      expect(await store.listActive()).toMatchObject([
        { id: "alloc-permanent", status: "destroy_failed" },
      ]);
    });

    test.each([
      SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
      "workflow_stop_failed",
    ])(
      "successful release clears only the recovered disconnect marker, including %s",
      async (failureCode) => {
        const { store, leaseId } = await createClaimedAllocation(
          "alloc-cleanup-reason",
        );
        await store.beginRelease({
          allocationId: "alloc-cleanup-reason",
          expectedStatus: "allocated",
          expectedGeneration: 1,
          expectedLeaseId: leaseId,
          failureCode: "workflow_stop_failed",
          failureMessage: "original reason",
        });
        if (failureCode === SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE) {
          await store.markDestroyFailed({
            allocationId: "alloc-cleanup-reason",
            expectedGeneration: 2,
            code: failureCode,
            message: "Sidecar did not reconnect before the cleanup deadline",
            countAttempt: false,
          });
          expect(
            await store.resumeCleanupAfterDisconnect("alloc-cleanup-reason", 2),
          ).toBe(true);
        }
        await h.db
          .update(sidecarAllocation)
          .set({ deploymentCleanupConfirmed: true, providerHoldReleased: true })
          .where(eq(sidecarAllocation.id, "alloc-cleanup-reason"));
        const released = await store.markReleased({
          allocationId: "alloc-cleanup-reason",
          generation: 2,
        });
        expect(released?.failureCode).toBe(
          failureCode === SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE
            ? undefined
            : failureCode,
        );
      },
    );

    test.each([
      "sidecar_retention_limit_exceeded",
      "workflow_stop_failed",
      "sidecar_initialization_uncertain",
      SIDECAR_DEPLOYMENT_STOPPED_FAILURE_CODE,
    ])(
      "a cleanup retry and successful release preserve %s",
      async (failureCode) => {
        const store = await bindFirstGeneration("alloc-reason", ANCHOR_RUN_ID);
        await store.beginRelease({
          allocationId: "alloc-reason",
          expectedStatus: "provisioning",
          expectedGeneration: 1,
          failureCode,
          failureMessage: "original reason",
        });
        const retried = await store.scheduleRetry({
          allocationId: "alloc-reason",
          expectedStatus: "releasing",
          expectedGeneration: 2,
          attempt: "destroy",
          nextAttemptAt: new Date(),
        });
        expect(retried).toMatchObject({
          status: "releasing",
          failureCode,
          failureMessage: "original reason",
          destroyAttempts: 1,
        });
        await h.db
          .update(sidecarAllocation)
          .set({ deploymentCleanupConfirmed: true, providerHoldReleased: true })
          .where(eq(sidecarAllocation.id, "alloc-reason"));
        const released = await store.markReleased({
          allocationId: "alloc-reason",
          generation: 2,
        });
        expect(released).toMatchObject({
          status: "released",
          failureCode,
          failureMessage: "original reason",
        });
      },
    );

    test.each([
      "allocation reason",
      "existing workflow reason",
      "deferred history",
    ])(
      "cleanup exhaustion preserves the workflow cause from %s",
      async (source) => {
        const store = await bindFirstGeneration(
          "alloc-workflow-cause",
          ANCHOR_RUN_ID,
        );
        await store.markAllocated({
          allocationId: "alloc-workflow-cause",
          generation: 1,
        });
        if (source === "existing workflow reason")
          await h.db
            .update(workflowRun)
            .set({
              failureCode: "tool_failed",
              failureMessage: "actual workflow error",
            })
            .where(eq(workflowRun.id, ANCHOR_RUN_ID));
        if (source === "deferred history")
          await h.db.insert(workflowPendingProjection).values({
            id: "pending-cleanup-cause",
            anchorRunId: ANCHOR_RUN_ID,
          });
        await store.beginRelease({
          allocationId: "alloc-workflow-cause",
          expectedStatus: "allocated",
          expectedGeneration: 1,
          failureCode: "workflow_stop_failed",
          failureMessage: "the workflow could not be stopped",
        });
        await store.scheduleRetry({
          allocationId: "alloc-workflow-cause",
          expectedStatus: "releasing",
          expectedGeneration: 2,
          attempt: "destroy",
          nextAttemptAt: new Date(),
        });
        const expectedCause =
          source === "existing workflow reason"
            ? {
                failureCode: "tool_failed",
                failureMessage: "actual workflow error",
              }
            : {
                failureCode: "workflow_stop_failed",
                failureMessage: "the workflow could not be stopped",
              };
        const failed = await store.markDestroyFailed({
          allocationId: "alloc-workflow-cause",
          expectedGeneration: 2,
          code: SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
          message: "cleanup budget exhausted",
        });
        expect(failed).toMatchObject({
          status: "destroy_failed",
          failureCode: SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
        });
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, ANCHOR_RUN_ID),
          }),
        ).toMatchObject(expectedCause);
        expect(await store.wakeReconciliation("alloc-workflow-cause", 2)).toBe(
          false,
        );
        if (source === "deferred history") {
          await h.db
            .delete(workflowPendingProjection)
            .where(eq(workflowPendingProjection.id, "pending-cleanup-cause"));
          await h.db.transaction(async (tx) => {
            await store.applyDeferredInfrastructureFailure(ANCHOR_RUN_ID, tx);
          });
        }
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, ANCHOR_RUN_ID),
          }),
        ).toMatchObject({ status: "failed", ...expectedCause });
      },
    );

    describe("placing an allocation on an existing sidecar", () => {
      test.each([1, 2])(
        "placement and adoption use the advertised active limit %s",
        async (maxActiveDeployments) => {
          const first = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
          await first.markAllocated({
            allocationId: "alloc-first",
            generation: 1,
          });
          await seedAnchor("anchor-second");
          await bindFirstGeneration("alloc-second", "anchor-second");
          const store = createSidecarAllocationStore(h.db, {
            getSidecarInventory: () => ({
              capacity: { maxActiveDeployments, maxRetainedDeployments: 0 },
              retainedIncarnations: [],
            }),
          });
          const placing = {
            allocationId: "alloc-second",
            generation: 1,
            sidecarId: "alloc-first-minted",
          };
          if (maxActiveDeployments === 1)
            await expect(store.markAllocated(placing)).rejects.toBeInstanceOf(
              SidecarReuseRejectedError,
            );
          else
            expect(await store.markAllocated(placing)).toMatchObject({
              sidecarId: placing.sidecarId,
            });
          await seedAnchor("anchor-adopted");
          await expect(
            store.createAdopted({
              id: "alloc-adopted",
              anchorRunId: "anchor-adopted",
              tenantId: TENANT_ID,
              provisionerId: "ec2-spot",
              provisionerApiVersion: 2,
              provisionerBindingFingerprint: "ec2-spot:test",
              adapterManifestDigest: MANIFEST_DIGEST,
              maxDisconnectedMs: 900_000,
              sidecarId: placing.sidecarId,
              generation: 1,
              connectDeadline: new Date(Date.now() + 60_000),
            }),
          ).rejects.toBeInstanceOf(SidecarReuseRejectedError);
        },
      );

      test("a larger advertised limit permits placement beyond the default", async () => {
        await fillActiveSlots();
        await seedAnchor("anchor-extra");
        await bindFirstGeneration("alloc-extra", "anchor-extra");
        const store = createSidecarAllocationStore(h.db, {
          getSidecarInventory: () => ({
            capacity: {
              maxActiveDeployments: DEFAULT_SIDECAR_ACTIVE_DEPLOYMENTS + 1,
              maxRetainedDeployments: 0,
            },
            retainedIncarnations: [],
          }),
        });
        expect(
          await store.markAllocated({
            allocationId: "alloc-extra",
            generation: 1,
            sidecarId: "alloc-first-minted",
          }),
        ).toMatchObject({
          status: "allocated",
          sidecarId: "alloc-first-minted",
        });
      });

      async function fillActiveSlots() {
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
        await store.markAllocated({
          allocationId: "alloc-first",
          generation: 1,
        });
        const others = Array.from(
          { length: DEFAULT_SIDECAR_ACTIVE_DEPLOYMENTS - 1 },
          (_, index) => `alloc-active-${String(index)}`,
        );
        for (const id of others) {
          await seedWorkflowRun(h.db, {
            id: `anchor-${id}`,
            anchorRunId: `anchor-${id}`,
            tenantId: TENANT_ID,
            definitionId: DEFINITION_ID,
          });
        }
        await h.db.insert(sidecarAllocation).values(
          others.map((id) => ({
            id,
            anchorRunId: `anchor-${id}`,
            tenantId: TENANT_ID,
            provisionerId: "ec2-spot",
            provisionerApiVersion: 2 as const,
            provisionerBindingFingerprint: "ec2-spot:test",
            adapterManifestDigest: MANIFEST_DIGEST,
            maxDisconnectedMs: 900_000,
            sidecarId: "alloc-first-minted",
            status: "allocated" as const,
            generation: 1,
            ensureAcceptedGeneration: 1,
          })),
        );
      }

      test.each(["replacement", "release", "unrecoverable release"] as const)(
        "a never-accepted %s frees its active reservation even when the provider permanently rejects cleanup",
        async (transition) => {
          await fillActiveSlots();
          // This allocation only reserved capacity: ensure was never accepted,
          // so the Hub could not send its first deployment.
          await h.db
            .update(sidecarAllocation)
            .set({
              status: "provisioning",
              ensureAcceptedGeneration: null,
              nextAttemptAt: new Date(0),
            })
            .where(eq(sidecarAllocation.id, "alloc-first"));
          const store = createSidecarAllocationStore(h.db, {
            getSidecarInventory: () => ({
              capacity: DEFAULT_SIDECAR_CAPACITY,
              retainedIncarnations: [],
            }),
          });
          const leaseId = "never-accepted-cleanup";
          expect(
            (
              await store.claimNextReconcilable({
                leaseId,
                leaseDurationMs: 60_000,
              })
            )?.id,
          ).toBe("alloc-first");
          const args = {
            allocationId: "alloc-first",
            expectedGeneration: 1,
            expectedLeaseId: leaseId,
            expectedStatus: "provisioning" as const,
            failureCode: "ensure_outcome_unknown",
            failureMessage: "Ensure interrupted",
            firstAttemptAt: new Date(0),
            nextAttemptAt: new Date(0),
          };
          const cleanup =
            transition === "replacement"
              ? await store.beginReplacement(args)
              : transition === "release"
                ? await store.beginRelease(args)
                : await store.beginUnrecoverableRelease(args);
          expect(cleanup).toMatchObject({
            generation: 2,
            deploymentCleanupConfirmed: true,
          });
          await store.markDestroyFailed({
            allocationId: "alloc-first",
            expectedGeneration: 2,
            code: "provider_permission_denied",
            message: "Provider hold remains unreleased",
          });
          await seedAnchor("anchor-last");
          await bindFirstGeneration("alloc-last", "anchor-last");
          // A fresh store reads durable proof, without an undeploy or inventory
          // entry for this allocation ever having existed.
          const restarted = createSidecarAllocationStore(h.db, {
            getSidecarInventory: () => ({
              capacity: DEFAULT_SIDECAR_CAPACITY,
              retainedIncarnations: [],
            }),
          });
          expect(
            await restarted.markAllocated({
              allocationId: "alloc-last",
              generation: 1,
              sidecarId: "alloc-first-minted",
            }),
          ).toMatchObject({ status: "allocated" });
          expect(await restarted.findById("alloc-first")).toMatchObject({
            status: "destroy_failed",
            deploymentCleanupConfirmed: true,
          });
        },
      );

      test("retained copies free active slots after the release fence advances", async () => {
        await fillActiveSlots();
        await seedAnchor("anchor-last");
        await bindFirstGeneration("alloc-last", "anchor-last");
        let retained: readonly RetainedSidecarIncarnation[] = [];
        const store = createSidecarAllocationStore(h.db, {
          getSidecarInventory: (sidecarId) => {
            expect(sidecarId).toBe("alloc-first-minted");
            return {
              capacity: DEFAULT_SIDECAR_CAPACITY,
              retainedIncarnations: retained,
            };
          },
        });
        const placing = {
          allocationId: "alloc-last",
          generation: 1,
          sidecarId: "alloc-first-minted",
        };
        await expect(store.markAllocated(placing)).rejects.toBeInstanceOf(
          SidecarReuseRejectedError,
        );
        const releasing = await store.beginRelease({
          allocationId: "alloc-first",
          expectedStatus: "allocated",
          expectedGeneration: 1,
        });
        expect(releasing).toMatchObject({
          generation: 2,
          ensureAcceptedGeneration: 1,
        });
        retained = [{ allocationId: "alloc-first", generation: 1 }];
        expect(await store.markAllocated(placing)).toMatchObject({
          status: "allocated",
          sidecarId: "alloc-first-minted",
        });
        expect(await store.findById("alloc-first")).toMatchObject({
          status: "releasing",
          generation: 2,
        });
      });

      for (const ensureAcceptedGeneration of [null, 2]) {
        test(`retained proofs cannot free a reservation whose accepted generation is ${String(ensureAcceptedGeneration)}`, async () => {
          await fillActiveSlots();
          await h.db
            .update(sidecarAllocation)
            .set({
              ensureAcceptedGeneration,
              generation: ensureAcceptedGeneration ?? 1,
            })
            .where(eq(sidecarAllocation.id, "alloc-first"));
          await seedAnchor("anchor-last");
          await bindFirstGeneration("alloc-last", "anchor-last");
          const store = createSidecarAllocationStore(h.db, {
            getSidecarInventory: () => ({
              capacity: DEFAULT_SIDECAR_CAPACITY,
              retainedIncarnations: [
                { allocationId: "alloc-first", generation: 1 },
                { allocationId: "unknown-allocation", generation: 1 },
              ],
            }),
          });
          await expect(
            store.markAllocated({
              allocationId: "alloc-last",
              generation: 1,
              sidecarId: "alloc-first-minted",
            }),
          ).rejects.toBeInstanceOf(SidecarReuseRejectedError);
          expect(await store.findById("alloc-last")).toMatchObject({
            status: "provisioning",
            sidecarId: "alloc-last-minted",
          });
        });
      }

      test.each([
        "releasing",
        "retry exhausted",
        "permanent rejection",
      ] as const)(
        "confirmed removal frees an active slot while provider cleanup remains %s",
        async (outcome) => {
          await fillActiveSlots();
          await seedAnchor("anchor-last");
          await bindFirstGeneration("alloc-last", "anchor-last");
          let inventoryKnown = true;
          const store = createSidecarAllocationStore(h.db, {
            getSidecarInventory: () =>
              inventoryKnown
                ? {
                    capacity: DEFAULT_SIDECAR_CAPACITY,
                    retainedIncarnations: [],
                  }
                : undefined,
          });
          const placing = {
            allocationId: "alloc-last",
            generation: 1,
            sidecarId: "alloc-first-minted",
          };
          await store.beginRelease({
            allocationId: "alloc-first",
            expectedGeneration: 1,
            expectedStatus: "allocated",
          });
          await expect(store.markAllocated(placing)).rejects.toBeInstanceOf(
            SidecarReuseRejectedError,
          );
          const leaseId = "confirmed-removal";
          expect(
            (
              await store.claimNextReconcilable({
                excludedAllocationIds: ["alloc-last"],
                leaseId,
                leaseDurationMs: 60_000,
              })
            )?.id,
          ).toBe("alloc-first");
          expect(
            await store.confirmDeploymentCleanup({
              allocationId: "alloc-first",
              generation: 1,
              expectedLeaseId: leaseId,
            }),
          ).toBe(false);
          await expect(store.markAllocated(placing)).rejects.toBeInstanceOf(
            SidecarReuseRejectedError,
          );
          expect(
            await store.confirmDeploymentCleanup({
              allocationId: "alloc-first",
              generation: 2,
              expectedLeaseId: leaseId,
            }),
          ).toBe(true);
          if (outcome !== "releasing") {
            await store.markDestroyFailed({
              allocationId: "alloc-first",
              expectedGeneration: 2,
              expectedLeaseId: leaseId,
              code:
                outcome === "retry exhausted"
                  ? SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE
                  : "provider_permission_denied",
              message: "Provider hold remains unreleased",
            });
          }
          inventoryKnown = false;
          await expect(store.markAllocated(placing)).rejects.toThrow(
            "no current inventory",
          );
          inventoryKnown = true;
          expect(await store.markAllocated(placing)).toMatchObject({
            status: "allocated",
            sidecarId: placing.sidecarId,
          });
          expect(await store.findById("alloc-first")).toMatchObject({
            status: outcome === "releasing" ? "releasing" : "destroy_failed",
            deploymentCleanupConfirmed: true,
          });
          // Adopting a probe uses the same accounting and cannot consume the
          // freed slot again after the placement above took it.
          await seedAnchor("anchor-adopted-after-cleanup");
          await expect(
            store.createAdopted({
              id: "adopted-after-cleanup",
              anchorRunId: "anchor-adopted-after-cleanup",
              tenantId: TENANT_ID,
              provisionerId: "ec2-spot",
              provisionerApiVersion: 2,
              provisionerBindingFingerprint: "ec2-spot:test",
              adapterManifestDigest: MANIFEST_DIGEST,
              maxDisconnectedMs: 900_000,
              sidecarId: placing.sidecarId,
              generation: 1,
              connectDeadline: new Date(Date.now() + 60_000),
            }),
          ).rejects.toBeInstanceOf(SidecarReuseRejectedError);
        },
      );

      test("refuses to adopt a probe's sidecar once it was retired", async () => {
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
        await store.markAllocated({
          allocationId: "alloc-first",
          generation: 1,
        });
        await store.retireSidecar({ sidecarId: "alloc-first-minted" });
        await seedAnchor("anchor-adopted");

        await expect(
          store.createAdopted({
            id: "probe-adopted",
            anchorRunId: "anchor-adopted",
            tenantId: TENANT_ID,
            provisionerId: "ec2-spot",
            provisionerApiVersion: 2,
            provisionerBindingFingerprint: "ec2-spot:test",
            adapterManifestDigest: MANIFEST_DIGEST,
            maxDisconnectedMs: 900_000,
            sidecarId: "alloc-first-minted",
            generation: 1,
            connectDeadline: new Date(Date.now() + 60_000),
          }),
        ).rejects.toThrow("it was retired");
        expect(await store.findById("probe-adopted")).toBeNull();
      });

      test("probe adoption uses the active slot freed by a retained copy", async () => {
        await fillActiveSlots();
        await seedAnchor("anchor-adopted");
        const store = createSidecarAllocationStore(h.db, {
          getSidecarInventory: (sidecarId) =>
            sidecarId === "alloc-first-minted"
              ? {
                  capacity: DEFAULT_SIDECAR_CAPACITY,
                  retainedIncarnations: [
                    { allocationId: "alloc-first", generation: 1 },
                  ],
                }
              : {
                  capacity: DEFAULT_SIDECAR_CAPACITY,
                  retainedIncarnations: [],
                },
        });
        expect(
          await store.createAdopted({
            id: "probe-adopted",
            anchorRunId: "anchor-adopted",
            tenantId: TENANT_ID,
            provisionerId: "ec2-spot",
            provisionerApiVersion: 2,
            provisionerBindingFingerprint: "ec2-spot:test",
            adapterManifestDigest: MANIFEST_DIGEST,
            maxDisconnectedMs: 900_000,
            sidecarId: "alloc-first-minted",
            generation: 1,
            connectDeadline: new Date(Date.now() + 60_000),
          }),
        ).toMatchObject({
          status: "allocated",
          sidecarId: "alloc-first-minted",
        });
      });

      test("records the reused sidecar and deletes the unused identity", async () => {
        await seedAnchor("anchor-second");
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
        await store.markAllocated({
          allocationId: "alloc-first",
          generation: 1,
        });
        await bindFirstGeneration("alloc-second", "anchor-second");

        const placed = await store.markAllocated({
          allocationId: "alloc-second",
          generation: 1,
          sidecarId: "alloc-first-minted",
          externalRef: "pid-shared",
        });

        expect(placed).toMatchObject({
          status: "allocated",
          sidecarId: "alloc-first-minted",
          ensureAcceptedGeneration: 1,
          externalRef: "pid-shared",
        });
        expect(
          await h.db.query.sidecar.findFirst({
            where: eq(sidecar.id, "alloc-second-minted"),
          }),
        ).toBeUndefined();
        expect(
          (await store.listActive()).map((allocation) => allocation.sidecarId),
        ).toEqual(["alloc-first-minted", "alloc-first-minted"]);
      });

      test("places an allocation on a sidecar that hosts nothing", async () => {
        await seedAnchor("anchor-second");
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
        await store.markAllocated({
          allocationId: "alloc-first",
          generation: 1,
        });
        await h.db
          .update(sidecarAllocation)
          .set({ status: "released" })
          .where(eq(sidecarAllocation.id, "alloc-first"));
        await bindFirstGeneration("alloc-second", "anchor-second");

        expect(
          await store.markAllocated({
            allocationId: "alloc-second",
            generation: 1,
            sidecarId: "alloc-first-minted",
          }),
        ).toMatchObject({
          status: "allocated",
          sidecarId: "alloc-first-minted",
        });
      });

      test.each(["allocated", "releasing", "destroy_failed"] as const)(
        "keeps %s deployments counted against sidecar capacity",
        async (status) => {
          const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
          await store.markAllocated({
            allocationId: "alloc-first",
            generation: 1,
          });
          const others = Array.from(
            { length: DEFAULT_SIDECAR_ACTIVE_DEPLOYMENTS - 1 },
            (_, index) => `alloc-full-${String(index)}`,
          );
          for (const id of others) {
            await seedWorkflowRun(h.db, {
              id: `anchor-${id}`,
              anchorRunId: `anchor-${id}`,
              tenantId: TENANT_ID,
              definitionId: DEFINITION_ID,
            });
          }
          await h.db.insert(sidecarAllocation).values(
            others.map((id) => ({
              id,
              anchorRunId: `anchor-${id}`,
              tenantId: TENANT_ID,
              provisionerId: "ec2-spot",
              provisionerApiVersion: 2 as const,
              provisionerBindingFingerprint: "ec2-spot:test",
              adapterManifestDigest: MANIFEST_DIGEST,
              maxDisconnectedMs: 900_000,
              sidecarId: "alloc-first-minted",
              status,
              generation: 1,
            })),
          );
          await seedAnchor("anchor-last");
          await bindFirstGeneration("alloc-last", "anchor-last");

          await expect(
            store.markAllocated({
              allocationId: "alloc-last",
              generation: 1,
              sidecarId: "alloc-first-minted",
            }),
          ).rejects.toThrow(
            `it already hosts ${String(DEFAULT_SIDECAR_ACTIVE_DEPLOYMENTS)} deployments`,
          );

          if (status === "releasing") {
            const removed = others[0];
            if (removed === undefined)
              throw new Error("Missing releasing allocation");
            await h.db
              .update(sidecarAllocation)
              .set({
                deploymentCleanupConfirmed: true,
                providerHoldReleased: true,
              })
              .where(eq(sidecarAllocation.id, removed));
            await store.markReleased({ allocationId: removed, generation: 1 });
            expect(
              await store.markAllocated({
                allocationId: "alloc-last",
                generation: 1,
                sidecarId: "alloc-first-minted",
              }),
            ).toMatchObject({
              status: "allocated",
              sidecarId: "alloc-first-minted",
            });
          }

          if (status === "destroy_failed") {
            const stopped = others[0];
            if (stopped === undefined)
              throw new Error("Missing stopped allocation");
            await h.db
              .update(sidecarAllocation)
              .set({
                failureCode: SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
                failureMessage: "Provider kept timing out",
              })
              .where(eq(sidecarAllocation.id, stopped));
            const release = {
              tenantId: TENANT_ID,
              anchorRunId: `anchor-${stopped}`,
              generation: 1,
              releasedBy: "principal-operator",
            };
            expect(
              await store.releaseStoppedCleanup({ ...release, generation: 2 }),
            ).toEqual({ kind: "generation_mismatch" });
            expect(
              await store.releaseStoppedCleanup({
                ...release,
                anchorRunId: ANCHOR_RUN_ID,
              }),
            ).toEqual({ kind: "not_stopped" });
            expect(
              await store.releaseStoppedCleanup({
                ...release,
                tenantId: "tnt-other",
              }),
            ).toEqual({ kind: "not_found" });
            expect(await store.releaseStoppedCleanup(release)).toMatchObject({
              kind: "released",
              allocation: {
                status: "released",
                deploymentCleanupConfirmed: true,
                providerHoldReleased: true,
                failureCode: SIDECAR_CLEANUP_OPERATOR_RELEASED_FAILURE_CODE,
                failureMessage: `Released by principal-operator after cleanup stopped with ${SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE}: Provider kept timing out`,
              },
            });
            expect(await store.releaseStoppedCleanup(release)).toEqual({
              kind: "already_released",
            });
            expect(
              await store.markAllocated({
                allocationId: "alloc-last",
                generation: 1,
                sidecarId: "alloc-first-minted",
              }),
            ).toMatchObject({
              status: "allocated",
              sidecarId: "alloc-first-minted",
            });
          }
        },
      );

      test("refuses to adopt a probe onto a sidecar whose active slots are full", async () => {
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
        await store.markAllocated({
          allocationId: "alloc-first",
          generation: 1,
        });
        const others = Array.from(
          { length: DEFAULT_SIDECAR_ACTIVE_DEPLOYMENTS - 1 },
          (_, index) => `alloc-full-${String(index)}`,
        );
        for (const id of others) {
          await seedWorkflowRun(h.db, {
            id: `anchor-${id}`,
            anchorRunId: `anchor-${id}`,
            tenantId: TENANT_ID,
            definitionId: DEFINITION_ID,
          });
        }
        await h.db.insert(sidecarAllocation).values(
          others.map((id) => ({
            id,
            anchorRunId: `anchor-${id}`,
            tenantId: TENANT_ID,
            provisionerId: "ec2-spot",
            provisionerApiVersion: 2 as const,
            provisionerBindingFingerprint: "ec2-spot:test",
            adapterManifestDigest: MANIFEST_DIGEST,
            maxDisconnectedMs: 900_000,
            sidecarId: "alloc-first-minted",
            status: "allocated" as const,
            generation: 1,
          })),
        );
        await seedAnchor("anchor-adopted");

        await expect(
          store.createAdopted({
            id: "probe-adopted",
            anchorRunId: "anchor-adopted",
            tenantId: TENANT_ID,
            provisionerId: "ec2-spot",
            provisionerApiVersion: 2,
            provisionerBindingFingerprint: "ec2-spot:test",
            adapterManifestDigest: MANIFEST_DIGEST,
            maxDisconnectedMs: 900_000,
            sidecarId: "alloc-first-minted",
            generation: 1,
            connectDeadline: new Date(Date.now() + 60_000),
          }),
        ).rejects.toThrow(
          `it already hosts ${String(DEFAULT_SIDECAR_ACTIVE_DEPLOYMENTS)} deployments`,
        );
        expect(await store.findById("probe-adopted")).toBeNull();
      });

      test("rejects a sidecar whose allocation booted with another adapter manifest", async () => {
        await seedAnchor("anchor-second");
        const store = await bindFirstGeneration(
          "alloc-first",
          ANCHOR_RUN_ID,
          "ec2-spot",
          OTHER_MANIFEST_DIGEST,
        );
        await store.markAllocated({
          allocationId: "alloc-first",
          generation: 1,
        });
        await bindFirstGeneration("alloc-second", "anchor-second");

        await expect(
          store.markAllocated({
            allocationId: "alloc-second",
            generation: 1,
            sidecarId: "alloc-first-minted",
          }),
        ).rejects.toBeInstanceOf(SidecarReuseRejectedError);
        expect(await store.findById("alloc-second")).toMatchObject({
          status: "provisioning",
          sidecarId: "alloc-second-minted",
          adapterManifestDigest: MANIFEST_DIGEST,
        });
      });

      test("retires an identity whose ensure was rejected and refuses to reuse it", async () => {
        await seedAnchor("anchor-second");
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
        await store.retireSidecar({ sidecarId: "alloc-first-minted" });
        await store.failWithoutInfrastructure({
          allocationId: "alloc-first",
          expectedStatus: "provisioning",
          expectedGeneration: 1,
          code: "ensure_rejected",
          message: "No capacity",
        });
        await bindFirstGeneration("alloc-second", "anchor-second");

        await expect(
          store.markAllocated({
            allocationId: "alloc-second",
            generation: 1,
            sidecarId: "alloc-first-minted",
          }),
        ).rejects.toThrow("it was retired");
        expect(await store.findById("alloc-second")).toMatchObject({
          status: "provisioning",
          sidecarId: "alloc-second-minted",
        });
      });

      test("refuses to reuse a retired sidecar that other work still uses", async () => {
        await seedAnchor("anchor-second");
        await seedAnchor("anchor-third");
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
        await store.markAllocated({
          allocationId: "alloc-first",
          generation: 1,
        });
        await store.retireSidecar({ sidecarId: "alloc-first-minted" });
        await bindFirstGeneration("alloc-second", "anchor-second");

        await expect(
          store.markAllocated({
            allocationId: "alloc-second",
            generation: 1,
            sidecarId: "alloc-first-minted",
          }),
        ).rejects.toThrow("it was retired");
        expect(await store.findById("alloc-first")).toMatchObject({
          status: "allocated",
          sidecarId: "alloc-first-minted",
        });
      });

      test("retires only a reported sidecar minted for the reporting binding", async () => {
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
        const binding = {
          provisionerId: "ec2-spot",
          provisionerApiVersion: 2 as const,
          provisionerBindingFingerprint: "ec2-spot:test",
        };
        const retiredAt = async () =>
          (
            await h.db.query.sidecar.findFirst({
              columns: { retiredAt: true },
              where: eq(sidecar.id, "alloc-first-minted"),
            })
          )?.retiredAt;

        expect(
          await store.retireProvisionerSidecar({
            sidecarId: "alloc-first-minted",
            binding: {
              ...binding,
              provisionerBindingFingerprint: "ec2-spot:other",
            },
            endReconnectWindow: false,
            isConnected: () => false,
          }),
        ).toBeNull();
        expect(
          await store.retireProvisionerSidecar({
            sidecarId: "unknown-sidecar",
            binding,
            endReconnectWindow: false,
            isConnected: () => false,
          }),
        ).toBeNull();
        expect(await retiredAt()).toBeNull();

        const report = {
          sidecarId: "alloc-first-minted",
          binding,
          endReconnectWindow: false,
          isConnected: () => false,
        };
        expect(await store.retireProvisionerSidecar(report)).toEqual({
          connectedAllocationIds: [],
        });
        const first = await retiredAt();
        expect(first).toBeInstanceOf(Date);
        // A repeated report succeeds and keeps the first retirement.
        expect(await store.retireProvisionerSidecar(report)).toEqual({
          connectedAllocationIds: [],
        });
        expect(await retiredAt()).toEqual(first);
      });

      test("ends the reconnect window of the sidecar's work only when the report ends it", async () => {
        await seedAnchor("anchor-live");
        await seedAnchor("anchor-unrecorded");
        const store = await bindFirstGeneration("alloc-lost", ANCHOR_RUN_ID);
        await store.markAllocated({
          allocationId: "alloc-lost",
          generation: 1,
        });
        for (const [allocationId, anchorRunId] of [
          ["alloc-live", "anchor-live"],
          ["alloc-unrecorded", "anchor-unrecorded"],
        ] as const) {
          await bindFirstGeneration(allocationId, anchorRunId);
          await store.markAllocated({
            allocationId,
            generation: 1,
            sidecarId: "alloc-lost-minted",
          });
        }
        await h.db
          .update(sidecarAllocation)
          .set({
            reconciliationLeaseId: "lease-waiting",
            reconciliationLeaseExpiresAt: new Date(Date.now() + 60_000),
          })
          .where(eq(sidecarAllocation.id, "alloc-lost"));
        // Connected but still initializing, so it keeps its deadline.
        await h.db
          .update(sidecarAllocation)
          .set({
            initializationLeaseId: "lease-initializing",
            reconciliationLeaseId: "lease-initializing",
            reconciliationLeaseExpiresAt: new Date(Date.now() + 60_000),
          })
          .where(eq(sidecarAllocation.id, "alloc-live"));
        // Its connection dropped before the Hub recorded the disconnect.
        await h.db
          .update(sidecarAllocation)
          .set({ connectDeadline: null, nextAttemptAt: null })
          .where(eq(sidecarAllocation.id, "alloc-unrecorded"));
        const report = {
          sidecarId: "alloc-lost-minted",
          binding: {
            provisionerId: "ec2-spot",
            provisionerApiVersion: 2 as const,
            provisionerBindingFingerprint: "ec2-spot:test",
          },
          isConnected: ({ allocationId }: { allocationId: string }) =>
            allocationId === "alloc-live",
        };
        const waitingDeadline = (await store.findById("alloc-lost"))
          ?.connectDeadline;
        const liveBefore = await store.findById("alloc-live");

        expect(
          await store.retireProvisionerSidecar({
            ...report,
            endReconnectWindow: false,
          }),
        ).toEqual({ connectedAllocationIds: ["alloc-live"] });
        expect(await store.findById("alloc-lost")).toMatchObject({
          connectDeadline: waitingDeadline,
          reconciliationLeaseId: "lease-waiting",
        });
        const reconnectWindowEndedAt = async () =>
          (
            await h.db.query.sidecar.findFirst({
              columns: { reconnectWindowEndedAt: true },
              where: eq(sidecar.id, "alloc-lost-minted"),
            })
          )?.reconnectWindowEndedAt;
        expect(await reconnectWindowEndedAt()).toBeNull();

        const reportedAt = new Date();
        expect(
          await store.retireProvisionerSidecar({
            ...report,
            endReconnectWindow: true,
            now: reportedAt,
          }),
        ).toEqual({ connectedAllocationIds: ["alloc-live"] });
        expect(await reconnectWindowEndedAt()).toEqual(reportedAt);
        for (const allocationId of ["alloc-lost", "alloc-unrecorded"]) {
          const ended = await store.findById(allocationId);
          expect(ended).toMatchObject({
            status: "allocated",
            connectDeadline: reportedAt,
            nextAttemptAt: reportedAt,
          });
          expect(ended?.reconciliationLeaseId).toBeUndefined();
        }
        expect(await store.findById("alloc-live")).toEqual(liveBefore);

        // A Hub restart keeps that work's deadline due, and work that
        // disconnects only later gets no reconnect window either.
        const startedAt = new Date(reportedAt.getTime() + 1_000);
        expect(
          (
            await store.scheduleReconnectAfterHubStart({
              allocationId: "alloc-lost",
              generation: 1,
              now: startedAt,
              firstConnectDeadline: new Date(startedAt.getTime() + 120_000),
            })
          )?.connectDeadline,
        ).toEqual(startedAt);
        const lostAt = new Date(startedAt.getTime() + 1_000);
        expect(
          (
            await store.markConnectionLost({
              allocationId: "alloc-live",
              generation: 1,
              now: lostAt,
              firstConnectDeadline: new Date(lostAt.getTime() + 120_000),
            })
          )?.connectDeadline,
        ).toEqual(lostAt);
      });

      test("a disconnect recorded while the report commits sees the ended window", async () => {
        const store = await bindFirstGeneration("alloc-racing", ANCHOR_RUN_ID);
        await store.markAllocated({
          allocationId: "alloc-racing",
          generation: 1,
        });
        await h.db
          .update(sidecarAllocation)
          .set({ connectDeadline: null, nextAttemptAt: null })
          .where(eq(sidecarAllocation.id, "alloc-racing"));
        const locked = Promise.withResolvers<number>();
        const release = Promise.withResolvers<undefined>();
        const reportedAt = new Date();
        // Holds the sidecar row the way an open report does.
        const report = h.db.transaction(async (tx) => {
          const [backend] = await tx.execute(
            sql`select pg_backend_pid() as pid`,
          );
          const pid = backend?.["pid"];
          if (typeof pid !== "number") throw new Error("Missing backend PID");
          await tx
            .select({ id: sidecar.id })
            .from(sidecar)
            .where(eq(sidecar.id, "alloc-racing-minted"))
            .for("update");
          await tx
            .update(sidecar)
            .set({ reconnectWindowEndedAt: reportedAt })
            .where(eq(sidecar.id, "alloc-racing-minted"));
          locked.resolve(pid);
          await release.promise;
        });
        const reportPid = await locked.promise;
        const lostAt = new Date(reportedAt.getTime() + 1_000);
        const disconnected = store.markConnectionLost({
          allocationId: "alloc-racing",
          generation: 1,
          now: lostAt,
          firstConnectDeadline: new Date(lostAt.getTime() + 120_000),
        });
        try {
          // Postgres signals no lock wait, so poll for it; a disconnect
          // that never blocks is a hang for the lane timeout to fail.
          for (;;) {
            const [waiting] = await h.db.execute(
              sql`select pid from pg_stat_activity where ${reportPid} = any(pg_blocking_pids(pid))`,
            );
            if (typeof waiting?.["pid"] === "number") break;
            await new Promise((resolve) => setTimeout(resolve, 1));
          }
        } finally {
          release.resolve(undefined);
          await report;
        }

        expect((await disconnected)?.connectDeadline).toEqual(lostAt);
      });

      test("rejects a sidecar recorded before its adapter manifest was", async () => {
        await seedAnchor("anchor-second");
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);
        await store.markAllocated({
          allocationId: "alloc-first",
          generation: 1,
        });
        await h.db
          .update(sidecar)
          .set({ adapterManifestDigest: "unrecorded" })
          .where(eq(sidecar.id, "alloc-first-minted"));
        await bindFirstGeneration("alloc-second", "anchor-second");

        await expect(
          store.markAllocated({
            allocationId: "alloc-second",
            generation: 1,
            sidecarId: "alloc-first-minted",
          }),
        ).rejects.toBeInstanceOf(SidecarReuseRejectedError);
      });

      test("places an allocation on a sidecar whose probe booted with the same adapter manifest", async () => {
        await seedProbeHost("probe-host", MANIFEST_DIGEST);
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);

        expect(
          await store.markAllocated({
            allocationId: "alloc-first",
            generation: 1,
            sidecarId: "probe-host-minted",
          }),
        ).toMatchObject({
          status: "allocated",
          sidecarId: "probe-host-minted",
          adapterManifestDigest: MANIFEST_DIGEST,
        });
      });

      test("rejects a sidecar whose probe booted with another adapter manifest", async () => {
        await seedProbeHost("probe-host", OTHER_MANIFEST_DIGEST);
        const store = await bindFirstGeneration("alloc-first", ANCHOR_RUN_ID);

        await expect(
          store.markAllocated({
            allocationId: "alloc-first",
            generation: 1,
            sidecarId: "probe-host-minted",
          }),
        ).rejects.toBeInstanceOf(SidecarReuseRejectedError);
        expect(await store.findById("alloc-first")).toMatchObject({
          status: "provisioning",
          sidecarId: "alloc-first-minted",
        });
      });

      test("rejects a sidecar another provisioner runs", async () => {
        await seedAnchor("anchor-second");
        const store = await bindFirstGeneration(
          "alloc-first",
          ANCHOR_RUN_ID,
          "other-backend",
        );
        await store.markAllocated({
          allocationId: "alloc-first",
          generation: 1,
        });
        await bindFirstGeneration("alloc-second", "anchor-second");

        await expect(
          store.markAllocated({
            allocationId: "alloc-second",
            generation: 1,
            sidecarId: "alloc-first-minted",
          }),
        ).rejects.toBeInstanceOf(SidecarReuseRejectedError);
      });
    });

    async function createClaimedAllocation(id: string) {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      await store.createPending({
        id,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      const leaseId = `${id}-lease`;
      await store.claimNextReconcilable({ leaseId, leaseDurationMs: 60_000 });
      await store.bindInitialSidecar({
        allocationId: id,
        expectedGeneration: 0,
        sidecarId: `${id}-sidecar`,
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(Date.now() + 60_000),
        expectedLeaseId: leaseId,
      });
      const allocation = await store.markAllocated({
        allocationId: id,
        generation: 1,
        expectedLeaseId: leaseId,
      });
      if (allocation === null) throw new Error("Failed to create allocation");
      return {
        store,
        allocation,
        leaseId,
        initialization: {
          allocationId: id,
          generation: allocation.generation,
          anchorRunId: ANCHOR_RUN_ID,
          tenantId: TENANT_ID,
          leaseId,
          signal: new AbortController().signal,
        },
      };
    }

    test("initialization reserves once and publishes the key and marker atomically", async () => {
      const { store, allocation, leaseId, initialization } =
        await createClaimedAllocation("alloc-initialization");
      expect(await store.beginInitialization(initialization)).toBe(true);
      expect(await store.beginInitialization(initialization)).toBe(false);
      expect((await store.findById(allocation.id))?.initializationLeaseId).toBe(
        leaseId,
      );
      expect(
        (
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, ANCHOR_RUN_ID),
          })
        )?.publicKey,
      ).toBeNull();
      expect(
        await store.markConnectionReady({
          allocationId: allocation.id,
          generation: allocation.generation,
          expectedLeaseId: leaseId,
        }),
      ).toBeNull();

      const credentialRefs = { credentialIds: ["credential-1"], bindings: [] };
      expect(
        await store.completeInitialization({
          ...initialization,
          publicKey: "new-key",
          credentialRefs,
        }),
      ).toBe(true);
      expect(
        (await store.findById(allocation.id))?.initializationLeaseId,
      ).toBeUndefined();
      expect(
        (await store.findById(allocation.id))?.connectDeadline,
      ).toBeUndefined();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, ANCHOR_RUN_ID),
        }),
      ).toMatchObject({ publicKey: "new-key", credentialRefs });
      expect(
        await store.completeInitialization({
          ...initialization,
          publicKey: "late-key",
        }),
      ).toBe(false);
    });

    test("a proven rejection survives disconnect and a new reconciliation claim", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-rejected-disconnected");
      expect(await store.beginInitialization(initialization)).toBe(true);
      await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        now: new Date(0),
        firstConnectDeadline: new Date(120_000),
      });
      const claimed = await store.claimNextReconcilable({
        leaseId: "recovery-owner",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.initializationLeaseId).toBe(initialization.leaseId);
      const controller = new AbortController();
      controller.abort(new Error("Disconnected after refusal"));
      const rejection = {
        ...initialization,
        signal: controller.signal,
        message: "No active slot",
      };
      for (const stale of [
        { ...rejection, leaseId: "different-attempt" },
        { ...rejection, generation: allocation.generation + 1 },
        { ...rejection, tenantId: "different-tenant" },
      ]) {
        expect(await store.rejectInitialization(stale)).toBeNull();
      }
      expect(await store.rejectInitialization(rejection)).toMatchObject({
        status: "releasing",
        generation: allocation.generation + 1,
        failureCode: "sidecar_deployment_rejected",
        failureMessage: "No active slot",
        // History may have been restored before the rejected deploy frame.
        deploymentCleanupConfirmed: false,
        providerHoldReleased: false,
      });
      const released = await store.findById(allocation.id);
      expect(released?.initializationLeaseId).toBeUndefined();
      expect(released?.reconciliationLeaseId).toBeUndefined();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, ANCHOR_RUN_ID),
        }),
      ).toMatchObject({
        status: "failed",
        failureCode: "sidecar_deployment_rejected",
      });
      expect(await store.rejectInitialization(rejection)).toBeNull();
      expect(
        await store.beginUnrecoverableRelease({
          allocationId: allocation.id,
          expectedStatus: "allocated",
          expectedGeneration: allocation.generation,
          expectedLeaseId: "recovery-owner",
          expectedInitializationLeaseId: initialization.leaseId,
          onlyIfInitializationIncomplete: true,
          failureCode: "sidecar_initialization_uncertain",
          failureMessage: "Stale claim observed an outstanding attempt",
        }),
      ).toBeNull();
      expect((await store.findById(allocation.id))?.failureCode).toBe(
        "sidecar_deployment_rejected",
      );
    });

    test("a stale refusal cannot undo a newer attempt or completed initialization", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-rejected-stale");
      expect(await store.beginInitialization(initialization)).toBe(true);
      expect(await store.clearUnsentInitialization(initialization)).toBe(true);
      await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        now: new Date(0),
        firstConnectDeadline: new Date(120_000),
      });
      await store.claimNextReconcilable({
        leaseId: "new-attempt",
        leaseDurationMs: 60_000,
      });
      const next = { ...initialization, leaseId: "new-attempt" };
      expect(await store.beginInitialization(next)).toBe(true);
      const rejection = { ...initialization, message: "Old refusal" };
      expect(await store.rejectInitialization(rejection)).toBeNull();
      expect(
        await store.completeInitialization({ ...next, publicKey: "new-key" }),
      ).toBe(true);
      expect(
        await store.rejectInitialization({ ...next, message: "Late refusal" }),
      ).toBeNull();
      expect(await store.findById(allocation.id)).toMatchObject({
        status: "allocated",
        generation: allocation.generation,
      });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, ANCHOR_RUN_ID),
        }),
      ).toMatchObject({ publicKey: "new-key" });
    });

    test("unsent rollback clears its marker after lease loss without overwriting a later completion", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-key-rollback");
      expect(await store.beginInitialization(initialization)).toBe(true);
      const controller = new AbortController();
      controller.abort(new Error("Old initialization cancelled"));
      const rollback = { ...initialization, signal: controller.signal };
      await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        now: new Date(0),
        firstConnectDeadline: new Date(120_000),
      });
      const nextLeaseId = "new-owner";
      await store.claimNextReconcilable({
        leaseId: nextLeaseId,
        leaseDurationMs: 60_000,
      });
      expect(await store.clearUnsentInitialization(rollback)).toBe(true);
      expect(
        (await store.findById(allocation.id))?.initializationLeaseId,
      ).toBeUndefined();
      expect(await store.clearUnsentInitialization(rollback)).toBe(false);
      expect(
        await store.beginUnrecoverableRelease({
          expectedStatus: "allocated",
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedLeaseId: nextLeaseId,
          onlyIfInitializationIncomplete: true,
          expectedInitializationLeaseId: initialization.leaseId,
          failureCode: "stale-uncertainty",
          failureMessage: "Claim observed the marker before rollback",
        }),
      ).toBeNull();
      const next = { ...initialization, leaseId: nextLeaseId };
      expect(await store.beginInitialization(next)).toBe(true);
      expect(await store.clearUnsentInitialization(rollback)).toBe(false);
      expect(
        await store.completeInitialization({ ...next, publicKey: "new-key" }),
      ).toBe(true);
      expect(await store.clearUnsentInitialization(rollback)).toBe(false);
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, ANCHOR_RUN_ID),
        }),
      ).toMatchObject({ publicKey: "new-key" });
    });

    for (const nextAttempt of [false, true]) {
      test(`cleanup cannot act on a rolled-back first attempt (nextAttempt=${String(nextAttempt)})`, async () => {
        const { store, allocation, initialization } =
          await createClaimedAllocation("alloc-first-rollback");
        expect(await store.beginInitialization(initialization)).toBe(true);
        await store.markConnectionLost({
          allocationId: allocation.id,
          generation: allocation.generation,
          now: new Date(0),
          firstConnectDeadline: new Date(120_000),
        });
        const leaseId = "recovery-owner";
        const claimed = await store.claimNextReconcilable({
          leaseId,
          leaseDurationMs: 60_000,
        });
        if (claimed?.initializationLeaseId === undefined)
          throw new Error("Recovery did not observe the pending attempt");
        expect(await store.clearUnsentInitialization(initialization)).toBe(
          true,
        );
        if (nextAttempt) {
          await store.beginInitialization({ ...initialization, leaseId });
        }
        const cleanup = {
          expectedStatus: "allocated" as const,
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedLeaseId: leaseId,
          onlyIfInitializationIncomplete: true,
          expectedInitializationLeaseId: claimed.initializationLeaseId,
          failureCode: "stale-uncertainty",
          failureMessage: "Recovery observed the marker before rollback",
        };
        expect(await store.beginUnrecoverableRelease(cleanup)).toBeNull();
        expect(await store.findById(allocation.id)).toMatchObject({
          status: "allocated",
          generation: allocation.generation,
        });
        expect(
          (await store.findById(allocation.id))?.initializationLeaseId,
        ).toBe(nextAttempt ? leaseId : undefined);
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, ANCHOR_RUN_ID),
          }),
        ).toMatchObject({ status: "running", publicKey: null });
        if (nextAttempt) {
          const currentCleanup = {
            ...cleanup,
            expectedInitializationLeaseId: leaseId,
          };
          expect(
            await store.beginUnrecoverableRelease(currentCleanup),
          ).toMatchObject({
            status: "releasing",
            generation: allocation.generation + 1,
          });
        }
      });
    }

    test("disconnect preserves uncertainty and stale callbacks cannot resolve it", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-initialization-disconnect");
      expect(await store.beginInitialization(initialization)).toBe(true);
      await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        now: new Date(0),
        firstConnectDeadline: new Date(120_000),
      });
      const claimed = await store.claimNextReconcilable({
        leaseId: "next-owner",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.initializationLeaseId).toBe(initialization.leaseId);
      // The stale owner may still null its own marker: the session-service
      // caller only invokes the clear for a proven-unsent frame, and marker
      // equality proves no newer attempt began. Uncertainty is preserved
      // below by the missing key, not by the marker.
      expect(await store.clearUnsentInitialization(initialization)).toBe(true);
      expect(
        await store.completeInitialization({
          ...initialization,
          publicKey: "stale-key",
        }),
      ).toBe(false);
      expect(
        await store.beginInitialization({
          ...initialization,
          leaseId: "next-owner",
        }),
      ).toBe(true);
      const releasing = await store.beginUnrecoverableRelease({
        expectedStatus: "allocated",
        allocationId: allocation.id,
        expectedGeneration: allocation.generation,
        expectedLeaseId: "next-owner",
        onlyIfInitializationIncomplete: true,
        failureCode: "uncertain",
        failureMessage: "Previous deploy was cancelled",
      });
      expect(releasing).toMatchObject({
        status: "releasing",
        generation: allocation.generation + 1,
      });
      expect(releasing?.initializationLeaseId).toBeUndefined();
    });

    test("an unsent attempt clears under its own marker after its lease lapses", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-unsent");
      await store.beginInitialization(initialization);
      expect(
        await store.clearUnsentInitialization({
          ...initialization,
          leaseId: "other-attempt",
        }),
      ).toBe(false);
      expect(await store.clearUnsentInitialization(initialization)).toBe(true);
      expect(await store.beginInitialization(initialization)).toBe(true);
      await h.db
        .update(sidecarAllocation)
        .set({ reconciliationLeaseExpiresAt: new Date(0) })
        .where(eq(sidecarAllocation.id, allocation.id));
      // Completion stays lease-guarded: the expired lease cannot publish,
      // and the failed publish leaves this attempt's marker behind.
      expect(
        await store.completeInitialization({
          ...initialization,
          publicKey: "expired-key",
        }),
      ).toBe(false);
      expect((await store.findById(allocation.id))?.initializationLeaseId).toBe(
        initialization.leaseId,
      );
      // A lapsed lease must not strand this attempt's own marker: marker
      // equality still proves no newer attempt began, so the proven-unsent
      // clear goes through.
      expect(await store.clearUnsentInitialization(initialization)).toBe(true);
      expect(
        (await store.findById(allocation.id))?.initializationLeaseId,
      ).toBeUndefined();
    });

    test("a clear only nulls its own attempt's still-current marker", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-unsent-scope");
      await store.beginInitialization(initialization);
      // A newer attempt's marker is not ours to clear.
      await h.db
        .update(sidecarAllocation)
        .set({ initializationLeaseId: "next-owner" })
        .where(eq(sidecarAllocation.id, allocation.id));
      expect(await store.clearUnsentInitialization(initialization)).toBe(false);
      expect((await store.findById(allocation.id))?.initializationLeaseId).toBe(
        "next-owner",
      );
      // Neither is a marker left behind by a generation advance.
      await h.db
        .update(sidecarAllocation)
        .set({
          generation: allocation.generation + 1,
          ensureAcceptedGeneration: allocation.generation + 1,
        })
        .where(eq(sidecarAllocation.id, allocation.id));
      expect(await store.clearUnsentInitialization(initialization)).toBe(false);
    });

    test("a committed initialization survives an ambiguous failure", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-completed");
      await store.beginInitialization(initialization);
      await store.completeInitialization({
        ...initialization,
        publicKey: "committed-key",
      });
      const common = {
        expectedStatus: "allocated" as const,
        allocationId: allocation.id,
        expectedGeneration: allocation.generation,
        expectedLeaseId: initialization.leaseId,
        onlyIfInitializationIncomplete: true,
        failureCode: "lost_commit_response",
        failureMessage: "The commit response was lost",
      };
      const changed = await store.beginUnrecoverableRelease(common);
      expect(changed).toBeNull();
      expect(await store.findById(allocation.id)).toMatchObject({
        status: "allocated",
        generation: allocation.generation,
      });
      expect(
        (
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, ANCHOR_RUN_ID),
          })
        )?.publicKey,
      ).toBe("committed-key");
    });

    test("cleanup waiting behind publication observes the committed key", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-completion-race");
      await store.beginInitialization(initialization);
      const locked = Promise.withResolvers<number>();
      const release = Promise.withResolvers<boolean>();
      const blocker = h.db.transaction(async (tx) => {
        const [backend] = await tx.execute(sql`select pg_backend_pid() as pid`);
        const pid = backend?.["pid"];
        if (typeof pid !== "number") throw new Error("Missing backend PID");
        await tx
          .select()
          .from(workflowRun)
          .where(eq(workflowRun.id, ANCHOR_RUN_ID))
          .for("update");
        locked.resolve(pid);
        await release.promise;
      });
      const blockerPid = await locked.promise;
      async function waitUntilBlocked(by: number): Promise<number> {
        const deadline = Date.now() + 5_000;
        while (Date.now() < deadline) {
          const [waiting] = await h.db.execute(
            sql`select pid from pg_stat_activity where ${by} = any(pg_blocking_pids(pid))`,
          );
          const pid = waiting?.["pid"];
          if (typeof pid === "number") return pid;
          await new Promise((resolve) => setTimeout(resolve, 1));
        }
        throw new Error("Expected transaction did not block");
      }
      const publishing = store.completeInitialization({
        ...initialization,
        publicKey: "committed-key",
      });
      let cleanup:
        | ReturnType<typeof store.beginUnrecoverableRelease>
        | undefined;
      try {
        const publishingPid = await waitUntilBlocked(blockerPid);
        cleanup = store.beginUnrecoverableRelease({
          expectedStatus: "allocated",
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedLeaseId: initialization.leaseId,
          onlyIfInitializationIncomplete: true,
          failureCode: "ambiguous_completion",
          failureMessage: "Completion response was lost",
        });
        // Establish both lock waits before publication commits. Cleanup's
        // initial statement snapshot therefore predates the new public key.
        await waitUntilBlocked(publishingPid);
      } finally {
        release.resolve(true);
        await blocker;
      }
      expect(await publishing).toBe(true);
      expect(await cleanup).toBeNull();
      expect(await store.findById(allocation.id)).toMatchObject({
        status: "allocated",
        generation: allocation.generation,
      });
    });

    for (const failure of ["cancelled", "expired"] as const) {
      test(`initialization completion rolls back if ${failure} while holding the allocation`, async () => {
        const { store, allocation, initialization } =
          await createClaimedAllocation("alloc-cancelled-write");
        await store.beginInitialization(initialization);
        const locked = Promise.withResolvers<boolean>();
        const release = Promise.withResolvers<boolean>();
        const blocker = h.db.transaction(async (tx) => {
          await tx
            .select()
            .from(workflowRun)
            .where(eq(workflowRun.id, ANCHOR_RUN_ID))
            .for("update");
          locked.resolve(true);
          await release.promise;
        });
        await locked.promise;
        const expiresAt = new Date(Date.now() + 1_000);
        if (failure === "expired") {
          await h.db
            .update(sidecarAllocation)
            .set({ reconciliationLeaseExpiresAt: expiresAt })
            .where(eq(sidecarAllocation.id, allocation.id));
        }
        const controller = new AbortController();
        const writing = store.completeInitialization({
          ...initialization,
          signal: controller.signal,
          publicKey: "cancelled-key",
        });
        // Wait until the initializer owns the allocation and is blocked on the
        // anchor. NOWAIT is independent of how long the database takes to run.
        try {
          const deadline = Date.now() + 5_000;
          for (;;) {
            if (Date.now() >= deadline)
              throw new Error("Initializer never acquired allocation lock");
            try {
              await h.db.transaction(async (tx) => {
                await tx.execute(
                  sql`select id from sidecar_allocation where id = ${allocation.id} for update nowait`,
                );
              });
            } catch (error) {
              const cause = error instanceof Error ? error.cause : undefined;
              if (
                cause instanceof Error &&
                "code" in cause &&
                cause.code === "55P03"
              )
                break;
              throw error;
            }
            await new Promise((resolve) => setTimeout(resolve, 1));
          }
          if (failure === "cancelled")
            controller.abort(new Error("Initialization cancelled"));
          else
            await new Promise((resolve) =>
              setTimeout(
                resolve,
                Math.max(0, expiresAt.getTime() - Date.now()) + 25,
              ),
            );
        } finally {
          release.resolve(true);
          await blocker;
        }
        await expect(writing).rejects.toThrow(
          failure === "cancelled"
            ? "Initialization cancelled"
            : "Initialization lease expired",
        );
        expect(
          (
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, ANCHOR_RUN_ID),
            })
          )?.publicKey,
        ).toBeNull();
        expect(
          (await store.findById(allocation.id))?.initializationLeaseId,
        ).toBe(initialization.leaseId);
      });
    }

    test("fences replacement before binding a new physical sidecar", async () => {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      const pending = await store.createPending({
        id: "alloc-1",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      expect(pending.status).toBe("pending");
      expect(pending.generation).toBe(0);

      const initial = await store.bindInitialSidecar({
        allocationId: pending.id,
        expectedGeneration: 0,
        sidecarId: "sidecar-generation-1",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(0),
      });
      expect(initial?.status).toBe("provisioning");
      expect(initial?.generation).toBe(1);
      expect(
        await h.db.query.sidecar.findFirst({
          where: (row, { eq }) => eq(row.id, "sidecar-generation-1"),
          columns: { status: true },
        }),
      ).toEqual({ status: "offline" });

      const claimed = await store.claimNextReconcilable({
        leaseId: "lease-current",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.id).toBe(pending.id);
      const replacementAt = new Date(Date.now() + 300_000);
      const previousRefs = {
        credentialIds: ["cred-previous"],
        bindings: [
          {
            handle: "api",
            credentialId: "cred-previous",
            consumer: "tool:bundle-id",
          },
        ],
      };

      await h.db
        .update(workflowRun)
        .set({
          publicKey: "generation-1-public-key",
          credentialRefs: previousRefs,
        })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));

      expect(
        await store.beginReplacement({
          allocationId: pending.id,
          expectedStatus: "provisioning",
          expectedGeneration: 1,
          expectedLeaseId: "lease-stale",
          firstAttemptAt: replacementAt,
          nextAttemptAt: replacementAt,
          failureCode: "ensure_failed",
          failureMessage: "stale reconciler",
        }),
      ).toBeNull();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: (row, { eq }) => eq(row.id, ANCHOR_RUN_ID),
          columns: { publicKey: true, credentialRefs: true },
        }),
      ).toEqual({
        publicKey: "generation-1-public-key",
        credentialRefs: previousRefs,
      });

      const replacing = await store.beginReplacement({
        allocationId: pending.id,
        expectedStatus: "provisioning",
        expectedGeneration: 1,
        expectedLeaseId: "lease-current",
        firstAttemptAt: replacementAt,
        nextAttemptAt: replacementAt,
        failureCode: "ensure_failed",
        failureMessage: "provider request timed out",
      });
      expect(replacing?.status).toBe("replacing");
      expect(replacing?.generation).toBe(2);
      expect(replacing?.sidecarId).toBe("sidecar-generation-1");
      expect(replacing?.nextAttemptAt).toEqual(replacementAt);
      expect(
        await store.claimNextReconcilable({
          leaseId: "lease-too-early",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: (row, { eq }) => eq(row.id, ANCHOR_RUN_ID),
          columns: { publicKey: true, credentialRefs: true },
        }),
      ).toEqual({ publicKey: null, credentialRefs: null });

      await h.db
        .update(sidecarAllocation)
        .set({ deploymentCleanupConfirmed: true, providerHoldReleased: true })
        .where(eq(sidecarAllocation.id, pending.id));
      const replacement = await store.bindReplacementSidecar({
        allocationId: pending.id,
        generation: 2,
        sidecarId: "sidecar-generation-3",
        tokenHashSha256: new Uint8Array([4, 5, 6]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(Date.now() + 60_000),
      });
      expect(replacement?.status).toBe("provisioning");
      expect(replacement?.generation).toBe(3);
      expect(replacement?.sidecarId).toBe("sidecar-generation-3");
      expect(replacement?.externalRef).toBeUndefined();
      expect(replacement?.destroyAttempts).toBe(1);
      expect(
        await h.db.query.sidecar.findFirst({
          where: (row, { eq }) => eq(row.id, "sidecar-generation-3"),
          columns: { status: true },
        }),
      ).toEqual({ status: "offline" });

      expect(
        await store.markAllocated({
          allocationId: pending.id,
          generation: 1,
          externalRef: "stale",
        }),
      ).toBeNull();
    });

    test("never replaces an allocated worker", async () => {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      const pending = await store.createPending({
        id: "alloc-allocated",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      await store.bindInitialSidecar({
        allocationId: pending.id,
        expectedGeneration: 0,
        sidecarId: "sidecar-allocated",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(0),
      });
      await store.markAllocated({ allocationId: pending.id, generation: 1 });
      const claimed = await store.claimNextReconcilable({
        leaseId: "lease-current",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.status).toBe("allocated");

      expect(
        await store.beginReplacement({
          allocationId: pending.id,
          expectedStatus: "provisioning",
          expectedGeneration: 1,
          expectedLeaseId: "lease-current",
          firstAttemptAt: new Date(),
          nextAttemptAt: new Date(),
          failureCode: "sidecar_connect_failed",
          failureMessage: "connect timeout",
        }),
      ).toBeNull();
      expect(await store.findById(pending.id)).toMatchObject({
        status: "allocated",
        generation: 1,
      });
    });

    test("requires a launch specification before allocating capacity", async () => {
      await seedWorkflowRun(h.db, {
        id: "anchor-without-spec",
        anchorRunId: "anchor-without-spec",
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
      const store = createSidecarAllocationStore(h.db);
      await expect(
        store.createPending({
          id: "alloc-without-spec",
          anchorRunId: "anchor-without-spec",
          tenantId: TENANT_ID,
          provisionerId: "ec2-spot",
          provisionerApiVersion: 2,
          provisionerBindingFingerprint: "ec2-spot:test",
          adapterManifestDigest: MANIFEST_DIGEST,
          maxDisconnectedMs: 900_000,
        }),
      ).rejects.toThrow(/has no launch specification/);
    });

    test("leases due allocations for one reconciler at a time", async () => {
      const store = createSidecarAllocationStore(h.db);
      await store.createPending({
        id: "alloc-claim",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });

      const first = await store.claimNextReconcilable({
        leaseId: "lease-1",
        leaseDurationMs: 60_000,
      });
      expect(first?.id).toBe("alloc-claim");
      expect(first?.reconciliationLeaseId).toBe("lease-1");
      expect(
        await store.claimNextReconcilable({
          leaseId: "lease-2",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
    });

    test("timed-out claims leave database capacity for unrelated queries", async () => {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      const locked = Promise.withResolvers<boolean>();
      const unlock = Promise.withResolvers<boolean>();
      const claims: ReturnType<typeof store.claimNextReconcilable>[] = [];
      const reconciler = createSidecarAllocationReconciler({
        allocationStore: {
          ...store,
          claimNextReconcilable(args) {
            const claim = store.claimNextReconcilable(args);
            claims.push(claim);
            return claim;
          },
        },
        plugins: createSidecarPluginRegistry({ provisioners: [] }),
        router: createSidecarRouter({
          withExecutableWorkflowRun: async (_target, send) => send(),
          authenticateSidecar: async () => null,
          validateSidecarIdentity: async () => false,
          resolveSidecarBindings: async () => [],
        }),
        hubWebSocketUrl: "ws://localhost",
        operationTimeoutMs: 20,
        maxConcurrentClaims: 1,
      });
      const blocker = h.db.transaction(async (tx) => {
        await tx.execute(
          sql`lock table sidecar_allocation in access exclusive mode`,
        );
        locked.resolve(true);
        await unlock.promise;
      });
      try {
        await locked.promise;
        await expect(reconciler.reconcileNext()).rejects.toThrow(
          "Sidecar allocation claim timed out",
        );
        for (let attempt = 0; attempt < 12; attempt += 1) {
          expect(await reconciler.reconcileNext()).toBe(false);
        }
        expect(claims).toHaveLength(1);
        // This uses the same ten-connection pool while the allocation table
        // remains locked. Abandoned claims must not consume the entire pool.
        const [unrelated] = await h.db.execute(sql`select 1 as value`);
        expect(unrelated?.["value"]).toBe(1);
      } finally {
        unlock.resolve(true);
        await blocker;
        await Promise.allSettled(claims);
      }
      expect(await reconciler.reconcileNext()).toBe(false);
      expect(claims).toHaveLength(2);
    });

    test("abandoned recovery reads leave database capacity for unrelated queries", async () => {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      for (let index = 0; index < 10; index += 1) {
        const id = `recovery-capacity-${String(index)}`;
        await seedWorkflowRun(h.db, {
          id,
          anchorRunId: id,
          tenantId: TENANT_ID,
          definitionId: DEFINITION_ID,
        });
        await h.db.insert(sidecar).values({
          id,
          tokenHashSha256: new Uint8Array(32).fill(index),
          provisionerId: "test",
          provisionerApiVersion: 2,
          provisionerBindingFingerprint: "test:v1",
          adapterManifestDigest: MANIFEST_DIGEST,
        });
        await store.createAdopted({
          id,
          anchorRunId: id,
          tenantId: TENANT_ID,
          provisionerId: "test",
          provisionerApiVersion: 2,
          provisionerBindingFingerprint: "test:v1",
          adapterManifestDigest: MANIFEST_DIGEST,
          maxDisconnectedMs: 900_000,
          sidecarId: id,
          generation: 1,
          connectDeadline: new Date(Date.now() + 60_000),
        });
      }
      const router = createSidecarRouter({
        withExecutableWorkflowRun: async (_target, send) => send(),
        authenticateSidecar: async () => null,
        validateSidecarIdentity: async () => false,
        resolveSidecarBindings: async () => [],
      });
      const recoveries: Promise<void>[] = [];
      const reconciler = createSidecarAllocationReconciler({
        allocationStore: store,
        plugins: createSidecarPluginRegistry({ provisioners: [] }),
        router,
        hubWebSocketUrl: "ws://localhost",
        operationTimeoutMs: 100,
        maxConcurrentClaims: 8,
        onInitializationRecovery(allocation, reconciliation) {
          const recovery = recoverSenderDeploy({
            db: h.db,
            sidecarRouter: router,
            allocation,
            reconciliation,
          });
          recoveries.push(recovery);
          return recovery;
        },
      });
      const locked = Promise.withResolvers<boolean>();
      const unlock = Promise.withResolvers<boolean>();
      const blocker = h.db.transaction(async (tx) => {
        await tx.execute(sql`lock table workflow_run in access exclusive mode`);
        locked.resolve(true);
        await unlock.promise;
      });
      try {
        await locked.promise;
        for (let attempt = 0; attempt < 8; attempt += 1) {
          expect(await reconciler.reconcileNext()).toBe(true);
        }
        for (let attempt = 0; attempt < 12; attempt += 1) {
          expect(await reconciler.reconcileNext()).toBe(false);
        }
        expect(recoveries).toHaveLength(8);
        // Eight blocked reads and the lock holder leave one pool connection.
        const [unrelated] = await h.db.execute(sql`select 1 as value`);
        expect(unrelated?.["value"]).toBe(1);
      } finally {
        unlock.resolve(true);
        await blocker;
        await Promise.allSettled(recoveries);
      }
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(recoveries).toHaveLength(9);
    });

    test("excludes due allocations without claiming or changing them", async () => {
      const secondAnchorRunId = "anchor-exclusion-second";
      await seedWorkflowRun(h.db, {
        id: secondAnchorRunId,
        anchorRunId: secondAnchorRunId,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
      const launchSpecStore = createWorkflowRunLaunchSpecStore(h.db);
      const launchSpec = await launchSpecStore.get(ANCHOR_RUN_ID);
      if (launchSpec === null) throw new Error("Expected launch specification");
      await launchSpecStore.create({
        ...launchSpec,
        anchorRunId: secondAnchorRunId,
        sessionId: "ses-exclusion-second",
      });
      const store = createSidecarAllocationStore(h.db);
      const common = {
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      } as const;
      const first = await store.createPending({
        ...common,
        id: "alloc-excluded",
        anchorRunId: ANCHOR_RUN_ID,
        now: new Date(0),
      });
      const second = await store.createPending({
        ...common,
        id: "alloc-eligible",
        anchorRunId: secondAnchorRunId,
        now: new Date(1),
      });

      expect(
        await store.claimNextReconcilable({
          excludedAllocationIds: [first.id],
          leaseId: "lease-eligible",
          leaseDurationMs: 60_000,
        }),
      ).toMatchObject({
        id: second.id,
        reconciliationLeaseId: "lease-eligible",
      });
      expect(await store.findById(first.id)).toEqual(first);
      expect(
        await store.claimNextReconcilable({
          excludedAllocationIds: [first.id],
          leaseId: "lease-none",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
      expect(
        await store.claimNextReconcilable({
          leaseId: "lease-previously-excluded",
          leaseDurationMs: 60_000,
        }),
      ).toMatchObject({
        id: first.id,
        reconciliationLeaseId: "lease-previously-excluded",
      });
    });

    test("parks an unscheduled allocation at a fenced fallback retry", async () => {
      const store = createSidecarAllocationStore(h.db);
      await store.createPending({
        id: "alloc-park-fallback",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      await store.claimNextReconcilable({
        leaseId: "lease-park-fallback",
        leaseDurationMs: 60_000,
      });
      const fallbackNextAttemptAt = new Date(Date.now() + 300_000);

      expect(
        await store.parkReconciliation("alloc-park-fallback", "lease-stale", {
          kind: "retry-after-error",
          notBefore: fallbackNextAttemptAt,
        }),
      ).toBe(false);
      expect(
        await store.parkReconciliation(
          "alloc-park-fallback",
          "lease-park-fallback",
          {
            kind: "retry-after-error",
            notBefore: fallbackNextAttemptAt,
          },
        ),
      ).toBe(true);

      const parked = await store.findById("alloc-park-fallback");
      expect(parked?.nextAttemptAt).toEqual(fallbackNextAttemptAt);
      expect(parked?.reconciliationLeaseId).toBeUndefined();
      expect(parked?.reconciliationLeaseExpiresAt).toBeUndefined();
      expect(
        await store.claimNextReconcilable({
          leaseId: "lease-too-early",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
    });

    test("parking preserves a reconnect wake that races an accepted worker", async () => {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      await store.createPending({
        id: "alloc-park-wake",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      await store.claimNextReconcilable({
        leaseId: "lease-park-wake",
        leaseDurationMs: 60_000,
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-park-wake",
        expectedGeneration: 0,
        sidecarId: "sidecar-park-wake",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(Date.now() + 60_000),
        expectedLeaseId: "lease-park-wake",
      });
      await store.markAllocated({
        allocationId: "alloc-park-wake",
        generation: 1,
        expectedLeaseId: "lease-park-wake",
      });
      await store.wakeReconciliation("alloc-park-wake", 1);

      expect(
        await store.parkReconciliation("alloc-park-wake", "lease-park-wake", {
          kind: "await-connection",
          fallbackNextAttemptAt: new Date(Date.now() + 300_000),
        }),
      ).toBe(true);

      const claimed = await store.claimNextReconcilable({
        leaseId: "lease-after-wake",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.id).toBe("alloc-park-wake");
      expect(claimed?.reconciliationLeaseId).toBe("lease-after-wake");
    });

    test("parking after an error floors an allocated retry at the backoff", async () => {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      await store.createPending({
        id: "alloc-park-backoff",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-park-backoff",
        expectedGeneration: 0,
        sidecarId: "sidecar-park-backoff",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(Date.now() + 60_000),
      });
      await store.markAllocated({
        allocationId: "alloc-park-backoff",
        generation: 1,
      });
      await store.wakeReconciliation("alloc-park-backoff", 1);
      await store.claimNextReconcilable({
        leaseId: "lease-park-backoff",
        leaseDurationMs: 60_000,
      });
      const notBefore = new Date(Date.now() + 300_000);

      expect(
        await store.parkReconciliation(
          "alloc-park-backoff",
          "lease-park-backoff",
          { kind: "retry-after-error", notBefore },
        ),
      ).toBe(true);

      const parked = await store.findById("alloc-park-backoff");
      expect(parked?.nextAttemptAt).toEqual(notBefore);
      expect(
        await store.claimNextReconcilable({
          leaseId: "lease-before-backoff",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();

      await store.wakeReconciliation("alloc-park-backoff", 1);
      const reclaimed = await store.claimNextReconcilable({
        leaseId: "lease-after-backoff-wake",
        leaseDurationMs: 60_000,
      });
      expect(reclaimed?.id).toBe("alloc-park-backoff");
      expect(reclaimed?.reconciliationLeaseId).toBe("lease-after-backoff-wake");
    });

    test("holds the reconciliation lease until the accepted worker connects", async () => {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      await store.createPending({
        id: "alloc-ready",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      const claimed = await store.claimNextReconcilable({
        leaseId: "lease-ready",
        leaseDurationMs: 60_000,
      });
      expect(claimed).not.toBeNull();
      const provisioning = await store.bindInitialSidecar({
        allocationId: "alloc-ready",
        expectedGeneration: 0,
        sidecarId: "sidecar-ready",
        tokenHashSha256: new Uint8Array([7, 8, 9]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(Date.now() + 60_000),
        expectedLeaseId: "lease-ready",
      });
      expect(provisioning).not.toBeNull();
      const accepted = await store.markAllocated({
        allocationId: "alloc-ready",
        generation: 1,
        expectedLeaseId: "lease-ready",
      });
      expect(accepted?.reconciliationLeaseId).toBe("lease-ready");
      expect(
        await store.extendReconciliationLease(
          "alloc-ready",
          "lease-ready",
          60_000,
        ),
      ).toBe(true);

      const ready = await store.markConnectionReady({
        allocationId: "alloc-ready",
        generation: 1,
        expectedLeaseId: "lease-ready",
      });

      expect(ready?.status).toBe("allocated");
      expect(ready?.reconciliationLeaseId).toBeUndefined();
      expect(ready?.connectDeadline).toBeUndefined();
      expect(ready?.nextAttemptAt).toBeUndefined();
    });

    test("a lease validation failure preserves its schedule and retries after lease expiry", async () => {
      const { store, allocation } = await createClaimedAllocation(
        "alloc-lease-query-failure",
      );
      const due = new Date(0);
      await h.db
        .update(sidecarAllocation)
        .set({ reconciliationLeaseExpiresAt: due, nextAttemptAt: due })
        .where(eq(sidecarAllocation.id, allocation.id));
      let validations = 0;
      let initializations = 0;
      let leases = 0;
      const reconciler = createSidecarAllocationReconciler({
        allocationStore: {
          ...store,
          isReconciliationLeaseCurrent: async (...args) => {
            if (++validations === 1)
              throw new Error("Database connection failed during validation");
            return store.isReconciliationLeaseCurrent(...args);
          },
        },
        plugins: createSidecarPluginRegistry({
          provisioners: [
            {
              id: "ec2-spot",
              apiVersion: 2,
              bindingFingerprint: "ec2-spot:test",
              capabilities: [],
              async ensure() {
                throw new Error("Must not reprovision the connected worker");
              },
              async destroy() {
                throw new Error("Must not destroy the connected worker");
              },
            },
          ],
        }),
        router: {
          getCleanupConnection: () => undefined,
          waitForSidecarInventory: async () => undefined,
          undeployAllocation: async () => undefined,
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          isAllocatedSidecarReady: async () => true,
          holdsAllocatedBinding: () => true,
          waitForAllocatedSidecar: async () => undefined,
          reportedDeploymentFailure: () => undefined,
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "ws://localhost/unused",
        createLeaseId: () => `recovery-${String(++leases)}`,
        onReady: async () => {
          initializations += 1;
        },
      });

      expect(await reconciler.reconcileNext()).toBe(true);
      expect(initializations).toBe(0);
      expect(await store.findById(allocation.id)).toMatchObject({
        status: "allocated",
        generation: allocation.generation,
        nextAttemptAt: due,
        reconciliationLeaseId: "recovery-1",
      });
      expect(await reconciler.reconcileNext()).toBe(false);

      // Expire only the lease: no retry write repairs or changes the schedule.
      await h.db
        .update(sidecarAllocation)
        .set({ reconciliationLeaseExpiresAt: due })
        .where(eq(sidecarAllocation.id, allocation.id));
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(initializations).toBe(1);
      const ready = await store.findById(allocation.id);
      expect(ready?.status).toBe("allocated");
      expect(ready?.generation).toBe(allocation.generation);
      expect(ready?.reconciliationLeaseId).toBeUndefined();
      expect(ready?.nextAttemptAt).toBeUndefined();
    });

    test("expired owners cannot renew or commit lifecycle transitions", async () => {
      const { store, allocation, leaseId } = await createClaimedAllocation(
        "alloc-expired-owner",
      );
      expect(
        await store.isReconciliationLeaseCurrent(
          allocation.id,
          allocation.generation,
          leaseId,
        ),
      ).toBe(true);
      await h.db
        .update(sidecarAllocation)
        .set({
          reconciliationLeaseExpiresAt: new Date(0),
          nextAttemptAt: new Date(0),
        })
        .where(eq(sidecarAllocation.id, allocation.id));
      expect(
        await store.isReconciliationLeaseCurrent(
          allocation.id,
          allocation.generation,
          leaseId,
        ),
      ).toBe(false);
      expect(
        await store.extendReconciliationLease(allocation.id, leaseId, 60_000),
      ).toBe(false);
      expect(
        await store.markConnectionReady({
          allocationId: allocation.id,
          generation: allocation.generation,
          expectedLeaseId: leaseId,
        }),
      ).toBeNull();
      expect(
        await store.scheduleRetry({
          allocationId: allocation.id,
          expectedStatus: "allocated",
          expectedGeneration: allocation.generation,
          expectedLeaseId: leaseId,
          nextAttemptAt: new Date(Date.now() + 300_000),
        }),
      ).toBeNull();
      expect(
        await store.parkReconciliation(allocation.id, leaseId, {
          kind: "retry-after-error",
          notBefore: new Date(Date.now() + 300_000),
        }),
      ).toBe(false);
      expect(
        await store.beginUnrecoverableRelease({
          expectedStatus: "allocated",
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedLeaseId: leaseId,
          failureCode: "stale_initializer",
          failureMessage: "The initializer lost its lease",
        }),
      ).toBeNull();

      const claimed = await store.claimNextReconcilable({
        leaseId: "replacement-owner",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.id).toBe(allocation.id);
      expect(claimed?.generation).toBe(allocation.generation);
      expect(claimed?.reconciliationLeaseId).toBe("replacement-owner");
    });

    test.each([false, true])(
      "reconnect clears only a completed deployment's disconnect window (initialized=%s)",
      async (initialized) => {
        const { store, allocation, initialization } =
          await createClaimedAllocation("alloc-reconnect-release");
        if (initialized) {
          expect(await store.beginInitialization(initialization)).toBe(true);
          expect(
            await store.completeInitialization({
              ...initialization,
              publicKey: "key",
            }),
          ).toBe(true);
        }
        const disconnectedAt = new Date();
        await store.markConnectionLost({
          allocationId: allocation.id,
          generation: allocation.generation,
          now: disconnectedAt,
          firstConnectDeadline: new Date(disconnectedAt.getTime() + 120_000),
        });
        const oldDeadline = (await store.findById(allocation.id))
          ?.connectDeadline;
        expect(oldDeadline).toBeDefined();
        await store.wakeReconciliation(
          allocation.id,
          allocation.generation + 1,
          { connected: true },
        );
        expect((await store.findById(allocation.id))?.connectDeadline).toEqual(
          oldDeadline,
        );
        await store.wakeReconciliation(allocation.id, allocation.generation, {
          connected: true,
        });
        expect((await store.findById(allocation.id))?.connectDeadline).toEqual(
          initialized ? undefined : oldDeadline,
        );

        // Lifecycle releases before the readiness pass. A subsequent drop must
        // get a full window, not inherit the pre-reconnect deadline.
        expect(
          await store.beginRelease({
            allocationId: allocation.id,
            expectedStatus: "allocated",
            expectedGeneration: allocation.generation,
          }),
        ).not.toBeNull();
        const leaseId = "cleanup-owner";
        await store.claimNextReconcilable({ leaseId, leaseDurationMs: 60_000 });
        const droppedAt = new Date(disconnectedAt.getTime() + 60_000);
        expect(
          await store.recordCleanupConnection({
            allocationId: allocation.id,
            generation: allocation.generation + 1,
            expectedLeaseId: leaseId,
            connected: false,
            now: droppedAt,
          }),
        ).toMatchObject({
          connectDeadline: new Date(
            droppedAt.getTime() + allocation.maxDisconnectedMs,
          ),
        });
      },
    );

    test.each([
      { status: "releasing", connected: true, destroyAttempts: 1 },
      { status: "releasing", connected: false, destroyAttempts: 1 },
      { status: "replacing", connected: true, destroyAttempts: 1 },
      { status: "replacing", connected: false, destroyAttempts: 1 },
      { status: "replacing", connected: true, destroyAttempts: 0 },
      { status: "replacing", connected: false, destroyAttempts: 0 },
    ] as const)(
      "cleanup and replacement backoff survive reconnects, but parked waits wake immediately: %j",
      async ({ status, connected, destroyAttempts }) => {
        const { store, allocation } = await createClaimedAllocation(
          "alloc-cleanup-reconnect-backoff",
        );
        const retryAt = new Date(Date.now() + 60_000);
        await h.db
          .update(sidecarAllocation)
          .set({
            status,
            destroyAttempts,
            connectDeadline: null,
            nextAttemptAt: retryAt,
            reconciliationLeaseId: null,
            reconciliationLeaseExpiresAt: null,
          })
          .where(eq(sidecarAllocation.id, allocation.id));
        // A reconnect may close again before its queued database write runs.
        const wake = () =>
          store.wakeReconciliation(allocation.id, allocation.generation, {
            connected,
            preserveRetryBackoff: true,
          });
        for (let reconnect = 0; reconnect < 10; reconnect++) {
          expect(await wake()).toBe(true);
          expect(await store.findById(allocation.id)).toMatchObject({
            status,
            destroyAttempts,
          });
          expect((await store.findById(allocation.id))?.nextAttemptAt).toEqual(
            retryAt,
          );
          expect(
            await store.claimNextReconcilable({
              leaseId: "flapping-connection",
              leaseDurationMs: 60_000,
            }),
          ).toBeNull();
        }
        const deadline = new Date(Date.now() + 900_000);
        await h.db
          .update(sidecarAllocation)
          .set({ connectDeadline: deadline, nextAttemptAt: deadline })
          .where(eq(sidecarAllocation.id, allocation.id));
        expect(await wake()).toBe(true);
        expect(
          await store.claimNextReconcilable({
            leaseId: "reconnected-cleanup",
            leaseDurationMs: 60_000,
          }),
        ).toMatchObject({ id: allocation.id, status, destroyAttempts });
        expect((await store.findById(allocation.id))?.connectDeadline).toEqual(
          connected ? undefined : deadline,
        );
      },
    );

    test("a disconnect after reconnect starts a fresh deadline that ordinary wakes preserve", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-new-disconnect");
      await store.beginInitialization(initialization);
      await store.completeInitialization({
        ...initialization,
        publicKey: "key",
      });
      const firstDrop = new Date();
      await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        now: firstDrop,
        firstConnectDeadline: firstDrop,
      });
      await store.wakeReconciliation(allocation.id, allocation.generation, {
        connected: true,
      });
      expect(
        (await store.findById(allocation.id))?.connectDeadline,
      ).toBeUndefined();
      const droppedAt = new Date(firstDrop.getTime() + 60_000);
      await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        now: droppedAt,
        firstConnectDeadline: droppedAt,
      });
      await store.wakeReconciliation(allocation.id, allocation.generation);
      expect((await store.findById(allocation.id))?.connectDeadline).toEqual(
        new Date(droppedAt.getTime() + allocation.maxDisconnectedMs),
      );
    });

    test("an inventory retry waits on the database clock and can still be woken", async () => {
      const { store, allocation, leaseId } =
        await createClaimedAllocation("alloc-clock-skew");
      const [before] = await h.db
        .select({
          nowMs:
            sql<number>`extract(epoch from clock_timestamp()) * 1000`.mapWith(
              Number,
            ),
        })
        .from(sidecarAllocation)
        .where(eq(sidecarAllocation.id, allocation.id));
      if (before === undefined) throw new Error("Expected database time");
      const scheduled = await store.scheduleRetry({
        allocationId: allocation.id,
        expectedStatus: "allocated",
        expectedGeneration: allocation.generation,
        expectedLeaseId: leaseId,
        nextAttemptAt: new Date(0),
        minimumDelayMs: 120_000,
        now: new Date(0),
      });
      if (scheduled?.nextAttemptAt === undefined)
        throw new Error("Expected a scheduled retry");
      expect(scheduled.nextAttemptAt.getTime()).toBeGreaterThanOrEqual(
        Math.floor(before.nowMs) + 120_000,
      );
      expect(
        await store.claimNextReconcilable({
          leaseId: "early-claim",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
      await store.wakeReconciliation(allocation.id, allocation.generation);
      expect(
        (
          await store.claimNextReconcilable({
            leaseId: "inventory-ready",
            leaseDurationMs: 60_000,
          })
        )?.id,
      ).toBe(allocation.id);
    });

    test("keeps when a first deploy first failed across its retries", async () => {
      const { store, allocation, leaseId } = await createClaimedAllocation(
        "alloc-first-deploy-failure",
      );
      const firstFailure = new Date(Date.now() - 60_000);
      const retried = await store.scheduleRetry({
        allocationId: allocation.id,
        expectedStatus: "allocated",
        expectedGeneration: allocation.generation,
        expectedLeaseId: leaseId,
        nextAttemptAt: new Date(Date.now() + 30_000),
        failure: {
          code: "sidecar_initialization_failed",
          message: "catalog temporarily unavailable",
        },
        firstDeployFailedAt: firstFailure,
      });
      expect(retried?.firstDeployFailedAt).toEqual(firstFailure);
      expect(retried?.failureMessage).toBe("catalog temporarily unavailable");

      // Later claims, retries that do not name it, and a lost connection all
      // leave it in place: the limit counts from the first failure.
      await store.wakeReconciliation(allocation.id, allocation.generation);
      const reclaimed = await store.claimNextReconcilable({
        leaseId: "lease-retry",
        leaseDurationMs: 60_000,
      });
      expect(reclaimed?.firstDeployFailedAt).toEqual(firstFailure);
      await store.scheduleRetry({
        allocationId: allocation.id,
        expectedStatus: "allocated",
        expectedGeneration: allocation.generation,
        expectedLeaseId: "lease-retry",
        nextAttemptAt: new Date(Date.now() + 30_000),
      });
      await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        firstConnectDeadline: new Date(Date.now() + 120_000),
      });
      expect(
        (await store.findById(allocation.id))?.firstDeployFailedAt,
      ).toEqual(firstFailure);
    });

    test("a ready pass clears a first-deploy retry's reason", async () => {
      const retrying = await createClaimedAllocation("alloc-ready-retry");
      const scheduled = await retrying.store.scheduleRetry({
        allocationId: retrying.allocation.id,
        expectedStatus: "allocated",
        expectedGeneration: retrying.allocation.generation,
        expectedLeaseId: retrying.leaseId,
        nextAttemptAt: new Date(0),
        failure: {
          code: "sidecar_initialization_failed",
          message: "catalog temporarily unavailable",
        },
      });
      expect(scheduled?.failureCode).toBe("sidecar_initialization_failed");
      await retrying.store.claimNextReconcilable({
        leaseId: "lease-retry-ready",
        leaseDurationMs: 60_000,
      });
      const recovered = await retrying.store.markConnectionReady({
        allocationId: retrying.allocation.id,
        generation: retrying.allocation.generation,
        expectedLeaseId: "lease-retry-ready",
      });
      if (recovered === null)
        throw new Error("expected the ready pass to land");
      expect(recovered.failureCode).toBeUndefined();
      expect(recovered.failureMessage).toBeUndefined();
    });

    test("disconnect invalidates the lease before an old initializer can finish", async () => {
      const { store, allocation, leaseId } = await createClaimedAllocation(
        "alloc-disconnected-owner",
      );
      const lostAt = new Date();
      const deadline = new Date(lostAt.getTime() + 120_000);
      const disconnected = await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        now: lostAt,
        firstConnectDeadline: deadline,
      });
      expect(disconnected?.reconciliationLeaseId).toBeUndefined();
      expect(
        await store.markConnectionReady({
          allocationId: allocation.id,
          generation: allocation.generation,
          expectedLeaseId: leaseId,
        }),
      ).toBeNull();
      expect(
        await store.scheduleRetry({
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedStatus: "allocated",
          expectedLeaseId: leaseId,
          nextAttemptAt: new Date(0),
        }),
      ).toBeNull();
      expect(
        await store.parkReconciliation(allocation.id, leaseId, {
          kind: "retry-after-error",
          notBefore: new Date(0),
        }),
      ).toBe(false);
      const stored = await store.findById(allocation.id);
      expect(stored?.connectDeadline).toEqual(deadline);
      expect(stored?.nextAttemptAt).toEqual(deadline);
    });

    test("gives the accepted generation its allocation's disconnect limit", async () => {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      await store.createPending({
        id: "alloc-reconnect",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 120_000,
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-reconnect",
        expectedGeneration: 0,
        sidecarId: "sidecar-reconnect",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(Date.now() + 60_000),
      });
      await store.markAllocated({
        allocationId: "alloc-reconnect",
        generation: 1,
      });
      await store.markConnectionReady({
        allocationId: "alloc-reconnect",
        generation: 1,
      });

      const lostAt = new Date();
      const firstConnectDeadline = new Date(lostAt.getTime() + 30_000);
      expect(
        await store.markConnectionLost({
          allocationId: "alloc-reconnect",
          generation: 0,
          now: lostAt,
          firstConnectDeadline,
        }),
      ).toBeNull();

      // Before its first deploy completes, it gets only the first-connect
      // window.
      const deploying = await store.markConnectionLost({
        allocationId: "alloc-reconnect",
        generation: 1,
        now: lostAt,
        firstConnectDeadline,
      });
      expect(deploying?.connectDeadline).toEqual(firstConnectDeadline);
      expect(deploying?.nextAttemptAt).toEqual(firstConnectDeadline);

      await h.db
        .update(workflowRun)
        .set({ publicKey: "committed-key" })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));
      const deadline = new Date(lostAt.getTime() + 120_000);
      const disconnected = await store.markConnectionLost({
        allocationId: "alloc-reconnect",
        generation: 1,
        now: lostAt,
        firstConnectDeadline,
      });

      expect(disconnected?.connectDeadline).toEqual(deadline);
      expect(disconnected?.nextAttemptAt).toEqual(deadline);

      // A later loss restarts the limit.
      const restartedAt = new Date(lostAt.getTime() + 600_000);
      const restarted = await store.markConnectionLost({
        allocationId: "alloc-reconnect",
        generation: 1,
        now: restartedAt,
        firstConnectDeadline: new Date(restartedAt.getTime() + 30_000),
      });
      expect(restarted?.connectDeadline).toEqual(
        new Date(restartedAt.getTime() + 120_000),
      );
    });

    test("at Hub start gives the first-connect window until the first deploy completes, then the disconnect limit", async () => {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      await store.createPending({
        id: "alloc-hub-start",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 600_000,
      });
      const stored = new Date(Date.now() + 60_000);
      await store.bindInitialSidecar({
        allocationId: "alloc-hub-start",
        expectedGeneration: 0,
        sidecarId: "sidecar-hub-start",
        tokenHashSha256: new Uint8Array([7, 8, 9]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: stored,
      });
      await store.markAllocated({
        allocationId: "alloc-hub-start",
        generation: 1,
      });
      const startedAt = new Date();
      const target = { allocationId: "alloc-hub-start", generation: 1 };

      // Its sidecar could not connect while the Hub was down, so it gets a
      // fresh first-connect window from Hub start.
      const fresh = new Date(startedAt.getTime() + 120_000);
      const waiting = await store.scheduleReconnectAfterHubStart({
        ...target,
        now: startedAt,
        firstConnectDeadline: fresh,
      });
      expect(waiting?.connectDeadline).toEqual(fresh);
      expect(waiting?.nextAttemptAt).toEqual(fresh);

      // A later deadline it already has is kept.
      const kept = await store.scheduleReconnectAfterHubStart({
        ...target,
        now: startedAt,
        firstConnectDeadline: new Date(startedAt.getTime() + 30_000),
      });
      expect(kept?.connectDeadline).toEqual(fresh);

      // Once its first deploy completed, it gets its whole disconnect limit.
      await h.db
        .update(workflowRun)
        .set({ publicKey: "committed-key" })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));
      const running = await store.scheduleReconnectAfterHubStart({
        ...target,
        now: startedAt,
        firstConnectDeadline: fresh,
      });
      const restarted = new Date(startedAt.getTime() + 600_000);
      expect(running?.connectDeadline).toEqual(restarted);
      expect(running?.nextAttemptAt).toEqual(restarted);
    });

    test("repairs a disconnect deadline only while an allocation remains unscheduled and unleased", async () => {
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      await store.createPending({
        id: "alloc-repair",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 120_000,
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-repair",
        expectedGeneration: 0,
        sidecarId: "sidecar-repair",
        tokenHashSha256: new Uint8Array([4, 5, 6]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(Date.now() + 60_000),
      });
      await store.markAllocated({
        allocationId: "alloc-repair",
        generation: 1,
      });
      await store.markConnectionReady({
        allocationId: "alloc-repair",
        generation: 1,
      });
      await h.db
        .update(workflowRun)
        .set({ publicKey: "committed-key" })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));

      const repairedAt = new Date();
      const deadline = new Date(repairedAt.getTime() + 120_000);
      const firstConnectDeadline = new Date(repairedAt.getTime() + 30_000);
      expect(
        await store.scheduleReconnectIfUnscheduled({
          allocationId: "alloc-repair",
          generation: 0,
          now: repairedAt,
          firstConnectDeadline,
        }),
      ).toBeNull();
      const repaired = await store.scheduleReconnectIfUnscheduled({
        allocationId: "alloc-repair",
        generation: 1,
        now: repairedAt,
        firstConnectDeadline,
      });

      expect(repaired?.connectDeadline).toEqual(deadline);
      expect(repaired?.nextAttemptAt).toEqual(deadline);

      expect(
        await store.scheduleReconnectIfUnscheduled({
          allocationId: "alloc-repair",
          generation: 1,
          now: new Date(deadline.getTime() + 120_000),
          firstConnectDeadline,
        }),
      ).toBeNull();
      const scheduled = await store.findById("alloc-repair");
      expect(scheduled?.connectDeadline).toEqual(deadline);
      expect(scheduled?.nextAttemptAt).toEqual(deadline);

      await store.wakeReconciliation("alloc-repair", 1);
      const claimed = await store.claimNextReconcilable({
        leaseId: "lease-repair",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.id).toBe("alloc-repair");

      expect(
        await store.scheduleReconnectIfUnscheduled({
          allocationId: "alloc-repair",
          generation: 1,
          now: new Date(deadline.getTime() + 120_000),
          firstConnectDeadline,
        }),
      ).toBeNull();
      const leased = await store.findById("alloc-repair");
      expect(leased?.reconciliationLeaseId).toBe("lease-repair");
      expect(leased?.connectDeadline).toEqual(deadline);
      expect(leased?.nextAttemptAt?.getTime()).toBeLessThanOrEqual(Date.now());
    });

    test("confirmed release abandons outstanding dispatches while history is pending", async () => {
      const { store, allocation, leaseId } =
        await createClaimedAllocation("alloc-release");
      const dispatches = createWorkflowRunDispatchStore(h.db);
      for (const id of ["pending", "acknowledged", "settled", "failed"]) {
        await dispatches.enqueue({
          id,
          anchorRunId: ANCHOR_RUN_ID,
          messageId: id,
          senderAddress: "sender@tenant.example",
          rawMessage: new Uint8Array([1]),
          stepGrants: [],
        });
      }
      expect(
        await dispatches.acknowledge({
          allocationId: allocation.id,
          anchorRunId: ANCHOR_RUN_ID,
          messageId: "acknowledged",
          generation: allocation.generation,
        }),
      ).toMatchObject({ status: "acknowledged" });
      const settled = await dispatches.settle(ANCHOR_RUN_ID, "settled");
      const failed = await dispatches.fail({
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "failed",
        code: "malformed_mail",
        message: "Missing MIME bytes",
      });
      await h.db.insert(workflowPendingProjection).values({
        id: "pending-release-history",
        anchorRunId: ANCHOR_RUN_ID,
      });
      expect(
        await store.beginRelease({
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedStatus: "allocated",
          expectedLeaseId: leaseId,
        }),
      ).toMatchObject({ status: "releasing", generation: 2 });
      const releaseLeaseId = "lease-release";
      expect(
        await store.claimNextReconcilable({
          leaseId: releaseLeaseId,
          leaseDurationMs: 60_000,
        }),
      ).toMatchObject({ id: allocation.id, status: "releasing" });
      const now = new Date();
      const releaseArgs = {
        allocationId: allocation.id,
        generation: 2,
        expectedLeaseId: releaseLeaseId,
        now,
      };
      expect(await store.confirmDeploymentCleanup(releaseArgs)).toBe(true);
      expect(await store.confirmProviderRelease(releaseArgs)).toBe(true);
      expect(
        await store.markReleased({ ...releaseArgs, generation: 1 }),
      ).toBeNull();
      expect(
        await store.markReleased({ ...releaseArgs, expectedLeaseId: "stale" }),
      ).toBeNull();
      expect((await dispatches.findById("pending"))?.status).toBe("pending");
      expect((await dispatches.findById("acknowledged"))?.status).toBe(
        "acknowledged",
      );

      expect(await store.markReleased(releaseArgs)).toMatchObject({
        status: "released",
        destroyAttempts: 1,
      });
      expect(
        await store.markReleased({
          ...releaseArgs,
          now: new Date(now.getTime() + 1_000),
        }),
      ).toBeNull();
      for (const id of ["pending", "acknowledged"]) {
        expect(await dispatches.findById(id)).toMatchObject({
          status: "abandoned",
          failureCode: "workflow_capacity_released",
          nextAttemptAt: now,
          updatedAt: now,
        });
      }
      expect(await dispatches.findById("settled")).toEqual(settled);
      expect(await dispatches.findById("failed")).toEqual(failed);
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, ANCHOR_RUN_ID),
          columns: { status: true, endedAt: true },
        }),
      ).toEqual({ status: "running", endedAt: null });
      expect(
        await h.db.query.workflowPendingProjection.findFirst({
          where: eq(workflowPendingProjection.id, "pending-release-history"),
        }),
      ).toBeDefined();
    });

    test("terminal allocation failure fails runs, principals, and dispatches", async () => {
      await seedPrincipal(h.db, {
        id: "prn-terminal-run",
        tenantId: TENANT_ID,
        kind: "workflow",
        refId: "run-terminal",
        status: "active",
      });
      await seedWorkflowRun(h.db, {
        id: "run-terminal",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
        principalId: "prn-terminal-run",
      });
      await seedRunToken(h.db, {
        id: "rtk_terminal_run",
        principalId: "prn-terminal-run",
      });
      const store = createSidecarAllocationStore(h.db);
      const dispatchStore = createWorkflowRunDispatchStore(h.db);
      await dispatchStore.enqueue({
        id: "dispatch-terminal",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "message-terminal",
        senderAddress: "principal-alloc@tenant.example",
        rawMessage: new Uint8Array([1, 2, 3]),
        stepGrants: [],
      });
      await store.createPending({
        id: "alloc-terminal",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-terminal",
        expectedGeneration: 0,
        sidecarId: "sidecar-terminal",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(0),
      });
      expect(
        await store.claimNextReconcilable({
          leaseId: "lease-terminal",
          leaseDurationMs: 60_000,
        }),
      ).not.toBeNull();

      const endedAt = new Date("2026-08-04T12:00:00.000Z");
      expect(
        await store.failWithoutInfrastructure({
          allocationId: "alloc-terminal",
          expectedStatus: "provisioning",
          expectedGeneration: 1,
          expectedLeaseId: "lease-stale",
          code: "quota_disabled",
          message: "stale reconciler",
          now: endedAt,
        }),
      ).toBeNull();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: (row, { eq }) => eq(row.id, "run-terminal"),
          columns: { status: true },
        }),
      ).toEqual({ status: "running" });
      expect(
        await h.db.query.principal.findFirst({
          where: (row, { eq }) => eq(row.id, "prn-terminal-run"),
          columns: { status: true },
        }),
      ).toEqual({ status: "active" });
      expect((await dispatchStore.findById("dispatch-terminal"))?.status).toBe(
        "pending",
      );

      const failed = await store.failWithoutInfrastructure({
        allocationId: "alloc-terminal",
        expectedStatus: "provisioning",
        expectedGeneration: 1,
        expectedLeaseId: "lease-terminal",
        code: "quota_disabled",
        message: "Provisioning is disabled for this account",
        now: endedAt,
      });

      expect(failed?.status).toBe("failed");
      expect(failed?.failureCode).toBe("quota_disabled");
      expect(
        await h.db.query.workflowRun.findFirst({
          where: (row, { eq }) => eq(row.id, ANCHOR_RUN_ID),
          columns: { status: true, endedAt: true },
        }),
      ).toEqual({ status: "failed", endedAt });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: (row, { eq }) => eq(row.id, "run-terminal"),
          columns: { status: true, endedAt: true },
        }),
      ).toEqual({ status: "failed", endedAt });
      expect(
        await h.db.query.principal.findFirst({
          where: (row, { eq }) => eq(row.id, "prn-terminal-run"),
          columns: { status: true },
        }),
      ).toEqual({ status: "deactivated" });
      expect(await h.db.select().from(runToken)).toEqual([]);
      expect(await dispatchStore.findById("dispatch-terminal")).toMatchObject({
        status: "abandoned",
        failureCode: "quota_disabled",
        failureMessage: "Provisioning is disabled for this account",
      });
      expect(
        await dispatchStore.claimNextPending({
          leaseId: "dispatch-lease-after-terminal-failure",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
    });

    test.each([
      { confirmed: false, providerReleased: false },
      { confirmed: false, providerReleased: true },
      { confirmed: true, providerReleased: false },
      { confirmed: true, providerReleased: true },
    ])(
      "releasing a parked replacement preserves cleanup proofs: %j",
      async ({ confirmed, providerReleased }) => {
        const { store, allocation, leaseId, initialization } =
          await createClaimedAllocation("alloc-release-parked-replacement");
        expect(await store.beginInitialization(initialization)).toBe(true);
        expect(
          await store.completeInitialization({
            ...initialization,
            publicKey: "a".repeat(64),
          }),
        ).toBe(true);
        expect(
          await store.beginReplacement({
            allocationId: allocation.id,
            expectedStatus: "allocated",
            expectedGeneration: 1,
            expectedLeaseId: leaseId,
            failureCode: "sidecar_connect_failed",
            failureMessage: "Parked worker disconnected",
            firstAttemptAt: new Date(0),
            nextAttemptAt: new Date(0),
          }),
        ).toMatchObject({
          status: "replacing",
          deploymentCleanupConfirmed: false,
        });
        await store.claimNextReconcilable({
          leaseId: "parked-cleanup",
          leaseDurationMs: 60_000,
        });
        if (confirmed)
          expect(
            await store.confirmDeploymentCleanup({
              allocationId: allocation.id,
              generation: 2,
              expectedLeaseId: "parked-cleanup",
            }),
          ).toBe(true);
        if (providerReleased)
          expect(
            await store.confirmProviderRelease({
              allocationId: allocation.id,
              generation: 2,
              expectedLeaseId: "parked-cleanup",
            }),
          ).toBe(true);
        expect(
          await store.beginRelease({
            allocationId: allocation.id,
            expectedStatus: "replacing",
            expectedGeneration: 2,
            expectedLeaseId: "parked-cleanup",
          }),
        ).toMatchObject({
          status: "releasing",
          generation: 3,
          deploymentCleanupConfirmed: confirmed,
          providerHoldReleased: providerReleased,
        });
      },
    );

    test("cancelling a failed placement leaves the workflow outcome to lifecycle when no copy ran", async () => {
      const store = await bindFirstGeneration(
        "alloc-cancel-placement",
        ANCHOR_RUN_ID,
      );
      await store.claimNextReconcilable({
        leaseId: "placement-lease",
        leaseDurationMs: 60_000,
      });
      await store.beginReplacement({
        allocationId: "alloc-cancel-placement",
        expectedStatus: "provisioning",
        expectedGeneration: 1,
        expectedLeaseId: "placement-lease",
        failureCode: "sidecar_reuse_rejected",
        failureMessage: "The previous placement was full",
        firstAttemptAt: new Date(0),
        nextAttemptAt: new Date(0),
      });
      await h.db
        .update(workflowRun)
        .set({
          cancellationRequestedAt: new Date(),
          cancellationDeadline: new Date(),
          cancellationReason: "User cancelled",
        })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));
      const released = await store.beginRelease({
        allocationId: "alloc-cancel-placement",
        expectedStatus: "replacing",
        expectedGeneration: 2,
      });
      expect(released?.failureCode).toBeUndefined();
      await store.markDestroyFailed({
        allocationId: "alloc-cancel-placement",
        expectedGeneration: 3,
        code: SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
        message: "Cleanup could not be confirmed",
      });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, ANCHOR_RUN_ID),
        }),
      ).toMatchObject({
        status: "running",
        failureCode: null,
        failureMessage: null,
        cancellationReason: "User cancelled",
      });
    });

    for (const status of ["replacing", "releasing"] as const) {
      test(`preserves capacity and stops reconciliation after ${status} fails permanently`, async () => {
        const store = createSidecarAllocationStore(h.db, {
          getSidecarInventory: () => ({
            capacity: DEFAULT_SIDECAR_CAPACITY,
            retainedIncarnations: [],
          }),
        });
        const dispatchStore = createWorkflowRunDispatchStore(h.db);
        await store.createPending({
          id: "alloc-destroy",
          anchorRunId: ANCHOR_RUN_ID,
          tenantId: TENANT_ID,
          provisionerId: "ec2-spot",
          provisionerApiVersion: 2,
          provisionerBindingFingerprint: "ec2-spot:test",
          adapterManifestDigest: MANIFEST_DIGEST,
          maxDisconnectedMs: 900_000,
        });
        await store.bindInitialSidecar({
          allocationId: "alloc-destroy",
          expectedGeneration: 0,
          sidecarId: "sidecar-destroy",
          tokenHashSha256: new Uint8Array([1, 2, 3]),
          adapterManifestDigest: MANIFEST_DIGEST,
          connectDeadline: new Date(0),
        });
        if (status === "releasing") {
          await store.markAllocated({
            allocationId: "alloc-destroy",
            generation: 1,
            externalRef: "vm-destroy",
          });
        }
        await store.claimNextReconcilable({
          leaseId: "lease-generation-1",
          leaseDurationMs: 60_000,
        });
        const failure = {
          allocationId: "alloc-destroy",
          expectedGeneration: 2,
          expectedLeaseId: "lease-destroy",
          code: "credentials_revoked",
          message: "Credentials no longer permit deleting this worker",
        };
        expect(
          await store.markDestroyFailed({
            ...failure,
            expectedGeneration: 1,
            expectedLeaseId: "lease-generation-1",
          }),
        ).toBeNull();

        const transition = {
          allocationId: "alloc-destroy",
          expectedGeneration: 1,
          expectedLeaseId: "lease-generation-1",
        };
        if (status === "replacing") {
          await store.beginReplacement({
            ...transition,
            expectedStatus: "provisioning",
            firstAttemptAt: new Date(0),
            nextAttemptAt: new Date(0),
            failureCode: "ensure_failed",
            failureMessage: "Provider request timed out",
          });
        } else {
          await store.beginRelease({
            ...transition,
            expectedStatus: "allocated",
          });
        }
        expect(
          await store.claimNextReconcilable({
            leaseId: "lease-destroy",
            leaseDurationMs: 60_000,
          }),
        ).toMatchObject({ status, generation: 2 });
        await dispatchStore.enqueue({
          id: "dispatch-destroy",
          anchorRunId: ANCHOR_RUN_ID,
          messageId: "message-destroy",
          senderAddress: "principal-alloc@tenant.example",
          rawMessage: new Uint8Array([1, 2, 3]),
          stepGrants: [],
        });

        expect(
          await store.markDestroyFailed({
            ...failure,
            expectedLeaseId: "lease-stale",
          }),
        ).toBeNull();
        expect(
          await store.markDestroyFailed({ ...failure, expectedGeneration: 1 }),
        ).toBeNull();
        expect((await store.findById("alloc-destroy"))?.status).toBe(status);
        expect((await dispatchStore.findById("dispatch-destroy"))?.status).toBe(
          "pending",
        );

        const failed = await store.markDestroyFailed(failure);
        expect(failed).toMatchObject({
          status: "destroy_failed",
          generation: 2,
          sidecarId: "sidecar-destroy",
          failureCode: failure.code,
          failureMessage: failure.message,
          destroyAttempts: 1,
        });
        expect(failed?.externalRef).toBe(
          status === "releasing" ? "vm-destroy" : undefined,
        );
        expect(failed?.nextAttemptAt).toBeUndefined();
        expect(failed?.reconciliationLeaseId).toBeUndefined();
        expect(failed?.reconciliationLeaseExpiresAt).toBeUndefined();
        expect(failed?.connectDeadline).toBeUndefined();
        expect(
          await h.db.query.workflowRun.findFirst({
            where: (row, { eq }) => eq(row.id, ANCHOR_RUN_ID),
            columns: { status: true, failureCode: true, failureMessage: true },
          }),
        ).toEqual({
          status: "failed",
          failureCode: failure.code,
          failureMessage: failure.message,
        });
        expect(await dispatchStore.findById("dispatch-destroy")).toMatchObject({
          status: "abandoned",
          failureCode: failure.code,
          failureMessage: failure.message,
        });
        expect(await store.listActive()).toMatchObject([
          { id: "alloc-destroy", status: "destroy_failed" },
        ]);
        expect(await store.wakeReconciliation("alloc-destroy", 2)).toBe(false);
        expect(
          await store.claimNextReconcilable({
            leaseId: "lease-after-failure",
            leaseDurationMs: 60_000,
          }),
        ).toBeNull();
        expect(await store.markDestroyFailed(failure)).toBeNull();
        expect(
          await store.markReleased({
            allocationId: "alloc-destroy",
            generation: 2,
          }),
        ).toBeNull();
        expect(
          await store.bindReplacementSidecar({
            allocationId: "alloc-destroy",
            generation: 2,
            sidecarId: "sidecar-replacement",
            tokenHashSha256: new Uint8Array([4, 5, 6]),
            adapterManifestDigest: MANIFEST_DIGEST,
            connectDeadline: new Date(0),
          }),
        ).toBeNull();

        await seedWorkflowRun(h.db, {
          id: "anchor-other",
          anchorRunId: "anchor-other",
          tenantId: TENANT_ID,
          definitionId: DEFINITION_ID,
        });
        const launchSpecStore = createWorkflowRunLaunchSpecStore(h.db);
        const launchSpec = await launchSpecStore.get(ANCHOR_RUN_ID);
        if (launchSpec === null) {
          throw new Error("Expected launch specification");
        }
        await launchSpecStore.create({
          ...launchSpec,
          anchorRunId: "anchor-other",
          sessionId: "ses-destroy-other",
        });
        await store.createPending({
          id: "alloc-other",
          anchorRunId: "anchor-other",
          tenantId: TENANT_ID,
          provisionerId: "ec2-spot",
          provisionerApiVersion: 2,
          provisionerBindingFingerprint: "ec2-spot:test",
          adapterManifestDigest: MANIFEST_DIGEST,
          maxDisconnectedMs: 900_000,
        });
        await store.bindInitialSidecar({
          allocationId: "alloc-other",
          expectedGeneration: 0,
          sidecarId: "sidecar-other",
          tokenHashSha256: new Uint8Array([4, 5, 7]),
          adapterManifestDigest: MANIFEST_DIGEST,
          connectDeadline: new Date(0),
        });
        // Capacity left for operator cleanup still counts against the
        // sidecar, but whether it takes new work is the provisioner's call.
        expect(
          await store.markAllocated({
            allocationId: "alloc-other",
            generation: 1,
            sidecarId: "sidecar-destroy",
          }),
        ).toMatchObject({ status: "allocated", sidecarId: "sidecar-destroy" });
      });
    }

    test("allocates and settles a deployed anchor torn down before its first trigger", async () => {
      // The whole deploy->first-trigger window before a run is ever triggered:
      // the anchor is born "deployed". `createPending` must accept it (allocation
      // happens at deploy), and a terminal allocation failure in that window must
      // settle the still-"deployed" anchor rather than leave it live forever.
      await h.db
        .update(workflowRun)
        .set({ status: "deployed" })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));

      const store = createSidecarAllocationStore(h.db);
      const pending = await store.createPending({
        id: "alloc-deployed",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      expect(pending.status).toBe("pending");

      await store.bindInitialSidecar({
        allocationId: "alloc-deployed",
        expectedGeneration: 0,
        sidecarId: "sidecar-deployed",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(0),
      });
      expect(
        await store.claimNextReconcilable({
          leaseId: "lease-deployed",
          leaseDurationMs: 60_000,
        }),
      ).not.toBeNull();

      const endedAt = new Date("2026-08-04T12:00:00.000Z");
      const failed = await store.failWithoutInfrastructure({
        allocationId: "alloc-deployed",
        expectedStatus: "provisioning",
        expectedGeneration: 1,
        expectedLeaseId: "lease-deployed",
        code: "quota_disabled",
        message: "Provisioning is disabled for this account",
        now: endedAt,
      });
      expect(failed?.status).toBe("failed");
      expect(
        await h.db.query.workflowRun.findFirst({
          where: (row, { eq }) => eq(row.id, ANCHOR_RUN_ID),
          columns: { status: true, endedAt: true },
        }),
      ).toEqual({ status: "failed", endedAt });
    });

    test("fails active runs when a lost worker is released", async () => {
      await seedPrincipal(h.db, {
        id: "prn-unrecoverable-run",
        tenantId: TENANT_ID,
        kind: "workflow",
        refId: "run-unrecoverable",
        status: "active",
      });
      await seedWorkflowRun(h.db, {
        id: "run-unrecoverable",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
        principalId: "prn-unrecoverable-run",
      });
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      const dispatchStore = createWorkflowRunDispatchStore(h.db);
      await store.createPending({
        id: "alloc-unrecoverable",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-unrecoverable",
        expectedGeneration: 0,
        sidecarId: "sidecar-unrecoverable",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(0),
      });
      await store.markAllocated({
        allocationId: "alloc-unrecoverable",
        generation: 1,
      });
      await dispatchStore.enqueue({
        id: "dispatch-unrecoverable-pending",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "message-unrecoverable-pending",
        senderAddress: "principal-alloc@tenant.example",
        rawMessage: new Uint8Array([1, 2, 3]),
        stepGrants: [],
      });
      await dispatchStore.enqueue({
        id: "dispatch-unrecoverable-acknowledged",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "message-unrecoverable-acknowledged",
        senderAddress: "principal-alloc@tenant.example",
        rawMessage: new Uint8Array([4, 5, 6]),
        stepGrants: [],
      });
      expect(
        await dispatchStore.acknowledge({
          allocationId: "alloc-unrecoverable",
          anchorRunId: ANCHOR_RUN_ID,
          messageId: "message-unrecoverable-acknowledged",
          generation: 1,
        }),
      ).toMatchObject({ status: "acknowledged" });
      const claimed = await store.claimNextReconcilable({
        leaseId: "lease-unrecoverable",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.id).toBe("alloc-unrecoverable");

      const endedAt = new Date("2026-08-04T12:00:00.000Z");
      expect(
        await store.beginUnrecoverableRelease({
          expectedStatus: "allocated",
          allocationId: "alloc-unrecoverable",
          expectedGeneration: 1,
          expectedLeaseId: "lease-stale",
          failureCode: "sidecar_connect_failed",
          failureMessage: "stale reconciler",
          now: endedAt,
        }),
      ).toBeNull();
      expect(
        await dispatchStore.findById("dispatch-unrecoverable-pending"),
      ).toMatchObject({ status: "pending" });
      expect(
        await dispatchStore.findById("dispatch-unrecoverable-acknowledged"),
      ).toMatchObject({ status: "acknowledged" });
      const releasing = await store.beginUnrecoverableRelease({
        expectedStatus: "allocated",
        allocationId: "alloc-unrecoverable",
        expectedGeneration: 1,
        expectedLeaseId: "lease-unrecoverable",
        failureCode: "sidecar_connect_failed",
        failureMessage: "connect timeout",
        now: endedAt,
      });

      expect(releasing?.status).toBe("releasing");
      expect(releasing?.generation).toBe(2);
      expect(releasing?.failureCode).toBe("sidecar_connect_failed");
      expect(
        await h.db.query.workflowRun.findFirst({
          where: (row, { eq }) => eq(row.id, ANCHOR_RUN_ID),
          columns: { status: true, endedAt: true },
        }),
      ).toEqual({ status: "failed", endedAt });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: (row, { eq }) => eq(row.id, "run-unrecoverable"),
          columns: { status: true, endedAt: true },
        }),
      ).toEqual({ status: "failed", endedAt });
      expect(
        await h.db.query.principal.findFirst({
          where: (row, { eq }) => eq(row.id, "prn-unrecoverable-run"),
          columns: { status: true },
        }),
      ).toEqual({ status: "deactivated" });
      expect(
        await dispatchStore.findById("dispatch-unrecoverable-pending"),
      ).toMatchObject({
        status: "abandoned",
        failureCode: "sidecar_connect_failed",
        failureMessage: "connect timeout",
      });
      expect(
        await dispatchStore.findById("dispatch-unrecoverable-acknowledged"),
      ).toMatchObject({
        status: "abandoned",
        failureCode: "sidecar_connect_failed",
        failureMessage: "connect timeout",
      });
      expect(
        await dispatchStore.claimNextPending({
          leaseId: "dispatch-lease-after-unrecoverable-release",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
    });

    test("fails a stopped deployment's runs and keeps its capacity", async () => {
      await seedWorkflowRun(h.db, {
        id: "run-stopped-child",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
      const store = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      const dispatchStore = createWorkflowRunDispatchStore(h.db);
      await store.createPending({
        id: "alloc-stopped",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "ec2-spot:test",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-stopped",
        expectedGeneration: 0,
        sidecarId: "sidecar-stopped",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
        adapterManifestDigest: MANIFEST_DIGEST,
        connectDeadline: new Date(0),
      });
      await store.markAllocated({
        allocationId: "alloc-stopped",
        generation: 1,
      });
      await dispatchStore.enqueue({
        id: "dispatch-stopped",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "message-stopped",
        senderAddress: "principal-alloc@tenant.example",
        rawMessage: new Uint8Array([1, 2, 3]),
        stepGrants: [],
      });
      await store.claimNextReconcilable({
        leaseId: "lease-stopped",
        leaseDurationMs: 60_000,
      });
      const failure = {
        allocationId: "alloc-stopped",
        expectedGeneration: 1,
        expectedLeaseId: "lease-stopped",
        failureCode: "sidecar_deployment_stopped",
        failureMessage: "The child ended itself",
        now: new Date("2026-08-04T12:00:00.000Z"),
      };

      expect(
        await store.failStoppedDeployment({
          ...failure,
          expectedLeaseId: "lease-stale",
        }),
      ).toBe(false);
      expect(await store.failStoppedDeployment(failure)).toBe(true);

      // It rests on its connection like a ready allocation and keeps the
      // reason it failed.
      const settled = await store.findById("alloc-stopped");
      expect(settled).toMatchObject({
        status: "allocated",
        generation: 1,
        failureCode: "sidecar_deployment_stopped",
        failureMessage: "The child ended itself",
      });
      expect(settled?.reconciliationLeaseId).toBeUndefined();
      expect(settled?.nextAttemptAt).toBeUndefined();
      expect(settled?.connectDeadline).toBeUndefined();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: (row, { eq }) => eq(row.id, ANCHOR_RUN_ID),
        }),
      ).toMatchObject({
        status: "failed",
        endedAt: failure.now,
        failureCode: "sidecar_deployment_stopped",
        failureMessage: "The child ended itself",
      });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: (row, { eq }) => eq(row.id, "run-stopped-child"),
          columns: { status: true },
        }),
      ).toEqual({ status: "failed" });
      expect(await dispatchStore.findById("dispatch-stopped")).toMatchObject({
        status: "abandoned",
        failureCode: "sidecar_deployment_stopped",
      });
    });

    test("a ready pass keeps why a stopped deployment failed", async () => {
      const stopped = await createClaimedAllocation("alloc-ready-stopped");
      expect(
        await stopped.store.failStoppedDeployment({
          allocationId: stopped.allocation.id,
          expectedGeneration: stopped.allocation.generation,
          expectedLeaseId: stopped.leaseId,
          failureCode: SIDECAR_DEPLOYMENT_STOPPED_FAILURE_CODE,
          failureMessage: "The child ended itself",
        }),
      ).toBe(true);
      // The sidecar reconnects during the failed retention, which wakes a
      // pass that finds the anchor terminal and settles as ready.
      await stopped.store.wakeReconciliation(
        stopped.allocation.id,
        stopped.allocation.generation,
      );
      await stopped.store.claimNextReconcilable({
        leaseId: "lease-stopped-ready",
        leaseDurationMs: 60_000,
      });
      const settled = await stopped.store.markConnectionReady({
        allocationId: stopped.allocation.id,
        generation: stopped.allocation.generation,
        expectedLeaseId: "lease-stopped-ready",
      });
      expect(settled).toMatchObject({
        failureCode: SIDECAR_DEPLOYMENT_STOPPED_FAILURE_CODE,
        failureMessage: "The child ended itself",
      });
    });
  },
);
