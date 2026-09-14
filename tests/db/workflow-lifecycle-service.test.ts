import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";
import { createSidecarAllocationStore } from "@intx/db";
import {
  principal,
  sidecar,
  sidecarAllocation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import {
  createSidecarAllocationReconciler,
  createSidecarPluginRegistry,
  createWorkflowLifecycleService,
  type WorkflowRunEvent,
} from "@intx/hub-sessions";

describe.skipIf(!harnessDbEnvAvailable())(
  "durable workflow capacity release",
  () => {
    let h: TestDb;
    let current: Date;
    let events: WorkflowRunEvent[];
    const createdAt = new Date(Date.now() - 3_600_000);
    const runId = "run_lifecycle";
    const tenantId = "tnt_lifecycle";
    const allocationId = "allocation_lifecycle";

    function service() {
      return createWorkflowLifecycleService({
        db: h.db,
        runReader: {
          listRunIds: async () => [runId],
          readRunEvents: async () => events,
        },
        now: () => current,
      });
    }

    beforeAll(async () => {
      h = await createTestDb();
    });
    afterAll(async () => {
      await h.close();
    });
    beforeEach(async () => {
      await h.reset();
      current = new Date(createdAt.getTime() + 1_000);
      events = [];
      await seedTenants(h.db, [{ id: tenantId }]);
      await h.db
        .insert(workflowDefinition)
        .values({ id: "definition", tenantId, name: "Example" });
      await seedPrincipal(h.db, {
        id: "principal",
        tenantId,
        kind: "workflow",
        refId: runId,
        status: "active",
      });
      await h.db.insert(workflowRun).values({
        id: runId,
        tenantId,
        definitionId: "definition",
        anchorRunId: runId,
        principalId: "principal",
        status: "running",
        address: `${runId}@example.test`,
        createdAt,
        lifecyclePolicy: {
          capacityRetention: { completed: "0s", failed: "15m" },
        },
      });
      await h.db
        .insert(sidecar)
        .values({ id: "sidecar", tokenHashSha256: new Uint8Array(32) });
      await h.db.insert(sidecarAllocation).values({
        id: allocationId,
        tenantId,
        anchorRunId: runId,
        provisionerId: "test",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "test:1",
        sidecarId: "sidecar",
        status: "allocated",
        generation: 1,
        ensureAcceptedGeneration: 1,
      });
    });

    function complete(type: "RunCompleted" | "RunFailed") {
      events = [{ seq: 1, type, body: { type, at: current.toISOString() } }];
    }

    test("repairs a missed terminal projection, deactivates its principal, and calls the bound provisioner", async () => {
      complete("RunCompleted");
      await Promise.all([service().reconcile(), service().reconcile()]);
      const state = await service().getStatus(tenantId, runId);
      expect(state?.status).toBe("completed");
      expect(state?.allocation?.status).toBe("releasing");
      expect(
        (await createSidecarAllocationStore(h.db).findById(allocationId))
          ?.generation,
      ).toBe(2);
      expect(
        (
          await h.db.query.principal.findFirst({
            where: eq(principal.id, "principal"),
          })
        )?.status,
      ).toBe("deactivated");

      const destroyed: string[] = [];
      const reconciler = createSidecarAllocationReconciler({
        allocationStore: createSidecarAllocationStore(h.db),
        plugins: createSidecarPluginRegistry({
          provisioners: [
            {
              id: "test",
              apiVersion: 1,
              bindingFingerprint: "test:1",
              capabilities: [],
              ensure: async () => {
                throw new Error(
                  "terminal capacity must not be provisioned again",
                );
              },
              destroy: async (request) => {
                destroyed.push(request.allocationId);
                return { kind: "destroyed" };
              },
            },
          ],
        }),
        router: {
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          isAllocatedSidecarReady: async () => true,
          waitForAllocatedSidecar: async () => undefined,
        },
        hubWebSocketUrl: "ws://hub.example/ws",
        now: () => current,
      });
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(destroyed).toEqual([allocationId]);
      expect(
        (await service().getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("released");
      expect(await service().releaseCapacity(tenantId, runId)).toBe("released");
    });

    test("keeps failure retention anchored to its original timestamp across restarts", async () => {
      complete("RunFailed");
      await service().reconcile();
      const releaseAt = new Date(current.getTime() + 900_000);
      expect(
        (await service().getStatus(tenantId, runId))?.capacityReleaseAt,
      ).toBe(releaseAt.toISOString());
      current = new Date(releaseAt.getTime() - 1);
      await service().reconcile();
      expect(
        (await createSidecarAllocationStore(h.db).findById(allocationId))
          ?.generation,
      ).toBe(1);
      current = releaseAt;
      await service().reconcile();
      expect(
        (await service().getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
    });

    test("manual release shortens retention without repeating the fence", async () => {
      complete("RunFailed");
      expect(await service().releaseCapacity(tenantId, runId)).toBe("pending");
      expect(await service().releaseCapacity(tenantId, runId)).toBe("pending");
      await service().reconcile();
      await service().reconcile();
      expect(
        (await createSidecarAllocationStore(h.db).findById(allocationId))
          ?.generation,
      ).toBe(2);
    });

    test("rejects live runs and does not expose another tenant or a child run", async () => {
      expect(await service().releaseCapacity(tenantId, runId)).toBe("live");
      expect(await service().releaseCapacity("other", runId)).toBe("not_found");
      await h.db.insert(workflowRun).values({
        id: "run_child",
        tenantId,
        definitionId: "definition",
        anchorRunId: runId,
        status: "completed",
      });
      expect(await service().releaseCapacity(tenantId, "run_child")).toBe(
        "not_found",
      );
      expect(
        (await createSidecarAllocationStore(h.db).findById(allocationId))
          ?.generation,
      ).toBe(1);
    });

    test("surfaces permanent cleanup failure without pretending it was requeued", async () => {
      await h.db
        .update(workflowRun)
        .set({ status: "failed", endedAt: current })
        .where(eq(workflowRun.id, runId));
      await h.db
        .update(sidecarAllocation)
        .set({
          status: "destroy_failed",
          failureCode: "provider_refused",
          failureMessage: "Manual cleanup needed",
        })
        .where(eq(sidecarAllocation.id, allocationId));
      expect(await service().releaseCapacity(tenantId, runId)).toBe(
        "cleanup_failed",
      );
      await service().reconcile();
      expect(
        (await service().getStatus(tenantId, runId))?.allocation?.failureCode,
      ).toBe("provider_refused");
      expect(
        (await createSidecarAllocationStore(h.db).findById(allocationId))
          ?.generation,
      ).toBe(1);
    });
  },
);
