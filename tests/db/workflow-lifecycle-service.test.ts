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
  type WorkflowLifecycleServiceDeps,
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

    function service(
      options: Pick<WorkflowLifecycleServiceDeps, "sendControl"> &
        Partial<Pick<WorkflowLifecycleServiceDeps, "runReader">> = {},
    ) {
      return createWorkflowLifecycleService({
        db: h.db,
        runReader: {
          listRunIds: async () => [runId],
          readRunEvents: async () => events,
        },
        now: () => current,
        cancelGraceMs: 1_000,
        ...options,
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
    async function expire() {
      await h.db
        .update(workflowRun)
        .set({
          expiresAt: current,
          lifecyclePolicy: {
            maxLifetime: "1s",
            capacityRetention: { cancelled: "0s" },
          },
        })
        .where(eq(workflowRun.id, runId));
    }

    test("expires a hung run, confirms process stop, and then applies cancellation retention", async () => {
      await expire();
      const actions: string[] = [];
      const options = {
        sendControl: async (_target: unknown, command: { action: string }) => {
          actions.push(command.action);
        },
      };
      await service(options).reconcile();
      const first = await service().getStatus(tenantId, runId);
      expect(first?.status).toBe("running");
      expect(first?.cancellationRequestedAt).toBe(current.toISOString());
      expect(actions).toEqual(["cancel"]);
      current = new Date(current.getTime() + 1_000);
      await service(options).reconcile();
      expect(actions).toEqual(["cancel", "stop"]);
      expect((await service().getStatus(tenantId, runId))?.status).toBe(
        "cancelled",
      );
      await service(options).reconcile();
      expect(
        (await service().getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
    });

    test("accepts cooperative cancellation without forcing the worker down", async () => {
      await expire();
      const actions: string[] = [];
      await service({
        sendControl: async (_target, command) => {
          actions.push(command.action);
          events = [
            {
              seq: 1,
              type: "RunCancelled",
              body: { at: current.toISOString() },
            },
          ];
        },
      }).reconcile();
      await service().reconcile();
      expect(actions).toEqual(["cancel"]);
      expect((await service().getStatus(tenantId, runId))?.status).toBe(
        "cancelled",
      );
      expect(
        (await service().getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
    });

    test("an unresponsive worker is not declared stopped until capacity destruction is confirmed", async () => {
      await expire();
      const options = {
        sendControl: async () => {
          throw new Error("Worker unavailable");
        },
      };
      await service(options).reconcile();
      current = new Date(current.getTime() + 1_000);
      await service(options).reconcile();
      expect((await service().getStatus(tenantId, runId))?.status).toBe(
        "running",
      );
      expect(
        (await service().getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
      await h.db
        .update(sidecarAllocation)
        .set({ status: "released" })
        .where(eq(sidecarAllocation.id, allocationId));
      await service(options).reconcile();
      expect((await service().getStatus(tenantId, runId))?.status).toBe(
        "cancelled",
      );
    });

    test("unreadable workflow history cannot bypass the lifetime deadline", async () => {
      await expire();
      const options = {
        runReader: {
          listRunIds: async () => [runId],
          readRunEvents: async () => {
            throw new Error("Unreadable repository");
          },
        },
        sendControl: async () => undefined,
      };
      await service(options).reconcile();
      current = new Date(current.getTime() + 1_000);
      await service(options).reconcile();
      expect((await service().getStatus(tenantId, runId))?.status).toBe(
        "cancelled",
      );
    });

    test("repeated manual cancellation keeps the first reason and deadline", async () => {
      expect(
        await service().requestCancellation(
          tenantId,
          runId,
          "Operator request",
        ),
      ).toBe("pending");
      const first = await service().getStatus(tenantId, runId);
      current = new Date(current.getTime() + 500);
      expect(
        await service().requestCancellation(tenantId, runId, "Another request"),
      ).toBe("pending");
      const second = await service().getStatus(tenantId, runId);
      expect(second?.cancellationDeadline).toBe(first?.cancellationDeadline);
      expect(second?.cancellationReason).toBe("Operator request");
    });

    test("later policy edits do not change a deployment's saved retention", async () => {
      complete("RunFailed");
      await h.db
        .update(workflowDefinition)
        .set({ lifecyclePolicy: { capacityRetention: { failed: "0s" } } })
        .where(eq(workflowDefinition.id, "definition"));
      await service().reconcile();
      expect(
        (await service().getStatus(tenantId, runId))?.capacityReleaseAt,
      ).toBe(new Date(current.getTime() + 900_000).toISOString());
      expect(
        (await service().getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");
    });
    test("expiry revokes a stalled ensure lease so the reconciler can destroy its capacity", async () => {
      await h.db
        .update(sidecarAllocation)
        .set({
          status: "pending",
          generation: 0,
          sidecarId: null,
          ensureAcceptedGeneration: null,
          nextAttemptAt: new Date(0),
        })
        .where(eq(sidecarAllocation.id, allocationId));
      const started = Promise.withResolvers<undefined>();
      let destroyed = false;
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
                started.resolve(undefined);
                return new Promise<never>(() => undefined);
              },
              destroy: async () => {
                destroyed = true;
                return { kind: "destroyed" };
              },
            },
          ],
        }),
        router: {
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          isAllocatedSidecarReady: async () => false,
          waitForAllocatedSidecar: async () => undefined,
        },
        hubWebSocketUrl: "ws://hub.example/ws",
        now: () => current,
        leaseDurationMs: 90,
      });
      const ensuring = reconciler.reconcileNext();
      await started.promise;
      await expire();
      await service().reconcile();
      await ensuring;
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(destroyed).toBe(true);
      await service().reconcile();
      expect((await service().getStatus(tenantId, runId))?.status).toBe(
        "cancelled",
      );
    });
  },
);
