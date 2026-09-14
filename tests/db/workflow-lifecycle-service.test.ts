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
  workflowPendingProjection,
  workflowRun,
} from "@intx/db/schema";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";
import {
  createSidecarAllocationReconciler,
  createSidecarPluginRegistry,
  createWorkflowHistoryReceiveTracker,
  createWorkflowLifecycleService,
  type WorkflowRunEvent,
  type WorkflowLifecycleServiceDeps,
  type WorkflowRunReader,
} from "@intx/hub-sessions";

describe.skipIf(!harnessDbEnvAvailable())(
  "durable workflow capacity release",
  () => {
    let h: TestDb;
    let current: Date;
    let events: WorkflowRunEvent[];
    // Run ids committed to each deployment's workflow-run repository.
    let repoRuns: Map<string, string[]>;
    const createdAt = new Date(Date.now() - 3_600_000);
    const runId = "run_lifecycle";
    const tenantId = "tnt_lifecycle";
    const allocationId = "allocation_lifecycle";

    function service(
      options: Partial<
        Pick<WorkflowLifecycleServiceDeps, "historyReceives">
      > & {
        runReader?: Partial<WorkflowRunReader>;
      } = {},
    ) {
      const configuredReader = {
        listRunIds: async (repoId: { id: string }) =>
          repoRuns.get(repoId.id) ?? [],
        readRunEvents: async () => events,
        ...options.runReader,
      };
      return createWorkflowLifecycleService({
        db: h.db,
        historyReceives:
          options.historyReceives ?? createWorkflowHistoryReceiveTracker(),
        runReader: {
          ...configuredReader,
          readLatestRunEvents:
            options.runReader?.readLatestRunEvents ??
            (async (repoId, ref, include) => {
              const latest = new Map<string, WorkflowRunEvent | null>();
              for (const id of await configuredReader.listRunIds(repoId, ref)) {
                if (!include(id)) continue;
                latest.set(
                  id,
                  (await configuredReader.readRunEvents(repoId, ref, id)).at(
                    -1,
                  ) ?? null,
                );
              }
              return { tip: "test-tip", events: latest };
            }),
          resolveRefTip:
            options.runReader?.resolveRefTip ?? (async () => "test-tip"),
          hasRepository:
            options.runReader?.hasRepository ?? (async () => false),
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
      repoRuns = new Map([[repoOf(runId), [runId]]]);
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

    function repoOf(id: string) {
      return deriveWorkflowRunRepoId(`${id}@example.test`);
    }

    function complete(type: "RunCompleted" | "RunFailed") {
      events = [{ seq: 1, type, body: { type, at: current.toISOString() } }];
    }

    // A receive whose run-status projection failed leaves this row behind.
    async function leavePending(
      anchorRunId = runId,
      id = `wpp_${anchorRunId}`,
      age = 60_000,
      historyRequired = true,
    ) {
      await h.db.insert(workflowPendingProjection).values({
        id,
        anchorRunId,
        createdAt: new Date(current.getTime() - age),
        historyRequired,
      });
      return id;
    }

    async function pendingIds() {
      const rows = await h.db
        .select({ id: workflowPendingProjection.id })
        .from(workflowPendingProjection);
      return rows.map((row) => row.id);
    }

    async function addChild(id: string) {
      repoRuns.get(repoOf(runId))?.push(id);
      await seedPrincipal(h.db, {
        id: `principal_${id}`,
        tenantId,
        kind: "workflow",
        refId: id,
        status: "active",
      });
      await h.db.insert(workflowRun).values({
        id,
        tenantId,
        definitionId: "definition",
        anchorRunId: runId,
        principalId: `principal_${id}`,
        status: "running",
        createdAt,
      });
    }

    function childHistory(histories: Record<string, WorkflowRunEvent[]>) {
      return {
        readRunEvents: async (_repo: unknown, _ref: string, id: string) =>
          histories[id] ?? [],
      };
    }

    async function runStates() {
      const rows = await h.db
        .select({ id: workflowRun.id, status: workflowRun.status })
        .from(workflowRun)
        .where(eq(workflowRun.anchorRunId, runId));
      return Object.fromEntries(rows.map((row) => [row.id, row.status]));
    }

    test("healthy runs cost no Git reads and a pending projection waits out its grace", async () => {
      let reads = 0;
      const lifecycle = service({
        runReader: {
          readRunEvents: async () => {
            reads += 1;
            return events;
          },
        },
      });
      await lifecycle.reconcile();
      expect(reads).toBe(0);

      complete("RunCompleted");
      await leavePending(runId, "wpp_young", 0);
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcile();
      expect(reads).toBe(0);
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "running",
      );

      current = new Date(current.getTime() + 30_000);
      await lifecycle.reconcile();
      expect(reads).toBe(1);
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "completed",
      );
      expect(await pendingIds()).toEqual([]);
    });

    test("a receive still in flight keeps its pending projection unclaimed", async () => {
      complete("RunCompleted");
      const historyReceives = createWorkflowHistoryReceiveTracker();
      const pending = await leavePending();
      historyReceives.begin(pending);
      let reads = 0;
      const lifecycle = service({
        historyReceives,
        runReader: {
          readRunEvents: async () => {
            reads += 1;
            return events;
          },
        },
      });
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe(
        "history_pending",
      );
      expect(reads).toBe(0);
      expect(await pendingIds()).toEqual([pending]);

      historyReceives.end(pending);
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe("pending");
      expect(reads).toBe(1);
      expect(await pendingIds()).toEqual([]);
    });

    test("rechecks a changed Git tip before applying a terminal decision", async () => {
      await leavePending();
      let reads = 0;
      const lifecycle = service({
        runReader: {
          readLatestRunEvents: async () => {
            reads += 1;
            return reads === 1
              ? { tip: "old-tip", events: new Map([[runId, null]]) }
              : {
                  tip: "new-tip",
                  events: new Map([
                    [
                      runId,
                      {
                        seq: 1,
                        type: "RunCompleted",
                        body: {
                          type: "RunCompleted",
                          at: current.toISOString(),
                        },
                      },
                    ],
                  ]),
                };
          },
          resolveRefTip: async () => "new-tip",
        },
      });

      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe("pending");
      expect(reads).toBe(2);
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "completed",
      );
    });

    test("a release for a run whose Git tip keeps moving reports unreconciled history", async () => {
      await leavePending();
      let reads = 0;
      const lifecycle = service({
        runReader: {
          readLatestRunEvents: async () => {
            reads += 1;
            return { tip: "old-tip", events: new Map([[runId, null]]) };
          },
          resolveRefTip: async () => "new-tip",
        },
      });

      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe(
        "history_pending",
      );
      expect(reads).toBe(3);
      expect(await pendingIds()).toEqual([`wpp_${runId}`]);
    });

    test("an explicit release reads Git despite backoff and reports unreconciled history as retryable", async () => {
      complete("RunCompleted");
      await leavePending();
      let unreadable = true;
      const lifecycle = service({
        runReader: {
          readRunEvents: async () => {
            if (unreadable) throw new Error("Unreadable repository");
            return events;
          },
        },
      });
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe(
        "history_pending",
      );
      unreadable = false;
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe("pending");
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "completed",
      );
    });

    test("history a receive settled does not lengthen a later recovery backoff", async () => {
      await leavePending(runId, "wpp_first");
      let reads = 0;
      const lifecycle = service({
        runReader: {
          readRunEvents: async () => {
            reads += 1;
            throw new Error("Unreadable Git");
          },
        },
      });
      await lifecycle.reconcile();
      expect(reads).toBe(1);
      // The receive settled its own row; a later receive's history fails too.
      await h.db
        .delete(workflowPendingProjection)
        .where(eq(workflowPendingProjection.id, "wpp_first"));
      await leavePending(runId, "wpp_second");
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcile();
      expect(reads).toBe(2);
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcile();
      expect(reads).toBe(3);
    });

    test("repeated failures reading the same history double the recovery backoff", async () => {
      await leavePending();
      let reads = 0;
      const lifecycle = service({
        runReader: {
          readRunEvents: async () => {
            reads += 1;
            throw new Error("Unreadable Git");
          },
        },
      });
      await lifecycle.reconcile();
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcile();
      expect(reads).toBe(2);
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcile();
      expect(reads).toBe(2);
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcile();
      expect(reads).toBe(3);
    });

    test("a request naming the run under another tenant leaves its recovery backoff", async () => {
      await leavePending();
      let reads = 0;
      const lifecycle = service({
        runReader: {
          readRunEvents: async () => {
            reads += 1;
            throw new Error("Unreadable Git");
          },
        },
      });
      await lifecycle.reconcile();
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcile();
      expect(reads).toBe(2);
      expect(await lifecycle.releaseCapacity("tenant_other", runId)).toBe(
        "not_found",
      );
      expect(reads).toBe(2);
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcile();
      expect(reads).toBe(2);
    });

    test("a missing repository keeps a receive's pending projection", async () => {
      await leavePending();
      const lifecycle = service({
        runReader: {
          readLatestRunEvents: async () => ({ tip: null, events: new Map() }),
          resolveRefTip: async () => null,
        },
      });
      await lifecycle.reconcile();
      expect(await pendingIds()).toEqual([`wpp_${runId}`]);
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe(
        "history_pending",
      );
    });

    test("a repository whose first genesis never committed clears a receive's pending projection", async () => {
      await leavePending();
      const lifecycle = service({
        runReader: {
          readLatestRunEvents: async () => ({ tip: null, events: new Map() }),
          resolveRefTip: async () => null,
          hasRepository: async () => true,
        },
      });
      await lifecycle.reconcile();
      expect(await pendingIds()).toEqual([]);
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe("live");
    });

    test("a backfilled pending projection clears when the deployment never pushed history", async () => {
      await leavePending(runId, "wpp_backfill", 60_000, false);
      const lifecycle = service({
        runReader: {
          readLatestRunEvents: async () => ({ tip: null, events: new Map() }),
          resolveRefTip: async () => null,
        },
      });
      await lifecycle.reconcile();
      expect(await pendingIds()).toEqual([]);
    });

    test("recovery repairs a missed child projection while its deployment stays live", async () => {
      await addChild("run_child_done");
      await leavePending();
      const lifecycle = service({
        runReader: childHistory({
          run_child_done: [
            {
              seq: 1,
              type: "RunFailed",
              body: { type: "RunFailed", at: current.toISOString() },
            },
          ],
        }),
      });
      await lifecycle.reconcile();
      expect(await runStates()).toEqual({
        [runId]: "running",
        run_child_done: "failed",
      });
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");
    });

    test("recovery records a child run the Hub never recorded at its own end time", async () => {
      const finishedAt = current;
      current = new Date(current.getTime() + 3_600_000);
      await leavePending();
      repoRuns.get(repoOf(runId))?.push("run_child_unrecorded");
      const lifecycle = service({
        runReader: childHistory({
          run_child_unrecorded: [
            {
              seq: 1,
              type: "RunFailed",
              body: { type: "RunFailed", at: finishedAt.toISOString() },
            },
          ],
        }),
      });
      await lifecycle.reconcile();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, "run_child_unrecorded"),
        }),
      ).toMatchObject({
        anchorRunId: runId,
        principalId: null,
        status: "failed",
        createdAt: finishedAt,
        endedAt: finishedAt,
      });
      expect(await pendingIds()).toEqual([]);
    });

    test("a terminal deployment's child is still recovered after its capacity is released", async () => {
      await h.db
        .update(workflowRun)
        .set({ status: "completed", endedAt: current })
        .where(eq(workflowRun.id, runId));
      await h.db
        .update(sidecarAllocation)
        .set({ status: "released" })
        .where(eq(sidecarAllocation.id, allocationId));
      await addChild("run_child_done");
      await leavePending();
      let reads = 0;
      const lifecycle = service({
        runReader: {
          readRunEvents: async (_repo, _ref, id) => {
            reads += 1;
            return id === "run_child_done"
              ? [
                  {
                    seq: 1,
                    type: "RunCompleted",
                    body: { type: "RunCompleted", at: current.toISOString() },
                  },
                ]
              : [];
          },
        },
      });
      await lifecycle.reconcile();
      expect(await runStates()).toEqual({
        [runId]: "completed",
        run_child_done: "completed",
      });
      expect(await pendingIds()).toEqual([]);
      reads = 0;
      await lifecycle.reconcile();
      expect(reads).toBe(0);
    });

    test("repairs a missed terminal projection, deactivates its principal, and calls the bound provisioner", async () => {
      complete("RunCompleted");
      await leavePending();
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
      await leavePending();
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
      await leavePending();
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
