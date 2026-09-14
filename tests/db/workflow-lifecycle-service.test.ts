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
  SidecarIdentityValidationError,
  WorkflowControlTimeoutError,
  WorkflowControlUnconfirmedError,
  WorkflowControlUnreachableError,
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
        Pick<WorkflowLifecycleServiceDeps, "sendControl" | "historyReceives">
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
        ...options,
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
        cancelGraceMs: 1_000,
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

    test.each([
      [
        "unreachable",
        () => new WorkflowControlUnreachableError("Not connected here"),
      ],
      [
        "unvalidated",
        () => new SidecarIdentityValidationError(allocationId, 1),
      ],
    ] as const)(
      "an %s stop is retried instead of destroying the worker",
      async (_kind, failure) => {
        await expire();
        let reachable = false;
        const actions: string[] = [];
        const lifecycle = service({
          sendControl: async (_target, command) => {
            actions.push(command.action);
            if (command.action === "stop" && !reachable) throw failure();
          },
        });

        await lifecycle.reconcile();
        current = new Date(current.getTime() + 1_000);
        await lifecycle.reconcile();
        expect(await lifecycle.getStatus(tenantId, runId)).toMatchObject({
          status: "running",
          allocation: { status: "allocated" },
        });

        reachable = true;
        current = new Date(current.getTime() + 1_000);
        await lifecycle.reconcile();
        expect(actions).toEqual(["cancel", "stop", "stop"]);
        expect(await lifecycle.getStatus(tenantId, runId)).toMatchObject({
          status: "cancelled",
          allocation: { status: "allocated" },
        });
      },
    );

    test("records cancellation for a run whose Git tip keeps moving", async () => {
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

      expect(await lifecycle.requestCancellation(tenantId, runId, "Stop")).toBe(
        "pending",
      );
      expect(reads).toBe(3);
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, runId),
        }),
      ).toMatchObject({
        status: "running",
        cancellationReason: "Stop",
      });
    });

    test("a run whose Git tip keeps moving still receives cancel and a forced stop", async () => {
      await expire();
      await leavePending();
      const actions: string[] = [];
      const lifecycle = service({
        runReader: {
          readLatestRunEvents: async () => ({
            tip: "old-tip",
            events: new Map([[runId, null]]),
          }),
          resolveRefTip: async () => "new-tip",
        },
        sendControl: async (_target, command) => {
          actions.push(command.action);
          if (command.action === "stop")
            throw new WorkflowControlTimeoutError("Stop timed out");
        },
      });

      await lifecycle.reconcile();
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcile();
      expect(actions).toEqual(["cancel", "stop"]);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
    });

    test("a confirmed stop is recorded only once its Git tip settles", async () => {
      await expire();
      await leavePending();
      let moving = true;
      const actions: string[] = [];
      const lifecycle = service({
        runReader: {
          readLatestRunEvents: async () => ({
            tip: moving ? "old-tip" : "settled-tip",
            events: new Map([[runId, null]]),
          }),
          resolveRefTip: async () => (moving ? "new-tip" : "settled-tip"),
        },
        sendControl: async (_target, command) => {
          actions.push(command.action);
        },
      });

      await lifecycle.reconcile();
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcile();
      expect(actions).toEqual(["cancel", "stop"]);
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "running",
      );

      moving = false;
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcile();
      expect(actions).toEqual(["cancel", "stop", "stop"]);
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "cancelled",
      );
    });

    test.each(["unreachable", "unconfirmed"] as const)(
      "a worker that stays %s is force-released after a short grace",
      async (condition) => {
        await expire();
        const options = {
          sendControl: async (
            _target: unknown,
            command: { action: "cancel" | "stop" },
          ) => {
            if (command.action !== "stop") return;
            if (condition === "unconfirmed")
              throw new WorkflowControlUnconfirmedError("Queued behind a pack");
            throw new WorkflowControlUnreachableError("Reconnecting again");
          },
        };
        const lifecycle = service(options);
        await lifecycle.reconcile();
        current = new Date(current.getTime() + 1_000);
        await lifecycle.reconcile();
        expect(
          (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
        ).toBe("allocated");

        current = new Date(current.getTime() + 59_000);
        await lifecycle.reconcile();
        expect(
          (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
        ).toBe("allocated");

        current = new Date(current.getTime() + 1_000);
        await lifecycle.reconcile();
        expect(await lifecycle.getStatus(tenantId, runId)).toMatchObject({
          status: "running",
          allocation: {
            status: "releasing",
            failureCode: "workflow_stop_failed",
          },
        });
      },
    );

    test("a restarted Hub gives an unreachable worker the full grace", async () => {
      await expire();
      const options = {
        sendControl: async (
          _target: unknown,
          command: { action: "cancel" | "stop" },
        ) => {
          if (command.action === "stop")
            throw new WorkflowControlUnreachableError("Not connected yet");
        },
      };
      await service(options).reconcile();
      current = new Date(current.getTime() + 600_000);
      const restarted = service(options);
      await restarted.reconcile();
      expect(
        (await restarted.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");

      current = new Date(current.getTime() + 60_000);
      await restarted.reconcile();
      expect(
        (await restarted.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
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
          await leavePending();
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

    test("forced stop keeps a child's accepted terminal outcome and cancels only live children", async () => {
      await expire();
      await leavePending();
      await addChild("run_child_done");
      await addChild("run_child_live");
      const actions: string[] = [];
      const options = {
        runReader: childHistory({
          run_child_done: [
            {
              seq: 1,
              type: "RunCompleted",
              body: { type: "RunCompleted", at: current.toISOString() },
            },
          ],
        }),
        sendControl: async (_target: unknown, command: { action: string }) => {
          actions.push(command.action);
        },
      };
      await service(options).reconcile();
      current = new Date(current.getTime() + 1_000);
      await service(options).reconcile();
      expect(actions).toEqual(["cancel", "stop"]);
      expect(await runStates()).toEqual({
        [runId]: "cancelled",
        run_child_done: "completed",
        run_child_live: "cancelled",
      });
      const principals = await h.db
        .select({ id: principal.id, status: principal.status })
        .from(principal);
      expect(principals.every((row) => row.status === "deactivated")).toBe(
        true,
      );
    });

    test("an unresponsive worker is not declared stopped until capacity destruction is confirmed", async () => {
      await expire();
      const options = {
        sendControl: async () => {
          throw new WorkflowControlTimeoutError("Worker did not answer");
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

    test("unreadable workflow history stops the worker but defers its recorded outcome", async () => {
      await expire();
      await addChild("run_child_done");
      await leavePending();
      let unreadable = true;
      const actions: string[] = [];
      const options = {
        runReader: {
          readRunEvents: async (
            _repo: unknown,
            _ref: string,
            id: string,
          ): Promise<WorkflowRunEvent[]> => {
            if (unreadable) throw new Error("Unreadable repository");
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
        sendControl: async (_target: unknown, command: { action: string }) => {
          actions.push(command.action);
        },
      };
      const lifecycle = service(options);
      await lifecycle.reconcile();
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcile();
      expect(actions).toEqual(["cancel", "stop"]);
      expect(await runStates()).toEqual({
        [runId]: "running",
        run_child_done: "running",
      });

      unreadable = false;
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcile();
      expect(actions).toEqual(["cancel", "stop", "stop"]);
      expect(await runStates()).toEqual({
        [runId]: "cancelled",
        run_child_done: "completed",
      });
    });

    test("released capacity with unreadable history defers the recorded outcome", async () => {
      await expire();
      await leavePending();
      await h.db
        .update(sidecarAllocation)
        .set({ status: "released" })
        .where(eq(sidecarAllocation.id, allocationId));
      let unreadable = true;
      const lifecycle = service({
        runReader: {
          readRunEvents: async () => {
            if (unreadable) throw new Error("Unreadable repository");
            return [];
          },
        },
      });
      await lifecycle.reconcile();
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "running",
      );

      unreadable = false;
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcile();
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "cancelled",
      );
    });

    test("a missing ref keeps a receive's pending projection and defers the stop", async () => {
      await expire();
      await leavePending();
      const missingRepo = {
        runReader: {
          readLatestRunEvents: async () => ({ tip: null, events: new Map() }),
          resolveRefTip: async () => null,
        },
        sendControl: async () => undefined,
      };
      const lifecycle = service(missingRepo);
      await lifecycle.reconcile();
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcile();
      expect(await pendingIds()).toEqual([`wpp_${runId}`]);
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "running",
      );
    });

    test("a deployment with nothing pending records a forced stop without reading Git", async () => {
      await expire();
      let reads = 0;
      const options = {
        runReader: {
          readRunEvents: async () => {
            reads += 1;
            throw new Error("Unreadable repository");
          },
        },
        sendControl: async () => undefined,
      };
      await service(options).reconcile();
      current = new Date(current.getTime() + 1_000);
      await service(options).reconcile();
      expect(reads).toBe(0);
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
      await leavePending();
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

    test("expiry waits for the active ensure lease before releasing capacity", async () => {
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
      const finishEnsure = Promise.withResolvers<{
        kind: "accepted";
        externalRef: string;
      }>();
      let destroyed = false;
      const allocationStore = createSidecarAllocationStore(h.db);
      const reconciler = createSidecarAllocationReconciler({
        allocationStore,
        plugins: createSidecarPluginRegistry({
          provisioners: [
            {
              id: "test",
              apiVersion: 1,
              bindingFingerprint: "test:1",
              capabilities: [],
              ensure: async () => {
                started.resolve(undefined);
                return finishEnsure.promise;
              },
              destroy: async (request) => {
                expect(request.externalRef).toBe("vm-1");
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
      });
      const ensuring = reconciler.reconcileNext();
      await started.promise;
      try {
        const provisioning = await allocationStore.findById(allocationId);
        expect(provisioning?.status).toBe("provisioning");
        expect(provisioning?.reconciliationLeaseId).toBeDefined();
        await expire();
        await service().reconcile();
        expect(await allocationStore.findById(allocationId)).toMatchObject({
          status: "provisioning",
          generation: provisioning?.generation,
          reconciliationLeaseId: provisioning?.reconciliationLeaseId,
        });
        expect(destroyed).toBe(false);
      } finally {
        finishEnsure.resolve({ kind: "accepted", externalRef: "vm-1" });
        await ensuring;
      }
      expect(await allocationStore.findById(allocationId)).toMatchObject({
        status: "allocated",
        externalRef: "vm-1",
      });
      expect(
        (await allocationStore.findById(allocationId))?.reconciliationLeaseId,
      ).toBeUndefined();
      current = new Date(current.getTime() + 1_000);
      await service().reconcile();
      expect((await allocationStore.findById(allocationId))?.status).toBe(
        "releasing",
      );
      await reconciler.reconcileNext();
      expect(destroyed).toBe(true);
      await service().reconcile();
      expect((await service().getStatus(tenantId, runId))?.status).toBe(
        "cancelled",
      );
    });
  },
);
