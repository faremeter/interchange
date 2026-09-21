import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq, inArray } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";

import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";
import {
  createSidecarAllocationStore,
  withExecutableWorkflowRun,
} from "@intx/db";
import * as dbSchema from "@intx/db/schema";
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
  createReconciliationScheduler,
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
        Pick<
          WorkflowLifecycleServiceDeps,
          "db" | "sendControl" | "historyReceives"
        >
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
        db: options.db ?? h.db,
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

    async function addRun(id: string, expiresAt?: Date) {
      repoRuns.set(repoOf(id), [id]);
      await h.db.insert(workflowRun).values({
        id,
        tenantId,
        definitionId: "definition",
        anchorRunId: id,
        status: "running",
        address: `${id}@example.test`,
        createdAt,
        lifecyclePolicy: { capacityRetention: { completed: "0s" } },
        ...(expiresAt === undefined ? {} : { expiresAt }),
      });
      await h.db.insert(sidecar).values({
        id: `sidecar_${id}`,
        tokenHashSha256: new Uint8Array(
          await crypto.subtle.digest("SHA-256", new TextEncoder().encode(id)),
        ),
      });
      await h.db.insert(sidecarAllocation).values({
        id: `allocation_${id}`,
        tenantId,
        anchorRunId: id,
        provisionerId: "test",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "test:1",
        sidecarId: `sidecar_${id}`,
        status: "allocated",
        generation: 1,
        ensureAcceptedGeneration: 1,
      });
    }

    test("the shared scheduler bounds expired runs to eight and refills a free slot independently", async () => {
      await expire();
      for (let index = 0; index < 9; index += 1)
        await addRun(`run_lifecycle_${String(index)}`, current);
      const started: string[] = [];
      const actions: string[] = [];
      const releases = new Map<string, () => void>();
      const firstWave = Promise.withResolvers<undefined>();
      const ninth = Promise.withResolvers<undefined>();
      const finished = Promise.withResolvers<undefined>();
      const tasks: Promise<boolean>[] = [];
      let closing = false;
      let active = 0;
      let peak = 0;
      let completed = 0;
      const lifecycle = service({
        sendControl: async (_target, command) => {
          started.push(command.runId);
          actions.push(command.action);
          if (started.length === 8) firstWave.resolve(undefined);
          if (started.length === 9) ninth.resolve(undefined);
          if (closing) return;
          const release = Promise.withResolvers<undefined>();
          releases.set(command.runId, () => release.resolve(undefined));
          await release.promise;
        },
      });
      const scheduler = createReconciliationScheduler({
        name: "workflow lifecycle test",
        reconcileNext() {
          active += 1;
          peak = Math.max(peak, active);
          const task = lifecycle.reconcileNext().then(
            (worked) => {
              active -= 1;
              if (worked) completed += 1;
              if (completed === 10) finished.resolve(undefined);
              return worked;
            },
            (error: unknown) => {
              active -= 1;
              throw error;
            },
          );
          tasks.push(task);
          return task;
        },
      });
      scheduler.start();
      try {
        await firstWave.promise;
        expect(started).toHaveLength(8);
        scheduler.wake();
        scheduler.wake();
        const second = started[1];
        if (second === undefined) throw new Error("Missing second worker");
        releases.get(second)?.();
        await ninth.promise;
        expect(completed).toBe(1);
        expect(started).toHaveLength(9);
        closing = true;
        for (const release of releases.values()) release();
        await finished.promise;
        expect(peak).toBe(8);
        expect(new Set(started).size).toBe(10);
        expect(actions).toEqual(Array.from({ length: 10 }, () => "cancel"));
        expect(await lifecycle.reconcileNext()).toBe(false);
      } finally {
        scheduler.stop();
        closing = true;
        for (const release of releases.values()) release();
        await Promise.all(tasks);
      }
    });

    test("a stalled history read leaves admission free while other runs progress", async () => {
      await addRun("run_z");
      await leavePending();
      await leavePending("run_z");
      const entered = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      const reads: string[] = [];
      const lifecycle = service({
        runReader: {
          readRunEvents: async (_repo, _ref, id) => {
            reads.push(id);
            if (id === runId) {
              entered.resolve(undefined);
              await release.promise;
            }
            return [];
          },
        },
      });
      const blocked = lifecycle.reconcileNext();
      try {
        await entered.promise;
        expect(
          await withExecutableWorkflowRun(
            h.db,
            {
              allocationId,
              generation: 1,
              sidecarId: "sidecar",
              tenantId,
              anchorRunId: runId,
              workflowRunAddress: `${runId}@example.test`,
            },
            () => true,
          ),
        ).toBe(true);
        expect(await lifecycle.reconcileNext()).toBe(true);
        expect(await lifecycle.reconcileNext()).toBe(false);
        // The stalled run is still due but stays excluded while it is active.
        await leavePending("run_z", "wpp_run_z_later");
        current = new Date(current.getTime() + 1_000);
        expect(await lifecycle.reconcileNext()).toBe(true);
        expect(await lifecycle.reconcileNext()).toBe(false);
        expect(reads).toEqual([runId, "run_z", "run_z"]);
      } finally {
        release.resolve(undefined);
        await blocked;
      }
      current = new Date(current.getTime() + 1_000);
      expect(await lifecycle.reconcileNext()).toBe(false);
      expect(reads).toEqual([runId, "run_z", "run_z"]);
      expect(await pendingIds()).toEqual([]);
    });

    test("a slow pack receive does not hold the sweep on a live run with nothing due", async () => {
      // The receive holding the allocation lock outlived its projection's
      // grace, so its own pending row makes the run a candidate.
      const historyReceives = createWorkflowHistoryReceiveTracker();
      const pending = await leavePending();
      historyReceives.begin(pending);
      const locked = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      const receive = h.db.transaction(async (tx) => {
        await tx
          .select()
          .from(sidecarAllocation)
          .where(eq(sidecarAllocation.id, allocationId))
          .for("update");
        locked.resolve(undefined);
        await release.promise;
      });
      const actions: string[] = [];
      const lifecycle = service({
        historyReceives,
        sendControl: async (_target, command) => {
          actions.push(command.action);
        },
      });
      try {
        await Promise.race([
          locked.promise,
          receive.then(() => {
            throw new Error("Receive ended before locking");
          }),
        ]);
        expect(await lifecycle.reconcileNext()).toBe(true);
        expect(actions).toEqual([]);
      } finally {
        release.resolve(undefined);
        await receive;
        historyReceives.end(pending);
      }
    });

    test("a live run is left alone until it expires", async () => {
      await h.db
        .update(workflowRun)
        .set({ expiresAt: new Date(current.getTime() + 60_000) })
        .where(eq(workflowRun.id, runId));
      const actions: string[] = [];
      const lifecycle = service({
        sendControl: async (_target, command) => {
          actions.push(command.action);
        },
      });
      expect(await lifecycle.reconcileNext()).toBe(false);
      current = new Date(current.getTime() + 60_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(actions).toEqual(["cancel"]);
    });

    test("retained capacity is not locked before its release time", async () => {
      const releaseAt = new Date(current.getTime() + 900_000);
      await h.db
        .update(workflowRun)
        .set({
          status: "failed",
          endedAt: current,
          capacityReleaseAt: releaseAt,
        })
        .where(eq(workflowRun.id, runId));
      const locked = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      const writer = h.db.transaction(async (tx) => {
        await tx
          .select()
          .from(sidecarAllocation)
          .where(eq(sidecarAllocation.id, allocationId))
          .for("update");
        locked.resolve(undefined);
        await release.promise;
      });
      const lifecycle = service();
      try {
        await Promise.race([
          locked.promise,
          writer.then(() => {
            throw new Error("Writer ended before locking");
          }),
        ]);
        expect(await lifecycle.reconcileNext()).toBe(false);
      } finally {
        release.resolve(undefined);
        await writer;
      }
      current = releaseAt;
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
    });

    test("a finished deployment without a lifecycle policy keeps its capacity until an explicit release", async () => {
      await h.db
        .update(workflowRun)
        .set({
          status: "cancelled",
          endedAt: current,
          lifecyclePolicy: null,
          cancellationRequestedAt: current,
          cancellationDeadline: current,
          cancellationReason: "Cancelled by request",
        })
        .where(eq(workflowRun.id, runId));
      const lifecycle = service();
      expect(await lifecycle.reconcileNext()).toBe(false);
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe("pending");
      current = new Date(current.getTime() + 1_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
    });

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
      await lifecycle.reconcileNext();
      expect(reads).toBe(0);

      complete("RunCompleted");
      await leavePending(runId, "wpp_young", 0);
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcileNext();
      expect(reads).toBe(0);
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "running",
      );

      current = new Date(current.getTime() + 30_000);
      await lifecycle.reconcileNext();
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

    test("a missing repository keeps a receive's pending projection", async () => {
      await leavePending();
      const lifecycle = service({
        runReader: {
          readLatestRunEvents: async () => ({ tip: null, events: new Map() }),
          resolveRefTip: async () => null,
        },
      });
      await lifecycle.reconcileNext();
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
      await lifecycle.reconcileNext();
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
      await lifecycle.reconcileNext();
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
      await lifecycle.reconcileNext();
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
      await lifecycle.reconcileNext();
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
      await lifecycle.reconcileNext();
      expect(await runStates()).toEqual({
        [runId]: "completed",
        run_child_done: "completed",
      });
      expect(await pendingIds()).toEqual([]);
      reads = 0;
      await lifecycle.reconcileNext();
      expect(reads).toBe(0);
    });

    test("repairs a missed terminal projection, deactivates its principal, and calls the bound provisioner", async () => {
      complete("RunCompleted");
      await leavePending();
      await Promise.all([service().reconcileNext(), service().reconcileNext()]);
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
      await service().reconcileNext();
      const releaseAt = new Date(current.getTime() + 900_000);
      expect(
        (await service().getStatus(tenantId, runId))?.capacityReleaseAt,
      ).toBe(releaseAt.toISOString());
      current = new Date(releaseAt.getTime() - 1);
      await service().reconcileNext();
      expect(
        (await createSidecarAllocationStore(h.db).findById(allocationId))
          ?.generation,
      ).toBe(1);
      current = releaseAt;
      await service().reconcileNext();
      expect(
        (await service().getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
    });

    test("manual release shortens retention without repeating the fence", async () => {
      complete("RunFailed");
      await leavePending();
      expect(await service().releaseCapacity(tenantId, runId)).toBe("pending");
      expect(await service().releaseCapacity(tenantId, runId)).toBe("pending");
      await service().reconcileNext();
      await service().reconcileNext();
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
      await service().reconcileNext();
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

        await lifecycle.reconcileNext();
        current = new Date(current.getTime() + 1_000);
        await lifecycle.reconcileNext();
        expect(await lifecycle.getStatus(tenantId, runId)).toMatchObject({
          status: "running",
          allocation: { status: "allocated" },
        });

        reachable = true;
        current = new Date(current.getTime() + 1_000);
        await lifecycle.reconcileNext();
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

      await lifecycle.reconcileNext();
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcileNext();
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

      await lifecycle.reconcileNext();
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcileNext();
      expect(actions).toEqual(["cancel", "stop"]);
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "running",
      );

      moving = false;
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcileNext();
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
        await lifecycle.reconcileNext();
        current = new Date(current.getTime() + 1_000);
        await lifecycle.reconcileNext();
        expect(
          (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
        ).toBe("allocated");

        current = new Date(current.getTime() + 59_000);
        await lifecycle.reconcileNext();
        expect(
          (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
        ).toBe("allocated");

        current = new Date(current.getTime() + 1_000);
        await lifecycle.reconcileNext();
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
      await service(options).reconcileNext();
      current = new Date(current.getTime() + 600_000);
      const restarted = service(options);
      await restarted.reconcileNext();
      expect(
        (await restarted.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");

      current = new Date(current.getTime() + 60_000);
      await restarted.reconcileNext();
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
      await service(options).reconcileNext();
      const first = await service().getStatus(tenantId, runId);
      expect(first?.status).toBe("running");
      expect(first?.cancellationRequestedAt).toBe(current.toISOString());
      expect(actions).toEqual(["cancel"]);
      current = new Date(current.getTime() + 1_000);
      await service(options).reconcileNext();
      expect(actions).toEqual(["cancel", "stop"]);
      expect((await service().getStatus(tenantId, runId))?.status).toBe(
        "cancelled",
      );
      await service(options).reconcileNext();
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
      }).reconcileNext();
      await service().reconcileNext();
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
      await service(options).reconcileNext();
      current = new Date(current.getTime() + 1_000);
      await service(options).reconcileNext();
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
      await service(options).reconcileNext();
      current = new Date(current.getTime() + 1_000);
      await service(options).reconcileNext();
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
      await service(options).reconcileNext();
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
      await lifecycle.reconcileNext();
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcileNext();
      expect(actions).toEqual(["cancel", "stop"]);
      expect(await runStates()).toEqual({
        [runId]: "running",
        run_child_done: "running",
      });

      unreadable = false;
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcileNext();
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
      await lifecycle.reconcileNext();
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "running",
      );

      unreadable = false;
      current = new Date(current.getTime() + 5_000);
      await lifecycle.reconcileNext();
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
      await lifecycle.reconcileNext();
      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcileNext();
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
      await service(options).reconcileNext();
      current = new Date(current.getTime() + 1_000);
      await service(options).reconcileNext();
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
      await service().reconcileNext();
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
        await service().reconcileNext();
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
      await service().reconcileNext();
      expect((await allocationStore.findById(allocationId))?.status).toBe(
        "releasing",
      );
      await reconciler.reconcileNext();
      expect(destroyed).toBe(true);
      await service().reconcileNext();
      expect((await service().getStatus(tenantId, runId))?.status).toBe(
        "cancelled",
      );
    });

    test("scans past runs that stay due and revisits arrivals on both sides of the cursor after a pause", async () => {
      await addRun("run_z");
      // A cancellation waiting on its capacity release stays due every pass.
      await h.db
        .update(workflowRun)
        .set({
          cancellationRequestedAt: current,
          cancellationDeadline: current,
          cancellationReason: "Cancelled by request",
        })
        .where(inArray(workflowRun.id, [runId, "run_z"]));
      await h.db
        .update(sidecarAllocation)
        .set({ status: "releasing" })
        .where(inArray(sidecarAllocation.anchorRunId, [runId, "run_z"]));
      await leavePending();
      await leavePending("run_z");
      const reads: string[] = [];
      const lifecycle = service({
        runReader: {
          readRunEvents: async (_repo, _ref, id) => {
            reads.push(id);
            return [];
          },
        },
      });
      expect(await lifecycle.reconcileNext()).toBe(true);
      await addRun("run_a");
      await addRun("run_zz");
      await leavePending("run_a");
      await leavePending("run_zz");
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(await lifecycle.reconcileNext()).toBe(false);
      expect(reads).toEqual([runId, "run_z"]);
      current = new Date(current.getTime() + 999);
      expect(await lifecycle.reconcileNext()).toBe(false);
      current = new Date(current.getTime() + 1);
      for (let index = 0; index < 4; index += 1)
        expect(await lifecycle.reconcileNext()).toBe(true);
      expect(await lifecycle.reconcileNext()).toBe(false);
      expect(reads).toEqual([runId, "run_z", "run_a", "run_zz"]);
    });

    test("a branch with nothing due is read once per pass, not once per candidate", async () => {
      // Every live deployment holds an allocation the release branch passes over.
      for (const id of ["run_a", "run_b", "run_c"]) await addRun(id, current);
      await h.db
        .update(workflowRun)
        .set({ expiresAt: current })
        .where(eq(workflowRun.id, runId));
      let releaseReads = 0;
      const lifecycle = service({
        db: drizzle(h.db.$client, {
          schema: dbSchema,
          logger: {
            logQuery(query) {
              if (
                query.includes("cross join lateral") &&
                query.includes('from "sidecar_allocation"')
              )
                releaseReads += 1;
            },
          },
        }),
        sendControl: async () => undefined,
      });
      let visits = 0;
      while (await lifecycle.reconcileNext()) visits += 1;
      expect(visits).toBe(4);
      expect(releaseReads).toBe(1);
    });

    test("work that becomes due behind a branch's next candidate is picked up by the next pass", async () => {
      await addRun("run_a", current);
      await addRun("run_m");
      await addRun("run_z", current);
      await leavePending();
      const cancelled: string[] = [];
      const lifecycle = service({
        sendControl: async (_target, command) => {
          if (command.action === "cancel") cancelled.push(command.runId);
        },
      });
      // run_a, then this deployment's history, by which point the expiry
      // branch has already moved on to run_z.
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(await lifecycle.reconcileNext()).toBe(true);
      await h.db
        .update(workflowRun)
        .set({ expiresAt: current })
        .where(eq(workflowRun.id, "run_m"));
      while (await lifecycle.reconcileNext());
      current = new Date(current.getTime() + 1_000);
      while (await lifecycle.reconcileNext());
      expect(cancelled).toContain("run_m");
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
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(reads).toBe(1);
      // The receive settled its own row; a later receive's history fails too.
      await h.db
        .delete(workflowPendingProjection)
        .where(eq(workflowPendingProjection.id, "wpp_first"));
      await leavePending(runId, "wpp_second");
      current = new Date(current.getTime() + 5_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(reads).toBe(2);
      current = new Date(current.getTime() + 5_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
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
      expect(await lifecycle.reconcileNext()).toBe(true);
      current = new Date(current.getTime() + 5_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(reads).toBe(2);
      current = new Date(current.getTime() + 5_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(reads).toBe(2);
      current = new Date(current.getTime() + 5_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
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
      expect(await lifecycle.reconcileNext()).toBe(true);
      current = new Date(current.getTime() + 5_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(reads).toBe(2);
      expect(await lifecycle.releaseCapacity("tenant_other", runId)).toBe(
        "not_found",
      );
      expect(
        await lifecycle.requestCancellation("tenant_other", runId, "Stop"),
      ).toBe("not_found");
      expect(reads).toBe(2);
      current = new Date(current.getTime() + 5_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(reads).toBe(2);
    });

    test("a failed run check does not block later candidates or its next sweep", async () => {
      await addRun("run_z", current);
      await leavePending();
      let unreadable = true;
      const controlled: string[] = [];
      const lifecycle = service({
        runReader: {
          readRunEvents: async (_repo, _ref, id) => {
            if (id === runId && unreadable) throw new Error("Unreadable Git");
            return events;
          },
        },
        sendControl: async (_target, command) => {
          controlled.push(command.runId);
        },
      });
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(controlled).toEqual(["run_z"]);
      expect(await lifecycle.reconcileNext()).toBe(false);
      unreadable = false;
      // The failed deployment backs off before its next Git read.
      current = new Date(current.getTime() + 5_000);
      complete("RunCompleted");
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "completed",
      );
    });
  },
);
