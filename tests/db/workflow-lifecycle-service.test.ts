import { DEFAULT_SIDECAR_CAPACITY } from "@intx/types/sidecar";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq, inArray, sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";

import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedPrincipal,
  seedTenants,
  seedRunToken,
} from "@intx/test-harness/seed";
import {
  createSidecarAllocationStore,
  createWorkflowRunDispatchStore,
  SidecarReuseRejectedError,
  SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE,
  SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE,
  SIDECAR_CLEANUP_OPERATOR_RELEASED_FAILURE_CODE,
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
  runToken,
} from "@intx/db/schema";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";
import {
  createReconciliationScheduler,
  createSidecarAllocationReconciler,
  createSidecarPluginRegistry,
  createWorkflowHistoryReceiveTracker,
  createWorkflowLifecycleService,
  SidecarIdentityValidationError,
  SidecarNotConnectedError,
  WorkflowControlInitializingError,
  WorkflowControlNotConnectedError,
  WorkflowControlTimeoutError,
  WorkflowControlUnconfirmedError,
  WorkflowControlUnreachableError,
  type WorkflowRunEvent,
  type WorkflowLifecycleServiceDeps,
  type WorkflowRunReader,
} from "@intx/hub-sessions";

const MANIFEST_DIGEST = "1".repeat(64);

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
          "db" | "sendControl" | "historyReceives" | "cancelGraceMs"
        >
      > & {
        runReader?: Partial<WorkflowRunReader>;
        retentionRouter?: Partial<
          WorkflowLifecycleServiceDeps["retentionRouter"]
        >;
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
        retentionRouter: {
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          getRetentionCandidates: () => [],
          holdsAllocatedBinding: () => true,
          getAllocationRetention: () => "kept",
          retainAllocation: async () => "kept",
          ...options.retentionRouter,
        },
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
        cancelGraceMs: options.cancelGraceMs ?? 1_000,
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
      await h.db.insert(sidecar).values({
        id: "sidecar",
        tokenHashSha256: new Uint8Array(32),
        provisionerId: "test",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:1",
        adapterManifestDigest: MANIFEST_DIGEST,
      });
      await h.db.insert(sidecarAllocation).values({
        id: allocationId,
        tenantId,
        anchorRunId: runId,
        provisionerId: "test",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:1",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
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
        provisionerId: "test",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:1",
        adapterManifestDigest: MANIFEST_DIGEST,
      });
      await h.db.insert(sidecarAllocation).values({
        id: `allocation_${id}`,
        tenantId,
        anchorRunId: id,
        provisionerId: "test",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:1",
        adapterManifestDigest: MANIFEST_DIGEST,
        maxDisconnectedMs: 900_000,
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

    test("a terminal copy without a retention deadline transfers once and keeps its allocation", async () => {
      await h.db
        .update(workflowRun)
        .set({
          status: "completed",
          endedAt: current,
          lifecyclePolicy: null,
          publicKey: "ab".repeat(32),
        })
        .where(eq(workflowRun.id, runId));
      let kept = false;
      let requests = 0;
      const lifecycle = service({
        retentionRouter: {
          getRetentionCandidates: () =>
            kept ? [] : [{ allocationId, generation: 1, sidecarId: "sidecar" }],
          holdsAllocatedBinding: () => true,
          getAllocationRetention: () => (kept ? "kept" : undefined),
          retainAllocation: async (target, timeoutMs) => {
            expect(timeoutMs).toBe(10_000);
            expect(target).toEqual({ allocationId, generation: 1 });
            requests += 1;
            kept = true;
            return "kept";
          },
        },
      });
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(requests).toBe(1);
      current = new Date(current.getTime() + 1000);
      expect(await lifecycle.reconcileNext()).toBe(false);
      expect(requests).toBe(1);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");
    });

    test("slow retains stop being selected and do not block another sidecar or cancellation", async () => {
      const template = await h.db.query.sidecarAllocation.findFirst({
        where: eq(sidecarAllocation.id, allocationId),
      });
      if (template === undefined) throw new Error("Expected allocation");
      await h.db.insert(sidecar).values({
        id: "healthy-sidecar",
        tokenHashSha256: new Uint8Array(32).fill(1),
        provisionerId: "test",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:1",
        adapterManifestDigest: MANIFEST_DIGEST,
      });
      const candidates: {
        allocationId: string;
        generation: number;
        sidecarId: string;
      }[] = [];
      for (let index = 0; index < 10; index++) {
        const id = `run_0_retaining_${String(index)}`;
        const retainedAllocationId = `allocation_retaining_${String(index)}`;
        await h.db.insert(workflowRun).values({
          id,
          anchorRunId: id,
          tenantId,
          definitionId: "definition",
          status: "completed",
          endedAt: current,
          publicKey: "ab".repeat(32),
          address: `${id}@example.test`,
          createdAt,
        });
        await h.db.insert(sidecarAllocation).values({
          ...template,
          id: retainedAllocationId,
          anchorRunId: id,
          sidecarId: index === 9 ? "healthy-sidecar" : "sidecar",
        });
        candidates.push({
          allocationId: retainedAllocationId,
          generation: 1,
          sidecarId: index === 9 ? "healthy-sidecar" : "sidecar",
        });
      }
      await h.db
        .update(workflowRun)
        .set({ expiresAt: current })
        .where(eq(workflowRun.id, runId));
      const acknowledge = Promise.withResolvers<undefined>();
      const retained = new Set<string>();
      const refused = new Set<string>();
      const requests: string[] = [];
      const pending: Promise<"kept" | "refused">[] = [];
      const controls: string[] = [];
      const lifecycle = service({
        retentionRouter: {
          getRetentionCandidates: () =>
            candidates.filter((target) => !retained.has(target.allocationId)),
          getAllocationRetention: (target) =>
            refused.has(target.allocationId)
              ? "refused"
              : retained.has(target.allocationId)
                ? "kept"
                : undefined,
          retainAllocation: (target) => {
            requests.push(target.allocationId);
            const ready =
              target.allocationId === "allocation_retaining_9"
                ? Promise.resolve()
                : acknowledge.promise;
            const response = ready.then(() => {
              if (refused.has(target.allocationId)) return "refused" as const;
              retained.add(target.allocationId);
              return "kept" as const;
            });
            pending.push(response);
            return response;
          },
        },
        sendControl: async (_target, command) => {
          controls.push(command.runId);
        },
      });
      try {
        for (let pass = 0; pass < 2; pass++) {
          // The first snapshot visits all ten copies. On the next pass only
          // cancellation remains due; neither the eight requests in flight nor
          // the ninth waiting on their sidecar consumes lifecycle work again.
          for (let index = 0; index < (pass === 0 ? 11 : 1); index++) {
            expect(await lifecycle.reconcileNext()).toBe(true);
          }
          expect(await lifecycle.reconcileNext()).toBe(false);
          expect(requests).toHaveLength(9);
          expect(new Set(requests).size).toBe(9);
          expect(retained.has("allocation_retaining_9")).toBe(true);
          expect(retained.has("allocation_retaining_0")).toBe(false);
          expect(controls).toHaveLength(pass + 1);
          expect(controls[pass]).toBe(runId);
          current = new Date(current.getTime() + 1000);
        }
        // Saturated retention must not hide newly due expiry, accepted history,
        // or a refusal already learned from an acknowledgement or hello.
        await h.db
          .update(workflowRun)
          .set({ capacityReleaseAt: current })
          .where(eq(workflowRun.id, "run_0_retaining_0"));
        await leavePending("run_0_retaining_1");
        refused.add("allocation_retaining_2");
        for (let index = 0; index < 3; index++) {
          expect(await lifecycle.reconcileNext()).toBe(true);
        }
        expect(await lifecycle.reconcileNext()).toBe(false);
        expect(await pendingIds()).toEqual([]);
        for (const index of [0, 2]) {
          expect(
            (
              await lifecycle.getStatus(
                tenantId,
                `run_0_retaining_${String(index)}`,
              )
            )?.allocation?.status,
          ).toBe("releasing");
        }
        expect(requests).toHaveLength(9);
        current = new Date(current.getTime() + 1000);
      } finally {
        acknowledge.resolve(undefined);
        await Promise.all(pending);
      }
      // The skipped copy remains due once a slot opens; no unbounded queue is
      // needed to remember it, and completed retains leave the candidate list.
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(requests).toHaveLength(10);
      await Promise.all(pending);
    });

    test("a kept copy remains sweep work until its history handoff completes", async () => {
      await h.db
        .update(workflowRun)
        .set({
          status: "completed",
          endedAt: current,
          lifecyclePolicy: null,
          publicKey: "ab".repeat(32),
        })
        .where(eq(workflowRun.id, runId));
      let recovered = false;
      let requests = 0;
      const lifecycle = service({
        retentionRouter: {
          getRetentionCandidates: () =>
            recovered
              ? []
              : [{ allocationId, generation: 1, sidecarId: "sidecar" }],
          holdsAllocatedBinding: () => true,
          getAllocationRetention: () => "kept",
          retainAllocation: async () => {
            requests++;
            if (requests === 1)
              throw new Error("Final history is still arriving");
            recovered = true;
            return "kept";
          },
        },
      });
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(requests).toBe(1);
      current = new Date(current.getTime() + 6000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(requests).toBe(2);
      current = new Date(current.getTime() + 6000);
      expect(await lifecycle.reconcileNext()).toBe(false);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");
    });

    test("unreceived retained history does not extend the release deadline", async () => {
      await h.db
        .update(workflowRun)
        .set({
          status: "failed",
          endedAt: current,
          lifecyclePolicy: { capacityRetention: { failed: "1m" } },
          publicKey: "ab".repeat(32),
        })
        .where(eq(workflowRun.id, runId));
      let requests = 0;
      const lifecycle = service({
        retentionRouter: {
          getRetentionCandidates: () => [
            { allocationId, generation: 1, sidecarId: "sidecar" },
          ],
          holdsAllocatedBinding: () => true,
          getAllocationRetention: () => "kept",
          retainAllocation: async () => {
            requests++;
            throw new Error("Final history is still missing");
          },
        },
      });
      await lifecycle.reconcileNext();
      expect(requests).toBe(1);
      current = new Date(current.getTime() + 60_000);
      await lifecycle.reconcileNext();
      expect(requests).toBe(1);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
    });

    test.each(["completed", "failed", "cancelled"] as const)(
      "retention overflow releases a %s copy without changing its outcome",
      async (status) => {
        await h.db
          .update(workflowRun)
          .set({
            status,
            endedAt: current,
            publicKey: "ab".repeat(32),
            failureCode: status === "failed" ? "original_failure" : null,
            lifecyclePolicy: { capacityRetention: { [status]: "1d" } },
          })
          .where(eq(workflowRun.id, runId));
        const lifecycle = service({
          retentionRouter: {
            getRetentionCandidates: () => [
              { allocationId, generation: 1, sidecarId: "sidecar" },
            ],
            holdsAllocatedBinding: () => true,
            getAllocationRetention: () => "refused",
            retainAllocation: async () => {
              throw new Error("Hello already confirmed refusal");
            },
          },
        });
        expect(await lifecycle.reconcileNext()).toBe(true);
        const run = await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, runId),
        });
        expect(run?.status).toBe(status);
        expect(run?.failureCode).toBe(
          status === "failed" ? "original_failure" : null,
        );
        expect(
          (await lifecycle.getStatus(tenantId, runId))?.allocation,
        ).toMatchObject({
          status: "releasing",
          failureCode: "sidecar_retention_limit_exceeded",
        });
      },
    );

    test.each(["saved", "derived"] as const)(
      "a %s release deadline that is already due never reserves a kept slot",
      async (deadline) => {
        await h.db
          .update(workflowRun)
          .set({
            status: "completed",
            endedAt: new Date(
              current.getTime() - (deadline === "saved" ? 30_000 : 60_000),
            ),
            publicKey: "ab".repeat(32),
            lifecyclePolicy: { capacityRetention: { completed: "1m" } },
            capacityReleaseAt: deadline === "saved" ? current : null,
          })
          .where(eq(workflowRun.id, runId));
        let requests = 0;
        const lifecycle = service({
          retentionRouter: {
            getRetentionCandidates: () => [
              { allocationId, generation: 1, sidecarId: "sidecar" },
            ],
            holdsAllocatedBinding: () => true,
            getAllocationRetention: () => undefined,
            retainAllocation: async () => {
              requests++;
              return "refused";
            },
          },
        });
        expect(await lifecycle.reconcileNext()).toBe(true);
        expect(requests).toBe(0);
        expect(
          (await lifecycle.getStatus(tenantId, runId))?.allocation,
        ).toMatchObject({
          status: "releasing",
          failureCode: null,
        });
      },
    );

    test("retention stops a terminal copy before history recovery but overflow cleanup waits for accepted history", async () => {
      await h.db
        .update(workflowRun)
        .set({ status: "failed", endedAt: current, publicKey: "ab".repeat(32) })
        .where(eq(workflowRun.id, runId));
      await leavePending();
      let refused = false;
      let historyAvailable = false;
      const calls: string[] = [];
      const refusalReported = Promise.withResolvers<undefined>();
      const lifecycle = service({
        retentionRouter: {
          getRetentionCandidates: () => [
            { allocationId, generation: 1, sidecarId: "sidecar" },
          ],
          holdsAllocatedBinding: () => true,
          getAllocationRetention: () => (refused ? "refused" : undefined),
          retainAllocation: async () => {
            calls.push("retain");
            refused = true;
            refusalReported.resolve(undefined);
            return "refused";
          },
        },
        runReader: {
          readLatestRunEvents: async () => {
            calls.push("history");
            if (!historyAvailable)
              throw new Error("history temporarily unavailable");
            return { tip: "test-tip", events: new Map() };
          },
        },
      });
      expect(await lifecycle.reconcileNext()).toBe(true);
      await refusalReported.promise;
      expect(calls).toEqual(["retain", "history"]);
      // The next sweep observes the refusal while accepted history is still
      // pending, so its locked release pass must keep the allocation.
      current = new Date(current.getTime() + 1_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");
      expect(await pendingIds()).toHaveLength(1);
      historyAvailable = true;
      current = new Date(current.getTime() + 6000);
      await lifecycle.reconcileNext();
      expect(await pendingIds()).toEqual([]);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation,
      ).toMatchObject({
        status: "releasing",
        failureCode: "sidecar_retention_limit_exceeded",
      });
    });

    test("refused retention still expires at its policy deadline when accepted history cannot be recovered", async () => {
      await h.db
        .update(workflowRun)
        .set({
          status: "failed",
          endedAt: current,
          publicKey: "ab".repeat(32),
          lifecyclePolicy: { capacityRetention: { failed: "1m" } },
        })
        .where(eq(workflowRun.id, runId));
      await leavePending();
      const lifecycle = service({
        retentionRouter: {
          getRetentionCandidates: () => [
            { allocationId, generation: 1, sidecarId: "sidecar" },
          ],
          holdsAllocatedBinding: () => true,
          getAllocationRetention: () => "refused",
          retainAllocation: async () => {
            throw new Error("already refused");
          },
        },
        runReader: {
          readLatestRunEvents: async () => {
            throw new Error("history unavailable");
          },
        },
      });
      await lifecycle.reconcileNext();
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");
      current = new Date(current.getTime() + 60_000);
      await lifecycle.reconcileNext();
      expect(await pendingIds()).toHaveLength(1);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation,
      ).toMatchObject({
        status: "releasing",
        failureCode: "sidecar_retention_limit_exceeded",
      });
    });

    test("an unconfirmed retain keeps its reservation until retained inventory confirms it without another request", async () => {
      await h.db
        .update(workflowRun)
        .set({
          status: "completed",
          endedAt: current,
          lifecyclePolicy: null,
          publicKey: "ab".repeat(32),
        })
        .where(eq(workflowRun.id, runId));
      // This copy occupies the last active slot until its retained proof arrives.
      const otherRuns = Array.from(
        { length: 128 },
        (_, index) => `capacity-run-${String(index)}`,
      );
      await h.db.insert(workflowRun).values(
        otherRuns.map((id) => ({
          id,
          tenantId,
          definitionId: "definition",
          anchorRunId: id,
          status: "running" as const,
          address: `${id}@example.test`,
        })),
      );
      await h.db.insert(sidecar).values({
        id: "candidate-sidecar",
        tokenHashSha256: new Uint8Array(32).fill(1),
        provisionerId: "test",
        provisionerApiVersion: 2,
        provisionerBindingFingerprint: "test:1",
        adapterManifestDigest: MANIFEST_DIGEST,
      });
      await h.db.insert(sidecarAllocation).values(
        otherRuns.map((anchorRunId, index) => ({
          id: `capacity-allocation-${String(index)}`,
          anchorRunId,
          tenantId,
          provisionerId: "test",
          provisionerApiVersion: 2 as const,
          provisionerBindingFingerprint: "test:1",
          adapterManifestDigest: MANIFEST_DIGEST,
          maxDisconnectedMs: 900_000,
          sidecarId: index === 127 ? "candidate-sidecar" : "sidecar",
          status:
            index === 127 ? ("provisioning" as const) : ("allocated" as const),
          generation: 1,
          ensureAcceptedGeneration: index === 127 ? null : 1,
        })),
      );
      let confirmed = false;
      const allocations = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () =>
          confirmed
            ? {
                capacity: DEFAULT_SIDECAR_CAPACITY,
                retainedIncarnations: [{ allocationId, generation: 1 }],
              }
            : { capacity: DEFAULT_SIDECAR_CAPACITY, retainedIncarnations: [] },
      });
      const placement = {
        allocationId: "capacity-allocation-127",
        generation: 1,
        sidecarId: "sidecar",
      };
      let requests = 0;
      const lifecycle = service({
        retentionRouter: {
          getRetentionCandidates: () =>
            confirmed
              ? []
              : [{ allocationId, generation: 1, sidecarId: "sidecar" }],
          holdsAllocatedBinding: () => true,
          getAllocationRetention: () => (confirmed ? "kept" : undefined),
          retainAllocation: async () => {
            requests += 1;
            throw new Error("acknowledgement lost");
          },
        },
      });
      await lifecycle.reconcileNext();
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");
      await expect(allocations.markAllocated(placement)).rejects.toBeInstanceOf(
        SidecarReuseRejectedError,
      );
      confirmed = true;
      current = new Date(current.getTime() + 1000);
      await lifecycle.reconcileNext();
      expect(requests).toBe(1);
      expect(await allocations.markAllocated(placement)).toMatchObject({
        status: "allocated",
        sidecarId: "sidecar",
      });
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");
    });

    test.each(["none", "other status", "future"] as const)(
      "a confirmed kept copy with %s retention work is not revisited",
      async (policy) => {
        await h.db
          .update(workflowRun)
          .set({
            status: "completed",
            endedAt: current,
            publicKey: "ab".repeat(32),
            lifecyclePolicy:
              policy === "none"
                ? null
                : policy === "other status"
                  ? { capacityRetention: { failed: "1h" } }
                  : { capacityRetention: { completed: "1h" } },
            capacityReleaseAt:
              policy === "future" ? new Date(current.getTime() + 60_000) : null,
          })
          .where(eq(workflowRun.id, runId));
        const lifecycle = service();
        expect(await lifecycle.reconcileNext()).toBe(false);
        current = new Date(current.getTime() + 1000);
        expect(await lifecycle.reconcileNext()).toBe(false);
        expect(
          (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
        ).toBe("allocated");
      },
    );

    test("kept copies still recover accepted history and release when their deadline arrives", async () => {
      const deadline = new Date(current.getTime() + 60_000);
      await h.db
        .update(workflowRun)
        .set({
          status: "completed",
          endedAt: current,
          publicKey: "ab".repeat(32),
          lifecyclePolicy: null,
          capacityReleaseAt: deadline,
        })
        .where(eq(workflowRun.id, runId));
      await leavePending();
      const lifecycle = service();
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(await pendingIds()).toEqual([]);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("allocated");
      current = new Date(current.getTime() + 1000);
      expect(await lifecycle.reconcileNext()).toBe(false);
      current = deadline;
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.status,
      ).toBe("releasing");
    });

    test("retention waits quietly while disconnected and resumes on a current connection", async () => {
      await h.db
        .update(workflowRun)
        .set({
          status: "completed",
          endedAt: current,
          publicKey: "ab".repeat(32),
          lifecyclePolicy: null,
        })
        .where(eq(workflowRun.id, runId));
      let connected = false;
      let kept = false;
      const requests: boolean[] = [];
      const lifecycle = service({
        retentionRouter: {
          getRetentionCandidates: () =>
            connected && !kept
              ? [{ allocationId, generation: 1, sidecarId: "sidecar" }]
              : [],
          holdsAllocatedBinding: () => connected,
          getAllocationRetention: () => (kept ? "kept" : undefined),
          retainAllocation: async () => {
            requests.push(connected);
            kept = true;
            return "kept";
          },
        },
      });
      expect(await lifecycle.reconcileNext()).toBe(false);
      expect(requests).toEqual([]);
      connected = true;
      current = new Date(current.getTime() + 1000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(requests).toEqual([true]);
      connected = false;
      kept = false;
      await leavePending();
      current = new Date(current.getTime() + 1000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(await pendingIds()).toEqual([]);
      expect(requests).toEqual([true]);
    });

    test("retention scheduling ignores a candidate for an older generation", async () => {
      await h.db
        .update(workflowRun)
        .set({
          status: "completed",
          endedAt: current,
          publicKey: "ab".repeat(32),
          lifecyclePolicy: null,
        })
        .where(eq(workflowRun.id, runId));
      let generation = 0;
      let requests = 0;
      const lifecycle = service({
        retentionRouter: {
          getRetentionCandidates: () => [
            { allocationId, generation, sidecarId: "sidecar" },
          ],
          holdsAllocatedBinding: () => true,
          getAllocationRetention: () => undefined,
          retainAllocation: async (target) => {
            expect(target.generation).toBe(1);
            requests += 1;
            return "kept";
          },
        },
      });
      expect(await lifecycle.reconcileNext()).toBe(false);
      expect(requests).toBe(0);
      generation = 1;
      current = new Date(current.getTime() + 1000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(requests).toBe(1);
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

    test("a terminal anchor cannot release capacity while a child's accepted history is pending", async () => {
      await h.db
        .update(workflowRun)
        .set({ status: "completed", endedAt: current, lifecyclePolicy: null })
        .where(eq(workflowRun.id, runId));
      await addChild("run_child");
      await leavePending();
      let unreadable = true;
      const lifecycle = service({
        runReader: {
          readRunEvents: async (_repo, _ref, id) => {
            if (unreadable) throw new Error("Unreadable repository");
            return id === "run_child"
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

      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe(
        "history_pending",
      );
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.capacityReleaseAt,
      ).toBeNull();
      expect(await pendingIds()).toEqual([`wpp_${runId}`]);

      unreadable = false;
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe("pending");
      expect(await runStates()).toEqual({
        [runId]: "completed",
        run_child: "completed",
      });
      expect(await pendingIds()).toEqual([]);
    });

    test("released capacity still reports unreconciled history as pending", async () => {
      await h.db
        .update(workflowRun)
        .set({ status: "completed", endedAt: current, lifecyclePolicy: null })
        .where(eq(workflowRun.id, runId));
      await h.db
        .update(sidecarAllocation)
        .set({ status: "released" })
        .where(eq(sidecarAllocation.id, allocationId));
      await addChild("run_child");
      await leavePending();
      const lifecycle = service({
        runReader: {
          readRunEvents: async () => {
            throw new Error("Unreadable repository");
          },
        },
      });

      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe(
        "history_pending",
      );
      expect(await pendingIds()).toEqual([`wpp_${runId}`]);
    });

    test("permanent cleanup failure is not hidden behind unreconciled history", async () => {
      await h.db
        .update(workflowRun)
        .set({ status: "completed", endedAt: current, lifecyclePolicy: null })
        .where(eq(workflowRun.id, runId));
      await h.db
        .update(sidecarAllocation)
        .set({
          status: "destroy_failed",
          failureCode: "provider_refused",
          failureMessage: "Manual cleanup needed",
        })
        .where(eq(sidecarAllocation.id, allocationId));
      await addChild("run_child");
      await leavePending();
      const lifecycle = service({
        runReader: {
          readRunEvents: async () => {
            throw new Error("Unreadable repository");
          },
        },
      });

      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe(
        "cleanup_failed",
      );
      expect(await pendingIds()).toEqual([`wpp_${runId}`]);
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
        allocationStore: createSidecarAllocationStore(h.db, {
          getSidecarInventory: () => ({
            capacity: DEFAULT_SIDECAR_CAPACITY,
            retainedIncarnations: [],
          }),
        }),
        plugins: createSidecarPluginRegistry({
          provisioners: [
            {
              id: "test",
              apiVersion: 2,
              bindingFingerprint: "test:1",
              capabilities: [],
              ensure: async () => {
                throw new Error(
                  "terminal capacity must not be provisioned again",
                );
              },
              destroy: async (request) => {
                destroyed.push(request.allocationId);
                return { kind: "destroyed", cleanup: "confirmed" };
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

    test.each([
      [SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE, "cleanup_exhausted"],
      [SIDECAR_CLEANUP_DISCONNECT_TIMEOUT_FAILURE_CODE, "cleanup_disconnected"],
    ] as const)(
      "reports cleanup failure %s without resetting its budget",
      async (failureCode, result) => {
        await h.db
          .update(workflowRun)
          .set({ status: "failed", endedAt: current })
          .where(eq(workflowRun.id, runId));
        await h.db
          .update(sidecarAllocation)
          .set({
            status: "destroy_failed",
            failureCode,
            destroyAttempts: 10,
            nextAttemptAt: null,
          })
          .where(eq(sidecarAllocation.id, allocationId));
        expect(await service().releaseCapacity(tenantId, runId)).toBe(result);
        expect(
          await h.db.query.sidecarAllocation.findFirst({
            where: eq(sidecarAllocation.id, allocationId),
          }),
        ).toMatchObject({
          status: "destroy_failed",
          destroyAttempts: 10,
          nextAttemptAt: null,
        });
      },
    );

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

    test("an operator releases stopped cleanup only at the inspected generation", async () => {
      expect(
        await service().releaseStoppedCleanup(tenantId, runId, 1, "operator"),
      ).toBe("not_stopped");
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
      const retired: unknown[] = [];
      const lifecycle = service({
        retentionRouter: {
          retireAllocation: (target) => {
            retired.push(target);
          },
        },
      });
      expect(
        await lifecycle.releaseStoppedCleanup("other", runId, 1, "operator"),
      ).toBe("not_found");
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation?.generation,
      ).toBe(1);
      expect(
        await lifecycle.releaseStoppedCleanup(tenantId, runId, 2, "operator"),
      ).toBe("generation_mismatch");
      expect(retired).toEqual([]);

      expect(
        await lifecycle.releaseStoppedCleanup(tenantId, runId, 1, "operator"),
      ).toBe("released");
      expect(retired).toEqual([{ allocationId, generation: 1 }]);
      expect(
        (await lifecycle.getStatus(tenantId, runId))?.allocation,
      ).toMatchObject({
        status: "released",
        failureCode: SIDECAR_CLEANUP_OPERATOR_RELEASED_FAILURE_CODE,
        failureMessage:
          "Released by operator after cleanup stopped with provider_refused: Manual cleanup needed",
      });
      // A retry finds the release it already made and does not repeat it.
      expect(
        await lifecycle.releaseStoppedCleanup(tenantId, runId, 1, "operator"),
      ).toBe("released");
      expect(retired).toHaveLength(1);
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe("released");
    });

    test("concurrent operator releases both report the release", async () => {
      await h.db
        .update(workflowRun)
        .set({ status: "failed", endedAt: current })
        .where(eq(workflowRun.id, runId));
      await h.db
        .update(sidecarAllocation)
        .set({ status: "destroy_failed", failureCode: "provider_refused" })
        .where(eq(sidecarAllocation.id, allocationId));
      const retired: unknown[] = [];
      const lifecycle = service({
        retentionRouter: {
          retireAllocation: (target) => {
            retired.push(target);
          },
        },
      });
      const locked = Promise.withResolvers<number>();
      const unlock = Promise.withResolvers<undefined>();
      // Holding the sidecar lock lets both requests read the stopped row
      // before either one can release it.
      const blocker = h.db.transaction(async (tx) => {
        await tx
          .select()
          .from(sidecar)
          .where(eq(sidecar.id, "sidecar"))
          .for("update");
        const [backend] = await tx.execute(sql`select pg_backend_pid() as pid`);
        const pid = backend?.["pid"];
        if (typeof pid !== "number") throw new Error("Missing backend PID");
        locked.resolve(pid);
        await unlock.promise;
      });
      void blocker.catch(locked.reject);
      const blockerPid = await locked.promise;
      let settled = 0;
      const releases = [
        lifecycle.releaseStoppedCleanup(tenantId, runId, 1, "operator"),
        lifecycle.releaseStoppedCleanup(tenantId, runId, 1, "operator"),
      ];
      const finished = Promise.allSettled(
        releases.map((release) =>
          release.finally(() => {
            settled += 1;
          }),
        ),
      );
      try {
        // Postgres exposes lock waits only through its activity view. A
        // second row-lock waiter queues behind the first, not the blocker.
        for (;;) {
          const [waiters] = await h.db.execute(sql`
            with recursive waiter(pid) as (
              select pid from pg_stat_activity
              where ${blockerPid} = any(pg_blocking_pids(pid))
              union
              select activity.pid from pg_stat_activity activity
              join waiter on waiter.pid = any(pg_blocking_pids(activity.pid))
            )
            select count(*)::int as count from waiter
          `);
          if (waiters?.["count"] === 2) break;
          if (settled > 0)
            throw new Error("A release did not wait for the sidecar lock");
          await new Promise<void>((resolve) => setImmediate(resolve));
        }
      } finally {
        unlock.resolve(undefined);
        await blocker;
        await finished;
      }
      expect(await Promise.all(releases)).toEqual(["released", "released"]);
      expect(retired).toEqual([{ allocationId, generation: 1 }]);
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
      expect(reads).toBe(0);
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

    test.each(["unreachable", "initializing", "unconfirmed"] as const)(
      "a worker that stays %s is force-released after a short grace",
      async (condition) => {
        await expire();
        const options = {
          sendControl: async (
            _target: unknown,
            command: { action: "cancel" | "stop" },
          ) => {
            if (command.action !== "stop") return;
            if (condition === "initializing")
              throw new WorkflowControlInitializingError();
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

    function notConnected(since: Date) {
      return new WorkflowControlNotConnectedError(
        new SidecarNotConnectedError(
          `Allocated sidecar is not connected for allocation ${allocationId} generation 1`,
          since,
        ),
      );
    }

    // A sidecar that is never connected to this Hub, since `since`.
    function goneSince(since: Date) {
      const actions: string[] = [];
      return {
        actions,
        sendControl: async (
          _target: unknown,
          command: { action: "cancel" | "stop" },
        ) => {
          actions.push(command.action);
          throw notConnected(since);
        },
      };
    }

    async function allocationState() {
      return h.db.query.sidecarAllocation.findFirst({
        where: eq(sidecarAllocation.id, allocationId),
        columns: { status: true, failureCode: true, failureMessage: true },
      });
    }

    function advance(ms: number) {
      current = new Date(current.getTime() + ms);
    }

    test("an operator cancel releases a sidecar gone past the reconnect grace on the first sweep", async () => {
      const gone = goneSince(new Date(current.getTime() - 20_000));
      const lifecycle = service({ sendControl: gone.sendControl });
      await lifecycle.requestCancellation(tenantId, runId, "User cancelled");

      await lifecycle.reconcileNext();
      expect(gone.actions).toEqual(["cancel"]);
      expect(await allocationState()).toMatchObject({
        status: "releasing",
        failureCode: "workflow_cancel_unreachable",
        failureMessage: expect.stringContaining("not connected"),
      });
      expect((await lifecycle.getStatus(tenantId, runId))?.status).toBe(
        "running",
      );
    });

    test("an operator cancel releases a gone sidecar only once the full reconnect grace has passed", async () => {
      const gone = goneSince(current);
      const lifecycle = service({ sendControl: gone.sendControl });
      await lifecycle.requestCancellation(tenantId, runId, "User cancelled");

      advance(14_000);
      await lifecycle.reconcileNext();
      expect((await allocationState())?.status).toBe("allocated");

      advance(1_000);
      await lifecycle.reconcileNext();
      expect(await allocationState()).toMatchObject({
        status: "releasing",
        failureCode: "workflow_cancel_unreachable",
      });
    });

    test("a sidecar that reconnects inside the reconnect grace finishes an operator cancel normally", async () => {
      let reachable = false;
      const since = new Date(current.getTime() - 2_000);
      const actions: string[] = [];
      const lifecycle = service({
        cancelGraceMs: 10_000,
        sendControl: async (_target, command) => {
          actions.push(command.action);
          if (!reachable) throw notConnected(since);
        },
      });
      await lifecycle.requestCancellation(tenantId, runId, "User cancelled");

      await lifecycle.reconcileNext();
      expect((await allocationState())?.status).toBe("allocated");

      reachable = true;
      advance(1_000);
      await lifecycle.reconcileNext();
      expect(actions).toEqual(["cancel", "cancel"]);
      expect(await lifecycle.getStatus(tenantId, runId)).toMatchObject({
        status: "running",
        allocation: { status: "allocated" },
      });

      advance(10_000);
      await lifecycle.reconcileNext();
      expect(actions).toEqual(["cancel", "cancel", "stop"]);
      expect(await lifecycle.getStatus(tenantId, runId)).toMatchObject({
        status: "cancelled",
        allocation: { status: "allocated" },
      });
    });

    test("an expiry stop of a gone sidecar still waits out the unreachable stop grace", async () => {
      await expire();
      const gone = goneSince(new Date(current.getTime() - 600_000));
      const lifecycle = service({ sendControl: gone.sendControl });

      await lifecycle.reconcileNext();
      advance(1_000);
      await lifecycle.reconcileNext();
      expect(gone.actions).toEqual(["cancel", "stop"]);
      expect((await allocationState())?.status).toBe("allocated");

      advance(59_000);
      await lifecycle.reconcileNext();
      expect((await allocationState())?.status).toBe("allocated");

      advance(1_000);
      await lifecycle.reconcileNext();
      expect(await allocationState()).toMatchObject({
        status: "releasing",
        failureCode: "workflow_stop_failed",
      });
    });

    test("an operator cancel keeps retrying a worker that is unreachable for another reason", async () => {
      const lifecycle = service({
        sendControl: async () => {
          throw new WorkflowControlUnreachableError(
            "Sidecar disconnected before the acknowledgement",
          );
        },
      });
      await lifecycle.requestCancellation(tenantId, runId, "User cancelled");

      await lifecycle.reconcileNext();
      advance(20_000);
      await lifecycle.reconcileNext();
      expect((await allocationState())?.status).toBe("allocated");
    });

    test("an operator request after an expiry cancel releases a gone sidecar", async () => {
      await expire();
      const gone = goneSince(new Date(current.getTime() - 20_000));
      const lifecycle = service({ sendControl: gone.sendControl });

      await lifecycle.reconcileNext();
      expect((await allocationState())?.status).toBe("allocated");

      advance(500);
      await lifecycle.requestCancellation(tenantId, runId, "User cancelled");
      advance(1_000);
      await lifecycle.reconcileNext();
      expect(await allocationState()).toMatchObject({
        status: "releasing",
        failureCode: "workflow_cancel_unreachable",
      });
    });

    test("an operator cancel release that a reconciliation lease blocks is retried on the next sweep", async () => {
      const gone = goneSince(new Date(current.getTime() - 20_000));
      const lifecycle = service({ sendControl: gone.sendControl });
      await lifecycle.requestCancellation(tenantId, runId, "User cancelled");
      // The lease compares against the database clock, not the test clock.
      await h.db
        .update(sidecarAllocation)
        .set({
          reconciliationLeaseId: "lease",
          reconciliationLeaseExpiresAt: new Date(Date.now() + 3_600_000),
        })
        .where(eq(sidecarAllocation.id, allocationId));

      await lifecycle.reconcileNext();
      expect((await allocationState())?.status).toBe("allocated");

      await h.db
        .update(sidecarAllocation)
        .set({
          reconciliationLeaseId: null,
          reconciliationLeaseExpiresAt: null,
        })
        .where(eq(sidecarAllocation.id, allocationId));
      advance(1_000);
      await lifecycle.reconcileNext();
      expect(await allocationState()).toMatchObject({
        status: "releasing",
        failureCode: "workflow_cancel_unreachable",
      });
    });

    test("an operator cancel of a gone sidecar records cancellation once the provisioner confirms destruction", async () => {
      const gone = goneSince(new Date(current.getTime() - 20_000));
      const lifecycle = service({ sendControl: gone.sendControl });
      await lifecycle.requestCancellation(tenantId, runId, "User cancelled");
      await lifecycle.reconcileNext();
      expect((await allocationState())?.status).toBe("releasing");

      const destroyed: string[] = [];
      const reconciler = createSidecarAllocationReconciler({
        allocationStore: createSidecarAllocationStore(h.db, {
          getSidecarInventory: () => ({
            capacity: DEFAULT_SIDECAR_CAPACITY,
            retainedIncarnations: [],
          }),
        }),
        plugins: createSidecarPluginRegistry({
          provisioners: [
            {
              id: "test",
              apiVersion: 2,
              bindingFingerprint: "test:1",
              capabilities: [],
              ensure: async () => {
                throw new Error(
                  "cancelled capacity must not be provisioned again",
                );
              },
              destroy: async (request) => {
                destroyed.push(request.allocationId);
                return { kind: "destroyed", cleanup: "confirmed" };
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
          isAllocatedSidecarReady: async () => false,
          holdsAllocatedBinding: () => false,
          waitForAllocatedSidecar: async () => undefined,
          reportedDeploymentFailure: () => undefined,
          syncSidecar: async () => undefined,
        },
        hubWebSocketUrl: "ws://hub.example/ws",
        now: () => current,
      });
      expect(await reconciler.reconcileNext()).toBe(true);
      expect(destroyed).toEqual([allocationId]);
      expect((await allocationState())?.status).toBe("released");

      advance(1_000);
      await lifecycle.reconcileNext();
      expect(await lifecycle.getStatus(tenantId, runId)).toMatchObject({
        status: "cancelled",
        cancellationReason: "User cancelled",
        allocation: {
          status: "released",
          failureCode: "workflow_cancel_unreachable",
        },
      });
    });

    async function cancelledCleanup(confirmed: boolean) {
      const dispatches = createWorkflowRunDispatchStore(h.db);
      for (const id of ["pending", "acknowledged"]) {
        await dispatches.enqueue({
          id,
          anchorRunId: runId,
          messageId: id,
          senderAddress: "sender@example.test",
          rawMessage: new Uint8Array([1]),
          stepGrants: [],
          now: current,
        });
      }
      expect(
        await dispatches.acknowledge({
          allocationId,
          anchorRunId: runId,
          generation: 1,
          messageId: "acknowledged",
          now: current,
        }),
      ).toMatchObject({ status: "acknowledged" });
      const lifecycle = service();
      await lifecycle.requestCancellation(tenantId, runId, "User cancelled");
      const store = createSidecarAllocationStore(h.db);
      const releasing = await store.beginRelease({
        allocationId,
        expectedStatus: "allocated",
        expectedGeneration: 1,
        failureCode: "workflow_stop_failed",
        failureMessage: "Stop was not acknowledged",
        now: current,
      });
      if (releasing === null) throw new Error("Expected release");
      const leaseId = "cancel-cleanup";
      expect(
        (
          await store.claimNextReconcilable({
            leaseId,
            leaseDurationMs: 60_000,
          })
        )?.id,
      ).toBe(allocationId);
      if (confirmed)
        expect(
          await store.confirmDeploymentCleanup({
            allocationId,
            generation: releasing.generation,
            expectedLeaseId: leaseId,
            now: current,
          }),
        ).toBe(true);
      return {
        lifecycle,
        dispatches,
        release: async () => {
          const target = {
            allocationId,
            generation: releasing.generation,
            expectedLeaseId: leaseId,
            now: current,
          };
          expect(await store.confirmDeploymentCleanup(target)).toBe(true);
          expect(await store.confirmProviderRelease(target)).toBe(true);
          return store.markReleased(target);
        },
        fail: (code = SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE) =>
          store.markDestroyFailed({
            allocationId,
            expectedGeneration: releasing.generation,
            expectedLeaseId: leaseId,
            code,
            message: "Provider hold could not be released",
            now: current,
          }),
      };
    }

    test.each([
      [SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE, "before"],
      [SIDECAR_CLEANUP_RETRY_EXHAUSTED_FAILURE_CODE, "after"],
      ["provider_rejected", "before"],
      ["provider_rejected", "after"],
      ["released", "before"],
      ["released", "after"],
    ] as const)(
      "confirmed cancellation owns dispatches when cleanup ends as %s %s the sweep",
      async (outcome, order) => {
        const { lifecycle, dispatches, release, fail } =
          await cancelledCleanup(true);
        const finish = () =>
          outcome === "released" ? release() : fail(outcome);
        if (order === "before") {
          await finish();
          for (const id of ["pending", "acknowledged"]) {
            expect(await dispatches.findById(id)).toMatchObject({
              status: id,
              failureCode: null,
            });
          }
          expect(
            await dispatches.claimNextPending({
              leaseId: "cancelled-dispatch",
              leaseDurationMs: 60_000,
            }),
          ).toBeNull();
        }
        await lifecycle.reconcileNext();
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, runId),
          }),
        ).toMatchObject({
          status: "cancelled",
          failureCode: null,
          cancellationReason: "User cancelled",
        });
        if (order === "after") await finish();
        expect(
          await h.db.query.sidecarAllocation.findFirst({
            where: eq(sidecarAllocation.id, allocationId),
          }),
        ).toMatchObject({
          status: outcome === "released" ? "released" : "destroy_failed",
          deploymentCleanupConfirmed: true,
          failureCode:
            outcome === "released" ? "workflow_stop_failed" : outcome,
        });
        for (const id of ["pending", "acknowledged"]) {
          expect(await dispatches.findById(id)).toMatchObject({
            status: "abandoned",
            failureCode: "workflow_cancelled",
            failureMessage: "User cancelled",
          });
        }
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, runId),
          }),
        ).toMatchObject({ status: "cancelled", failureCode: null });
        expect(
          await h.db.query.principal.findFirst({
            where: eq(principal.id, "principal"),
          }),
        ).toMatchObject({ status: "deactivated" });
      },
    );

    test.each(["released", "provider_rejected"] as const)(
      "cleanup ending as %s leaves dispatches to cancellation after history recovery",
      async (outcome) => {
        const { dispatches, release, fail } = await cancelledCleanup(true);
        await leavePending();
        if (outcome === "released") await release();
        else await fail(outcome);
        await service({
          runReader: {
            readLatestRunEvents: async () => {
              throw new Error("Accepted history temporarily unavailable");
            },
          },
        }).reconcileNext();
        expect(await pendingIds()).toHaveLength(1);
        for (const id of ["pending", "acknowledged"]) {
          expect(await dispatches.findById(id)).toMatchObject({
            status: id,
            failureCode: null,
          });
        }
        expect(
          await dispatches.claimNextPending({
            leaseId: "awaiting-cancellation-history",
            leaseDurationMs: 60_000,
          }),
        ).toBeNull();
        await service({
          runReader: { hasRepository: async () => true },
        }).reconcileNext();
        expect(await pendingIds()).toHaveLength(0);
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, runId),
          }),
        ).toMatchObject({ status: "cancelled", failureCode: null });
        for (const id of ["pending", "acknowledged"]) {
          expect(await dispatches.findById(id)).toMatchObject({
            status: "abandoned",
            failureCode: "workflow_cancelled",
            failureMessage: "User cancelled",
          });
        }
      },
    );

    test.each(["RunCompleted", "RunFailed"] as const)(
      "confirmed cancellation waits for accepted history and preserves %s",
      async (outcome) => {
        const { fail } = await cancelledCleanup(true);
        await leavePending();
        await fail();
        await service({
          runReader: {
            readLatestRunEvents: async () => {
              throw new Error("Accepted history temporarily unavailable");
            },
          },
        }).reconcileNext();
        expect(await pendingIds()).toHaveLength(1);
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, runId),
          }),
        ).toMatchObject({
          status: "running",
          failureCode: null,
          infrastructureFailedAt: null,
        });
        complete(outcome);
        await service({
          runReader: { hasRepository: async () => true },
        }).reconcileNext();
        expect(await pendingIds()).toHaveLength(0);
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, runId),
          }),
        ).toMatchObject({
          status: outcome === "RunCompleted" ? "completed" : "failed",
          failureCode: null,
        });
      },
    );

    test("confirmed cancellation does not replace an earlier deferred workflow failure", async () => {
      const { lifecycle, fail } = await cancelledCleanup(true);
      await h.db
        .update(workflowRun)
        .set({
          failureCode: "workflow_error",
          failureMessage: "The workflow failed before cleanup",
          infrastructureFailedAt: current,
        })
        .where(eq(workflowRun.id, runId));
      await fail();
      await lifecycle.reconcileNext();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, runId),
        }),
      ).toMatchObject({
        status: "failed",
        failureCode: "workflow_error",
        failureMessage: "The workflow failed before cleanup",
      });
    });

    test.each([
      "missing provisioner",
      "last ensure throws",
      "last ensure rejects",
    ] as const)(
      "pending cancellation survives %s and settles dispatches after history recovery",
      async (failure) => {
        const dispatches = createWorkflowRunDispatchStore(h.db);
        for (const id of ["pending", "acknowledged"]) {
          await dispatches.enqueue({
            id,
            anchorRunId: runId,
            messageId: id,
            senderAddress: "sender@example.test",
            rawMessage: new Uint8Array([1]),
            stepGrants: [],
            now: current,
          });
        }
        await dispatches.acknowledge({
          allocationId,
          anchorRunId: runId,
          generation: 1,
          messageId: "acknowledged",
          now: current,
        });
        const store = createSidecarAllocationStore(h.db, {
          getSidecarInventory: () => ({
            capacity: DEFAULT_SIDECAR_CAPACITY,
            retainedIncarnations: [],
          }),
        });
        const lifecycle = service();
        const cancel = async () => {
          expect(
            await lifecycle.requestCancellation(
              tenantId,
              runId,
              "User cancelled",
            ),
          ).toBe("pending");
          await leavePending();
        };
        await h.db
          .update(sidecarAllocation)
          .set({
            status: "replacing",
            generation: 2,
            ensureAcceptedGeneration: null,
            ensureAttempts: failure === "missing provisioner" ? 1 : 9,
            deploymentCleanupConfirmed: true,
            providerHoldReleased: true,
            nextAttemptAt: current,
          })
          .where(eq(sidecarAllocation.id, allocationId));
        const provisioner = {
          id: "test",
          apiVersion: 2 as const,
          bindingFingerprint: "test:1",
          capabilities: [],
          ensure: async () => {
            await cancel();
            if (failure === "last ensure throws")
              throw new Error("Provisioning unavailable");
            return {
              kind: "rejected" as const,
              code: "provider_unavailable",
              message: "Try again",
              retryable: true,
            };
          },
          destroy: async () => ({
            kind: "destroyed" as const,
            cleanup: "confirmed" as const,
          }),
        };
        const reconciler = createSidecarAllocationReconciler({
          allocationStore: store,
          plugins: createSidecarPluginRegistry({
            provisioners:
              failure === "missing provisioner" ? [] : [provisioner],
          }),
          router: {
            fenceAllocation: () => undefined,
            retireAllocation: () => undefined,
            getCleanupConnection: () => undefined,
            holdsAllocatedBinding: () => false,
            isAllocatedSidecarReady: async () => false,
            reportedDeploymentFailure: () => undefined,
            syncSidecar: async () => undefined,
            waitForAllocatedSidecar: async () => undefined,
            waitForSidecarInventory: async () => undefined,
            undeployAllocation: async () => undefined,
          },
          hubWebSocketUrl: "wss://hub.example/ws",
          now: () => current,
        });
        if (failure === "missing provisioner") await cancel();
        expect(await reconciler.reconcileNext()).toBe(true);
        if (failure === "missing provisioner") {
          current = new Date(current.getTime() + 300_000);
          await store.wakeReconciliation(allocationId, 2);
          expect(await reconciler.reconcileNext()).toBe(true);
        }
        expect(await store.findById(allocationId)).toMatchObject({
          status: failure === "missing provisioner" ? "failed" : "releasing",
          failureCode:
            failure === "missing provisioner"
              ? "provisioner_unavailable"
              : "sidecar_provisioning_retry_exhausted",
          deploymentCleanupConfirmed: true,
        });
        await service({
          runReader: {
            readLatestRunEvents: async () => {
              throw new Error("Accepted history temporarily unavailable");
            },
          },
        }).reconcileNext();
        expect(await pendingIds()).toHaveLength(1);
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, runId),
          }),
        ).toMatchObject({
          status: "running",
          failureCode: null,
          infrastructureFailedAt: null,
          cancellationReason: "User cancelled",
        });
        for (const id of ["pending", "acknowledged"])
          expect(await dispatches.findById(id)).toMatchObject({
            status: id,
            failureCode: null,
          });
        await service({
          runReader: { hasRepository: async () => true },
        }).reconcileNext();
        expect(await pendingIds()).toHaveLength(0);
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, runId),
          }),
        ).toMatchObject({
          status: "cancelled",
          failureCode: null,
        });
        if (failure !== "missing provisioner") {
          expect(await reconciler.reconcileNext()).toBe(true);
          expect((await store.findById(allocationId))?.status).toBe("released");
        }
        for (const id of ["pending", "acknowledged"])
          expect(await dispatches.findById(id)).toMatchObject({
            status: "abandoned",
            failureCode: "workflow_cancelled",
            failureMessage: "User cancelled",
          });
      },
    );

    test.each([false, true])(
      "unrecoverable replacement preserves cancellation only after confirmed removal: %s",
      async (confirmed) => {
        const { lifecycle, dispatches } = await cancelledCleanup(confirmed);
        await h.db
          .update(sidecarAllocation)
          .set({ status: "replacing" })
          .where(eq(sidecarAllocation.id, allocationId));
        const store = createSidecarAllocationStore(h.db);
        expect(
          await store.beginUnrecoverableRelease({
            allocationId,
            expectedStatus: "replacing",
            expectedGeneration: 2,
            expectedLeaseId: "cancel-cleanup",
            failureCode: "sidecar_provisioning_retry_exhausted",
            failureMessage: "Provisioning exhausted",
            now: current,
          }),
        ).toMatchObject({
          status: "releasing",
          deploymentCleanupConfirmed: confirmed,
        });
        await lifecycle.reconcileNext();
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, runId),
          }),
        ).toMatchObject({
          status: confirmed ? "cancelled" : "failed",
          failureCode: confirmed
            ? null
            : "sidecar_provisioning_retry_exhausted",
        });
        for (const id of ["pending", "acknowledged"])
          expect(await dispatches.findById(id)).toMatchObject({
            status: "abandoned",
            failureCode: confirmed
              ? "workflow_cancelled"
              : "sidecar_provisioning_retry_exhausted",
          });
      },
    );

    test("unconfirmed cancellation keeps the cleanup failure outcome", async () => {
      const { lifecycle, fail } = await cancelledCleanup(false);
      await fail();
      await lifecycle.reconcileNext();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, runId),
        }),
      ).toMatchObject({
        status: "failed",
        failureCode: "workflow_stop_failed",
      });
    });

    test.each(["removed", "unconfirmed", "no infrastructure"] as const)(
      "capacity failure preserves cancellation only when no copy remains: %s",
      async (outcome) => {
        const { lifecycle, dispatches } = await cancelledCleanup(
          outcome !== "unconfirmed",
        );
        await h.db
          .update(sidecarAllocation)
          .set({
            status:
              outcome === "no infrastructure" ? "provisioning" : "allocated",
            ensureAcceptedGeneration: outcome === "unconfirmed" ? 2 : null,
          })
          .where(eq(sidecarAllocation.id, allocationId));
        const store = createSidecarAllocationStore(h.db);
        const target = {
          allocationId,
          expectedGeneration: 2,
          expectedLeaseId: "cancel-cleanup",
          now: current,
        };
        const failed =
          outcome === "no infrastructure"
            ? await store.failWithoutInfrastructure({
                ...target,
                expectedStatus: "provisioning",
                code: "capacity_failed",
                message: "Capacity unavailable",
              })
            : await store.beginUnrecoverableRelease({
                ...target,
                expectedStatus: "allocated",
                failureCode: "capacity_failed",
                failureMessage: "Capacity unavailable",
              });
        expect(failed?.status).toBe(
          outcome === "no infrastructure" ? "failed" : "releasing",
        );
        if (outcome !== "unconfirmed") {
          expect(
            await h.db.query.workflowRun.findFirst({
              where: eq(workflowRun.id, runId),
            }),
          ).toMatchObject({
            status: "running",
            failureCode: null,
            infrastructureFailedAt: null,
          });
          for (const id of ["pending", "acknowledged"])
            expect(await dispatches.findById(id)).toMatchObject({
              status: id,
              failureCode: null,
            });
        }
        await lifecycle.reconcileNext();
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, runId),
          }),
        ).toMatchObject({
          status: outcome === "unconfirmed" ? "failed" : "cancelled",
          failureCode: outcome === "unconfirmed" ? "capacity_failed" : null,
        });
        for (const id of ["pending", "acknowledged"])
          expect(await dispatches.findById(id)).toMatchObject({
            status: "abandoned",
            failureCode:
              outcome === "unconfirmed"
                ? "capacity_failed"
                : "workflow_cancelled",
          });
      },
    );

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
      for (const { id } of await h.db
        .select({ id: principal.id })
        .from(principal)) {
        await seedRunToken(h.db, { id: `rtk_${id}`, principalId: id });
      }
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
      expect(await h.db.select().from(runToken)).toEqual([]);
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

    // getStatus does not return the operator request time.
    async function readOperatorCancellationRequestedAt() {
      const [row] = await h.db
        .select({ at: workflowRun.operatorCancellationRequestedAt })
        .from(workflowRun)
        .where(eq(workflowRun.id, runId));
      return row?.at;
    }

    async function expireAndSweep() {
      await expire();
      await service({ sendControl: async () => undefined }).reconcileNext();
    }

    test("an operator cancellation request records that an operator asked", async () => {
      expect(
        await service().requestCancellation(
          tenantId,
          runId,
          "Operator request",
        ),
      ).toBe("pending");
      expect(await readOperatorCancellationRequestedAt()).toEqual(current);
    });

    test("an expiry cancellation does not record an operator request", async () => {
      await expireAndSweep();
      expect(
        (await service().getStatus(tenantId, runId))?.cancellationRequestedAt,
      ).toBe(current.toISOString());
      expect(await readOperatorCancellationRequestedAt()).toBeNull();
    });

    test("an operator request after an expiry cancellation keeps the expiry reason and deadline", async () => {
      await expireAndSweep();
      const expired = await service().getStatus(tenantId, runId);
      current = new Date(current.getTime() + 500);
      expect(
        await service().requestCancellation(
          tenantId,
          runId,
          "Operator request",
        ),
      ).toBe("pending");
      expect(await readOperatorCancellationRequestedAt()).toEqual(current);
      const status = await service().getStatus(tenantId, runId);
      expect(status?.cancellationReason).toBe(
        "Maximum deployment lifetime exceeded",
      );
      expect(status?.cancellationDeadline).toBe(expired?.cancellationDeadline);
      expect(status?.cancellationRequestedAt).toBe(
        expired?.cancellationRequestedAt,
      );
    });

    test("a repeated operator request keeps the first operator request time", async () => {
      await service().requestCancellation(tenantId, runId, "Operator request");
      const first = new Date(current);
      current = new Date(current.getTime() + 500);
      await service().requestCancellation(tenantId, runId, "Another request");
      expect(await readOperatorCancellationRequestedAt()).toEqual(first);
    });

    test("an expiry after an operator request keeps the operator request", async () => {
      await service().requestCancellation(tenantId, runId, "Operator request");
      const requested = await service().getStatus(tenantId, runId);
      const operatorAt = await readOperatorCancellationRequestedAt();
      await expireAndSweep();
      const status = await service().getStatus(tenantId, runId);
      expect(status?.cancellationReason).toBe("Operator request");
      expect(status?.cancellationDeadline).toBe(
        requested?.cancellationDeadline,
      );
      expect(await readOperatorCancellationRequestedAt()).toEqual(operatorAt);
    });

    test("an operator request for a terminal run records no operator request", async () => {
      await h.db
        .update(workflowRun)
        .set({ status: "completed" })
        .where(eq(workflowRun.id, runId));
      expect(
        await service().requestCancellation(
          tenantId,
          runId,
          "Operator request",
        ),
      ).toBe("terminal");
      expect(await readOperatorCancellationRequestedAt()).toBeNull();
    });

    // Bounds a request that must not wait on a lock, so a regression
    // fails here instead of at the test runner's timeout.
    async function withinSeconds<T>(work: Promise<T>): Promise<T> {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        return await Promise.race([
          work,
          new Promise<never>((_resolve, reject) => {
            timer = setTimeout(() => {
              reject(new Error("The request waited on a lock"));
            }, 5_000);
          }),
        ]);
      } finally {
        clearTimeout(timer);
      }
    }

    test("a cancellation request does not wait on a held allocation row", async () => {
      // Old unclaimed rows are what a Hub restart finds after a receive
      // whose projection did not finish.
      await leavePending();
      const locked = Promise.withResolvers<undefined>();
      const unlock = Promise.withResolvers<undefined>();
      // A receive holds the allocation row. The key-share lock on the
      // anchor stands in for a child run insert's foreign-key check, which
      // a FOR UPDATE request would wait on.
      const blocker = h.db.transaction(async (tx) => {
        await tx
          .select()
          .from(sidecarAllocation)
          .where(eq(sidecarAllocation.id, allocationId))
          .for("update");
        await tx
          .select()
          .from(workflowRun)
          .where(eq(workflowRun.id, runId))
          .for("key share");
        locked.resolve(undefined);
        await unlock.promise;
      });
      void blocker.catch(locked.reject);
      await locked.promise;
      const lifecycle = service({
        runReader: {
          readLatestRunEvents: async () => {
            throw new Error("A cancellation request read Git");
          },
        },
      });
      try {
        expect(
          await withinSeconds(
            lifecycle.requestCancellation(tenantId, runId, "Operator request"),
          ),
        ).toBe("pending");
      } finally {
        unlock.resolve(undefined);
        await blocker;
      }
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, runId),
        }),
      ).toMatchObject({
        status: "running",
        cancellationReason: "Operator request",
      });
    });

    test("a request for a run whose accepted history ended keeps that outcome", async () => {
      complete("RunCompleted");
      await leavePending();

      expect(
        await service().requestCancellation(
          tenantId,
          runId,
          "Operator request",
        ),
      ).toBe("pending");
      await service().reconcileNext();

      expect((await service().getStatus(tenantId, runId))?.status).toBe(
        "completed",
      );
    });

    test("a request that waits on the anchor keeps an earlier cancellation", async () => {
      const locked = Promise.withResolvers<number>();
      const unlock = Promise.withResolvers<undefined>();
      const expiredAt = new Date(current.getTime() - 500);
      const blocker = h.db.transaction(async (tx) => {
        await tx
          .select()
          .from(workflowRun)
          .where(eq(workflowRun.id, runId))
          .for("update");
        const [backend] = await tx.execute(sql`select pg_backend_pid() as pid`);
        const pid = backend?.["pid"];
        if (typeof pid !== "number") throw new Error("Missing backend PID");
        locked.resolve(pid);
        await unlock.promise;
        await tx
          .update(workflowRun)
          .set({
            cancellationRequestedAt: expiredAt,
            cancellationDeadline: expiredAt,
            cancellationReason: "Expired",
          })
          .where(eq(workflowRun.id, runId));
      });
      void blocker.catch(locked.reject);
      const blockerPid = await locked.promise;
      let settled = false;
      const request = service()
        .requestCancellation(tenantId, runId, "Operator request")
        .finally(() => {
          settled = true;
        });
      try {
        for (;;) {
          const [waiters] = await h.db.execute(sql`
            select count(*)::int as count from pg_stat_activity
            where ${blockerPid} = any(pg_blocking_pids(pid))
          `);
          if (waiters?.["count"] === 1) break;
          if (settled)
            throw new Error("The request did not wait for the anchor lock");
          await new Promise<void>((resolve) => setImmediate(resolve));
        }
      } finally {
        unlock.resolve(undefined);
        await blocker;
      }

      expect(await request).toBe("pending");
      expect(await service().getStatus(tenantId, runId)).toMatchObject({
        cancellationReason: "Expired",
        cancellationDeadline: expiredAt.toISOString(),
      });
    });

    test("a capacity failure that meets a request in progress leaves the run to cancellation", async () => {
      await h.db
        .update(sidecarAllocation)
        .set({ status: "provisioning", ensureAcceptedGeneration: null })
        .where(eq(sidecarAllocation.id, allocationId));
      const locked = Promise.withResolvers<number>();
      const unlock = Promise.withResolvers<undefined>();
      // The request holds the anchor lock that requestCancellation takes,
      // and commits its cancellation after the failure reaches the anchor.
      const request = h.db.transaction(async (tx) => {
        await tx
          .select()
          .from(workflowRun)
          .where(eq(workflowRun.id, runId))
          .for("no key update");
        const [backend] = await tx.execute(sql`select pg_backend_pid() as pid`);
        const pid = backend?.["pid"];
        if (typeof pid !== "number") throw new Error("Missing backend PID");
        locked.resolve(pid);
        await unlock.promise;
        await tx
          .update(workflowRun)
          .set({
            cancellationRequestedAt: current,
            cancellationDeadline: current,
            cancellationReason: "Operator request",
          })
          .where(eq(workflowRun.id, runId));
      });
      void request.catch(locked.reject);
      const requestPid = await locked.promise;
      const failure = createSidecarAllocationStore(
        h.db,
      ).failWithoutInfrastructure({
        allocationId,
        expectedStatus: "provisioning",
        expectedGeneration: 1,
        code: "capacity_failed",
        message: "Capacity unavailable",
        now: current,
      });
      let settled = false;
      const failed = failure.finally(() => {
        settled = true;
      });
      try {
        for (;;) {
          const [waiters] = await h.db.execute(sql`
            select count(*)::int as count from pg_stat_activity
            where ${requestPid} = any(pg_blocking_pids(pid))
          `);
          if (waiters?.["count"] === 1) break;
          if (settled)
            throw new Error("The capacity failure did not wait for the anchor");
          await new Promise<void>((resolve) => setImmediate(resolve));
        }
      } finally {
        unlock.resolve(undefined);
        await request;
      }

      expect((await failed)?.status).toBe("failed");
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, runId),
        }),
      ).toMatchObject({
        status: "running",
        failureCode: null,
        cancellationReason: "Operator request",
      });
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
      const allocationStore = createSidecarAllocationStore(h.db, {
        getSidecarInventory: () => ({
          capacity: DEFAULT_SIDECAR_CAPACITY,
          retainedIncarnations: [],
        }),
      });
      const reconciler = createSidecarAllocationReconciler({
        allocationStore,
        plugins: createSidecarPluginRegistry({
          provisioners: [
            {
              id: "test",
              apiVersion: 2,
              bindingFingerprint: "test:1",
              capabilities: [],
              ensure: async () => {
                started.resolve(undefined);
                return finishEnsure.promise;
              },
              destroy: async (request) => {
                expect(request.externalRef).toBe("vm-1");
                destroyed = true;
                return { kind: "destroyed", cleanup: "confirmed" };
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
          isAllocatedSidecarReady: async () => false,
          holdsAllocatedBinding: () => false,
          waitForAllocatedSidecar: async () => undefined,
          reportedDeploymentFailure: () => undefined,
          syncSidecar: async () => undefined,
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

    async function loseCapacity(failedAt: Date) {
      await h.db
        .update(sidecarAllocation)
        .set({
          reconciliationLeaseId: "lease",
          reconciliationLeaseExpiresAt: new Date(Date.now() + 3_600_000),
        })
        .where(eq(sidecarAllocation.id, allocationId));
      expect(
        await createSidecarAllocationStore(h.db).beginUnrecoverableRelease({
          expectedStatus: "allocated",
          allocationId,
          expectedGeneration: 1,
          expectedLeaseId: "lease",
          failureCode: "sidecar_lost",
          failureMessage: "The worker is gone",
          now: failedAt,
        }),
      ).not.toBeNull();
    }

    test("an unrecoverable capacity loss waits for accepted history before failing live runs", async () => {
      await addChild("run_child_done");
      await leavePending();
      const failedAt = current;
      await loseCapacity(failedAt);
      expect(await runStates()).toEqual({
        [runId]: "running",
        run_child_done: "running",
      });

      current = new Date(current.getTime() + 60_000);
      await service({
        runReader: childHistory({
          run_child_done: [
            {
              seq: 1,
              type: "RunCompleted",
              body: { type: "RunCompleted", at: failedAt.toISOString() },
            },
          ],
        }),
      }).reconcileNext();
      expect(await runStates()).toEqual({
        [runId]: "failed",
        run_child_done: "completed",
      });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, runId),
        }),
      ).toMatchObject({
        endedAt: failedAt,
        infrastructureFailedAt: null,
        failureCode: "sidecar_lost",
        failureMessage: "The worker is gone",
      });
      // A run that ended on its own carries no reason from the Hub.
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, "run_child_done"),
        }),
      ).toMatchObject({ failureCode: null, failureMessage: null });
      expect(await pendingIds()).toEqual([]);
    });

    test("an accepted anchor completion survives unrecoverable capacity loss", async () => {
      complete("RunCompleted");
      await leavePending();
      await loseCapacity(current);
      expect(await runStates()).toEqual({ [runId]: "running" });

      current = new Date(current.getTime() + 60_000);
      await service().reconcileNext();
      expect(await runStates()).toEqual({ [runId]: "completed" });
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, runId),
        }),
      ).toMatchObject({
        infrastructureFailedAt: null,
        failureCode: null,
        failureMessage: null,
      });
      expect(await pendingIds()).toEqual([]);
    });

    test("an unrecoverable capacity loss with nothing pending fails live runs immediately", async () => {
      await addChild("run_child_live");
      await loseCapacity(current);
      expect(await runStates()).toEqual({
        [runId]: "failed",
        run_child_live: "failed",
      });
      for (const id of [runId, "run_child_live"]) {
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, id),
          }),
        ).toMatchObject({
          infrastructureFailedAt: null,
          failureCode: "sidecar_lost",
          failureMessage: "The worker is gone",
        });
      }
    });

    test("a deferred capacity failure keeps its reason when the runs fail before it is applied", async () => {
      await leavePending();
      await loseCapacity(current);
      const lifecycle = service();
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe("live");
      expect(
        await createSidecarAllocationStore(h.db).markDestroyFailed({
          allocationId,
          expectedGeneration: 2,
          code: "destroy_failed",
          message: "The worker could not be destroyed",
          now: current,
        }),
      ).not.toBeNull();
      expect(await runStates()).toEqual({ [runId]: "failed" });

      current = new Date(current.getTime() + 1_000);
      await lifecycle.reconcileNext();
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, runId),
        }),
      ).toMatchObject({
        infrastructureFailedAt: null,
        failureCode: "sidecar_lost",
        failureMessage: "The worker is gone",
      });
    });

    test("a deferred capacity failure drops its reason when the run's own history ended it first", async () => {
      complete("RunFailed");
      await leavePending();
      await loseCapacity(current);
      const lifecycle = service();
      await lifecycle.releaseCapacity(tenantId, runId);
      expect(await runStates()).toEqual({ [runId]: "failed" });
      expect(
        await createSidecarAllocationStore(h.db).markDestroyFailed({
          allocationId,
          expectedGeneration: 2,
          code: "destroy_failed",
          message: "The worker could not be destroyed",
          now: current,
        }),
      ).not.toBeNull();

      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, runId),
        }),
      ).toMatchObject({
        status: "failed",
        infrastructureFailedAt: null,
        failureCode: null,
        failureMessage: null,
      });
    });

    test("a deferred capacity failure is applied after an explicit request reconciles history", async () => {
      await leavePending();
      const failedAt = current;
      await loseCapacity(failedAt);
      const lifecycle = service();
      expect(await lifecycle.releaseCapacity(tenantId, runId)).toBe("live");
      expect(await pendingIds()).toEqual([]);

      current = new Date(current.getTime() + 1_000);
      expect(await lifecycle.reconcileNext()).toBe(true);
      expect(await runStates()).toEqual({ [runId]: "failed" });
    });
  },
);
