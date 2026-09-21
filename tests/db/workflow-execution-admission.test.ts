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
  createSidecarAllocationStore,
  withExecutableWorkflowRun,
  WorkflowRunNotExecutableError,
} from "@intx/db";
import {
  sidecar,
  sidecarAllocation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants } from "@intx/test-harness/seed";
import {
  createWorkflowHistoryReceiveTracker,
  createSidecarRouter,
  createWorkflowLifecycleService,
  type SidecarRouterConfig,
} from "@intx/hub-sessions";
import type { HarnessConfig } from "@intx/types/runtime";
import { HubFrame } from "@intx/types/sidecar";
import { createMockWs } from "./sidecar-test-helpers";

const target = {
  allocationId: "allocation_admission",
  generation: 1,
  sidecarId: "sidecar_admission",
  tenantId: "tenant_admission",
  anchorRunId: "run_admission",
  workflowRunAddress: "run_admission@example.test",
};

const deployConfig: HarnessConfig = {
  sessionId: "session_admission",
  agentId: "workflow",
  tenantId: target.tenantId,
  principalId: "principal_admission",
  agentAddress: target.workflowRunAddress,
  systemPrompt: "test",
  tools: [],
  grants: [],
  sources: [],
  defaultSource: "test",
};

describe.skipIf(!harnessDbEnvAvailable())(
  "workflow execution admission",
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
      await seedTenants(h.db, [{ id: target.tenantId }]);
      await h.db.insert(workflowDefinition).values({
        id: "definition",
        tenantId: target.tenantId,
        name: "Example",
      });
      await h.db.insert(workflowRun).values({
        id: target.anchorRunId,
        anchorRunId: target.anchorRunId,
        tenantId: target.tenantId,
        definitionId: "definition",
        address: target.workflowRunAddress,
        status: "running",
      });
      await h.db
        .insert(sidecar)
        .values({ id: target.sidecarId, tokenHashSha256: new Uint8Array(32) });
      await h.db.insert(sidecarAllocation).values({
        id: target.allocationId,
        tenantId: target.tenantId,
        anchorRunId: target.anchorRunId,
        sidecarId: target.sidecarId,
        generation: 1,
        ensureAcceptedGeneration: 1,
        status: "allocated",
        provisionerId: "test",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "test:1",
      });
    });

    async function connectForInitialization(
      config: Partial<SidecarRouterConfig> = {},
    ) {
      const registered = Promise.withResolvers<undefined>();
      const router = createSidecarRouter({
        hubPublicKey: "a".repeat(64),
        authenticateSidecar: async () => ({ kind: "allocated", ...target }),
        validateSidecarIdentity: async () => true,
        withExecutableWorkflowRun: (identity, send, signal) =>
          withExecutableWorkflowRun(h.db, identity, send, signal),
        ...config,
      });
      router.events.on("sidecar.allocated.connected", () =>
        registered.resolve(undefined),
      );
      router.fenceAllocation(target.allocationId, target.generation);
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: target.sidecarId,
          token: "token",
          agentAddresses: [],
        }),
      );
      await registered.promise;
      return { router, ws };
    }

    test.each(["cancel", "stop", "expire"] as const)(
      "refuses a reserved deploy after %s during preparation",
      async (change) => {
        const { router, ws } = await connectForInitialization();
        const allocations = createSidecarAllocationStore(h.db);
        await h.db
          .update(sidecarAllocation)
          .set({
            reconciliationLeaseId: "initialization",
            reconciliationLeaseExpiresAt: new Date(Date.now() + 120_000),
          })
          .where(eq(sidecarAllocation.id, target.allocationId));
        const initialization = {
          ...target,
          leaseId: "initialization",
          signal: new AbortController().signal,
        };
        const prepared = Promise.withResolvers<undefined>();
        const release = Promise.withResolvers<undefined>();
        const before = [...ws.sent];
        const deployment = router
          .sendAgentDeployToAllocation(
            target,
            target.workflowRunAddress,
            deployConfig,
            undefined,
            initialization.signal,
            async () => {
              const reserved =
                await allocations.beginInitialization(initialization);
              if (reserved === null)
                throw new Error("Initialization not reserved");
              prepared.resolve(undefined);
              await release.promise;
            },
          )
          .catch((cause: unknown) => cause);
        try {
          await Promise.race([prepared.promise, deployment]);
          const lifecycle = createWorkflowLifecycleService({
            db: h.db,
            historyReceives: createWorkflowHistoryReceiveTracker(),
            runReader: {
              listRunIds: async () => [],
              readRunEvents: async () => [],
              readLatestRunEvents: async () => ({
                tip: null,
                events: new Map(),
              }),
              resolveRefTip: async () => null,
              hasRepository: async () => false,
            },
            cancelGraceMs: 0,
            sendControl: async () => undefined,
          });
          if (change === "expire") {
            await h.db
              .update(workflowRun)
              .set({ expiresAt: new Date(0) })
              .where(eq(workflowRun.id, target.anchorRunId));
          } else {
            await lifecycle.requestCancellation(
              target.tenantId,
              target.anchorRunId,
              "Stop",
            );
            if (change === "stop") {
              await lifecycle.reconcile();
              expect(
                (await lifecycle.getStatus(target.tenantId, target.anchorRunId))
                  ?.status,
              ).toBe("cancelled");
            }
          }
          release.resolve(undefined);
          const error = await deployment;
          expect(error).toMatchObject({ frameSent: false });
          if (!(error instanceof Error))
            throw new Error("Expected deploy failure");
          expect(error.cause).toBeInstanceOf(WorkflowRunNotExecutableError);
          expect(ws.sent).toEqual(before);
          expect(router.getRoutableAddresses()).toEqual([]);
          expect(
            await allocations.clearUnsentInitialization({
              ...initialization,
              previousPublicKey: null,
            }),
          ).toBe(true);
        } finally {
          release.resolve(undefined);
          await deployment;
          router.handleClose(ws);
        }
      },
    );

    test("refuses a restore when expiry wins while admission waits", async () => {
      const admitting = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      const { router, ws } = await connectForInitialization({
        withExecutableWorkflowRun: async (identity, send, signal) => {
          admitting.resolve(undefined);
          await release.promise;
          return withExecutableWorkflowRun(h.db, identity, send, signal);
        },
      });
      const before = [...ws.sent];
      const restoring = router
        .sendWorkflowRunPackToAllocation(
          target,
          target.workflowRunAddress,
          new Uint8Array([1]),
          "refs/heads/main",
          "a".repeat(40),
        )
        .catch((cause: unknown) => cause);
      try {
        await Promise.race([admitting.promise, restoring]);
        await h.db
          .update(workflowRun)
          .set({ expiresAt: new Date(0) })
          .where(eq(workflowRun.id, target.anchorRunId));
        release.resolve(undefined);
        expect(await restoring).toBeInstanceOf(WorkflowRunNotExecutableError);
        expect(ws.sent).toEqual(before);
      } finally {
        release.resolve(undefined);
        router.handleClose(ws);
        await restoring;
      }
    });

    test.each(["cancel", "expire"] as const)(
      "refuses staging frames after %s",
      async (change) => {
        const { router, ws } = await connectForInitialization();
        const stepAddress = "step_admission@example.test";
        await router.bindAllocatedStepRoute(target, stepAddress);
        try {
          await h.db
            .update(workflowRun)
            .set(
              change === "expire"
                ? { expiresAt: new Date(0) }
                : {
                    cancellationRequestedAt: new Date(),
                    cancellationDeadline: new Date(),
                    cancellationReason: "Stop",
                  },
            )
            .where(eq(workflowRun.id, target.anchorRunId));
          // A frame reaching the socket means admission let it through.
          const sent = Promise.withResolvers<"sent">();
          const send = ws.send.bind(ws);
          ws.send = (raw) => {
            send(raw);
            sent.resolve("sent");
          };
          const outcome = (sending: Promise<void>) =>
            Promise.race([
              sending.then(
                () => "delivered",
                (cause: unknown) => cause,
              ),
              sent.promise,
            ]);
          expect(
            await outcome(
              router.sendProvisionStepToAllocation(
                target,
                stepAddress,
                deployConfig,
              ),
            ),
          ).toBeInstanceOf(WorkflowRunNotExecutableError);
          expect(
            await outcome(
              router.sendPackToAllocation(
                target,
                stepAddress,
                new Uint8Array([1]),
                "refs/heads/main",
                "a".repeat(40),
              ),
            ),
          ).toBeInstanceOf(WorkflowRunNotExecutableError);
        } finally {
          router.handleClose(ws);
        }
      },
    );

    test("sends staging frames while the run can execute", async () => {
      const { router, ws } = await connectForInitialization();
      const stepAddress = "step_admission@example.test";
      await router.bindAllocatedStepRoute(target, stepAddress);
      const types = new Set<string>();
      const sent = Promise.withResolvers<undefined>();
      const send = ws.send.bind(ws);
      ws.send = (raw) => {
        send(raw);
        types.add(HubFrame.assert(JSON.parse(raw)).type);
        if (types.has("agent.deploy") && types.has("repo.pack.done"))
          sent.resolve(undefined);
      };
      const provisioned = router
        .sendProvisionStepToAllocation(target, stepAddress, deployConfig)
        .catch((cause: unknown) => cause);
      const packed = router
        .sendPackToAllocation(
          target,
          stepAddress,
          new Uint8Array([1]),
          "refs/heads/main",
          "a".repeat(40),
        )
        .catch((cause: unknown) => cause);
      try {
        await Promise.race([sent.promise, Promise.all([provisioned, packed])]);
        expect([...types]).toEqual(
          expect.arrayContaining([
            "agent.deploy",
            "repo.pack.push",
            "repo.pack.done",
          ]),
        );
      } finally {
        router.handleClose(ws);
        await provisioned;
        await packed;
      }
    });

    test("cancellation can commit after the deploy send without waiting for its acknowledgement", async () => {
      const { router, ws } = await connectForInitialization();
      const sent = Promise.withResolvers<undefined>();
      const send = ws.send.bind(ws);
      ws.send = (raw) => {
        send(raw);
        if (HubFrame.assert(JSON.parse(raw)).type === "agent.deploy")
          sent.resolve(undefined);
      };
      const deployment = router.sendAgentDeployToAllocation(
        target,
        target.workflowRunAddress,
        deployConfig,
      );
      const result = deployment.catch((cause: unknown) => cause);
      try {
        await Promise.race([sent.promise, result]);
        const lifecycle = createWorkflowLifecycleService({
          db: h.db,
          historyReceives: createWorkflowHistoryReceiveTracker(),
          runReader: {
            listRunIds: async () => [],
            readRunEvents: async () => [],
            readLatestRunEvents: async () => ({ tip: null, events: new Map() }),
            resolveRefTip: async () => null,
            hasRepository: async () => false,
          },
        });
        expect(
          await lifecycle.requestCancellation(
            target.tenantId,
            target.anchorRunId,
            "Stop",
          ),
        ).toBe("pending");
        router.handleMessage(
          ws,
          JSON.stringify({
            type: "agent.deploy.ack",
            agentAddress: target.workflowRunAddress,
            publicKey: "b".repeat(64),
          }),
        );
        expect(await result).toEqual({ publicKey: "b".repeat(64) });
      } finally {
        router.handleClose(ws);
        await result;
      }
    });

    test("admits a live run and rejects stopping and terminal runs and stale owners", async () => {
      let sends = 0;
      const send = () => {
        sends += 1;
        return true;
      };
      expect(await withExecutableWorkflowRun(h.db, target, send)).toBe(true);
      await expect(
        withExecutableWorkflowRun(h.db, { ...target, generation: 2 }, send),
      ).rejects.toThrow("not available");
      await h.db
        .update(workflowRun)
        .set({ cancellationRequestedAt: new Date() })
        .where(eq(workflowRun.id, target.anchorRunId));
      await expect(
        withExecutableWorkflowRun(h.db, target, send),
      ).rejects.toMatchObject({
        name: "WorkflowRunNotExecutableError",
        reason: "stopping",
      });
      await h.db
        .update(workflowRun)
        .set({ status: "completed" })
        .where(eq(workflowRun.id, target.anchorRunId));
      await expect(
        withExecutableWorkflowRun(h.db, target, send),
      ).rejects.toMatchObject({
        name: "WorkflowRunNotExecutableError",
        reason: "terminal",
      });
      expect(sends).toBe(1);
    });

    test("reports a stopping run as not executable after its allocation is retired", async () => {
      await h.db
        .update(workflowRun)
        .set({ cancellationRequestedAt: new Date() })
        .where(eq(workflowRun.id, target.anchorRunId));
      await h.db
        .update(sidecarAllocation)
        .set({ status: "releasing" })
        .where(eq(sidecarAllocation.id, target.allocationId));
      let sends = 0;
      await expect(
        withExecutableWorkflowRun(h.db, target, () => {
          sends += 1;
          return true;
        }),
      ).rejects.toMatchObject({
        name: "WorkflowRunNotExecutableError",
        reason: "stopping",
      });
      expect(sends).toBe(0);
    });

    test("a cancellation committed during sender preparation prevents the actual router send", async () => {
      const registered = Promise.withResolvers<undefined>();
      const preparing = Promise.withResolvers<undefined>();
      const senderKey = Promise.withResolvers<string>();
      const router = createSidecarRouter({
        authenticateSidecar: async () => ({ kind: "allocated", ...target }),
        validateSidecarIdentity: async () => true,
        withExecutableWorkflowRun: (identity, send, signal) =>
          withExecutableWorkflowRun(h.db, identity, send, signal),
        lookups: {
          resolveSenderKey: () => {
            preparing.resolve(undefined);
            return senderKey.promise;
          },
        },
      });
      router.events.on("sidecar.allocated.connected", () =>
        registered.resolve(undefined),
      );
      router.fenceAllocation(target.allocationId, target.generation);
      const ws = createMockWs();
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "register",
          sidecarId: target.sidecarId,
          token: "token",
          agentAddresses: [target.workflowRunAddress],
        }),
      );
      await registered.promise;
      const before = [...ws.sent];
      const delivery = router
        .sendWorkflowRunDispatchToAllocation(
          target,
          target.workflowRunAddress,
          target.anchorRunId,
          [],
          "bWFpbA==",
          "sender@example.test",
          "message-1",
        )
        .catch((cause: unknown) => cause);
      try {
        await preparing.promise;
        const lifecycle = createWorkflowLifecycleService({
          db: h.db,
          historyReceives: createWorkflowHistoryReceiveTracker(),
          runReader: {
            listRunIds: async () => [],
            readRunEvents: async () => [],
            readLatestRunEvents: async () => ({ tip: null, events: new Map() }),
            resolveRefTip: async () => null,
            hasRepository: async () => false,
          },
        });
        expect(
          await lifecycle.requestCancellation(
            target.tenantId,
            target.anchorRunId,
            "Operator cancelled",
          ),
        ).toBe("pending");
        senderKey.resolve("a".repeat(64));
        expect(await delivery).toBeInstanceOf(WorkflowRunNotExecutableError);
        expect(ws.sent).toEqual(before);
      } finally {
        senderKey.resolve("a".repeat(64));
        await delivery;
        router.handleClose(ws);
      }
    });

    test.each(["cancel", "expire", "retire", "abort"] as const)(
      "rechecks admission after waiting for a concurrent %s",
      async (change) => {
        const locked = Promise.withResolvers<number>();
        const release = Promise.withResolvers<undefined>();
        const controller = new AbortController();
        // Cancellation and retirement of a runnable deployment both write the
        // anchor row, so the blocker holds that lock while it decides.
        const blocker = h.db.transaction(async (tx) => {
          await tx
            .select()
            .from(workflowRun)
            .where(eq(workflowRun.id, target.anchorRunId))
            .for("update");
          const rows = await tx.execute(
            sql`select pg_backend_pid()::integer as pid`,
          );
          const pid = rows[0]?.["pid"];
          if (typeof pid !== "number") throw new Error("Missing backend pid");
          locked.resolve(pid);
          await release.promise;
          if (change === "cancel")
            await tx
              .update(workflowRun)
              .set({ cancellationRequestedAt: new Date() })
              .where(eq(workflowRun.id, target.anchorRunId));
          if (change === "expire")
            await tx
              .update(workflowRun)
              .set({ expiresAt: new Date() })
              .where(eq(workflowRun.id, target.anchorRunId));
          if (change === "retire")
            await tx
              .update(sidecarAllocation)
              .set({ status: "releasing" })
              .where(eq(sidecarAllocation.id, target.allocationId));
        });
        const pid = await Promise.race([
          locked.promise,
          blocker.then(() => {
            throw new Error("Blocker ended before locking");
          }),
        ]);
        let sent = false;
        let finished = false;
        const delivery = withExecutableWorkflowRun(
          h.db,
          target,
          () => {
            sent = true;
            return true;
          },
          controller.signal,
        )
          .catch((cause: unknown) => cause)
          .finally(() => {
            finished = true;
          });
        try {
          // Observe the actual database wait, without relying on a sleep duration.
          while (true) {
            if (finished)
              throw new Error("Delivery did not wait for the anchor lock");
            const rows = await h.db.execute(sql`select exists (
            select 1 from pg_stat_activity where ${pid} = any(pg_blocking_pids(pid))
          ) as blocked`);
            if (rows[0]?.["blocked"] === true) break;
            await new Promise<void>((resolve) => setImmediate(resolve));
          }
          if (change === "abort")
            controller.abort(new Error("Delivery lease expired"));
        } finally {
          release.resolve(undefined);
          await blocker;
        }
        expect(await delivery).toBeInstanceOf(Error);
        expect(sent).toBe(false);
      },
    );

    test("delivery does not wait for a pack receive holding the allocation lock", async () => {
      const locked = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      const receive = h.db.transaction(async (tx) => {
        await tx
          .select()
          .from(sidecarAllocation)
          .where(eq(sidecarAllocation.id, target.allocationId))
          .for("update");
        locked.resolve(undefined);
        await release.promise;
      });
      try {
        await Promise.race([
          locked.promise,
          receive.then(() => {
            throw new Error("Receive ended before locking");
          }),
        ]);
        expect(await withExecutableWorkflowRun(h.db, target, () => true)).toBe(
          true,
        );
      } finally {
        release.resolve(undefined);
        await receive;
      }
    });
  },
);
