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
  createSidecarAllocationReconciler,
  createSidecarPluginRegistry,
  createSidecarRouter,
  recoverSenderDeploy,
} from "@intx/hub-sessions";

import {
  createSidecarAllocationStore,
  createWorkflowRunDispatchStore,
  createWorkflowRunLaunchSpecStore,
} from "@intx/db";
import {
  sidecar,
  sidecarAllocation,
  workflowDefinition,
  workflowPendingProjection,
  workflowRun,
} from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT_ID = "tnt-allocation";
const DEFINITION_ID = "wfd-allocation";
const ANCHOR_RUN_ID = "dep-allocation";
const PRINCIPAL_ID = "prn-allocation";

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

    async function createClaimedAllocation(id: string) {
      const store = createSidecarAllocationStore(h.db);
      await store.createPending({
        id,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
      });
      const leaseId = `${id}-lease`;
      await store.claimNextReconcilable({ leaseId, leaseDurationMs: 60_000 });
      await store.bindInitialSidecar({
        allocationId: id,
        expectedGeneration: 0,
        sidecarId: `${id}-sidecar`,
        tokenHashSha256: new Uint8Array([1, 2, 3]),
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
      await h.db
        .update(workflowRun)
        .set({ publicKey: "previous-key" })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));
      expect(await store.beginInitialization(initialization)).toEqual({
        previousPublicKey: "previous-key",
      });
      expect(await store.beginInitialization(initialization)).toBeNull();
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

    test("unsent rollback restores the captured key after lease loss without overwriting a later completion", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-key-rollback");
      await h.db
        .update(workflowRun)
        .set({ publicKey: "previous-key" })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));
      const reserved = await store.beginInitialization(initialization);
      expect(reserved).toEqual({ previousPublicKey: "previous-key" });
      if (reserved === null) throw new Error("Reservation failed");
      const controller = new AbortController();
      controller.abort(new Error("Old initialization cancelled"));
      const rollback = {
        ...initialization,
        ...reserved,
        signal: controller.signal,
      };
      await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        connectDeadline: new Date(0),
      });
      const nextLeaseId = "new-owner";
      await store.claimNextReconcilable({
        leaseId: nextLeaseId,
        leaseDurationMs: 60_000,
      });
      expect(await store.clearUnsentInitialization(rollback)).toBe(true);
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, ANCHOR_RUN_ID),
        }),
      ).toMatchObject({ publicKey: "previous-key" });
      expect(
        (await store.findById(allocation.id))?.initializationLeaseId,
      ).toBeUndefined();
      expect(await store.clearUnsentInitialization(rollback)).toBe(false);
      expect(
        await store.beginUnrecoverableRelease({
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedLeaseId: nextLeaseId,
          onlyIfInitializationIncomplete: true,
          failureCode: "stale-uncertainty",
          failureMessage: "Claim observed the marker before rollback",
        }),
      ).toBeNull();
      const next = { ...initialization, leaseId: nextLeaseId };
      expect(await store.beginInitialization(next)).toEqual({
        previousPublicKey: "previous-key",
      });
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

    for (const recovery of [false, true]) {
      for (const nextAttempt of [false, true]) {
        test(`cleanup cannot act on a rolled-back first attempt (recovery=${String(recovery)}, nextAttempt=${String(nextAttempt)})`, async () => {
          const { store, allocation, initialization } =
            await createClaimedAllocation("alloc-first-rollback");
          const reserved = await store.beginInitialization(initialization);
          expect(reserved).toEqual({ previousPublicKey: null });
          await store.markConnectionLost({
            allocationId: allocation.id,
            generation: allocation.generation,
            connectDeadline: new Date(0),
          });
          const leaseId = "recovery-owner";
          const claimed = await store.claimNextReconcilable({
            leaseId,
            leaseDurationMs: 60_000,
          });
          if (claimed?.initializationLeaseId === undefined)
            throw new Error("Recovery did not observe the pending attempt");
          expect(
            await store.clearUnsentInitialization({
              ...initialization,
              previousPublicKey: null,
            }),
          ).toBe(true);
          if (nextAttempt) {
            await store.beginInitialization({ ...initialization, leaseId });
          }
          const cleanup = {
            allocationId: allocation.id,
            expectedGeneration: allocation.generation,
            expectedLeaseId: leaseId,
            onlyIfInitializationIncomplete: true,
            expectedInitializationLeaseId: claimed.initializationLeaseId,
            failureCode: "stale-uncertainty",
            failureMessage: "Recovery observed the marker before rollback",
          };
          expect(
            recovery
              ? await store.beginReplacement({
                  ...cleanup,
                  expectedStatus: "allocated",
                  nextAttemptAt: new Date(),
                })
              : await store.beginUnrecoverableRelease(cleanup),
          ).toBeNull();
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
              recovery
                ? await store.beginReplacement({
                    ...currentCleanup,
                    expectedStatus: "allocated",
                    nextAttemptAt: new Date(),
                  })
                : await store.beginUnrecoverableRelease(currentCleanup),
            ).toMatchObject({
              status: recovery ? "replacing" : "releasing",
              generation: allocation.generation + 1,
            });
          }
        });
      }
    }

    test("a failed marker clear rolls back key restoration too", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-atomic-rollback");
      await h.db
        .update(workflowRun)
        .set({ publicKey: "previous-key" })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));
      const reserved = await store.beginInitialization(initialization);
      if (reserved === null) throw new Error("Reservation failed");
      await h.db.execute(
        sql`alter table sidecar_allocation add constraint test_reject_clear check (initialization_lease_id is not null)`,
      );
      try {
        await expect(
          store.clearUnsentInitialization({ ...initialization, ...reserved }),
        ).rejects.toThrow();
        expect(
          await h.db.query.workflowRun.findFirst({
            where: eq(workflowRun.id, ANCHOR_RUN_ID),
          }),
        ).toMatchObject({ publicKey: null });
        expect(
          (await store.findById(allocation.id))?.initializationLeaseId,
        ).toBe(initialization.leaseId);
      } finally {
        await h.db.execute(
          sql`alter table sidecar_allocation drop constraint test_reject_clear`,
        );
      }
      expect(
        await store.clearUnsentInitialization({
          ...initialization,
          ...reserved,
        }),
      ).toBe(true);
      expect(
        await h.db.query.workflowRun.findFirst({
          where: eq(workflowRun.id, ANCHOR_RUN_ID),
        }),
      ).toMatchObject({ publicKey: "previous-key" });
    });

    test("disconnect preserves uncertainty and stale callbacks cannot resolve it", async () => {
      const { store, allocation, initialization } =
        await createClaimedAllocation("alloc-initialization-disconnect");
      expect(await store.beginInitialization(initialization)).toEqual({
        previousPublicKey: null,
      });
      await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        connectDeadline: new Date(0),
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
      expect(
        await store.clearUnsentInitialization({
          ...initialization,
          previousPublicKey: null,
        }),
      ).toBe(true);
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
      ).toEqual({ previousPublicKey: null });
      const releasing = await store.beginUnrecoverableRelease({
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
          previousPublicKey: null,
        }),
      ).toBe(false);
      expect(
        await store.clearUnsentInitialization({
          ...initialization,
          previousPublicKey: null,
        }),
      ).toBe(true);
      expect(await store.beginInitialization(initialization)).toEqual({
        previousPublicKey: null,
      });
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
      expect(
        await store.clearUnsentInitialization({
          ...initialization,
          previousPublicKey: null,
        }),
      ).toBe(true);
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
      expect(
        await store.clearUnsentInitialization({
          ...initialization,
          previousPublicKey: null,
        }),
      ).toBe(false);
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
      expect(
        await store.clearUnsentInitialization({
          ...initialization,
          previousPublicKey: null,
        }),
      ).toBe(false);
    });

    for (const recovery of [false, true]) {
      test(`a committed initialization survives an ambiguous failure with recovery=${String(recovery)}`, async () => {
        const { store, allocation, initialization } =
          await createClaimedAllocation("alloc-completed");
        await store.beginInitialization(initialization);
        await store.completeInitialization({
          ...initialization,
          publicKey: "committed-key",
        });
        const common = {
          allocationId: allocation.id,
          expectedGeneration: allocation.generation,
          expectedLeaseId: initialization.leaseId,
          onlyIfInitializationIncomplete: true,
          failureCode: "lost_commit_response",
          failureMessage: "The commit response was lost",
        };
        const changed = recovery
          ? await store.beginReplacement({
              ...common,
              expectedStatus: "allocated",
              nextAttemptAt: new Date(),
            })
          : await store.beginUnrecoverableRelease(common);
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
    }

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

    for (const completing of [false, true]) {
      for (const failure of ["cancelled", "expired"] as const) {
        test(`initialization ${completing ? "completion" : "reservation"} rolls back if ${failure} while holding the allocation`, async () => {
          const { store, allocation, initialization } =
            await createClaimedAllocation("alloc-cancelled-write");
          await h.db
            .update(workflowRun)
            .set({ publicKey: "old-key" })
            .where(eq(workflowRun.id, ANCHOR_RUN_ID));
          if (completing) await store.beginInitialization(initialization);
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
          const writing = completing
            ? store.completeInitialization({
                ...initialization,
                signal: controller.signal,
                publicKey: "cancelled-key",
              })
            : store.beginInitialization({
                ...initialization,
                signal: controller.signal,
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
          ).toBe(completing ? null : "old-key");
          expect(
            (await store.findById(allocation.id))?.initializationLeaseId,
          ).toBe(completing ? initialization.leaseId : undefined);
        });
      }
    }

    test("fences replacement before binding a new physical sidecar", async () => {
      const store = createSidecarAllocationStore(h.db);
      const pending = await store.createPending({
        id: "alloc-1",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
      });
      expect(pending.status).toBe("pending");
      expect(pending.generation).toBe(0);

      const initial = await store.bindInitialSidecar({
        allocationId: pending.id,
        expectedGeneration: 0,
        sidecarId: "sidecar-generation-1",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
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

      const allocated = await store.markAllocated({
        allocationId: pending.id,
        generation: 1,
        externalRef: "i-generation-1",
      });
      expect(allocated?.status).toBe("allocated");
      expect(allocated?.ensureAcceptedGeneration).toBe(1);

      const claimed = await store.claimNextReconcilable({
        leaseId: "lease-current",
        leaseDurationMs: 60_000,
      });
      expect(claimed?.id).toBe(pending.id);
      const replacementAt = new Date(Date.now() + 300_000);

      await h.db
        .update(workflowRun)
        .set({ publicKey: "generation-1-public-key" })
        .where(eq(workflowRun.id, ANCHOR_RUN_ID));

      expect(
        await store.beginReplacement({
          allocationId: pending.id,
          expectedStatus: "allocated",
          expectedGeneration: 1,
          expectedLeaseId: "lease-stale",
          nextAttemptAt: replacementAt,
          failureCode: "connection_lost",
          failureMessage: "stale reconciler",
        }),
      ).toBeNull();

      const replacing = await store.beginReplacement({
        allocationId: pending.id,
        expectedStatus: "allocated",
        expectedGeneration: 1,
        expectedLeaseId: "lease-current",
        nextAttemptAt: replacementAt,
        failureCode: "connection_lost",
        failureMessage: "replacement grace expired",
      });
      expect(replacing?.status).toBe("replacing");
      expect(replacing?.generation).toBe(2);
      expect(replacing?.sidecarId).toBe("sidecar-generation-1");
      expect(replacing?.externalRef).toBe("i-generation-1");
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
          columns: { publicKey: true },
        }),
      ).toEqual({ publicKey: null });

      const replacement = await store.bindReplacementSidecar({
        allocationId: pending.id,
        generation: 2,
        sidecarId: "sidecar-generation-2",
        tokenHashSha256: new Uint8Array([4, 5, 6]),
        connectDeadline: new Date(Date.now() + 60_000),
      });
      expect(replacement?.status).toBe("provisioning");
      expect(replacement?.generation).toBe(2);
      expect(replacement?.sidecarId).toBe("sidecar-generation-2");
      expect(replacement?.externalRef).toBeUndefined();
      expect(replacement?.destroyAttempts).toBe(1);
      expect(
        await h.db.query.sidecar.findFirst({
          where: (row, { eq }) => eq(row.id, "sidecar-generation-2"),
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
          provisionerApiVersion: 1,
          provisionerBindingFingerprint: "ec2-spot:test",
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
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
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
      const store = createSidecarAllocationStore(h.db);
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
      const store = createSidecarAllocationStore(h.db);
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
        });
        await store.createAdopted({
          id,
          anchorRunId: id,
          tenantId: TENANT_ID,
          provisionerId: "test",
          provisionerApiVersion: 1,
          provisionerBindingFingerprint: "test:v1",
          sidecarId: id,
          generation: 1,
          connectDeadline: new Date(Date.now() + 60_000),
        });
      }
      const router = createSidecarRouter({
        withExecutableWorkflowRun: async (_target, send) => send(),
        authenticateSidecar: async () => null,
        validateSidecarIdentity: async () => false,
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
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
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
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
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
      const store = createSidecarAllocationStore(h.db);
      await store.createPending({
        id: "alloc-park-wake",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
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
      const store = createSidecarAllocationStore(h.db);
      await store.createPending({
        id: "alloc-park-backoff",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-park-backoff",
        expectedGeneration: 0,
        sidecarId: "sidecar-park-backoff",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
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
      const store = createSidecarAllocationStore(h.db);
      await store.createPending({
        id: "alloc-ready",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
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
              apiVersion: 1,
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
          fenceAllocation: () => undefined,
          retireAllocation: () => undefined,
          isAllocatedSidecarReady: async () => true,
          waitForAllocatedSidecar: async () => undefined,
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
        await store.beginReplacement({
          allocationId: allocation.id,
          expectedStatus: "allocated",
          expectedGeneration: allocation.generation,
          expectedLeaseId: leaseId,
          nextAttemptAt: new Date(0),
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

    test("disconnect invalidates the lease before an old initializer can finish", async () => {
      const { store, allocation, leaseId } = await createClaimedAllocation(
        "alloc-disconnected-owner",
      );
      const deadline = new Date(Date.now() + 60_000);
      const disconnected = await store.markConnectionLost({
        allocationId: allocation.id,
        generation: allocation.generation,
        connectDeadline: deadline,
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

    test("persists reconnect grace only for the accepted generation", async () => {
      const store = createSidecarAllocationStore(h.db);
      await store.createPending({
        id: "alloc-reconnect",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-reconnect",
        expectedGeneration: 0,
        sidecarId: "sidecar-reconnect",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
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

      const deadline = new Date(Date.now() + 120_000);
      expect(
        await store.markConnectionLost({
          allocationId: "alloc-reconnect",
          generation: 0,
          connectDeadline: deadline,
        }),
      ).toBeNull();
      const disconnected = await store.markConnectionLost({
        allocationId: "alloc-reconnect",
        generation: 1,
        connectDeadline: deadline,
      });

      expect(disconnected?.connectDeadline).toEqual(deadline);
      expect(disconnected?.nextAttemptAt).toEqual(deadline);
    });

    test("repairs reconnect grace only while an allocation remains unscheduled and unleased", async () => {
      const store = createSidecarAllocationStore(h.db);
      await store.createPending({
        id: "alloc-repair",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-repair",
        expectedGeneration: 0,
        sidecarId: "sidecar-repair",
        tokenHashSha256: new Uint8Array([4, 5, 6]),
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

      const deadline = new Date(Date.now() + 120_000);
      expect(
        await store.scheduleReconnectIfUnscheduled({
          allocationId: "alloc-repair",
          generation: 0,
          connectDeadline: deadline,
        }),
      ).toBeNull();
      const repaired = await store.scheduleReconnectIfUnscheduled({
        allocationId: "alloc-repair",
        generation: 1,
        connectDeadline: deadline,
      });

      expect(repaired?.connectDeadline).toEqual(deadline);
      expect(repaired?.nextAttemptAt).toEqual(deadline);

      expect(
        await store.scheduleReconnectIfUnscheduled({
          allocationId: "alloc-repair",
          generation: 1,
          connectDeadline: new Date(deadline.getTime() + 120_000),
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
          connectDeadline: new Date(deadline.getTime() + 120_000),
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
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-terminal",
        expectedGeneration: 0,
        sidecarId: "sidecar-terminal",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
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

    for (const status of ["replacing", "releasing"] as const) {
      test(`preserves capacity and stops reconciliation after ${status} fails permanently`, async () => {
        const store = createSidecarAllocationStore(h.db);
        const dispatchStore = createWorkflowRunDispatchStore(h.db);
        await store.createPending({
          id: "alloc-destroy",
          anchorRunId: ANCHOR_RUN_ID,
          tenantId: TENANT_ID,
          provisionerId: "ec2-spot",
          provisionerApiVersion: 1,
          provisionerBindingFingerprint: "ec2-spot:test",
        });
        await store.bindInitialSidecar({
          allocationId: "alloc-destroy",
          expectedGeneration: 0,
          sidecarId: "sidecar-destroy",
          tokenHashSha256: new Uint8Array([1, 2, 3]),
          connectDeadline: new Date(0),
        });
        await store.markAllocated({
          allocationId: "alloc-destroy",
          generation: 1,
          externalRef: "vm-destroy",
        });
        await store.claimNextReconcilable({
          leaseId: "lease-allocated",
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
            expectedLeaseId: "lease-allocated",
          }),
        ).toBeNull();

        const transition = {
          allocationId: "alloc-destroy",
          expectedGeneration: 1,
          expectedLeaseId: "lease-allocated",
          expectedStatus: "allocated" as const,
        };
        if (status === "replacing") {
          await store.beginReplacement({
            ...transition,
            nextAttemptAt: new Date(0),
            failureCode: "connection_lost",
            failureMessage: "Worker disconnected",
          });
        } else {
          await store.beginRelease(transition);
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
          externalRef: "vm-destroy",
          failureCode: failure.code,
          failureMessage: failure.message,
          destroyAttempts: 1,
        });
        expect(failed?.nextAttemptAt).toBeUndefined();
        expect(failed?.reconciliationLeaseId).toBeUndefined();
        expect(failed?.reconciliationLeaseExpiresAt).toBeUndefined();
        expect(failed?.connectDeadline).toBeUndefined();
        expect(
          await h.db.query.workflowRun.findFirst({
            where: (row, { eq }) => eq(row.id, ANCHOR_RUN_ID),
            columns: { status: true },
          }),
        ).toEqual({ status: "failed" });
        expect(await dispatchStore.findById("dispatch-destroy")).toMatchObject({
          status: "abandoned",
          failureCode: failure.code,
          failureMessage: failure.message,
        });
        expect(await store.listActive()).toEqual([]);
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
            connectDeadline: new Date(0),
          }),
        ).toBeNull();

        await seedWorkflowRun(h.db, {
          id: "anchor-other",
          anchorRunId: "anchor-other",
          tenantId: TENANT_ID,
          definitionId: DEFINITION_ID,
        });
        await expect(
          store.createAdopted({
            id: "alloc-other",
            anchorRunId: "anchor-other",
            tenantId: TENANT_ID,
            provisionerId: "ec2-spot",
            provisionerApiVersion: 1,
            provisionerBindingFingerprint: "ec2-spot:test",
            sidecarId: "sidecar-destroy",
            generation: 1,
            connectDeadline: new Date(0),
          }),
        ).rejects.toMatchObject({
          cause: expect.objectContaining({
            constraint_name: "sidecar_allocation_active_sidecar_idx",
          }),
        });
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
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
      });
      expect(pending.status).toBe("pending");

      await store.bindInitialSidecar({
        allocationId: "alloc-deployed",
        expectedGeneration: 0,
        sidecarId: "sidecar-deployed",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
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

    test("fails active runs when replacement recovery is disabled", async () => {
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
      const store = createSidecarAllocationStore(h.db);
      const dispatchStore = createWorkflowRunDispatchStore(h.db);
      await store.createPending({
        id: "alloc-unrecoverable",
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "ec2-spot",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "ec2-spot:test",
      });
      await store.bindInitialSidecar({
        allocationId: "alloc-unrecoverable",
        expectedGeneration: 0,
        sidecarId: "sidecar-unrecoverable",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
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
        allocationId: "alloc-unrecoverable",
        expectedGeneration: 1,
        expectedLeaseId: "lease-unrecoverable",
        failureCode: "sidecar_connect_failed",
        failureMessage: "Automatic recovery is disabled: connect timeout",
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
        failureMessage: "Automatic recovery is disabled: connect timeout",
      });
      expect(
        await dispatchStore.findById("dispatch-unrecoverable-acknowledged"),
      ).toMatchObject({
        status: "abandoned",
        failureCode: "sidecar_connect_failed",
        failureMessage: "Automatic recovery is disabled: connect timeout",
      });
      expect(
        await dispatchStore.claimNextPending({
          leaseId: "dispatch-lease-after-unrecoverable-release",
          leaseDurationMs: 60_000,
        }),
      ).toBeNull();
    });
  },
);
