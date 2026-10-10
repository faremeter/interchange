import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq, sql } from "drizzle-orm";
import { MAX_SIDECAR_ACTIVE_DEPLOYMENTS } from "@intx/types/sidecar";

import {
  createSidecarAllocationStore,
  createWorkflowProbeStore,
  SidecarReuseRejectedError,
  SidecarInventoryUnavailableError,
} from "@intx/db";
import { sidecar, sidecarAllocation, workflowProbe } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT_ID = "tenant-reuse-locks";
const ASSET_ID = "asset-reuse-locks";
const SHARED_SIDECAR_ID = "sidecar-shared";
const BINDING = {
  provisionerId: "test",
  provisionerApiVersion: 1,
  provisionerBindingFingerprint: "test:v1",
} as const;

function observe(work: Promise<unknown>) {
  let settled = false;
  const result = work.then(
    (value) => {
      settled = true;
      return { status: "fulfilled", value } as const;
    },
    (reason: unknown) => {
      settled = true;
      return { status: "rejected", reason } as const;
    },
  );
  return { result, isSettled: () => settled };
}

describe.skipIf(!harnessDbEnvAvailable())("sidecar ownership locking", () => {
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
    await seedAsset(h.db, {
      id: ASSET_ID,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: "reuse-locks",
    });
    for (const id of [SHARED_SIDECAR_ID, "sidecar-offered", "sidecar-other"]) {
      await h.db.insert(sidecar).values({
        id,
        url: null,
        tokenHashSha256: new TextEncoder().encode(id),
      });
    }
  });

  async function seedAllocation(
    id: string,
    status: "provisioning" | "allocated" | "releasing" | "replacing",
    sidecarId = SHARED_SIDECAR_ID,
  ) {
    await seedWorkflowRun(h.db, {
      id: `run-${id}`,
      anchorRunId: `run-${id}`,
      tenantId: TENANT_ID,
    });
    await h.db.insert(sidecarAllocation).values({
      id,
      anchorRunId: `run-${id}`,
      tenantId: TENANT_ID,
      sidecarId,
      status,
      generation: 1,
      ensureAcceptedGeneration: status === "allocated" ? 1 : null,
      maxDisconnectedMs: 900_000,
      ...BINDING,
    });
  }

  async function seedProbe(status: "provisioning" | "probing" | "releasing") {
    await h.db.insert(workflowProbe).values({
      id: "probe-holder",
      tenantId: TENANT_ID,
      definitionAssetId: ASSET_ID,
      sidecarId: SHARED_SIDECAR_ID,
      source: { kind: "registry", registry: "npmjs" },
      entry: "./workflow.js",
      status,
      generation: 1,
      result: {
        projection: { id: "workflow", triggers: [], stepOrder: [], steps: {} },
        grants: [],
        grantWalkSnapshot: { perStep: [], grantRequirements: [] },
        wireHash: "a".repeat(64),
      },
      ...BINDING,
    });
  }

  async function waitForBlocked(
    blockerPid: number,
    operation: ReturnType<typeof observe>,
  ) {
    // Postgres exposes lock waits through its activity view, without a notification.
    for (;;) {
      const waiting = await h.db.execute(
        sql`select pid from pg_stat_activity where ${blockerPid} = any(pg_blocking_pids(pid))`,
      );
      const pid = waiting[0]?.["pid"];
      if (typeof pid === "number") return pid;
      if (operation.isSettled()) {
        throw new Error(
          "Ownership changed without waiting for the sidecar lock",
        );
      }
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
  }

  async function runInLockOrder(
    first: () => Promise<unknown>,
    second: () => Promise<unknown>,
    lockedSidecarId = SHARED_SIDECAR_ID,
  ) {
    const locked = Promise.withResolvers<number>();
    const release = Promise.withResolvers<undefined>();
    const blocker = h.db.transaction(async (tx) => {
      await tx
        .select()
        .from(sidecar)
        .where(eq(sidecar.id, lockedSidecarId))
        .for("update");
      const [backend] = await tx.execute(sql`select pg_backend_pid() as pid`);
      const pid = backend?.["pid"];
      if (typeof pid !== "number") throw new Error("Missing backend PID");
      locked.resolve(pid);
      await release.promise;
    });
    void blocker.catch(locked.reject);
    let firstOperation: ReturnType<typeof observe> | undefined;
    let secondOperation: ReturnType<typeof observe> | undefined;
    try {
      const blockerPid = await locked.promise;
      firstOperation = observe(first());
      const firstPid = await waitForBlocked(blockerPid, firstOperation);
      secondOperation = observe(second());
      await waitForBlocked(firstPid, secondOperation);
    } finally {
      release.resolve(undefined);
      await blocker;
      await Promise.all([firstOperation?.result, secondOperation?.result]);
    }
    return [await firstOperation.result, await secondOperation.result];
  }

  test("shared placement blocks unknown inventory even below capacity", async () => {
    await seedAllocation("holder", "allocated");
    await seedAllocation("placing", "provisioning", "sidecar-offered");
    const store = createSidecarAllocationStore(h.db, {
      getRetainedIncarnations: () => undefined,
    });
    await expect(
      store.markAllocated({
        allocationId: "placing",
        generation: 1,
        sidecarId: SHARED_SIDECAR_ID,
      }),
    ).rejects.toBeInstanceOf(SidecarInventoryUnavailableError);
    expect(await store.findById("placing")).toMatchObject({
      status: "provisioning",
      sidecarId: "sidecar-offered",
    });
  });

  test.each([false, true])(
    "placement uses inventory after acquiring the sidecar lock, available = %s",
    async (available) => {
      await seedAllocation("holder", "allocated");
      await seedAllocation("placing", "provisioning", "sidecar-offered");
      let known = !available;
      const store = createSidecarAllocationStore(h.db, {
        getRetainedIncarnations: () => (known ? [] : undefined),
      });
      const locked = Promise.withResolvers<number>();
      const release = Promise.withResolvers<undefined>();
      const blocker = h.db.transaction(async (tx) => {
        await tx
          .select()
          .from(sidecar)
          .where(eq(sidecar.id, SHARED_SIDECAR_ID))
          .for("update");
        const [backend] = await tx.execute(sql`select pg_backend_pid() as pid`);
        const pid = backend?.["pid"];
        if (typeof pid !== "number") throw new Error("Missing backend PID");
        locked.resolve(pid);
        await release.promise;
      });
      void blocker.catch(locked.reject);
      const pid = await locked.promise;
      const placing = observe(
        store.markAllocated({
          allocationId: "placing",
          generation: 1,
          sidecarId: SHARED_SIDECAR_ID,
        }),
      );
      try {
        await waitForBlocked(pid, placing);
        known = available;
      } finally {
        release.resolve(undefined);
        await blocker;
      }
      const result = await placing.result;
      expect(result).toMatchObject(
        available
          ? { status: "fulfilled", value: { status: "allocated" } }
          : {
              status: "rejected",
              reason: expect.any(SidecarInventoryUnavailableError),
            },
      );
      expect(await store.findById("placing")).toMatchObject({
        status: available ? "allocated" : "provisioning",
        sidecarId: available ? SHARED_SIDECAR_ID : "sidecar-offered",
      });
    },
  );

  test.each(["retained", "removed"] as const)(
    "%s capacity admits only one of two placements into the last active slot",
    async (proof) => {
      for (let index = 0; index < MAX_SIDECAR_ACTIVE_DEPLOYMENTS; index++) {
        await seedAllocation(`holder-${String(index)}`, "allocated");
      }
      await seedAllocation("first", "provisioning", "sidecar-offered");
      await seedAllocation("second", "provisioning", "sidecar-other");
      const store = createSidecarAllocationStore(h.db, {
        getRetainedIncarnations: (sidecarId) =>
          proof === "retained" && sidecarId === SHARED_SIDECAR_ID
            ? [{ allocationId: "holder-0", generation: 1 }]
            : [],
      });
      if (proof === "removed") {
        await store.beginRelease({
          allocationId: "holder-0",
          expectedGeneration: 1,
          expectedStatus: "allocated",
        });
        const leaseId = "cleanup";
        expect(
          (
            await store.claimNextReconcilable({
              leaseId,
              leaseDurationMs: 60_000,
            })
          )?.id,
        ).toBe("holder-0");
        expect(
          await store.confirmDeploymentCleanup({
            allocationId: "holder-0",
            generation: 2,
            expectedLeaseId: leaseId,
          }),
        ).toBe(true);
        await store.markDestroyFailed({
          allocationId: "holder-0",
          expectedGeneration: 2,
          expectedLeaseId: leaseId,
          code: "provider_permission_denied",
          message: "Provider hold still requires recovery",
        });
      }
      const results = await runInLockOrder(
        () =>
          store.markAllocated({
            allocationId: "first",
            generation: 1,
            sidecarId: SHARED_SIDECAR_ID,
          }),
        () =>
          store.markAllocated({
            allocationId: "second",
            generation: 1,
            sidecarId: SHARED_SIDECAR_ID,
          }),
      );
      expect(results[0]).toMatchObject({
        status: "fulfilled",
        value: { status: "allocated", sidecarId: SHARED_SIDECAR_ID },
      });
      expect(results[1]).toMatchObject({
        status: "rejected",
        reason: expect.any(SidecarReuseRejectedError),
      });
      expect(await store.findById("second")).toMatchObject({
        status: "provisioning",
        sidecarId: "sidecar-other",
      });
    },
  );

  for (const removal of [
    "release",
    "destroy failure",
    "ensure failure",
    "replacement",
  ] as const) {
    for (const placementFirst of [true, false]) {
      test(`${removal} serializes with placement (placement first: ${String(placementFirst)})`, async () => {
        const status =
          removal === "ensure failure"
            ? "provisioning"
            : removal === "replacement"
              ? "replacing"
              : "releasing";
        await seedAllocation("holder", status);
        await seedAllocation("joining", "provisioning", "sidecar-offered");
        const store = createSidecarAllocationStore(h.db, {
          getRetainedIncarnations: () => [],
        });
        const place = () =>
          store.markAllocated({
            allocationId: "joining",
            generation: 1,
            sidecarId: SHARED_SIDECAR_ID,
          });
        const remove = () => {
          switch (removal) {
            case "release":
              return store.markReleased({
                allocationId: "holder",
                generation: 1,
              });
            case "destroy failure":
              return store.markDestroyFailed({
                allocationId: "holder",
                expectedGeneration: 1,
                code: "destroy_failed",
                message: "Destroy refused",
              });
            case "ensure failure":
              return store.failWithoutInfrastructure({
                allocationId: "holder",
                expectedStatus: "provisioning",
                expectedGeneration: 1,
                code: "ensure_failed",
                message: "Ensure refused",
              });
            case "replacement":
              return store.bindReplacementSidecar({
                allocationId: "holder",
                generation: 1,
                sidecarId: "sidecar-replacement",
                tokenHashSha256: new TextEncoder().encode("replacement"),
                connectDeadline: new Date(Date.now() + 60_000),
              });
          }
        };
        const results = placementFirst
          ? await runInLockOrder(place, remove)
          : await runInLockOrder(remove, place);
        const placement = results[placementFirst ? 0 : 1];
        expect(results[placementFirst ? 1 : 0]).toMatchObject({
          status: "fulfilled",
          value: { id: "holder" },
        });
        if (placementFirst) {
          expect(placement).toMatchObject({
            status: "fulfilled",
            value: { status: "allocated", sidecarId: SHARED_SIDECAR_ID },
          });
        } else {
          expect(placement).toMatchObject({
            status: "rejected",
            reason: expect.any(SidecarReuseRejectedError),
          });
          expect(await store.findById("joining")).toMatchObject({
            status: "provisioning",
            sidecarId: "sidecar-offered",
          });
        }
      });
    }
  }

  for (const placementFirst of [true, false]) {
    test(`probe completion serializes with placement (placement first: ${String(placementFirst)})`, async () => {
      await seedProbe("releasing");
      await seedAllocation("joining", "provisioning", "sidecar-offered");
      const store = createSidecarAllocationStore(h.db, {
        getRetainedIncarnations: () => [],
      });
      const probes = createWorkflowProbeStore(h.db);
      const place = () =>
        store.markAllocated({
          allocationId: "joining",
          generation: 1,
          sidecarId: SHARED_SIDECAR_ID,
        });
      const complete = () =>
        probes.transition("probe-holder", ["releasing"], "succeeded");
      const results = placementFirst
        ? await runInLockOrder(place, complete)
        : await runInLockOrder(complete, place);
      expect(results[placementFirst ? 1 : 0]).toMatchObject({
        status: "fulfilled",
        value: { status: "succeeded" },
      });
      expect(results[placementFirst ? 0 : 1]).toMatchObject(
        placementFirst
          ? { status: "fulfilled", value: { sidecarId: SHARED_SIDECAR_ID } }
          : {
              status: "rejected",
              reason: expect.any(SidecarReuseRejectedError),
            },
      );
    });
  }

  test("probe adoption preserves a holder while another placement waits", async () => {
    await seedProbe("probing");
    await seedWorkflowRun(h.db, {
      id: "run-adopted",
      anchorRunId: "run-adopted",
      tenantId: TENANT_ID,
    });
    await seedAllocation("joining", "provisioning", "sidecar-offered");
    const store = createSidecarAllocationStore(h.db, {
      getRetainedIncarnations: () => [],
    });
    const probes = createWorkflowProbeStore(h.db);
    const results = await runInLockOrder(
      () =>
        h.db.transaction(async (tx) => {
          const adopted = await store.createAdopted(
            {
              id: "probe-holder",
              anchorRunId: "run-adopted",
              tenantId: TENANT_ID,
              sidecarId: SHARED_SIDECAR_ID,
              generation: 1,
              maxDisconnectedMs: 900_000,
              connectDeadline: new Date(Date.now() + 60_000),
              ...BINDING,
            },
            tx,
          );
          expect(
            await probes.transition("probe-holder", ["probing"], "succeeded", {
              tx,
            }),
          ).not.toBeNull();
          return adopted;
        }),
      () =>
        store.markAllocated({
          allocationId: "joining",
          generation: 1,
          sidecarId: SHARED_SIDECAR_ID,
        }),
    );
    for (const result of results) {
      expect(result).toMatchObject({
        status: "fulfilled",
        value: { status: "allocated", sidecarId: SHARED_SIDECAR_ID },
      });
    }
  });

  test("opposite placements lock both sidecars in the same order", async () => {
    await seedAllocation("first", "provisioning", SHARED_SIDECAR_ID);
    await seedAllocation("second", "provisioning", "sidecar-other");
    const store = createSidecarAllocationStore(h.db, {
      getRetainedIncarnations: () => [],
    });
    const [first, second] = await runInLockOrder(
      () =>
        store.markAllocated({
          allocationId: "first",
          generation: 1,
          sidecarId: "sidecar-other",
        }),
      () =>
        store.markAllocated({
          allocationId: "second",
          generation: 1,
          sidecarId: SHARED_SIDECAR_ID,
        }),
      "sidecar-other",
    );
    expect(first).toMatchObject({
      status: "fulfilled",
      value: { sidecarId: "sidecar-other" },
    });
    expect(second).toMatchObject({
      status: "rejected",
      reason: expect.any(SidecarReuseRejectedError),
    });
  });

  test("does not finalize a probe that moved while its old sidecar was locked", async () => {
    await seedProbe("provisioning");
    await seedAllocation("other-holder", "provisioning", "sidecar-other");
    const probes = createWorkflowProbeStore(h.db);
    const [placed, completed] = await runInLockOrder(
      () =>
        probes.markProbing({
          probeId: "probe-holder",
          sidecarId: "sidecar-other",
        }),
      () =>
        probes.transition(
          "probe-holder",
          ["provisioning", "probing"],
          "failed",
        ),
    );
    expect(placed).toMatchObject({
      status: "fulfilled",
      value: { status: "probing", sidecarId: "sidecar-other" },
    });
    expect(completed).toEqual({ status: "fulfilled", value: null });
    expect(await probes.get("probe-holder")).toMatchObject({
      status: "probing",
      sidecarId: "sidecar-other",
    });
  });
});
