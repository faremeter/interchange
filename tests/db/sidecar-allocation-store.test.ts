import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  createSidecarAllocationStore,
  createWorkflowRunLaunchSpecStore,
} from "@intx/db";
import { workflowDefinition } from "@intx/db/schema";
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
        deploymentId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        definitionId: DEFINITION_ID,
      });
      await createWorkflowRunLaunchSpecStore(h.db).create({
        anchorRunId: ANCHOR_RUN_ID,
        sessionId: "ses-allocation",
        deploymentDomain: "tenant.example",
        sourceAuthorityPrincipalId: PRINCIPAL_ID,
        definitionSnapshot: {
          id: "workflow",
          triggers: [{ type: "manual" }],
          steps: { work: { id: "work", kind: "step" } },
          stepOrder: ["work"],
        },
        definitionHash: "aabbccdd",
        sourceOfferingIds: ["offering-primary"],
        defaultSourceOfferingId: "offering-primary",
        deployContent: { systemPrompt: "" },
      });
    });

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
          columns: { credentialScope: true },
        }),
      ).toEqual({ credentialScope: "allocated" });

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

      expect(
        await store.beginReplacement({
          allocationId: pending.id,
          expectedStatus: "allocated",
          expectedGeneration: 1,
          expectedLeaseId: "lease-stale",
          failureCode: "connection_lost",
          failureMessage: "stale reconciler",
        }),
      ).toBeNull();

      const replacing = await store.beginReplacement({
        allocationId: pending.id,
        expectedStatus: "allocated",
        expectedGeneration: 1,
        expectedLeaseId: "lease-current",
        failureCode: "connection_lost",
        failureMessage: "replacement grace expired",
      });
      expect(replacing?.status).toBe("replacing");
      expect(replacing?.generation).toBe(2);
      expect(replacing?.sidecarId).toBe("sidecar-generation-1");
      expect(replacing?.externalRef).toBe("i-generation-1");

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
          columns: { credentialScope: true },
        }),
      ).toEqual({ credentialScope: "allocated" });

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
        deploymentId: "anchor-without-spec",
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
  },
);
