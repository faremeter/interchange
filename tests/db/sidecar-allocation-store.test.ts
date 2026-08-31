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
  createSidecarAllocationStore,
  createWorkflowRunDispatchStore,
  createWorkflowRunLaunchSpecStore,
} from "@intx/db";
import { workflowDefinition, workflowRun } from "@intx/db/schema";
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
        status: "failed",
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
        rawMessage: new Uint8Array([1, 2, 3]),
        stepGrants: [],
      });
      await dispatchStore.enqueue({
        id: "dispatch-unrecoverable-acknowledged",
        anchorRunId: ANCHOR_RUN_ID,
        messageId: "message-unrecoverable-acknowledged",
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
        status: "failed",
        failureCode: "sidecar_connect_failed",
        failureMessage: "Automatic recovery is disabled: connect timeout",
      });
      expect(
        await dispatchStore.findById("dispatch-unrecoverable-acknowledged"),
      ).toMatchObject({
        status: "failed",
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
