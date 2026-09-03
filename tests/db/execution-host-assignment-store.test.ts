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
  createExecutionHostAssignmentStore,
  type ExecutionHostClaimCandidate,
} from "@intx/db";
import {
  executionHost,
  executionHostSession,
  sidecar,
  sidecarAllocation,
  workflowDefinition,
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

const TENANT_ID = "tnt-host-assignment";
const OWNER_PRINCIPAL_ID = "prn-host-assignment-owner";
const HOST_PRINCIPAL_ID = "prn-host-assignment";
const HOST_ID = "hst-assignment";
const NOW = new Date("2026-09-03T12:00:00Z");

const candidate: ExecutionHostClaimCandidate = {
  hostId: HOST_ID,
  principalId: HOST_PRINCIPAL_ID,
  ownerPrincipalId: OWNER_PRINCIPAL_ID,
  tenantId: TENANT_ID,
  sessionId: "host-session-1",
  sessionGeneration: 1,
  hubInstanceId: "hub-1",
  capabilities: [{ capability: "runtime:browser", state: "available" }],
};

describe.skipIf(!harnessDbEnvAvailable())(
  "execution host assignments (real DB)",
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
        id: OWNER_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
      });
      await seedPrincipal(h.db, {
        id: HOST_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "host",
        refId: HOST_ID,
      });
      await h.db.insert(executionHost).values({
        id: HOST_ID,
        tenantId: TENANT_ID,
        principalId: HOST_PRINCIPAL_ID,
        ownerPrincipalId: OWNER_PRINCIPAL_ID,
        displayName: "Assignment host",
        tokenHashSha256: new Uint8Array([1, 2, 3]),
      });
      await h.db.insert(executionHostSession).values({
        hostId: HOST_ID,
        sessionId: candidate.sessionId,
        generation: candidate.sessionGeneration,
        hubInstanceId: candidate.hubInstanceId,
        capabilities: candidate.capabilities,
        leaseExpiresAt: new Date(NOW.getTime() + 60_000),
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd-host-assignment",
        tenantId: TENANT_ID,
        name: "host-assignment",
      });
      await createAllocation("allocation-1", "run-1", "sidecar-1");
    });

    async function createAllocation(
      allocationId: string,
      runId: string,
      sidecarId: string,
    ): Promise<void> {
      await seedWorkflowRun(h.db, {
        id: runId,
        anchorRunId: runId,
        tenantId: TENANT_ID,
        definitionId: "wfd-host-assignment",
      });
      await h.db.insert(sidecar).values({
        id: sidecarId,
        tokenHashSha256: new Uint8Array([
          sidecarId.charCodeAt(sidecarId.length - 1),
        ]),
      });
      await h.db.insert(sidecarAllocation).values({
        id: allocationId,
        anchorRunId: runId,
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_PRINCIPAL_ID,
        targetHostPrincipalId: HOST_PRINCIPAL_ID,
        placementPolicy: { tenantPolicies: [], workflowRules: [] },
        provisionerId: "host-capacity",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "host-capacity:v1",
        sidecarId,
        status: "provisioning",
        generation: 1,
      });
    }

    test("claims and settles only the exact host session", async () => {
      const store = createExecutionHostAssignmentStore(h.db);
      const claimed = await store.claim({
        allocationId: "allocation-1",
        generation: 1,
        sidecarId: "sidecar-1",
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_PRINCIPAL_ID,
        targetHostPrincipalId: HOST_PRINCIPAL_ID,
        candidate,
        now: NOW,
      });

      expect(claimed).toMatchObject({
        status: "claiming",
        hostId: HOST_ID,
        hostSessionId: candidate.sessionId,
        hostSessionGeneration: 1,
      });
      expect(
        await store.markAssigned({
          allocationId: "allocation-1",
          generation: 1,
          sidecarId: "sidecar-1",
          hostId: HOST_ID,
          hostSessionId: candidate.sessionId,
          hostSessionGeneration: 2,
          now: NOW,
        }),
      ).toBeNull();
      expect(
        await store.markAssigned({
          allocationId: "allocation-1",
          generation: 1,
          sidecarId: "sidecar-1",
          hostId: HOST_ID,
          hostSessionId: candidate.sessionId,
          hostSessionGeneration: 1,
          now: NOW,
        }),
      ).toMatchObject({ status: "assigned" });
    });

    test("allows a replacement sidecar after the old identity is destroyed", async () => {
      const store = createExecutionHostAssignmentStore(h.db);
      await store.claim({
        allocationId: "allocation-1",
        generation: 1,
        sidecarId: "sidecar-1",
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_PRINCIPAL_ID,
        candidate,
        now: NOW,
      });
      expect(
        await store.destroy({
          allocationId: "allocation-1",
          generation: 2,
          sidecarId: "sidecar-1",
          now: NOW,
        }),
      ).toMatchObject({ status: "destroyed", destroyedGeneration: 2 });

      await h.db.insert(sidecar).values({
        id: "sidecar-2",
        tokenHashSha256: new Uint8Array([9, 9, 9]),
      });
      await h.db
        .update(sidecarAllocation)
        .set({ generation: 2, sidecarId: "sidecar-2" })
        .where(eq(sidecarAllocation.id, "allocation-1"));

      expect(
        await store.claim({
          allocationId: "allocation-1",
          generation: 2,
          sidecarId: "sidecar-2",
          tenantId: TENANT_ID,
          placementPrincipalId: OWNER_PRINCIPAL_ID,
          candidate,
          now: NOW,
        }),
      ).toMatchObject({ status: "claiming", sidecarId: "sidecar-2" });
      expect(
        await store.claim({
          allocationId: "allocation-1",
          generation: 1,
          sidecarId: "sidecar-1",
          tenantId: TENANT_ID,
          placementPrincipalId: OWNER_PRINCIPAL_ID,
          candidate,
          now: NOW,
        }),
      ).toBeNull();
    });

    test("lets only one allocation claim a host concurrently", async () => {
      await createAllocation("allocation-2", "run-2", "sidecar-2");
      const store = createExecutionHostAssignmentStore(h.db);

      const claims = await Promise.all([
        store.claim({
          allocationId: "allocation-1",
          generation: 1,
          sidecarId: "sidecar-1",
          tenantId: TENANT_ID,
          placementPrincipalId: OWNER_PRINCIPAL_ID,
          candidate,
          now: NOW,
        }),
        store.claim({
          allocationId: "allocation-2",
          generation: 1,
          sidecarId: "sidecar-2",
          tenantId: TENANT_ID,
          placementPrincipalId: OWNER_PRINCIPAL_ID,
          candidate,
          now: NOW,
        }),
      ]);

      expect(claims.filter((claim) => claim !== null)).toHaveLength(1);
    });

    test("rejects stale sessions, expired leases, and the wrong owner", async () => {
      const store = createExecutionHostAssignmentStore(h.db);
      const base = {
        allocationId: "allocation-1",
        generation: 1,
        sidecarId: "sidecar-1",
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_PRINCIPAL_ID,
        candidate,
        now: NOW,
      };

      expect(
        await store.claim({
          ...base,
          candidate: { ...candidate, sessionGeneration: 0 },
        }),
      ).toBeNull();
      expect(
        await store.claim({
          ...base,
          now: new Date(NOW.getTime() + 120_000),
        }),
      ).toBeNull();
      expect(
        await store.claim({
          ...base,
          placementPrincipalId: "principal-other",
        }),
      ).toBeNull();
    });
  },
);
