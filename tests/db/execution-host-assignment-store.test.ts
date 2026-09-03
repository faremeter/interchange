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
  createWorkflowProbeStore,
  type ExecutionHostClaimCandidate,
} from "@intx/db";
import {
  asset,
  executionHost,
  executionHostSession,
  principal,
  sidecar,
  sidecarAllocation,
  sidecarOperation,
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
      await h.db.insert(asset).values({
        id: "ast-host-assignment",
        tenantId: TENANT_ID,
        kind: "workflow",
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
      await h.db.insert(sidecarOperation).values({ id: allocationId });
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
        operationId: "allocation-1",
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
      const target = {
        operationId: "allocation-1",
        generation: 1,
        sidecarId: "sidecar-1",
        hostPrincipalId: HOST_PRINCIPAL_ID,
      };
      expect(await store.matchesTargetHost(target)).toBe(false);
      expect(await store.listAvailableCandidates([candidate], NOW)).toEqual([]);
      expect(
        await store.markAssigned({
          operationId: "allocation-1",
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
          operationId: "allocation-1",
          generation: 1,
          sidecarId: "sidecar-1",
          hostId: HOST_ID,
          hostSessionId: candidate.sessionId,
          hostSessionGeneration: 1,
          now: NOW,
        }),
      ).toMatchObject({ status: "assigned" });
      expect(await store.matchesTargetHost(target)).toBe(true);
      expect(
        await store.matchesTargetHost({
          ...target,
          hostPrincipalId: OWNER_PRINCIPAL_ID,
        }),
      ).toBe(false);
      expect(await store.matchesTargetHost({ ...target, generation: 2 })).toBe(
        false,
      );
      expect(
        await store.matchesTargetHost({
          ...target,
          operationId: "other-operation",
        }),
      ).toBe(false);
      expect(
        await store.matchesTargetHost({
          ...target,
          sidecarId: "other-sidecar",
        }),
      ).toBe(false);
      expect(
        await store.markAssigned({
          operationId: "allocation-1",
          generation: 1,
          sidecarId: "sidecar-1",
          hostId: HOST_ID,
          hostSessionId: candidate.sessionId,
          hostSessionGeneration: 1,
          now: NOW,
        }),
      ).toMatchObject({ status: "assigned" });
    });

    test.each([
      "replacing",
      "releasing",
      "destroy_failed",
      "released",
      "failed",
    ] as const)(
      "rejects claims for a %s allocation even when its identity and generation match",
      async (status) => {
        await h.db
          .update(sidecarAllocation)
          .set({ status })
          .where(eq(sidecarAllocation.id, "allocation-1"));
        const store = createExecutionHostAssignmentStore(h.db);

        expect(
          await store.claim({
            operationId: "allocation-1",
            generation: 1,
            sidecarId: "sidecar-1",
            tenantId: TENANT_ID,
            placementPrincipalId: OWNER_PRINCIPAL_ID,
            targetHostPrincipalId: HOST_PRINCIPAL_ID,
            candidate,
            now: NOW,
          }),
        ).toBeNull();
        expect(await store.listAvailableCandidates([candidate], NOW)).toEqual([
          candidate,
        ]);
      },
    );

    test("returns the existing assignment after its allocation becomes allocated", async () => {
      const store = createExecutionHostAssignmentStore(h.db);
      const request = {
        operationId: "allocation-1",
        generation: 1,
        sidecarId: "sidecar-1",
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_PRINCIPAL_ID,
        targetHostPrincipalId: HOST_PRINCIPAL_ID,
        candidate,
        now: NOW,
      };
      const claimed = await store.claim(request);
      expect(claimed).not.toBeNull();
      const assigned = await store.markAssigned({
        operationId: request.operationId,
        generation: request.generation,
        sidecarId: request.sidecarId,
        hostId: candidate.hostId,
        hostSessionId: candidate.sessionId,
        hostSessionGeneration: candidate.sessionGeneration,
        now: NOW,
      });
      expect(assigned).toMatchObject({ status: "assigned" });
      await h.db
        .update(sidecarAllocation)
        .set({ status: "allocated" })
        .where(eq(sidecarAllocation.id, request.operationId));

      expect(await store.claim(request)).toEqual(assigned);
    });

    test("offers only active hosts with a current unexpired session", async () => {
      const store = createExecutionHostAssignmentStore(h.db);
      expect(await store.listAvailableCandidates([candidate], NOW)).toEqual([
        candidate,
      ]);
      expect(
        await store.listAvailableCandidates(
          [{ ...candidate, sessionGeneration: 2 }],
          NOW,
        ),
      ).toEqual([]);
      expect(
        await store.listAvailableCandidates(
          [{ ...candidate, hubInstanceId: "other-hub" }],
          NOW,
        ),
      ).toEqual([]);
      expect(
        await store.listAvailableCandidates(
          [candidate],
          new Date(NOW.getTime() + 120_000),
        ),
      ).toEqual([]);
      await h.db
        .update(principal)
        .set({ status: "suspended" })
        .where(eq(principal.id, HOST_PRINCIPAL_ID));
      expect(await store.listAvailableCandidates([candidate], NOW)).toEqual([]);
    });

    test("claims capacity for a workflow probe operation", async () => {
      const probeStore = createWorkflowProbeStore(h.db);
      await probeStore.create({
        id: "probe-1",
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_PRINCIPAL_ID,
        placementPolicy: { tenantPolicies: [], workflowRules: [] },
        definitionAssetId: "ast-host-assignment",
        source: {
          kind: "asset",
          assetId: "ast-host-assignment",
          package: {
            format: "source",
            commitSha: "c0ffee".padEnd(40, "0"),
          },
        },
        entry: "./workflow.mjs",
        provisionerId: "host-capacity",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "host-capacity:v1",
        now: NOW,
      });
      await probeStore.bindSidecar({
        probeId: "probe-1",
        sidecarId: "probe-sidecar-1",
        tokenHashSha256: new Uint8Array([4, 5, 6]),
        now: NOW,
      });

      const store = createExecutionHostAssignmentStore(h.db);
      expect(
        await store.claim({
          operationId: "probe-1",
          generation: 0,
          sidecarId: "probe-sidecar-1",
          tenantId: TENANT_ID,
          placementPrincipalId: "prn-other",
          candidate,
          now: NOW,
        }),
      ).toBeNull();

      const claimed = await store.claim({
        operationId: "probe-1",
        generation: 0,
        sidecarId: "probe-sidecar-1",
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_PRINCIPAL_ID,
        candidate,
        now: NOW,
      });

      expect(claimed).toMatchObject({
        operationId: "probe-1",
        sidecarId: "probe-sidecar-1",
        status: "claiming",
        capabilities: candidate.capabilities,
      });
    });

    test("keeps destroyed assignments terminal after a replacement claims the host", async () => {
      const store = createExecutionHostAssignmentStore(h.db);
      await store.claim({
        operationId: "allocation-1",
        generation: 1,
        sidecarId: "sidecar-1",
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_PRINCIPAL_ID,
        targetHostPrincipalId: HOST_PRINCIPAL_ID,
        candidate,
        now: NOW,
      });
      expect(
        await store.beginRelease({
          operationId: "allocation-1",
          generation: 2,
          sidecarId: "sidecar-1",
          candidate,
          now: NOW,
        }),
      ).toMatchObject({ status: "releasing", destroyedGeneration: 2 });
      expect(await store.listAvailableCandidates([candidate], NOW)).toEqual([]);
      expect(
        await store.markDestroyed({
          operationId: "allocation-1",
          generation: 1,
          destroyedGeneration: 2,
          sidecarId: "sidecar-1",
          hostId: HOST_ID,
          hostSessionId: candidate.sessionId,
          hostSessionGeneration: candidate.sessionGeneration,
          now: NOW,
        }),
      ).toMatchObject({ status: "destroyed", destroyedGeneration: 2 });
      expect(await store.listAvailableCandidates([candidate], NOW)).toEqual([
        candidate,
      ]);

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
          operationId: "allocation-1",
          generation: 2,
          sidecarId: "sidecar-2",
          tenantId: TENANT_ID,
          placementPrincipalId: OWNER_PRINCIPAL_ID,
          targetHostPrincipalId: HOST_PRINCIPAL_ID,
          candidate,
          now: NOW,
        }),
      ).toMatchObject({ status: "claiming", sidecarId: "sidecar-2" });
      expect(
        await store.beginRelease({
          operationId: "allocation-1",
          generation: 3,
          sidecarId: "sidecar-1",
          candidate,
          now: NOW,
        }),
      ).toMatchObject({ status: "destroyed", destroyedGeneration: 2 });
      expect(await store.findBySidecarId("sidecar-1")).toMatchObject({
        status: "destroyed",
      });
      expect(await store.findBySidecarId("sidecar-2")).toMatchObject({
        status: "claiming",
        generation: 2,
      });
      expect(
        await store.claim({
          operationId: "allocation-1",
          generation: 1,
          sidecarId: "sidecar-1",
          tenantId: TENANT_ID,
          placementPrincipalId: OWNER_PRINCIPAL_ID,
          targetHostPrincipalId: HOST_PRINCIPAL_ID,
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
          operationId: "allocation-1",
          generation: 1,
          sidecarId: "sidecar-1",
          tenantId: TENANT_ID,
          placementPrincipalId: OWNER_PRINCIPAL_ID,
          targetHostPrincipalId: HOST_PRINCIPAL_ID,
          candidate,
          now: NOW,
        }),
        store.claim({
          operationId: "allocation-2",
          generation: 1,
          sidecarId: "sidecar-2",
          tenantId: TENANT_ID,
          placementPrincipalId: OWNER_PRINCIPAL_ID,
          targetHostPrincipalId: HOST_PRINCIPAL_ID,
          candidate,
          now: NOW,
        }),
      ]);

      expect(claims.filter((claim) => claim !== null)).toHaveLength(1);
    });

    test("rejects stale sessions, expired leases, and altered placement", async () => {
      const store = createExecutionHostAssignmentStore(h.db);
      const base = {
        operationId: "allocation-1",
        generation: 1,
        sidecarId: "sidecar-1",
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_PRINCIPAL_ID,
        targetHostPrincipalId: HOST_PRINCIPAL_ID,
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

    test("rejects acknowledgements after the host session is replaced", async () => {
      const store = createExecutionHostAssignmentStore(h.db);
      await store.claim({
        operationId: "allocation-1",
        generation: 1,
        sidecarId: "sidecar-1",
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_PRINCIPAL_ID,
        targetHostPrincipalId: HOST_PRINCIPAL_ID,
        candidate,
        now: NOW,
      });
      await h.db
        .update(executionHostSession)
        .set({
          sessionId: "host-session-2",
          generation: 2,
          leaseExpiresAt: new Date(NOW.getTime() + 120_000),
        })
        .where(eq(executionHostSession.hostId, HOST_ID));

      expect(
        await store.markAssigned({
          operationId: "allocation-1",
          generation: 1,
          sidecarId: "sidecar-1",
          hostId: HOST_ID,
          hostSessionId: candidate.sessionId,
          hostSessionGeneration: candidate.sessionGeneration,
          now: NOW,
        }),
      ).toBeNull();
      expect((await store.findBySidecarId("sidecar-1"))?.status).toBe(
        "claiming",
      );
    });
  },
);
