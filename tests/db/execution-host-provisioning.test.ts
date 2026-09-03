import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { sha256 } from "@intx/crypto";
import {
  createExecutionHostAssignmentStore,
  createExecutionHostSessionStore,
} from "@intx/db";
import {
  executionHost,
  sidecar,
  sidecarAllocation,
  sidecarOperation,
  workflowDefinition,
} from "@intx/db/schema";
import { eq } from "drizzle-orm";
import {
  createExistingSidecarCapacity,
  createExecutionHostControlRouter,
  type SidecarProvisioner,
  type WsHandle,
} from "@intx/hub-sessions";
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

const TENANT_ID = "tnt-host-provisioning";
const OWNER_A = "prn-owner-a";
const OWNER_B = "prn-owner-b";
const HOST_A = "hst-a";
const HOST_B = "hst-b";
const HOST_PRINCIPAL_A = "prn-host-a";
const HOST_PRINCIPAL_B = "prn-host-b";
const TOKEN_A = "intx_host_a";
const TOKEN_B = "intx_host_b";

function createWs(): WsHandle & { sent: string[]; closed: boolean } {
  return {
    sent: [],
    closed: false,
    send(data) {
      this.sent.push(data);
    },
    close() {
      this.closed = true;
    },
  };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = performance.now() + 1_000;
  while (performance.now() < deadline) {
    if (predicate()) return;
    await Bun.sleep(5);
  }
  throw new Error("Timed out waiting for execution host provisioning");
}

function frameOfType(ws: { sent: string[] }, type: string) {
  return ws.sent
    .map((frame): unknown => JSON.parse(frame))
    .find(
      (frame) =>
        typeof frame === "object" &&
        frame !== null &&
        Reflect.get(frame, "type") === type,
    );
}

function createReuseFirstProvisioner(): SidecarProvisioner {
  return {
    id: "browser-host",
    apiVersion: 1,
    bindingFingerprint: "browser-host:v1",
    capabilities: [{ capability: "runtime:browser", state: "available" }],
    async ensure(request, { existingSidecars }) {
      return (
        (await existingSidecars.claim(request, {
          chooseHost: (candidates) => candidates[0]?.hostId ?? null,
        })) ?? {
          kind: "accepted",
          externalRef: "created-capacity",
        }
      );
    },
    async destroy(request, { existingSidecars }) {
      return (await existingSidecars.release(request)) ?? { kind: "destroyed" };
    },
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "execution host provisioning (real DB)",
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
        id: OWNER_A,
        tenantId: TENANT_ID,
        kind: "user",
      });
      await seedPrincipal(h.db, {
        id: OWNER_B,
        tenantId: TENANT_ID,
        kind: "user",
      });
      await createHost(HOST_A, HOST_PRINCIPAL_A, OWNER_A, TOKEN_A);
      await createHost(HOST_B, HOST_PRINCIPAL_B, OWNER_B, TOKEN_B);
      await h.db.insert(workflowDefinition).values({
        id: "wfd-host-provisioning",
        tenantId: TENANT_ID,
        name: "host-provisioning",
      });
      await createAllocation();
    });

    async function createHost(
      hostId: string,
      principalId: string,
      ownerPrincipalId: string,
      token: string,
    ): Promise<void> {
      await seedPrincipal(h.db, {
        id: principalId,
        tenantId: TENANT_ID,
        kind: "host",
        refId: hostId,
      });
      await h.db.insert(executionHost).values({
        id: hostId,
        tenantId: TENANT_ID,
        principalId,
        ownerPrincipalId,
        displayName: hostId,
        tokenHashSha256: await sha256(token),
      });
    }

    async function createAllocation(): Promise<void> {
      await seedWorkflowRun(h.db, {
        id: "run-host-provisioning",
        anchorRunId: "run-host-provisioning",
        tenantId: TENANT_ID,
        definitionId: "wfd-host-provisioning",
      });
      await h.db.insert(sidecar).values({
        id: "sidecar-host-provisioning",
        tokenHashSha256: new Uint8Array([7, 8, 9]),
      });
      await h.db
        .insert(sidecarOperation)
        .values({ id: "allocation-host-provisioning" });
      await h.db.insert(sidecarAllocation).values({
        id: "allocation-host-provisioning",
        anchorRunId: "run-host-provisioning",
        tenantId: TENANT_ID,
        placementPrincipalId: OWNER_A,
        targetHostPrincipalId: HOST_PRINCIPAL_A,
        placementPolicy: {
          tenantPolicies: [],
          workflowRules: [{ capability: "runtime:browser", effect: "require" }],
        },
        provisionerId: "browser-host",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "browser-host:v1",
        sidecarId: "sidecar-host-provisioning",
        status: "provisioning",
        generation: 1,
      });
    }

    test("delivers and releases an allocation only through its targeted host", async () => {
      let sessionCounter = 0;
      const router = createExecutionHostControlRouter({
        store: createExecutionHostSessionStore(h.db),
        hubInstanceId: "hub-test",
        createSessionId: () => {
          sessionCounter += 1;
          return `host-session-${String(sessionCounter)}`;
        },
      });
      const hostA = createWs();
      const hostB = createWs();
      register(router, hostA, HOST_A, TOKEN_A);
      register(router, hostB, HOST_B, TOKEN_B);
      await waitFor(
        () =>
          frameOfType(hostA, "host.registered") !== undefined &&
          frameOfType(hostB, "host.registered") !== undefined,
      );
      const existingSidecars = createExistingSidecarCapacity({
        router,
        assignments: createExecutionHostAssignmentStore(h.db),
        acknowledgementTimeoutMs: 500,
      });
      const provisioner = createReuseFirstProvisioner();

      const ensured = provisioner.ensure(
        {
          allocationId: "allocation-host-provisioning",
          generation: 1,
          tenantId: TENANT_ID,
          placementPrincipalId: OWNER_A,
          targetHostPrincipalId: HOST_PRINCIPAL_A,
          placementPolicy: {
            tenantPolicies: [],
            workflowRules: [
              { capability: "runtime:browser", effect: "require" },
            ],
          },
          anchorRunId: "run-host-provisioning",
          sidecarId: "sidecar-host-provisioning",
          token: "runtime-token",
          hubWebSocketUrl: "wss://hub.example/api/sidecars/ws",
        },
        { existingSidecars },
      );
      await waitFor(() => frameOfType(hostA, "host.assignment") !== undefined);
      expect(frameOfType(hostB, "host.assignment")).toBeUndefined();
      router.handleMessage(
        hostA,
        JSON.stringify({
          type: "host.assignment.ack",
          allocationId: "allocation-host-provisioning",
          generation: 1,
          sidecarId: "sidecar-host-provisioning",
        }),
      );
      expect(await ensured).toEqual({ kind: "accepted", externalRef: HOST_A });

      const released = provisioner.destroy(
        {
          allocationId: "allocation-host-provisioning",
          generation: 2,
          sidecarId: "sidecar-host-provisioning",
        },
        { existingSidecars },
      );
      await waitFor(() => frameOfType(hostA, "host.release") !== undefined);
      expect(frameOfType(hostB, "host.release")).toBeUndefined();
      router.handleMessage(
        hostA,
        JSON.stringify({
          type: "host.release.ack",
          allocationId: "allocation-host-provisioning",
          generation: 2,
          sidecarId: "sidecar-host-provisioning",
        }),
      );
      expect(await released).toEqual({ kind: "destroyed" });
    });

    test("does not assign another principal's connected host", async () => {
      await h.db
        .update(sidecarAllocation)
        .set({ targetHostPrincipalId: null })
        .where(eq(sidecarAllocation.id, "allocation-host-provisioning"));
      const router = createExecutionHostControlRouter({
        store: createExecutionHostSessionStore(h.db),
        hubInstanceId: "hub-test",
        createSessionId: () => "host-session-b",
      });
      const hostB = createWs();
      register(router, hostB, HOST_B, TOKEN_B);
      await waitFor(() => frameOfType(hostB, "host.registered") !== undefined);
      const existingSidecars = createExistingSidecarCapacity({
        router,
        assignments: createExecutionHostAssignmentStore(h.db),
      });
      const provisioner = createReuseFirstProvisioner();

      expect(
        await provisioner.ensure(
          {
            allocationId: "allocation-host-provisioning",
            generation: 1,
            tenantId: TENANT_ID,
            placementPrincipalId: OWNER_A,
            placementPolicy: {
              tenantPolicies: [],
              workflowRules: [
                { capability: "runtime:browser", effect: "require" },
              ],
            },
            anchorRunId: "run-host-provisioning",
            sidecarId: "sidecar-host-provisioning",
            token: "runtime-token",
            hubWebSocketUrl: "wss://hub.example/api/sidecars/ws",
          },
          { existingSidecars },
        ),
      ).toEqual({
        kind: "accepted",
        externalRef: "created-capacity",
      });
      expect(frameOfType(hostB, "host.assignment")).toBeUndefined();
    });

    test("assigns a host shared with the placement principal", async () => {
      await h.db
        .update(sidecarAllocation)
        .set({ targetHostPrincipalId: null })
        .where(eq(sidecarAllocation.id, "allocation-host-provisioning"));
      const router = createExecutionHostControlRouter({
        store: createExecutionHostSessionStore(h.db),
        hubInstanceId: "hub-test",
        createSessionId: () => "host-session-b",
      });
      const hostB = createWs();
      register(router, hostB, HOST_B, TOKEN_B);
      await waitFor(() => frameOfType(hostB, "host.registered") !== undefined);
      const existingSidecars = createExistingSidecarCapacity({
        router,
        assignments: createExecutionHostAssignmentStore(h.db),
        canUseHost: async ({ hostId }) => hostId === HOST_B,
        acknowledgementTimeoutMs: 500,
      });
      const provisioner = createReuseFirstProvisioner();

      const ensured = provisioner.ensure(
        {
          allocationId: "allocation-host-provisioning",
          generation: 1,
          tenantId: TENANT_ID,
          placementPrincipalId: OWNER_A,
          placementPolicy: {
            tenantPolicies: [],
            workflowRules: [
              { capability: "runtime:browser", effect: "require" },
            ],
          },
          anchorRunId: "run-host-provisioning",
          sidecarId: "sidecar-host-provisioning",
          token: "runtime-token",
          hubWebSocketUrl: "wss://hub.example/api/sidecars/ws",
        },
        { existingSidecars },
      );
      await waitFor(() => frameOfType(hostB, "host.assignment") !== undefined);
      router.handleMessage(
        hostB,
        JSON.stringify({
          type: "host.assignment.ack",
          allocationId: "allocation-host-provisioning",
          generation: 1,
          sidecarId: "sidecar-host-provisioning",
        }),
      );

      expect(await ensured).toEqual({ kind: "accepted", externalRef: HOST_B });
    });

    function register(
      router: ReturnType<typeof createExecutionHostControlRouter>,
      ws: WsHandle,
      hostId: string,
      token: string,
    ): void {
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "host.register",
          hostId,
          token,
          capabilities: [{ capability: "runtime:browser", state: "available" }],
        }),
      );
    }
  },
);
