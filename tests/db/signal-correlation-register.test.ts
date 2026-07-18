import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { generateKeyPair, signEd25519 } from "@intx/crypto";
import { hexDecode, hexEncode, signalName } from "@intx/types";
import { createApprovalStore, createSignalCorrelationStore } from "@intx/db";
import {
  approval,
  signalCorrelation,
  workflowDeployment,
} from "@intx/db/schema";
import { generateId } from "@intx/hub-common";
import {
  createHubSessionLookups,
  createSidecarRouter,
  type AgentRepoStore,
  type SidecarAuthenticator,
  type WsHandle,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedTenants,
  seedWorkflowDeployment,
} from "@intx/test-harness/seed";

// The register handler never touches the repo store, so a throwing stub keeps
// the AgentRepoStore surface satisfied without a real on-disk store. Mirrors
// deployment-key-lookup.test.ts.
// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; registerSignalCorrelation does not touch the repo store
const stubRepoStore = new Proxy(
  {},
  {
    get() {
      throw new Error(
        "agentRepoStore is not used by registerSignalCorrelation",
      );
    },
  },
) as AgentRepoStore;

const acceptAnySidecar: SidecarAuthenticator = async ({ sidecarId }) => ({
  kind: "sidecar",
  sidecarId,
});

function createMockWs(): WsHandle & { sent: string[]; closed: boolean } {
  return {
    sent: [],
    closed: false,
    send(data: string) {
      this.sent.push(data);
    },
    close() {
      this.closed = true;
    },
  };
}

function findFrame(ws: ReturnType<typeof createMockWs>, type: string) {
  return ws.sent.map((s) => JSON.parse(s)).find((f) => f.type === type);
}

async function signChallenge(
  nonce: string,
  address: string,
  privateKey: Uint8Array,
): Promise<string> {
  const nonceBytes = hexDecode(nonce);
  const addressBytes = new TextEncoder().encode(address);
  const payload = new Uint8Array(nonceBytes.length + addressBytes.length);
  payload.set(nonceBytes);
  payload.set(addressBytes, nonceBytes.length);
  const sig = await signEd25519(privateKey, payload);
  return hexEncode(new Uint8Array(sig));
}

const TENANT = "t1";
const ASSET = "asset1";
const DEPLOYMENT = "dep1";
const WF_ADDR = "ins_dep_abc@wf.example";

// The register frame requires an approver-facing snapshot: the ask rail is its
// only producer and always carries one. Frames built without it fail the union
// parse at the receiver, so every frame these tests send carries this snapshot.
const SNAPSHOT = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

// A second live deployment on the same tenant, so a connection can own an
// address OTHER than WF_ADDR for the ownership-gate rejection case.
const DEPLOYMENT_2 = "dep2";
const WF_ADDR_2 = "ins_dep_xyz@wf.example";

describe.skipIf(!harnessDbEnvAvailable())(
  "signal.correlation.register co-write (real DB)",
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
    });

    // Seed a live deployment whose address resolves to `publicKeyHex`, so the
    // reconnect challenge that routes WF_ADDR onto the connection passes.
    async function seedDeployment(publicKeyHex: string): Promise<void> {
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: "wf",
      });
      await seedWorkflowDeployment(h.db, {
        id: DEPLOYMENT,
        tenantId: TENANT,
        definitionAssetId: ASSET,
        address: WF_ADDR,
        publicKey: publicKeyHex,
        status: "deployed",
      });
    }

    // Bring WF_ADDR up as an owned workflow address on `ws` through the real
    // challenged reconnect path, so the register handler's ownership gate lets
    // the frame through.
    async function reconnectAndVerify(
      router: ReturnType<typeof createSidecarRouter>,
      ws: ReturnType<typeof createMockWs>,
      privateKey: Uint8Array,
    ): Promise<void> {
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [WF_ADDR],
        }),
      );
      await new Promise((res) => setTimeout(res, 50));

      const challenge = findFrame(ws, "challenge");
      const { address, nonce } = challenge.challenges[0];
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [
            {
              address,
              signature: await signChallenge(nonce, address, privateKey),
            },
          ],
        }),
      );
      await new Promise((res) => setTimeout(res, 50));
    }

    // Bring an arbitrary workflow address up as an owned route on `ws` through
    // the same challenged reconnect path `reconnectAndVerify` uses, so a
    // negative-path case can own a DIFFERENT address than the frame it delivers.
    async function reconnectAddress(
      router: ReturnType<typeof createSidecarRouter>,
      ws: ReturnType<typeof createMockWs>,
      address: string,
      privateKey: Uint8Array,
    ): Promise<void> {
      router.handleOpen(ws);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [address],
        }),
      );
      await new Promise((res) => setTimeout(res, 50));

      const challenge = findFrame(ws, "challenge");
      const entry = challenge.challenges.find(
        (c: { address: string }) => c.address === address,
      );
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "challenge.response",
          responses: [
            {
              address: entry.address,
              signature: await signChallenge(
                entry.nonce,
                entry.address,
                privateKey,
              ),
            },
          ],
        }),
      );
      await new Promise((res) => setTimeout(res, 50));
    }

    function buildRouter() {
      const lookups = createHubSessionLookups({
        db: h.db,
        agentRepoStore: stubRepoStore,
      });
      return createSidecarRouter({
        authenticateSidecar: acceptAnySidecar,
        challengeTimeoutMs: 5000,
        lookups,
      });
    }

    function registerFrame() {
      return JSON.stringify({
        type: "signal.correlation.register",
        correlationId: "corr-1",
        runId: "run-1",
        deploymentId: DEPLOYMENT,
        agentAddress: WF_ADDR,
        kind: "approval",
        snapshot: SNAPSHOT,
      });
    }

    // Wait for the router's per-ws message chain to drain, since the register
    // frame is dispatched asynchronously through it.
    async function drain(): Promise<void> {
      await new Promise((res) => setTimeout(res, 50));
    }

    test("co-writes the correlation and approval rows for a delivered frame", async () => {
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      const router = buildRouter();
      const ws = createMockWs();
      await reconnectAndVerify(router, ws, kp.privateKey);
      expect(router.getRoutableAddresses()).toContain(WF_ADDR);

      router.handleMessage(ws, registerFrame());
      await drain();

      const correlations = await h.db.select().from(signalCorrelation);
      expect(correlations).toHaveLength(1);
      const corr = correlations[0];
      expect(corr?.correlationId).toBe("corr-1");
      expect(corr?.tenantId).toBe(TENANT);
      expect(corr?.deploymentId).toBe(DEPLOYMENT);
      expect(corr?.agentAddress).toBe(WF_ADDR);
      expect(corr?.runId).toBe("run-1");
      expect(corr?.kind).toBe("approval");
      // signalName is derived by the hub, not carried on the wire.
      expect(corr?.signalName).toBe(signalName("corr-1"));
      expect(corr?.signalName).toBe("__signal__:corr-1");
      expect(corr?.resolvedAt).toBeNull();

      const approvals = await h.db.select().from(approval);
      expect(approvals).toHaveLength(1);
      const appr = approvals[0];
      expect(appr?.correlationId).toBe("corr-1");
      expect(appr?.tenantId).toBe(TENANT);
      expect(appr?.deploymentId).toBe(DEPLOYMENT);
      expect(appr?.runId).toBe("run-1");
      expect(appr?.agentAddress).toBe(WF_ADDR);
      expect(appr?.status).toBe("pending");
      // The register frame's snapshot is co-written verbatim: the tool
      // definition (name/description/inputSchema) and the live arguments.
      expect(appr?.toolDefinition).toEqual({
        name: SNAPSHOT.name,
        description: SNAPSHOT.description,
        inputSchema: SNAPSHOT.inputSchema,
      });
      expect(appr?.toolArguments).toEqual(SNAPSHOT.arguments);
      expect(appr?.scope).toBeNull();
      // hold-indefinitely: no deadline reaches the co-write.
      expect(appr?.timeoutAt).toBeNull();
      expect(appr?.resolvedAt).toBeNull();
    });

    test("a duplicate frame is an idempotent no-op", async () => {
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      const router = buildRouter();
      const ws = createMockWs();
      await reconnectAndVerify(router, ws, kp.privateKey);

      router.handleMessage(ws, registerFrame());
      await drain();

      const firstCorr = await h.db.select().from(signalCorrelation);
      const firstAppr = await h.db.select().from(approval);
      expect(firstCorr).toHaveLength(1);
      expect(firstAppr).toHaveLength(1);
      const approvalId = firstAppr[0]?.id;
      const createdAt = firstCorr[0]?.createdAt;

      // Redeliver the identical frame: reconnect replay / supervisor restart.
      router.handleMessage(ws, registerFrame());
      await drain();

      const secondCorr = await h.db.select().from(signalCorrelation);
      const secondAppr = await h.db.select().from(approval);
      expect(secondCorr).toHaveLength(1);
      expect(secondAppr).toHaveLength(1);
      // The original rows are untouched -- no second insert, no id churn.
      expect(secondAppr[0]?.id).toBe(approvalId);
      expect(secondCorr[0]?.createdAt).toEqual(createdAt);
    });

    test("rejects a frame for an address the connection does not own", async () => {
      // The connection owns WF_ADDR_2, not WF_ADDR. The delivered frame targets
      // WF_ADDR -- an address that IS seeded as a live deployment, so the only
      // thing standing between the spoofed frame and a co-write is the handler's
      // ownership gate. Removing that gate would let this frame write rows.
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      // seedDeployment already seeded the tenant and asset; add a second
      // deployment on them so the connection can own WF_ADDR_2.
      const kp2 = await generateKeyPair();
      await seedWorkflowDeployment(h.db, {
        id: DEPLOYMENT_2,
        tenantId: TENANT,
        definitionAssetId: ASSET,
        address: WF_ADDR_2,
        publicKey: hexEncode(kp2.publicKey),
        status: "deployed",
      });

      const router = buildRouter();
      const ws = createMockWs();
      await reconnectAddress(router, ws, WF_ADDR_2, kp2.privateKey);
      expect(router.getRoutableAddresses()).toContain(WF_ADDR_2);
      expect(router.getRoutableAddresses()).not.toContain(WF_ADDR);

      // The default registerFrame targets WF_ADDR, which this connection does
      // not own.
      router.handleMessage(ws, registerFrame());
      await drain();

      const correlations = await h.db
        .select()
        .from(signalCorrelation)
        .where(eq(signalCorrelation.correlationId, "corr-1"));
      expect(correlations).toHaveLength(0);
      const approvals = await h.db
        .select()
        .from(approval)
        .where(eq(approval.correlationId, "corr-1"));
      expect(approvals).toHaveLength(0);
    });

    test("rejects a frame whose deploymentId does not match the address", async () => {
      // WF_ADDR is owned and resolves to DEPLOYMENT, but the frame claims
      // DEPLOYMENT_2. registerSignalCorrelation cross-checks the frame's
      // deploymentId against the deployment the address resolves to and throws
      // on a mismatch; the handler swallows the throw, so no rows are written.
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      await seedWorkflowDeployment(h.db, {
        id: DEPLOYMENT_2,
        tenantId: TENANT,
        definitionAssetId: ASSET,
        address: WF_ADDR_2,
        publicKey: null,
        status: "deployed",
      });

      const router = buildRouter();
      const ws = createMockWs();
      await reconnectAndVerify(router, ws, kp.privateKey);
      expect(router.getRoutableAddresses()).toContain(WF_ADDR);

      router.handleMessage(
        ws,
        JSON.stringify({
          type: "signal.correlation.register",
          correlationId: "corr-1",
          runId: "run-1",
          deploymentId: DEPLOYMENT_2,
          agentAddress: WF_ADDR,
          kind: "approval",
          // Carry a snapshot so this frame passes the parse and the test
          // exercises tenancy rejection, not accidental parse-drop.
          snapshot: SNAPSHOT,
        }),
      );
      await drain();

      const correlations = await h.db.select().from(signalCorrelation);
      expect(correlations).toHaveLength(0);
      const approvals = await h.db.select().from(approval);
      expect(approvals).toHaveLength(0);
    });

    test("rejects a frame whose deployment is no longer deployed", async () => {
      // Bring WF_ADDR up while its deployment is live, then tear the deployment
      // down (status flips off "deployed") with the connection still owning the
      // address. registerSignalCorrelation filters to a deployed deployment, so
      // the now-torn-down address resolves no row and it throws; the handler
      // swallows the throw and writes nothing.
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));

      const router = buildRouter();
      const ws = createMockWs();
      await reconnectAndVerify(router, ws, kp.privateKey);
      expect(router.getRoutableAddresses()).toContain(WF_ADDR);

      // Tear the deployment down after the address is already routed, so the
      // ownership gate still passes but the deployed-only resolution misses.
      await h.db
        .update(workflowDeployment)
        .set({ status: "error" })
        .where(eq(workflowDeployment.id, DEPLOYMENT));

      router.handleMessage(ws, registerFrame());
      await drain();

      const correlations = await h.db.select().from(signalCorrelation);
      expect(correlations).toHaveLength(0);
      const approvals = await h.db.select().from(approval);
      expect(approvals).toHaveLength(0);
    });

    test("store inserts are idempotent: second call returns null, not a throw", async () => {
      // Direct store test, bypassing the error-swallowing handler. The
      // handler-level idempotency test cannot tell a clean onConflictDoNothing
      // no-op apart from a throw-and-rollback, because the handler swallows
      // throws either way. This pins the onConflictDoNothing contract: the first
      // insert returns the parsed row, the second is a no-op that returns null
      // WITHOUT throwing.
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: "wf",
      });
      await seedWorkflowDeployment(h.db, {
        id: DEPLOYMENT,
        tenantId: TENANT,
        definitionAssetId: ASSET,
        address: WF_ADDR,
        publicKey: null,
        status: "deployed",
      });

      const signalCorrelationStore = createSignalCorrelationStore(h.db);
      const approvalStore = createApprovalStore(h.db);

      const correlationRow = {
        correlationId: "corr-1",
        tenantId: TENANT,
        deploymentId: DEPLOYMENT,
        agentAddress: WF_ADDR,
        runId: "run-1",
        signalName: signalName("corr-1"),
        kind: "approval" as const,
      };

      const firstCorr =
        await signalCorrelationStore.registerIfAbsent(correlationRow);
      expect(firstCorr).not.toBeNull();
      expect(firstCorr?.correlationId).toBe("corr-1");

      const secondCorr =
        await signalCorrelationStore.registerIfAbsent(correlationRow);
      expect(secondCorr).toBeNull();

      const approvalRow = {
        id: generateId("approval"),
        tenantId: TENANT,
        deploymentId: DEPLOYMENT,
        runId: "run-1",
        agentAddress: WF_ADDR,
        correlationId: "corr-1",
        status: "pending" as const,
        toolDefinition: null,
        toolArguments: null,
        scope: null,
        timeoutAt: null,
      };

      const firstAppr = await approvalStore.createIfAbsent(approvalRow);
      expect(firstAppr).not.toBeNull();
      expect(firstAppr?.correlationId).toBe("corr-1");

      // A fresh id on the redelivered row: the dedup key is correlationId, not
      // the primary key, so a distinct id must still conflict-and-no-op.
      const secondAppr = await approvalStore.createIfAbsent({
        ...approvalRow,
        id: generateId("approval"),
      });
      expect(secondAppr).toBeNull();

      // Exactly one of each row survived the duplicate inserts.
      expect(await h.db.select().from(signalCorrelation)).toHaveLength(1);
      expect(await h.db.select().from(approval)).toHaveLength(1);
    });
  },
);
