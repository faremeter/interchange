import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq, ne, sql } from "drizzle-orm";

import { generateKeyPair } from "@intx/crypto";
import { hexEncode, signalName } from "@intx/types";
import { waitUntil } from "@intx/types/testing";
import {
  createApprovalStore,
  createDB,
  createSignalCorrelationStore,
} from "@intx/db";
import {
  approval,
  signalCorrelation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import { generateId } from "@intx/hub-common";
import {
  createWorkflowHistoryReceiveTracker,
  createHubSessionLookups,
  createSidecarRouter,
  type AgentRepoStore,
  type SidecarAuthenticator,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  loadHarnessDbConfig,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";
import {
  seedAsset,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

import { createMockWs } from "./sidecar-test-helpers";

// The lookups factory reads repoStore when constructing dispatch projection,
// but registerSignalCorrelation never performs a committed read.
// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; registerSignalCorrelation does not touch the repo store
const stubRepoStore = new Proxy(
  {},
  {
    get(_target, property) {
      if (property === "repoStore") {
        return {
          openCommittedReads() {
            throw new Error(
              "repoStore is not used by registerSignalCorrelation",
            );
          },
        };
      }
      throw new Error(
        "agentRepoStore is not used by registerSignalCorrelation",
      );
    },
  },
) as AgentRepoStore;

let authenticatedAddress = "";
let authenticatedAnchorRunId = "";
const acceptAnySidecar: SidecarAuthenticator = async ({ sidecarId }) => ({
  kind: "allocated",
  sidecarId,
  allocationId: "allocation-test",
  tenantId: TENANT,
  anchorRunId: authenticatedAnchorRunId,
  workflowRunAddress: authenticatedAddress,
  generation: 1,
});

// Backend pid of a handle's single connection; stable only for a `max: 1`
// handle, where every query reuses the one physical connection.
async function backendPid(
  handle: ReturnType<typeof createDB>,
): Promise<number> {
  const rows = await handle.db.execute(sql`SELECT pg_backend_pid() AS pid`);
  const row = rows[0];
  if (row === undefined) {
    throw new Error("pg_backend_pid returned no row");
  }
  return Number(row["pid"]);
}

const TENANT = "t1";
const ASSET = "asset1";
// Raw `run_...` id a deploy stamps onto the deployment's anchor run, NOT the
// workflow-run repo slug. The `signal_correlation.deployment_id` and
// `approval.deployment_id` FKs reference `workflow_run.id`, so this raw id is
// what the co-write writes into those columns.
const DEPLOYMENT = "run_abc123";
const WF_ADDR = "run_abc@wf.example";
// The workflow-run repo slug the supervisor derives from the address and stamps
// onto the frame's `anchorRunId` (every `@`/`.` substituted); what the
// co-write's cross-check compares against. Distinct from the raw id above.
const DEPLOYMENT_SLUG = deriveWorkflowRunRepoId(WF_ADDR);

// The register frame requires an approver-facing snapshot; frames built
// without one fail the union parse at the receiver.
const SNAPSHOT = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

// A second live deployment so a connection can own an address other than
// WF_ADDR (ownership-gate rejection case).
const DEPLOYMENT_2 = "run_xyz456";
const WF_ADDR_2 = "run_xyz@wf.example";
const DEPLOYMENT_2_SLUG = deriveWorkflowRunRepoId(WF_ADDR_2);

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

    // Seed the deployment's anchor run, which the co-write resolves by address
    // as its tenancy and definition origin.
    async function seedAnchorRun(
      id: string,
      address: string,
      publicKeyHex: string | null,
    ): Promise<void> {
      await seedWorkflowRun(h.db, {
        id,
        tenantId: TENANT,
        anchorRunId: id,
        address,
        publicKey: publicKeyHex,
        status: "running",
      });
    }

    // Seed a live deployment whose anchor row resolves from WF_ADDR.
    async function seedDeployment(publicKeyHex: string): Promise<void> {
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: "wf",
      });
      await seedAnchorRun(DEPLOYMENT, WF_ADDR, publicKeyHex);
    }

    // Bring WF_ADDR up as an owned workflow address on `ws` through the real
    // allocation-authenticated reconnect path, so the ownership gate lets the
    // frame through.
    async function reconnectAndVerify(
      router: ReturnType<typeof createSidecarRouter>,
      ws: ReturnType<typeof createMockWs>,
      _privateKey: Uint8Array,
    ): Promise<void> {
      authenticatedAddress = WF_ADDR;
      authenticatedAnchorRunId = DEPLOYMENT;
      router.fenceAllocation("allocation-test", 1);
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
      await waitUntil(() => router.getRoutableAddresses().includes(WF_ADDR));
    }

    // Bring an arbitrary workflow address up as an owned route on `ws` through
    // the same reconnect path `reconnectAndVerify` uses, so a negative-path
    // case can own a different address than the frame it delivers.
    async function reconnectAddress(
      router: ReturnType<typeof createSidecarRouter>,
      ws: ReturnType<typeof createMockWs>,
      address: string,
      _privateKey: Uint8Array,
    ): Promise<void> {
      authenticatedAddress = address;
      authenticatedAnchorRunId =
        address === WF_ADDR_2 ? DEPLOYMENT_2 : DEPLOYMENT;
      router.fenceAllocation("allocation-test", 1);
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
      await waitUntil(() => router.getRoutableAddresses().includes(address));
    }

    function buildRouter() {
      const lookups = createHubSessionLookups({
        db: h.db,
        agentRepoStore: stubRepoStore,
        historyReceives: createWorkflowHistoryReceiveTracker(),
      });
      return createSidecarRouter({
        withExecutableWorkflowRun: async (_target, send) => send(),
        authenticateSidecar: acceptAnySidecar,
        validateSidecarIdentity: async () => true,
        lookups,
      });
    }

    function registerFrame() {
      return JSON.stringify({
        type: "signal.correlation.register",
        correlationId: "corr-1",
        runId: "run-1",
        // The supervisor stamps the workflow-run repo slug, not the raw
        // deployment id, onto the frame.
        anchorRunId: DEPLOYMENT_SLUG,
        agentAddress: WF_ADDR,
        kind: "approval",
        snapshot: SNAPSHOT,
      });
    }

    // Wait for the router's per-ws message chain to drain, since the register
    // frame is dispatched asynchronously through it. The chain serializes every
    // non-bypass frame in arrival order and both `reconnect` and
    // `signal.correlation.register` are non-bypass, so queueing a reconnect
    // that claims a fresh address and waiting for it to appear in the routing
    // index proves the register queued ahead of it finished -- the only
    // evidence for a rejected register, which writes no row. Each barrier
    // claims a distinct, non-run address: a re-claim leaves the index
    // unchanged, and a run address would trip the handler's credential resync.
    let barrierCount = 0;
    async function drain(
      router: ReturnType<typeof createSidecarRouter>,
      ws: ReturnType<typeof createMockWs>,
    ): Promise<void> {
      barrierCount += 1;
      const barrierAddress = `barrier-${String(barrierCount)}@wf.example`;
      authenticatedAddress = barrierAddress;
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "reconnect",
          sidecarId: "sc-1",
          token: "tok",
          agentAddresses: [barrierAddress],
        }),
      );
      await waitUntil(() =>
        router.getRoutableAddresses().includes(barrierAddress),
      );
    }

    test("co-writes the correlation and approval rows for a delivered frame", async () => {
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      const router = buildRouter();
      const ws = createMockWs();
      await reconnectAndVerify(router, ws, kp.privateKey);
      expect(router.getRoutableAddresses()).toContain(WF_ADDR);

      router.handleMessage(ws, registerFrame());
      await drain(router, ws);

      const correlations = await h.db.select().from(signalCorrelation);
      expect(correlations).toHaveLength(1);
      const corr = correlations[0];
      expect(corr?.correlationId).toBe("corr-1");
      expect(corr?.tenantId).toBe(TENANT);
      // The FK column carries the raw deployment id, not the frame's slug.
      expect(corr?.anchorRunId).toBe(DEPLOYMENT);
      expect(corr?.anchorRunId).not.toBe(DEPLOYMENT_SLUG);
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
      expect(appr?.anchorRunId).toBe(DEPLOYMENT);
      expect(appr?.anchorRunId).not.toBe(DEPLOYMENT_SLUG);
      expect(appr?.runId).toBe("run-1");
      expect(appr?.agentAddress).toBe(WF_ADDR);
      expect(appr?.status).toBe("pending");
      // The frame's snapshot is co-written verbatim: the tool definition and
      // the live arguments.
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

      // The co-write lazily anchored the run: a workflow_run row keyed on the
      // frame's runId, same deployment and tenant, principal null (an internal
      // workflow-spawned run inherits the deployment's grants). Exclude the
      // anchor run (id == DEPLOYMENT) to isolate the child.
      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(ne(workflowRun.id, DEPLOYMENT));
      expect(runs).toHaveLength(1);
      const run = runs[0];
      expect(run?.id).toBe("run-1");
      expect(run?.anchorRunId).toBe(DEPLOYMENT);
      expect(run?.tenantId).toBe(TENANT);
      expect(run?.principalId).toBeNull();
      expect(run?.status).toBe("running");
      // The child inherits its anchor run's definition_id.
      const [anchor] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, DEPLOYMENT));
      expect(run?.definitionId).toBe(anchor?.definitionId);
    });

    test("anchors the lazily-created run on its deployment's definition", async () => {
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      // The anchor run carries a definition, so the lazily-anchored child run
      // inherits it.
      await h.db.insert(workflowDefinition).values({
        id: "wfd_native",
        tenantId: TENANT,
        name: "native",
        assetId: ASSET,
      });
      await h.db
        .update(workflowRun)
        .set({ definitionId: "wfd_native" })
        .where(eq(workflowRun.id, DEPLOYMENT));
      const router = buildRouter();
      const ws = createMockWs();
      await reconnectAndVerify(router, ws, kp.privateKey);

      router.handleMessage(ws, registerFrame());
      await drain(router, ws);

      const run = (
        await h.db
          .select()
          .from(workflowRun)
          .where(ne(workflowRun.id, DEPLOYMENT))
      )[0];
      expect(run?.id).toBe("run-1");
      expect(run?.definitionId).toBe("wfd_native");
    });

    test("writes both rows for a real raw-id deployment addressed by a slug frame", async () => {
      // Regression: a real deployment's anchor-run id is the raw `run_...` id,
      // while the frame's `anchorRunId` is the repo slug derived from the
      // address. Before the co-write's slug cross-check and raw-id FK writes
      // this path threw (raw id never equals the slug) and wrote no approval
      // row; here it must co-write both rows keyed by the raw id.
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      expect(DEPLOYMENT).not.toBe(DEPLOYMENT_SLUG);

      const lookups = createHubSessionLookups({
        db: h.db,
        agentRepoStore: stubRepoStore,
        historyReceives: createWorkflowHistoryReceiveTracker(),
      });

      // Call the co-write directly so a regression surfaces as a rejected
      // promise, not a silently-dropped frame (the router's handler swallows
      // throws).
      await lookups.registerSignalCorrelation({
        correlationId: "corr-1",
        runId: "run-1",
        anchorRunId: DEPLOYMENT_SLUG,
        agentAddress: WF_ADDR,
        kind: "approval",
        approvalSnapshot: SNAPSHOT,
      });

      const correlations = await h.db.select().from(signalCorrelation);
      expect(correlations).toHaveLength(1);
      const corr = correlations[0];
      expect(corr?.correlationId).toBe("corr-1");
      expect(corr?.tenantId).toBe(TENANT);
      // The FK column takes the raw deployment id, never the frame's slug.
      expect(corr?.anchorRunId).toBe(DEPLOYMENT);
      expect(corr?.anchorRunId).not.toBe(DEPLOYMENT_SLUG);
      expect(corr?.agentAddress).toBe(WF_ADDR);
      expect(corr?.runId).toBe("run-1");
      expect(corr?.signalName).toBe(signalName("corr-1"));

      const approvals = await h.db.select().from(approval);
      expect(approvals).toHaveLength(1);
      const appr = approvals[0];
      expect(appr?.correlationId).toBe("corr-1");
      expect(appr?.tenantId).toBe(TENANT);
      expect(appr?.anchorRunId).toBe(DEPLOYMENT);
      expect(appr?.anchorRunId).not.toBe(DEPLOYMENT_SLUG);
      expect(appr?.status).toBe("pending");
      expect(appr?.agentAddress).toBe(WF_ADDR);
    });

    test("a duplicate frame is an idempotent no-op", async () => {
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      const router = buildRouter();
      const ws = createMockWs();
      await reconnectAndVerify(router, ws, kp.privateKey);

      router.handleMessage(ws, registerFrame());
      await drain(router, ws);

      const firstCorr = await h.db.select().from(signalCorrelation);
      const firstAppr = await h.db.select().from(approval);
      // Exclude the deployment's anchor run so only the lazily-anchored child
      // run is counted for the idempotency check.
      const firstRun = await h.db
        .select()
        .from(workflowRun)
        .where(ne(workflowRun.id, DEPLOYMENT));
      expect(firstCorr).toHaveLength(1);
      expect(firstAppr).toHaveLength(1);
      expect(firstRun).toHaveLength(1);
      const approvalId = firstAppr[0]?.id;
      const createdAt = firstCorr[0]?.createdAt;
      const runCreatedAt = firstRun[0]?.createdAt;

      // Redeliver the identical frame: reconnect replay / supervisor restart.
      router.handleMessage(ws, registerFrame());
      await drain(router, ws);

      const secondCorr = await h.db.select().from(signalCorrelation);
      const secondAppr = await h.db.select().from(approval);
      const secondRun = await h.db
        .select()
        .from(workflowRun)
        .where(ne(workflowRun.id, DEPLOYMENT));
      expect(secondCorr).toHaveLength(1);
      expect(secondAppr).toHaveLength(1);
      // The lazy run-row ensure is redelivery-safe: no re-insert, so exactly
      // one survives and its timestamp is untouched.
      expect(secondRun).toHaveLength(1);
      // The original rows are untouched -- no second insert, no id churn.
      expect(secondAppr[0]?.id).toBe(approvalId);
      expect(secondCorr[0]?.createdAt).toEqual(createdAt);
      expect(secondRun[0]?.createdAt).toEqual(runCreatedAt);
    });

    test("rejects a frame for an address the connection does not own", async () => {
      // The connection owns WF_ADDR_2 but the frame targets WF_ADDR, which is
      // seeded as a live deployment; only the handler's ownership gate stands
      // between the spoofed frame and a co-write.
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      // seedDeployment seeded the tenant and asset; add a second anchor run so
      // the connection can own WF_ADDR_2.
      const kp2 = await generateKeyPair();
      await seedAnchorRun(DEPLOYMENT_2, WF_ADDR_2, hexEncode(kp2.publicKey));

      const router = buildRouter();
      const ws = createMockWs();
      await reconnectAddress(router, ws, WF_ADDR_2, kp2.privateKey);
      expect(router.getRoutableAddresses()).toContain(WF_ADDR_2);
      expect(router.getRoutableAddresses()).not.toContain(WF_ADDR);

      // The default registerFrame targets WF_ADDR, which this connection does
      // not own.
      router.handleMessage(ws, registerFrame());
      await drain(router, ws);

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

    test("rejects a frame whose anchorRunId does not match the address", async () => {
      // WF_ADDR derives DEPLOYMENT_SLUG but the frame claims DEPLOYMENT_2_SLUG.
      // The co-write cross-checks the frame's anchorRunId against the slug
      // re-derived from the address and throws on mismatch; the handler
      // swallows the throw, so no rows are written.
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      await seedAnchorRun(DEPLOYMENT_2, WF_ADDR_2, null);

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
          anchorRunId: DEPLOYMENT_2_SLUG,
          agentAddress: WF_ADDR,
          kind: "approval",
          // Carry a snapshot so the frame passes the parse and the test
          // exercises the slug mismatch, not a parse drop.
          snapshot: SNAPSHOT,
        }),
      );
      await drain(router, ws);

      const correlations = await h.db.select().from(signalCorrelation);
      expect(correlations).toHaveLength(0);
      const approvals = await h.db.select().from(approval);
      expect(approvals).toHaveLength(0);
    });

    test("rejects a frame whose anchor run is no longer running", async () => {
      // registerSignalCorrelation gates on a running anchor run; flipping the
      // run terminal resolves no row, the co-write throws, and the handler
      // swallows it.
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));

      const router = buildRouter();
      const ws = createMockWs();
      await reconnectAndVerify(router, ws, kp.privateKey);
      expect(router.getRoutableAddresses()).toContain(WF_ADDR);

      // Flip the anchor run terminal after the address is already routed, so the
      // ownership gate still passes but the running-only resolution misses.
      await h.db
        .update(workflowRun)
        .set({ status: "cancelled" })
        .where(eq(workflowRun.id, DEPLOYMENT));

      router.handleMessage(ws, registerFrame());
      await drain(router, ws);

      const correlations = await h.db.select().from(signalCorrelation);
      expect(correlations).toHaveLength(0);
      const approvals = await h.db.select().from(approval);
      expect(approvals).toHaveLength(0);
    });

    test("a teardown interleaved mid-register never orphans a correlation pair", async () => {
      // The window the row lock closes: a teardown flips the anchor run off
      // "running" while a register is in flight. The register co-writes both
      // rows in one transaction that takes a `SELECT ... FOR UPDATE` on the
      // anchor run row, so a concurrent teardown holding the same lock makes it
      // block; when the teardown commits, the in-transaction re-check finds no
      // running row and throws, so the pair is never written against a
      // torn-down deployment. Two dedicated single-connection handles drive the
      // interleave: one holds an uncommitted teardown UPDATE, the other runs
      // the register whose FOR UPDATE must wait on it. `h.db` stays free to
      // observe the block.
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));

      const config = loadHarnessDbConfig();
      const registerHandle = createDB({ ...config, schema: h.schema, max: 1 });
      const teardownHandle = createDB({ ...config, schema: h.schema, max: 1 });
      try {
        const registerPid = await backendPid(registerHandle);
        const teardownPid = await backendPid(teardownHandle);

        const lookups = createHubSessionLookups({
          db: registerHandle.db,
          agentRepoStore: stubRepoStore,
          historyReceives: createWorkflowHistoryReceiveTracker(),
        });

        let outcome: unknown;
        let settled = false;
        let sawBlock = false;
        let registerPromise: Promise<unknown> = Promise.resolve();

        await teardownHandle.transaction(async (txT) => {
          // Lock the anchor run row and flip it terminal, held uncommitted
          // during the register attempt.
          await txT
            .update(workflowRun)
            .set({ status: "cancelled" })
            .where(eq(workflowRun.id, DEPLOYMENT));

          // Fire the register on its own backend without awaiting: awaiting
          // here would deadlock against the teardown transaction that must
          // commit to release the lock.
          registerPromise = lookups
            .registerSignalCorrelation({
              correlationId: "corr-1",
              runId: "run-1",
              anchorRunId: DEPLOYMENT_SLUG,
              agentAddress: WF_ADDR,
              kind: "approval",
              approvalSnapshot: SNAPSHOT,
            })
            .then(() => null)
            .catch((err: unknown) => err)
            .then((res) => {
              settled = true;
              outcome = res;
              return res;
            });

          // Wait until the register backend is blocked by the teardown backend,
          // or has already settled without blocking (the pre-lock behavior,
          // which writes the orphan). 12ms is just the poll interval; the block
          // persists until this transaction commits. A register blocked by some
          // other backend would loop here rather than fail, which the lane
          // timeout catches.
          for (;;) {
            const blocked = await h.db.execute(
              sql`SELECT pg_blocking_pids(${registerPid}) @> ARRAY[${teardownPid}]::int[] AS blocked`,
            );
            if (blocked[0]?.["blocked"] === true) {
              sawBlock = true;
              break;
            }
            if (settled) break;
            await new Promise((res) => setTimeout(res, 12));
          }
          // Returning commits the teardown and releases the lock; a blocked
          // register then re-checks and finds no running row.
        });

        await registerPromise;

        // The register waited on the teardown rather than racing past it, then
        // threw once the run was no longer live.
        expect(sawBlock).toBe(true);
        expect(outcome).toBeInstanceOf(Error);
        if (outcome instanceof Error) {
          expect(outcome.message).toContain("No live workflow run");
        }

        // The invariant: no orphaned pair pointing at the torn-down deployment.
        expect(
          await registerHandle.db.select().from(signalCorrelation),
        ).toHaveLength(0);
        expect(await registerHandle.db.select().from(approval)).toHaveLength(0);

        // The anchor run survived the teardown (flipped, not deleted).
        const anchorRuns = await registerHandle.db
          .select()
          .from(workflowRun)
          .where(eq(workflowRun.id, DEPLOYMENT));
        expect(anchorRuns).toHaveLength(1);
        expect(anchorRuns[0]?.status).toBe("cancelled");
      } finally {
        await registerHandle.close();
        await teardownHandle.close();
      }
    });

    test("store inserts are idempotent: second call returns null, not a throw", async () => {
      // Direct store test: the handler swallows throws either way, so the
      // handler-level idempotency test cannot tell a clean onConflictDoNothing
      // no-op from a throw-and-rollback. Here the first insert returns the
      // row, the second no-ops and returns null without throwing.
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: "wf",
      });
      await seedAnchorRun(DEPLOYMENT, WF_ADDR, null);
      // Anchor the run row so the FK to workflow_run resolves; the co-write
      // path seeds this itself but this test bypasses it.
      await seedWorkflowRun(h.db, {
        id: "run-1",
        anchorRunId: DEPLOYMENT,
        tenantId: TENANT,
      });

      const signalCorrelationStore = createSignalCorrelationStore(h.db);
      const approvalStore = createApprovalStore(h.db);

      const correlationRow = {
        correlationId: "corr-1",
        tenantId: TENANT,
        anchorRunId: DEPLOYMENT,
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
        anchorRunId: DEPLOYMENT,
        runId: "run-1",
        agentAddress: WF_ADDR,
        correlationId: "corr-1",
        status: "pending" as const,
        toolDefinition: {
          name: SNAPSHOT.name,
          description: SNAPSHOT.description,
          inputSchema: SNAPSHOT.inputSchema,
        },
        toolArguments: SNAPSHOT.arguments,
        scope: null,
        timeoutAt: null,
      };

      const firstAppr = await approvalStore.createIfAbsent(approvalRow);
      expect(firstAppr).not.toBeNull();
      expect(firstAppr?.correlationId).toBe("corr-1");

      // Fresh id on the redelivered row: the dedup key is correlationId, not
      // the primary key.
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
