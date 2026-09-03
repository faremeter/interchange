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
  createHubSessionLookups,
  createSidecarRouter,
  type AgentRepoStore,
  type SidecarAuthenticator,
  type WsHandle,
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

// The register handler never touches the repo store, so a throwing stub keeps
// the AgentRepoStore surface satisfied without a real on-disk store.
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

// The backend pid of a handle's single connection. Only meaningful for a
// `max: 1` handle, where every query reuses the one physical connection, so the
// pid stays stable for later `pg_blocking_pids` checks.
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
// The raw `run_...` id a deploy stamps onto the deployment's anchor run --
// NOT the workflow-run repo slug. The
// `signal_correlation.deployment_id` and `approval.deployment_id` FKs both
// reference `workflow_run.id`, so this raw id is what the co-write writes into
// those columns.
const DEPLOYMENT = "run_abc123";
const WF_ADDR = "run_abc@wf.example";
// The workflow-run repo slug the supervisor derives from the address and stamps
// onto the register frame's `anchorRunId` (address with every `@`/`.`
// substituted). This is what the co-write's cross-check compares against, and
// it is distinct from the raw deployment id above.
const DEPLOYMENT_SLUG = deriveWorkflowRunRepoId(WF_ADDR);

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

    // Seed the deployment's anchor run, which the signal-correlation co-write
    // resolves by deployment address as its tenancy and definition origin.
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
    // allocation-authenticated reconnect path, so the register handler's
    // ownership gate lets the frame through.
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
      await new Promise((res) => setTimeout(res, 50));
    }

    // Bring an arbitrary workflow address up as an owned route on `ws` through
    // the same allocation-authenticated reconnect path `reconnectAndVerify`
    // uses, so a negative-path case can own a DIFFERENT address than the frame
    // it delivers.
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
      await new Promise((res) => setTimeout(res, 50));
    }

    function buildRouter() {
      const lookups = createHubSessionLookups({
        db: h.db,
        agentRepoStore: stubRepoStore,
      });
      return createSidecarRouter({
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
      // The FK column carries the raw deployment id the row is keyed by, not the
      // workflow-run repo slug the frame's `anchorRunId` is stamped with.
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

      // The co-write lazily anchored the run: a workflow_run row keyed on the
      // frame's runId, on the same deployment and tenant. Its principal is null
      // -- an internal, workflow-spawned run inherits the deployment's grants
      // and has no principal of its own. Exclude the deployment's anchor run
      // (id == DEPLOYMENT) to isolate the lazily-anchored child.
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
      // The lazily-anchored child inherits its deployment anchor run's
      // definition, so it carries the anchor's definition_id.
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
      await drain();

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
      // Regression: a real deployment's anchor-run id is the raw
      // `run_...` id a deploy stamps, while the frame's
      // `anchorRunId` is the workflow-run repo slug the supervisor derives from
      // the address. seedDeployment seeds exactly that shape (raw id
      // DEPLOYMENT, address WF_ADDR), and the frame carries DEPLOYMENT_SLUG.
      // Before the co-write's slug cross-check and raw-id FK writes, this path
      // threw (raw id never equals the slug) and no approval row was ever
      // written; here it must co-write both rows keyed by the raw id.
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      expect(DEPLOYMENT).not.toBe(DEPLOYMENT_SLUG);

      const lookups = createHubSessionLookups({
        db: h.db,
        agentRepoStore: stubRepoStore,
      });

      // Call the co-write directly with the real-shaped frame: the raw-id
      // deployment resolved by address, the slug on the frame's anchorRunId.
      // The direct call bypasses the router's error-swallowing handler, so a
      // regression would surface here as a rejected promise rather than a
      // silently-dropped frame.
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
      await drain();

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
      await drain();

      const secondCorr = await h.db.select().from(signalCorrelation);
      const secondAppr = await h.db.select().from(approval);
      const secondRun = await h.db
        .select()
        .from(workflowRun)
        .where(ne(workflowRun.id, DEPLOYMENT));
      expect(secondCorr).toHaveLength(1);
      expect(secondAppr).toHaveLength(1);
      // The lazy run-row ensure is redelivery-safe: the run row is not
      // re-inserted, so exactly one survives and its timestamp is untouched.
      expect(secondRun).toHaveLength(1);
      // The original rows are untouched -- no second insert, no id churn.
      expect(secondAppr[0]?.id).toBe(approvalId);
      expect(secondCorr[0]?.createdAt).toEqual(createdAt);
      expect(secondRun[0]?.createdAt).toEqual(runCreatedAt);
    });

    test("rejects a frame for an address the connection does not own", async () => {
      // The connection owns WF_ADDR_2, not WF_ADDR. The delivered frame targets
      // WF_ADDR -- an address that IS seeded as a live deployment, so the only
      // thing standing between the spoofed frame and a co-write is the handler's
      // ownership gate. Removing that gate would let this frame write rows.
      const kp = await generateKeyPair();
      await seedDeployment(hexEncode(kp.publicKey));
      // seedDeployment already seeded the tenant and asset; add a second
      // anchor run on them so the connection can own WF_ADDR_2.
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

    test("rejects a frame whose anchorRunId does not match the address", async () => {
      // WF_ADDR is owned and derives DEPLOYMENT_SLUG, but the frame claims
      // DEPLOYMENT_2_SLUG (the other deployment's workflow-run repo slug).
      // registerSignalCorrelation cross-checks the frame's anchorRunId against
      // the slug re-derived from the address and throws on a mismatch; the
      // handler swallows the throw, so no rows are written.
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

    test("rejects a frame whose anchor run is no longer running", async () => {
      // Bring WF_ADDR up while its anchor run is live, then flip the anchor run
      // terminal with the connection still owning the address.
      // registerSignalCorrelation gates on a running anchor run, so the
      // now-terminal address resolves no row and it throws; the handler
      // swallows the throw and writes nothing.
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
      await drain();

      const correlations = await h.db.select().from(signalCorrelation);
      expect(correlations).toHaveLength(0);
      const approvals = await h.db.select().from(approval);
      expect(approvals).toHaveLength(0);
    });

    test("a teardown interleaved mid-register never orphans a correlation pair", async () => {
      // The window the row lock closes: an anchor-run teardown that flips the
      // run off "running" while a register is in flight. The register resolves
      // the anchor run and co-writes both rows in one transaction, taking a
      // `SELECT ... FOR UPDATE` on the anchor run row; a concurrent teardown
      // that has locked the same row makes the register block, and once the
      // teardown commits the register's in-transaction re-check finds no
      // running row and throws -- so the pair is never written against a
      // torn-down deployment.
      //
      // Two dedicated single-connection handles drive the interleave
      // deterministically: one holds an uncommitted teardown UPDATE (the row's
      // FOR NO KEY UPDATE lock), the other runs the register whose FOR UPDATE
      // must wait on it. `h.db` stays free to observe the block.
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
        });

        let outcome: unknown;
        let settled = false;
        let sawBlock = false;
        let registerPromise: Promise<unknown> = Promise.resolve();

        await teardownHandle.transaction(async (txT) => {
          // Lock the anchor run row and flip it terminal, held uncommitted for
          // the duration of the register attempt.
          await txT
            .update(workflowRun)
            .set({ status: "cancelled" })
            .where(eq(workflowRun.id, DEPLOYMENT));

          // Fire the register on its own backend without awaiting: it blocks on
          // the row lock, and awaiting it here would deadlock against the
          // teardown transaction that must commit to release the lock.
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

          // Wait until the register backend is blocked specifically by the
          // teardown backend, or has already settled without blocking (the
          // pre-lock behavior, which writes the orphan). The cap stays well
          // under bun's 5s default per-test timeout for tests/db.
          for (let i = 0; i < 150 && !settled; i++) {
            const blocked = await h.db.execute(
              sql`SELECT pg_blocking_pids(${registerPid}) @> ARRAY[${teardownPid}]::int[] AS blocked`,
            );
            if (blocked[0]?.["blocked"] === true) {
              sawBlock = true;
              break;
            }
            await new Promise((res) => setTimeout(res, 12));
          }
          // Returning commits the teardown (status = "cancelled") and releases
          // the lock; a blocked register then re-checks and finds no running
          // row.
        });

        await registerPromise;

        // The register waited on the teardown rather than racing past it, then
        // threw once the anchor run was no longer live.
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
      await seedAnchorRun(DEPLOYMENT, WF_ADDR, null);
      // The direct store inserts carry a runId; anchor its run row so the FK
      // to workflow_run resolves. The co-write path seeds this itself, but this
      // test bypasses it to exercise the stores directly.
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
