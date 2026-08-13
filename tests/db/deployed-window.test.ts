import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { createWorkflowRunStore } from "@intx/db";
import { workflowDefinition, workflowRun } from "@intx/db/schema";
import {
  createHubSessionLookups,
  type AgentRepoStore,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
const DEFINITION = "wfd";
// The anchor id is the freshly-minted run id; the address is `run_<id>@domain`.
const RUN_ID = "run_window";
const ADDRESS = "run_window@wf.example";
const PUBLIC_KEY = "pk-deploy";

// The deploy->first-trigger window is live but not yet running. A run is born
// "deployed" at deploy; every gate that must treat a live run as live has to
// accept it, and the first trigger flips it to "running". This test walks that
// window end to end: a reconnect key lookup succeeds while "deployed", the first
// trigger flips the anchor, and a second trigger is a no-op on the status.
describe.skipIf(!harnessDbEnvAvailable())(
  "deployed-window run lifecycle (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: ASSET,
      });
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION,
        tenantId: TENANT,
        name: DEFINITION,
      });
      // The deploy-time anchor: self-referential (id === anchorRunId), carrying
      // the routing address and the deploy-acked key, born "deployed" with a
      // null principal. The self-FK requires the row to reference its own id, so
      // this single insert is both parent and child.
      await seedWorkflowRun(h.db, {
        id: RUN_ID,
        anchorRunId: RUN_ID,
        tenantId: TENANT,
        definitionId: DEFINITION,
        address: ADDRESS,
        publicKey: PUBLIC_KEY,
        status: "deployed",
      });
    });

    // lookupPublicKey never touches the repo store, so a throwing stub keeps the
    // AgentRepoStore surface satisfied without a real on-disk store.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; lookupPublicKey does not touch the repo store
    const stubRepoStore = new Proxy(
      {},
      {
        get() {
          throw new Error("agentRepoStore is not used by lookupPublicKey");
        },
      },
    ) as AgentRepoStore;

    function lookupPublicKey(address: string): Promise<string | null> {
      return createHubSessionLookups({
        db: h.db,
        agentRepoStore: stubRepoStore,
      }).lookupPublicKey(address);
    }

    test("stays live before the first trigger, flips on it, and no-ops on the second", async () => {
      // Pre-trigger: the reconnect ownership challenge must resolve the key off
      // the "deployed" anchor. A "running"-only gate would fail this closed.
      expect(await lookupPublicKey(ADDRESS)).toBe(PUBLIC_KEY);

      const [beforeTrigger] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN_ID));
      expect(beforeTrigger?.status).toBe("deployed");
      expect(beforeTrigger?.principalId).toBeNull();

      // The first trigger mints a principal and reconciles the anchor.
      await seedPrincipal(h.db, {
        id: "prn-run",
        tenantId: TENANT,
        kind: "workflow",
        refId: RUN_ID,
        status: "active",
      });
      const store = createWorkflowRunStore(h.db);
      await store.anchorWithPrincipal({
        id: RUN_ID,
        anchorRunId: RUN_ID,
        tenantId: TENANT,
        definitionId: DEFINITION,
        principalId: "prn-run",
        status: "running",
      });

      const [afterFirst] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN_ID));
      expect(afterFirst?.status).toBe("running");
      expect(afterFirst?.principalId).toBe("prn-run");

      // The key still resolves once running (the live gate accepts both).
      expect(await lookupPublicKey(ADDRESS)).toBe(PUBLIC_KEY);

      // A second trigger (a redelivery) reconciles nothing: the three-part guard
      // no longer matches once the anchor is "running", so it is a no-op on the
      // status and never re-attaches a principal.
      await seedPrincipal(h.db, {
        id: "prn-run-2",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run-other",
        status: "active",
      });
      await store.anchorWithPrincipal({
        id: RUN_ID,
        anchorRunId: RUN_ID,
        tenantId: TENANT,
        definitionId: DEFINITION,
        principalId: "prn-run-2",
        status: "running",
      });

      const [afterSecond] = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN_ID));
      expect(afterSecond?.status).toBe("running");
      expect(afterSecond?.principalId).toBe("prn-run");
    });
  },
);
