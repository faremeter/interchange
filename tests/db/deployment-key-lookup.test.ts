import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  createHubSessionLookups,
  type AgentRepoStore,
} from "@intx/hub-sessions";
import { workflowDeployment } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedTenants } from "@intx/test-harness/seed";

// The reconnect ownership challenge verifies a deployment address against a
// public key resolved by `lookupPublicKey`. These tests pin the workflow-
// deployment side of that lookup: keyed by address, gated on a live
// ("deployed") deployment, fail-closed on a missing/null key, and routed by
// address space so a launched-agent address never resolves against the
// deployment table.
describe.skipIf(!harnessDbEnvAvailable())(
  "lookupPublicKey deployment-key routing (real DB)",
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

    // lookupPublicKey never touches the repo store, so a throwing stub keeps
    // the AgentRepoStore surface satisfied without a real on-disk store.
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

    async function seedDeployment(opts: {
      address: string;
      publicKey: string | null;
      status: "deployed" | "error";
    }): Promise<void> {
      await seedTenants(h.db, [{ id: "t1" }]);
      await seedAsset(h.db, {
        id: "asset1",
        tenantId: "t1",
        kind: "workflow",
        name: "wf",
      });
      await h.db.insert(workflowDeployment).values({
        id: "dep1",
        tenantId: "t1",
        definitionAssetId: "asset1",
        address: opts.address,
        publicKey: opts.publicKey,
        status: opts.status,
      });
    }

    test("resolves a deployed deployment's key by address", async () => {
      await seedDeployment({
        address: "ins_dep_abc@wf.example",
        publicKey: "pk1",
        status: "deployed",
      });
      expect(await lookupPublicKey("ins_dep_abc@wf.example")).toBe("pk1");
    });

    test("returns null when the deployment has not yet acked a key", async () => {
      await seedDeployment({
        address: "ins_dep_abc@wf.example",
        publicKey: null,
        status: "deployed",
      });
      expect(await lookupPublicKey("ins_dep_abc@wf.example")).toBeNull();
    });

    test("returns null for a non-deployed (torn-down) deployment", async () => {
      await seedDeployment({
        address: "ins_dep_abc@wf.example",
        publicKey: "pk1",
        status: "error",
      });
      expect(await lookupPublicKey("ins_dep_abc@wf.example")).toBeNull();
    });

    test("returns null for an unknown deployment address", async () => {
      expect(await lookupPublicKey("ins_dep_missing@wf.example")).toBeNull();
    });

    test("routes a launched-agent address to agent_instance, never the deployment table", async () => {
      // A workflow_deployment row must not answer for a launched-agent
      // (ins_...) address. The two address spaces are disjoint and the lookup
      // routes by discriminator, so an absent agent_instance row returns null
      // rather than leaking a deployment key.
      await seedDeployment({
        address: "ins_dep_abc@wf.example",
        publicKey: "pk1",
        status: "deployed",
      });
      expect(await lookupPublicKey("ins_launched@wf.example")).toBeNull();
    });
  },
);
