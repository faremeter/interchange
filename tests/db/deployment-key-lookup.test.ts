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
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants, seedWorkflowRun } from "@intx/test-harness/seed";

// The reconnect ownership challenge verifies a deployment address against a
// public key resolved by `lookupPublicKey`. These tests pin the workflow-
// derived side of that lookup: the key now lives on the deployment's anchor
// workflow_run row (the deployment projection is only the FK parent), keyed by
// address, gated on a live run ("deployed" in its pre-trigger window or
// "running"), fail-closed on a missing/null key, and routed by address space so
// a launched-agent address never resolves against the anchor run.
describe.skipIf(!harnessDbEnvAvailable())(
  "lookupPublicKey workflow-derived key routing (real DB)",
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

    // Seed a deployment's anchor run -- the workflow_run whose id equals its
    // deployment id. The key and liveness gate live on the run. `runStatus`
    // "deployed" (pre-trigger) and "running" are both live deployments; a
    // terminal status is the decommissioned case the read gate excludes.
    async function seedAnchor(opts: {
      address: string;
      publicKey: string | null;
      runStatus: "deployed" | "running" | "cancelled";
    }): Promise<void> {
      await seedTenants(h.db, [{ id: "t1" }]);
      await seedWorkflowRun(h.db, {
        id: "dep1",
        tenantId: "t1",
        deploymentId: "dep1",
        address: opts.address,
        publicKey: opts.publicKey,
        status: opts.runStatus,
      });
    }

    test("resolves a live anchor run's key by address", async () => {
      await seedAnchor({
        address: "ins_dep_abc@wf.example",
        publicKey: "pk1",
        runStatus: "running",
      });
      expect(await lookupPublicKey("ins_dep_abc@wf.example")).toBe("pk1");
    });

    test("resolves a deployed (pre-trigger) anchor run's key by address", async () => {
      // The reconnect challenge fires in the deploy->first-trigger window, when
      // the anchor is still "deployed". A "running"-only gate would fail the
      // challenge closed here; the live gate must resolve the key.
      await seedAnchor({
        address: "ins_dep_abc@wf.example",
        publicKey: "pk1",
        runStatus: "deployed",
      });
      expect(await lookupPublicKey("ins_dep_abc@wf.example")).toBe("pk1");
    });

    test("returns null when the anchor run has not yet acked a key", async () => {
      await seedAnchor({
        address: "ins_dep_abc@wf.example",
        publicKey: null,
        runStatus: "running",
      });
      expect(await lookupPublicKey("ins_dep_abc@wf.example")).toBeNull();
    });

    test("returns null for a decommissioned (terminal) anchor run", async () => {
      await seedAnchor({
        address: "ins_dep_abc@wf.example",
        publicKey: "pk1",
        runStatus: "cancelled",
      });
      expect(await lookupPublicKey("ins_dep_abc@wf.example")).toBeNull();
    });

    test("returns null for an unknown workflow-derived address", async () => {
      expect(await lookupPublicKey("ins_dep_missing@wf.example")).toBeNull();
    });

    test("routes a launched-agent address to the plain path, never the anchor run", async () => {
      // A plain launched-agent address must not resolve against the anchor run.
      // The address spaces are disjoint and the lookup routes by discriminator,
      // so an absent agent_instance / folded run returns null rather than
      // leaking the anchor's key.
      await seedAnchor({
        address: "ins_dep_abc@wf.example",
        publicKey: "pk1",
        runStatus: "running",
      });
      expect(await lookupPublicKey("ins_launched@wf.example")).toBeNull();
    });

    test("two concurrent deployments resolve to their own distinct anchor-run keys", async () => {
      // Each deployment owns its own anchor run at its own address with its own
      // minted key; the lookup keys on the address and never crosses them, so
      // two concurrent deployments verify reconnect against distinct keys.
      await seedTenants(h.db, [{ id: "t1" }]);
      const deployments = [
        {
          id: "dep_one",
          address: "ins_dep_one@wf.example",
          publicKey: "pk-one",
        },
        {
          id: "dep_two",
          address: "ins_dep_two@wf.example",
          publicKey: "pk-two",
        },
      ];
      for (const d of deployments) {
        await seedWorkflowRun(h.db, {
          id: d.id,
          tenantId: "t1",
          deploymentId: d.id,
          address: d.address,
          publicKey: d.publicKey,
          status: "running",
        });
      }
      expect(await lookupPublicKey("ins_dep_one@wf.example")).toBe("pk-one");
      expect(await lookupPublicKey("ins_dep_two@wf.example")).toBe("pk-two");
    });
  },
);

// A folded run is a supervised workflow-process child, pinned forever: the
// reconnect deploy-ref catch-up must skip it. `lookupDeployRef` resolves the
// address and returns null for a run WITHOUT reading the repo store, so the
// caller's null short-circuit excludes it.
describe.skipIf(!harnessDbEnvAvailable())(
  "lookupDeployRef fold-aware reconcile guard (real DB)",
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
      await seedTenants(h.db, [{ id: "t1" }]);
    });

    test("returns null for a folded run without reading the repo store", async () => {
      await seedWorkflowRun(h.db, {
        id: "ins_folded",
        tenantId: "t1",
        address: "ins_folded@wf.example",
        status: "running",
      });
      // Any repo-store access is a bug: a run short-circuits to null before the
      // deploy-ref read, so a throwing stub proves the short-circuit.
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- throwing stub; the run path must not read the repo store
      const throwingRepoStore = new Proxy(
        {},
        {
          get() {
            throw new Error("agentRepoStore must not be read for a folded run");
          },
        },
      ) as AgentRepoStore;

      const ref = await createHubSessionLookups({
        db: h.db,
        agentRepoStore: throwingRepoStore,
      }).lookupDeployRef("ins_folded@wf.example");
      expect(ref).toBeNull();
    });
  },
);
