import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { readFileSync } from "node:fs";
import { eq, sql } from "drizzle-orm";

import {
  createHubSessionLookups,
  type AgentRepoStore,
} from "@intx/hub-sessions";
import {
  workflowDefinition,
  workflowDeployment,
  workflowRun,
} from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Load migration 0055 itself so the test and the migration cannot drift. It
// applies to an empty test schema at harness setup (no deployments), so this
// replays it against seeded pre-branch deployments that lack an anchor run.
const MIGRATION_SQL = readFileSync(
  new URL(
    "../../packages/db/migrations/0055_backfill_anchor_workflow_runs.sql",
    import.meta.url,
  ),
  "utf-8",
);
const BACKFILL = sql.raw(MIGRATION_SQL);

const TENANT = "tnt";
const CREATOR = "prn_creator";
const CREATED_AT = new Date("2025-02-03T04:05:06.000Z");

describe.skipIf(!harnessDbEnvAvailable())(
  "backfill anchor workflow_runs (migration 0055)",
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
      await seedPrincipal(h.db, {
        id: CREATOR,
        tenantId: TENANT,
        kind: "user",
        refId: "creator",
      });
    });

    async function seedDeployment(opts: {
      id: string;
      assetId: string;
      address: string;
      publicKey: string | null;
      folded: boolean;
    }): Promise<void> {
      await seedAsset(h.db, {
        id: opts.assetId,
        tenantId: TENANT,
        kind: "workflow",
        name: opts.assetId,
        creatorPrincipalId: CREATOR,
      });
      if (opts.folded) {
        await h.db.insert(workflowDefinition).values({
          id: `wfd_${opts.id}`,
          tenantId: TENANT,
          name: `def-${opts.id}`,
          assetId: opts.assetId,
        });
      }
      await h.db.insert(workflowDeployment).values({
        id: opts.id,
        tenantId: TENANT,
        definitionAssetId: opts.assetId,
        address: opts.address,
        publicKey: opts.publicKey,
        status: "deployed",
        createdAt: CREATED_AT,
      });
    }

    async function anchorRun(id: string) {
      return (
        await h.db.select().from(workflowRun).where(eq(workflowRun.id, id))
      )[0];
    }

    test("reconstructs an anchor run for a deployment lacking one", async () => {
      await seedDeployment({
        id: "dep_folded",
        assetId: "ast_folded",
        address: "ins_dep_folded@wf.example",
        publicKey: "pk-folded",
        folded: true,
      });

      await h.db.execute(BACKFILL);

      const run = await anchorRun("dep_folded");
      expect(run?.id).toBe("dep_folded");
      expect(run?.deploymentId).toBe("dep_folded");
      expect(run?.address).toBe("ins_dep_folded@wf.example");
      // The reconnect key is carried over from the deployment -- the line that
      // closes the reconnect hole for pre-branch deployments.
      expect(run?.publicKey).toBe("pk-folded");
      expect(run?.status).toBe("running");
      expect(run?.createdAt).toEqual(CREATED_AT);
      expect(run?.definitionId).toBe("wfd_dep_folded");
      // Anchor runs carry no principal or runtime bindings.
      expect(run?.principalId).toBeNull();
      expect(run?.sidecarId).toBeNull();
      expect(run?.kernelId).toBeNull();
      expect(run?.endedAt).toBeNull();
    });

    test("aborts writing nothing when a deployment's asset was never folded", async () => {
      // An unfolded deployment would get a definition-less anchor run, which the
      // run-anchored readers silently drop. The guard aborts the whole migration
      // instead, so the operator runs the fold first -- and, because the guard
      // precedes the insert on a non-transactional runner, nothing is written.
      await seedDeployment({
        id: "dep_unfolded",
        assetId: "ast_unfolded",
        address: "ins_dep_unfolded@wf.example",
        publicKey: "pk-unfolded",
        folded: false,
      });

      // The driver wraps the postgres error, so the RAISE message lands on the
      // error's cause; resolve to that message (null if the migration did not
      // abort) and assert the guard is what rejected.
      const abortMessage = await h.db.execute(BACKFILL).then(
        () => null,
        (e) =>
          e instanceof Error && e.cause instanceof Error
            ? e.cause.message
            : null,
      );
      expect(abortMessage).toMatch(/workflow-asset fold has not run/);

      expect(await anchorRun("dep_unfolded")).toBeUndefined();
    });

    test("does not clobber a deployment that already has an anchor run", async () => {
      await seedDeployment({
        id: "dep_existing",
        assetId: "ast_existing",
        address: "ins_dep_existing@wf.example",
        publicKey: "pk-deployment",
        folded: true,
      });
      // A runtime-created anchor run already exists with its own (later) key.
      await h.db.insert(workflowRun).values({
        id: "dep_existing",
        tenantId: TENANT,
        deploymentId: "dep_existing",
        definitionId: "wfd_dep_existing",
        address: "ins_dep_existing@wf.example",
        publicKey: "pk-runtime",
        status: "running",
      });

      await h.db.execute(BACKFILL);

      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "dep_existing"));
      expect(runs).toHaveLength(1);
      // Untouched: the runtime key, not the deployment's stale one.
      expect(runs[0]?.publicKey).toBe("pk-runtime");
    });

    test("is idempotent: a second run inserts nothing", async () => {
      await seedDeployment({
        id: "dep_folded",
        assetId: "ast_folded",
        address: "ins_dep_folded@wf.example",
        publicKey: "pk-folded",
        folded: true,
      });

      await h.db.execute(BACKFILL);
      await h.db.execute(BACKFILL);

      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, "dep_folded"));
      expect(runs).toHaveLength(1);
    });

    test("backfills many deployments in one pass, including a null-key one", async () => {
      // The production shape: several pre-existing deployments backfilled in a
      // single set-based INSERT, one not yet acked (null key), one already
      // carrying a runtime anchor run. All are folded -- the guard requires it.
      await seedDeployment({
        id: "dep_a",
        assetId: "ast_a",
        address: "ins_dep_a@wf.example",
        publicKey: "pk-a",
        folded: true,
      });
      await seedDeployment({
        id: "dep_b",
        assetId: "ast_b",
        address: "ins_dep_b@wf.example",
        publicKey: null,
        folded: true,
      });
      await seedDeployment({
        id: "dep_c",
        assetId: "ast_c",
        address: "ins_dep_c@wf.example",
        publicKey: "pk-c-deployment",
        folded: true,
      });
      await h.db.insert(workflowRun).values({
        id: "dep_c",
        tenantId: TENANT,
        deploymentId: "dep_c",
        definitionId: "wfd_dep_c",
        address: "ins_dep_c@wf.example",
        publicKey: "pk-c-runtime",
        status: "running",
      });

      await h.db.execute(BACKFILL);

      const all = await h.db.select().from(workflowRun);
      expect(all).toHaveLength(3);

      const a = await anchorRun("dep_a");
      expect(a?.publicKey).toBe("pk-a");
      expect(a?.definitionId).toBe("wfd_dep_a");

      // Not-yet-acked deployment: anchor created with a null key, but its folded
      // definition still resolves.
      const b = await anchorRun("dep_b");
      expect(b?.id).toBe("dep_b");
      expect(b?.publicKey).toBeNull();
      expect(b?.definitionId).toBe("wfd_dep_b");
      expect(b?.status).toBe("running");

      // Pre-anchored deployment keeps its runtime key.
      const c = await anchorRun("dep_c");
      expect(c?.publicKey).toBe("pk-c-runtime");
    });

    test("the backfilled key satisfies the reconnect lookup", async () => {
      await seedDeployment({
        id: "dep_folded",
        assetId: "ast_folded",
        address: "ins_dep_folded@wf.example",
        publicKey: "pk-folded",
        folded: true,
      });

      await h.db.execute(BACKFILL);

      // The end-to-end proof: after the backfill, the reconnect key lookup that
      // b2b routed to workflow_run resolves the deployment's key by address.
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; lookupPublicKey does not touch the repo store
      const stubRepoStore = new Proxy(
        {},
        {
          get() {
            throw new Error("agentRepoStore is not used by lookupPublicKey");
          },
        },
      ) as AgentRepoStore;
      const lookups = createHubSessionLookups({
        db: h.db,
        agentRepoStore: stubRepoStore,
      });
      expect(await lookups.lookupPublicKey("ins_dep_folded@wf.example")).toBe(
        "pk-folded",
      );
    });
  },
);
