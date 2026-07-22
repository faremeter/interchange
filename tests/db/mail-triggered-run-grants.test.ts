import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { createGrantStore } from "@intx/db";
import { grant, principal, workflowRun } from "@intx/db/schema";
import type { AssetService } from "@intx/hub-sessions";
import { createMailTriggeredRunGrantsMaterializer } from "@intx/hub-api";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedTenants,
  seedWorkflowDeployment,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
const DEPLOYMENT = "dep_real";
const WORKFLOW_ADDRESS = "ins_dep_real@tenant.example";
const CREATOR = "prn_creator";
const RUN_ID = "<mail-run-1@tenant.example>";

// A one-step workflow whose agent declares one tool plus a creator-sourced
// grant requirement. The walk yields a `tool:read_file/invoke` runtime
// grant; the creator requirement resolves against the creator principal's
// seeded grants.
function workflowJson(creatorRequirementResource: string): string {
  return JSON.stringify({
    id: "wf_mail",
    triggers: [{ type: "mail", to: WORKFLOW_ADDRESS }],
    stepOrder: ["work"],
    steps: {
      work: {
        kind: "step",
        id: "work",
        agent: {
          id: "worker",
          systemPrompt: "do work",
          toolFactories: [{ id: "fac", definitions: [{ name: "read_file" }] }],
          capabilities: [],
          inference: { sources: [{ provider: "anthropic", model: "m" }] },
        },
        after: [],
      },
    },
    grantRequirements: [
      {
        resource: creatorRequirementResource,
        action: "use",
        source: "creator",
      },
    ],
  });
}

function mockAssetService(json: string): AssetService {
  function notImpl(name: string): never {
    throw new Error(`mock: assetService.${name} not implemented`);
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only readAssetBlob is exercised by the materializer
  return {
    createAsset: () => notImpl("createAsset"),
    populateAsset: () => notImpl("populateAsset"),
    attachAsset: () => notImpl("attachAsset"),
    listAgentAssets: () => notImpl("listAgentAssets"),
    readAssetBlob: async () => new TextEncoder().encode(json),
  } as unknown as AssetService;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "createMailTriggeredRunGrantsMaterializer (real DB)",
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
        refId: "creator-user",
      });
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: ASSET,
        creatorPrincipalId: CREATOR,
      });
      await seedWorkflowDeployment(h.db, {
        id: DEPLOYMENT,
        tenantId: TENANT,
        definitionAssetId: ASSET,
        address: WORKFLOW_ADDRESS,
      });
      // The creator holds the grant its creator-sourced requirement demands,
      // so the happy path resolves it. The rejection test overrides the
      // requirement to a resource the creator does NOT hold.
      await seedGrant(h.db, {
        id: "grt_creator_vault",
        tenantId: TENANT,
        principalId: CREATOR,
        resource: "secret:vault",
        action: "use",
        effect: "allow",
        origin: "creator",
      });
    });

    async function materializeOnce(
      json: string,
      runId: string,
    ): ReturnType<ReturnType<typeof createMailTriggeredRunGrantsMaterializer>> {
      const materialize = createMailTriggeredRunGrantsMaterializer({
        db: h.db,
        assetService: mockAssetService(json),
        grantStore: createGrantStore(h.db),
      });
      return materialize({ agentAddress: WORKFLOW_ADDRESS, runId });
    }

    test("commits exactly one principal, run, and grant set on the happy path", async () => {
      const result = await materializeOnce(
        workflowJson("secret:vault"),
        RUN_ID,
      );
      if (result.outcome !== "materialized") {
        throw new Error(`expected materialized, got ${result.outcome}`);
      }
      await result.commit();

      // Exactly one workflow-kind principal keyed on the runId.
      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, RUN_ID));
      expect(principals).toHaveLength(1);
      expect(principals[0]?.kind).toBe("workflow");
      const runPrincipalId = principals[0]?.id;

      // Exactly one run row whose deployment_id is the SEEDED deployment id
      // -- the real workflow_deployment.id, NOT the address-derived substrate
      // slug. The row inserts at all only because that real id was written:
      // the slug is not a workflow_deployment.id, so the NOT NULL foreign key
      // would reject it.
      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN_ID));
      expect(runs).toHaveLength(1);
      expect(runs[0]?.deploymentId).toBe(DEPLOYMENT);
      expect(runs[0]?.principalId).toBe(runPrincipalId);
      expect(runs[0]?.status).toBe("running");

      // Every staged grant persisted and FK-resolves to the run principal.
      const grants = await h.db
        .select()
        .from(grant)
        .where(eq(grant.principalId, runPrincipalId ?? ""));
      expect(grants.length).toBe(result.stepGrants.length);
      const resources = grants.map((g) => `${g.resource}/${g.action}`).sort();
      expect(resources).toContain("tool:read_file/invoke");
      expect(resources).toContain("secret:vault/use");
    });

    test("a redelivery with the same runId neither throws nor duplicates", async () => {
      const first = await materializeOnce(workflowJson("secret:vault"), RUN_ID);
      if (first.outcome !== "materialized") {
        throw new Error(`expected materialized, got ${first.outcome}`);
      }
      await first.commit();

      const firstGrants = await h.db.select().from(grant);
      const firstGrantCount = firstGrants.length;
      expect(firstGrantCount).toBeGreaterThan(0);

      // Second delivery of the SAME runId: the deterministic principal id and
      // the in-transaction guard make the commit a true no-op. A fresh random
      // principal id would break the principal foreign key on redelivery
      // (the conflict-noop principal insert would leave the new id unwritten
      // while the grant rows referenced it), and re-running the grant inserts
      // would duplicate rows -- both of which the assertions below rule out.
      const second = await materializeOnce(
        workflowJson("secret:vault"),
        RUN_ID,
      );
      if (second.outcome !== "materialized") {
        throw new Error(`expected materialized, got ${second.outcome}`);
      }

      // The two stagings derive the SAME run principal id from the shared
      // runId, confirming the derivation is deterministic. A random id would
      // differ here and leave the persisted grant rows referencing an id the
      // conflict-noop principal insert never wrote.
      const firstPrincipalIds = new Set(
        first.stepGrants.map((g) => g.principalId),
      );
      const secondPrincipalIds = new Set(
        second.stepGrants.map((g) => g.principalId),
      );
      expect(firstPrincipalIds.size).toBe(1);
      expect(secondPrincipalIds).toEqual(firstPrincipalIds);

      await second.commit();

      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, RUN_ID));
      expect(principals).toHaveLength(1);
      // The single persisted principal is exactly the id both stagings
      // derived, so the grant rows' FK resolves against it.
      expect(firstPrincipalIds.has(principals[0]?.id ?? "")).toBe(true);

      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN_ID));
      expect(runs).toHaveLength(1);

      const grants = await h.db.select().from(grant);
      expect(grants.length).toBe(firstGrantCount);
    });

    test("a creator shortfall rejects and writes zero rows", async () => {
      // The creator-sourced requirement names a resource the creator does not
      // hold, so staging rejects (403). The rejection is RETURNED (not
      // thrown), and because staging never committed, NOTHING is written --
      // no orphaned principal, run, or grant rows.
      const result = await materializeOnce(
        workflowJson("secret:locked"),
        RUN_ID,
      );
      expect(result.outcome).toBe("rejected");
      if (result.outcome === "rejected") {
        expect(result.status).toBe(403);
      }

      // No creator-requirement rejection ever staged a commit closure, so the
      // tables carry no run principal, run row, or run grants.
      const principals = await h.db
        .select()
        .from(principal)
        .where(eq(principal.refId, RUN_ID));
      expect(principals).toHaveLength(0);

      const runs = await h.db
        .select()
        .from(workflowRun)
        .where(eq(workflowRun.id, RUN_ID));
      expect(runs).toHaveLength(0);

      // The only grant present is the creator's seeded grant; no run grants
      // were written.
      const runGrants = await h.db
        .select()
        .from(grant)
        .where(eq(grant.resource, "tool:read_file"));
      expect(runGrants).toHaveLength(0);
    });
  },
);
