// The production hub runs the lapsed deployer sweep. A deployment whose
// deployer is no longer active holds an invoker row that the sweep removes,
// and the hub's first pass runs when it starts. The schema is migrated and
// seeded before the real hub is spawned on it.

import { afterEach, describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";

import { createDB, dropSchema, runMigrations } from "@intx/db";
import { grant, principal, workflowDefinition } from "@intx/db/schema";
import {
  loadHarnessDbConfig,
  randomSchemaName,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
  seedWorkflowRunLaunchSpec,
} from "@intx/test-harness/seed";

import { harnessHubEnvAvailable, startHub } from "./lib/git-harness";

const TENANT = "tnt_sweep";
const CREATOR = "prn_creator";
const LAPSED_DEPLOYER = "prn_lapsed_deployer";
const ACTIVE_DEPLOYER = "prn_active_deployer";
const VAULT = { resource: "secret:vault", action: "use" } as const;

const cleanups: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) {
    await cleanup();
  }
});

type Db = ReturnType<typeof createDB>["db"];

// A live deployment with an invoker requirement whose run holds the
// unexpired invoker row the requirement produced. Returns the run principal.
async function seedDeployment(
  db: Db,
  anchorRunId: string,
  deployer: string,
): Promise<string> {
  const runPrincipalId = `prn_${anchorRunId}`;
  await seedAsset(db, {
    id: `ast_${anchorRunId}`,
    tenantId: TENANT,
    kind: "workflow",
    name: `ast_${anchorRunId}`,
    creatorPrincipalId: CREATOR,
  });
  await db.insert(workflowDefinition).values({
    id: `wfd_${anchorRunId}`,
    tenantId: TENANT,
    name: `wfd_${anchorRunId}`,
    assetId: `ast_${anchorRunId}`,
  });
  await db.insert(principal).values({
    id: runPrincipalId,
    tenantId: TENANT,
    kind: "workflow",
    refId: anchorRunId,
    status: "active",
  });
  await seedWorkflowRun(db, {
    id: anchorRunId,
    tenantId: TENANT,
    anchorRunId,
    definitionId: `wfd_${anchorRunId}`,
    principalId: runPrincipalId,
    address: `${anchorRunId}@sweep.example`,
  });
  await seedWorkflowRunLaunchSpec(db, {
    anchorRunId,
    sourceAuthorityPrincipalId: deployer,
    grantSnapshot: {
      perStep: [],
      grantRequirements: [{ ...VAULT, source: "invoker" }],
    },
  });
  await seedGrant(db, {
    id: `grt_${anchorRunId}_vault`,
    tenantId: TENANT,
    principalId: runPrincipalId,
    ...VAULT,
    origin: "invoker",
    expiresAt: new Date(Date.now() + 60 * 60 * 1000),
  });
  return runPrincipalId;
}

describe.skipIf(!harnessHubEnvAvailable())(
  "the hub's lapsed deployer sweep",
  () => {
    test("removes the invoker row of a deployment whose deployer is not active", async () => {
      const config = loadHarnessDbConfig();
      const schema = randomSchemaName();
      await runMigrations(config, { schema });
      const handle = createDB({ ...config, schema });
      cleanups.push(async () => {
        await handle.close();
      });
      const { db } = handle;

      let lapsedRun: string;
      let activeRun: string;
      try {
        await seedTenants(db, [{ id: TENANT }]);
        for (const id of [CREATOR, LAPSED_DEPLOYER, ACTIVE_DEPLOYER]) {
          await seedPrincipal(db, {
            id,
            tenantId: TENANT,
            kind: "user",
            refId: `${id}-user`,
          });
          await seedGrant(db, {
            id: `grt_${id}_vault`,
            tenantId: TENANT,
            principalId: id,
            ...VAULT,
          });
        }
        await db
          .update(principal)
          .set({ status: "deactivated" })
          .where(eq(principal.id, LAPSED_DEPLOYER));
        lapsedRun = await seedDeployment(db, "run_lapsed", LAPSED_DEPLOYER);
        activeRun = await seedDeployment(db, "run_active", ACTIVE_DEPLOYER);
      } catch (e) {
        await dropSchema(config, { schema });
        throw e;
      }

      // The hub drops the schema when it stops.
      const hub = await startHub({ dbSchemaName: schema, migrated: true });
      cleanups.push(hub.stop);

      const rowsOf = async (principalId: string) =>
        (
          await db
            .select({ id: grant.id })
            .from(grant)
            .where(eq(grant.principalId, principalId))
        ).map((r) => r.id);

      const deadline = Date.now() + 20_000;
      while ((await rowsOf(lapsedRun)).length > 0) {
        if (Date.now() > deadline) {
          throw new Error("the hub's sweep did not remove the lapsed row");
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }

      expect(await rowsOf(lapsedRun)).toEqual([]);
      expect(await rowsOf(activeRun)).toEqual(["grt_run_active_vault"]);
    });
  },
);
