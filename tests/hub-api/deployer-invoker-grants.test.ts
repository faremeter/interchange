import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createGrantStore } from "@intx/db";
import { workflowRunLaunchSpec } from "@intx/db/schema";
import { collectDeployerInvokerGrants } from "@intx/hub-api";
import type { GrantRequirement } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedGrant,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

// Invoker-sourced requirements resolve against the deployment's deployer, read
// from its launch spec. This drives that lookup against a real schema so the
// join key and the tenant filter are exercised, not mocked.

const TENANT = "tnt_deployer";
const OTHER_TENANT = "tnt_deployer_other";
const ANCHOR = "run_deployer_anchor";
const DEPLOYER = "prn_deployer";

const INVOKER_REQUIREMENT: GrantRequirement = {
  resource: "secret:vault",
  action: "use",
  source: "invoker",
};

describe.skipIf(!harnessDbEnvAvailable())(
  "collectDeployerInvokerGrants (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT }, { id: OTHER_TENANT }]);
      await seedWorkflowRun(h.db, {
        id: ANCHOR,
        tenantId: TENANT,
        anchorRunId: ANCHOR,
      });
    });

    async function seedDeployer(args: {
      tenantId: string;
      status?: "active" | "deactivated";
    }): Promise<void> {
      await seedPrincipal(h.db, {
        id: DEPLOYER,
        tenantId: args.tenantId,
        ...(args.status !== undefined ? { status: args.status } : {}),
      });
      await seedGrant(h.db, {
        id: "grt_deployer_vault",
        tenantId: args.tenantId,
        principalId: DEPLOYER,
        resource: "secret:vault",
        action: "use",
        origin: "system",
      });
      await h.db.insert(workflowRunLaunchSpec).values({
        anchorRunId: ANCHOR,
        sessionId: "ses_deployer",
        deploymentDomain: "deployer.example",
        sourceAuthorityPrincipalId: DEPLOYER,
        frozenApprovalBundle: {},
        sourceOfferingIds: [],
        defaultSourceOfferingId: "ofr_deployer",
        deployContent: {},
      });
    }

    function collect(grantRequirements: readonly GrantRequirement[]) {
      return collectDeployerInvokerGrants({
        db: h.db,
        grantStore: createGrantStore(h.db),
        tenantId: TENANT,
        anchorRunId: ANCHOR,
        grantRequirements,
      });
    }

    test("returns the active deployer's grants", async () => {
      await seedDeployer({ tenantId: TENANT });
      const result = await collect([INVOKER_REQUIREMENT]);
      if (!result.ok) throw new Error("expected the deployer's grants");
      expect(result.invokerGrants.map((g) => g.id)).toEqual([
        "grt_deployer_vault",
      ]);
    });

    test("refuses a deactivated deployer", async () => {
      await seedDeployer({ tenantId: TENANT, status: "deactivated" });
      const result = await collect([INVOKER_REQUIREMENT]);
      expect(result).toMatchObject({
        ok: false,
        rejection: { status: 409, code: "deployer_unavailable" },
      });
    });

    test("refuses a deployment with no launch spec", async () => {
      const result = await collect([INVOKER_REQUIREMENT]);
      expect(result).toMatchObject({
        ok: false,
        rejection: { code: "deployer_unavailable" },
      });
    });

    test("refuses a deployer outside the deployment's tenant", async () => {
      await seedDeployer({ tenantId: OTHER_TENANT });
      const result = await collect([INVOKER_REQUIREMENT]);
      expect(result).toMatchObject({
        ok: false,
        rejection: { code: "deployer_unavailable" },
      });
    });

    test("needs no deployer without an invoker-sourced requirement", async () => {
      const result = await collect([
        { resource: "secret:vault", action: "use", source: "creator" },
      ]);
      expect(result).toEqual({ ok: true, invokerGrants: [] });
    });
  },
);
