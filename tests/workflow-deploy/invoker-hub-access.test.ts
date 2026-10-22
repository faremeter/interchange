// A deployed workflow's invoker requirement delegates the deployer's grant to
// the run, end to end, and not the grant of whoever triggers the run.
//
// The deployment's deployer is its invoker of record. The test deploys on
// behalf of a separate deployer principal, so the launch spec records it as
// the deployer, and the caller triggers through the real `/mail` route. The
// grants are split so that each half passes only if the trigger resolves the
// invoker requirement against the deployer:
//
//   - Deployment A requires `workflow-run:self/read` from its invoker. Only
//     the deployer holds `workflow-run:*/read`; the caller holds no read
//     grant. The run receives the grant as an invoker grant that expires a
//     day after it is made, and reads its own run through the hub.
//   - Deployment B requires `secret:vault/use` from its invoker. Only the
//     caller holds it. The trigger refuses the run, which gets no principal
//     and no token.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { and, eq } from "drizzle-orm";

import { grant, principal, runToken } from "@intx/db/schema";
import { deriveRunPrincipalId } from "@intx/hub-common";
import type { GrantRequirement } from "@intx/types";
import { harnessDbEnvAvailable } from "@intx/test-harness/db-harness";
import { seedGrant, seedPrincipal } from "@intx/test-harness/seed";

import {
  HUB_CALLER_HANDLER,
  hubAccessWorkflowEntry,
} from "./fixtures/hub-access-workflow";
import {
  hubCallOutcomes,
  startHubAccessEnv,
  type HubAccessEnv,
} from "./hub-access-env";

const TENANT_ID = "tnt_invoker_hub";
const OTHER_TENANT_ID = "tnt_invoker_hub_other";
const CALLER_PRINCIPAL_ID = "prn_invoker_hub_caller";
const DEPLOYER_PRINCIPAL_ID = "prn_invoker_hub_deployer";
const RUN_A = "run_invoker-hub-a";
const RUN_B = "run_invoker-hub-b";

// The invoker grant lifetime AUTH.md documents.
const INVOKER_GRANT_LIFETIME_MS = 24 * 60 * 60 * 1000;

let t: HubAccessEnv;

function deploy(anchorRunId: string, requirement: GrantRequirement) {
  return t.deploy({
    anchorRunId,
    deployerPrincipalId: DEPLOYER_PRINCIPAL_ID,
    actions: "./workflow.mjs",
    entryModule: (address) =>
      hubAccessWorkflowEntry({
        address,
        workflowId: `wf_${anchorRunId.replaceAll("-", "_")}`,
        calls: { ownRun: `${t.runsPath}/${anchorRunId}` },
        grantRequirements: [requirement],
      }),
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "an invoker requirement delegates the deployer's grant to the run",
  () => {
    beforeAll(async () => {
      t = await startHubAccessEnv({
        tenantId: TENANT_ID,
        callerUserId: "usr_invoker_hub_caller",
        callerPrincipalId: CALLER_PRINCIPAL_ID,
      });
    });

    afterAll(async () => {
      if (t !== undefined) await t.teardown();
    });

    test("the run holds the deployer's grant, not the caller's, for a day", async () => {
      await t.seedTenancy(OTHER_TENANT_ID);
      // Only the deployer can delegate a read of the run; only the caller
      // holds the secret.
      await t.h.db
        .delete(grant)
        .where(
          and(
            eq(grant.principalId, CALLER_PRINCIPAL_ID),
            eq(grant.resource, "workflow-run:*"),
            eq(grant.action, "read"),
          ),
        );
      await t.grantCaller("secret:vault", "use");
      await seedPrincipal(t.h.db, {
        id: DEPLOYER_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
        refId: "usr_invoker_hub_deployer",
        status: "active",
      });
      await seedGrant(t.h.db, {
        id: "grant-deployer-read",
        tenantId: TENANT_ID,
        principalId: DEPLOYER_PRINCIPAL_ID,
        resource: "workflow-run:*",
        action: "read",
        effect: "allow",
        origin: "system",
      });

      // The fixture hub routes the packs of the newest deployment only, so A
      // runs before B deploys.
      await deploy(RUN_A, {
        resource: "workflow-run:self",
        action: "read",
        source: "invoker",
      });
      const events = await t.runOnce(RUN_A);
      expect(hubCallOutcomes(events, HUB_CALLER_HANDLER)).toEqual({
        ownRun: { status: 200, id: RUN_A, message: null },
      });

      const principalA = await deriveRunPrincipalId(TENANT_ID, RUN_A);
      const delegated = await t.h.db
        .select()
        .from(grant)
        .where(eq(grant.principalId, principalA));
      const selfRead = delegated.find(
        (g) => g.resource === `workflow-run:${RUN_A}` && g.action === "read",
      );
      if (selfRead?.expiresAt == null) {
        throw new Error("expected an expiring self-read grant on the run");
      }
      expect(selfRead.origin).toBe("invoker");
      expect(selfRead.expiresAt.getTime() - selfRead.createdAt.getTime()).toBe(
        INVOKER_GRANT_LIFETIME_MS,
      );

      await deploy(RUN_B, {
        resource: "secret:vault",
        action: "use",
        source: "invoker",
      });
      const refused = await t.asCaller(
        `/api/tenants/${TENANT_ID}/workflows/${RUN_B}/mail`,
        { method: "POST", body: JSON.stringify({ content: "call the hub" }) },
      );
      expect(refused.status).toBe(403);
      expect(await refused.json()).toMatchObject({
        error: { code: "insufficient_grants" },
      });
      const principalB = await deriveRunPrincipalId(TENANT_ID, RUN_B);
      expect(
        await t.h.db
          .select({ id: principal.id })
          .from(principal)
          .where(eq(principal.id, principalB)),
      ).toEqual([]);
      expect(
        await t.h.db
          .select({ id: runToken.id })
          .from(runToken)
          .where(eq(runToken.principalId, principalB)),
      ).toEqual([]);
    });
  },
);
