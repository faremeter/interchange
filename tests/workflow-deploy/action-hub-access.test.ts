// A deployed workflow's action calls the hub REST API as its run principal.
//
// Two source-ref deployments of `fixtures/hub-access-workflow.ts` run on the
// real sidecar. Each action declares the reserved hub handle and calls the
// hub through it. The hub here is the real REST app, served on a real port so
// the child's mediated fetch reaches it over HTTP. The test triggers each run
// through the real `/mail` route, so the trigger mints the run's token and the
// sidecar router delivers it with the run's grants.
//
//   - Deployment B declares no grant requirement. Its run can authenticate,
//     but holds no grant to read even its own run.
//   - Deployment A requires `workflow-run:self/read` from its creator. Its run
//     reads its own run, and only its own run: another run of the same tenant,
//     a route off the run-token allow-list, and another tenant all refuse it.
//
// The test then deactivates A's run principal through the principals route and
// shows that the deactivation deletes A's token and that the bearer A's action
// presented no longer authenticates.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";

import { runToken } from "@intx/db/schema";
import { deriveRunPrincipalId, RUN_TOKEN_PREFIX } from "@intx/hub-common";
import type { GrantRequirement } from "@intx/types";
import { harnessDbEnvAvailable } from "@intx/test-harness/db-harness";

import {
  HUB_CALLER_HANDLER,
  hubAccessWorkflowEntry,
  type HubCallOutcome,
} from "./fixtures/hub-access-workflow";
import {
  hubCallOutcomes,
  NO_GRANT,
  OFF_LIST,
  SELF_READ,
  WRONG_TENANT,
  startHubAccessEnv,
  type HubAccessEnv,
} from "./hub-access-env";

const TENANT_ID = "tnt_hub_access";
const OTHER_TENANT_ID = "tnt_hub_access_other";
const RUN_A = "run_hub-access-a";
const RUN_B = "run_hub-access-b";

let t: HubAccessEnv;

/** Deploy the fixture, run it once, and return its action's outcomes. */
async function deployAndRun(args: {
  anchorRunId: string;
  calls: Record<string, string>;
  grantRequirements?: readonly GrantRequirement[];
}) {
  await t.deploy({
    anchorRunId: args.anchorRunId,
    actions: "./workflow.mjs",
    entryModule: (address) =>
      hubAccessWorkflowEntry({
        address,
        workflowId: `wf_${args.anchorRunId.replaceAll("-", "_")}`,
        calls: args.calls,
        ...(args.grantRequirements !== undefined
          ? { grantRequirements: args.grantRequirements }
          : {}),
      }),
  });
  const events = await t.runOnce(args.anchorRunId);
  return { events, outcomes: hubCallOutcomes(events, HUB_CALLER_HANDLER) };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "an action calls the hub REST API as its run",
  () => {
    beforeAll(async () => {
      t = await startHubAccessEnv({
        tenantId: TENANT_ID,
        callerUserId: "usr_hub_access_caller",
        callerPrincipalId: "prn_hub_access_caller",
      });
    });

    afterAll(async () => {
      if (t !== undefined) await t.teardown();
    });

    test("the run reads only its own run, and only while its principal is active", async () => {
      await t.seedTenancy(OTHER_TENANT_ID);
      const principalA = await deriveRunPrincipalId(TENANT_ID, RUN_A);
      await t.grantCaller(`principal:${principalA}`, "manage");
      const runs = t.runsPath;

      // The fixture hub routes the packs of the newest deployment only, so
      // each run finishes before the next deployment goes out.
      const b = await deployAndRun({
        anchorRunId: RUN_B,
        calls: { ownRun: `${runs}/${RUN_B}` },
      });
      // B's token authenticated: the refusal is the grant check's, not the
      // tenant check's that an unknown principal gets.
      expect(b.outcomes).toEqual({
        ownRun: { status: 403, id: null, message: NO_GRANT },
      });
      const callsFromB = t.received.length;

      const a = await deployAndRun({
        anchorRunId: RUN_A,
        grantRequirements: [SELF_READ],
        calls: {
          ownRun: `${runs}/${RUN_A}`,
          otherRun: `${runs}/${RUN_B}`,
          offList: `/api/tenants/${TENANT_ID}/grants`,
          otherTenant: `/api/tenants/${OTHER_TENANT_ID}/workflows/runs`,
        },
      });
      const expected: Record<string, HubCallOutcome> = {
        ownRun: { status: 200, id: RUN_A, message: null },
        otherRun: { status: 403, id: null, message: NO_GRANT },
        offList: { status: 403, id: null, message: OFF_LIST },
        otherTenant: { status: 403, id: null, message: WRONG_TENANT },
      };
      expect(a.outcomes).toEqual(expected);

      // Every call from a run carried a run token, and no request from a run
      // has a session, so each status above is the run principal's own.
      for (const call of t.received) {
        expect(
          call.authorization?.startsWith(`Bearer ${RUN_TOKEN_PREFIX}`),
        ).toBe(true);
      }

      // A's calls share one bearer, and it never reaches the run's events.
      const bearersA = new Set(
        t.received.slice(callsFromB).map((c) => c.authorization),
      );
      expect(bearersA.size).toBe(1);
      const [bearerA] = bearersA;
      if (typeof bearerA !== "string") throw new Error("expected A's bearer");
      const tokenA = bearerA.slice("Bearer ".length);
      expect(JSON.stringify(a.events)).not.toContain(tokenA);

      // Production deactivates a run principal when its run ends. The fixture
      // hub does not project the run's terminal event, so A's principal is
      // still active here and still holds its token; the principals route
      // deactivates it instead.
      const tokensOfA = () =>
        t.h.db
          .select({ id: runToken.id })
          .from(runToken)
          .where(eq(runToken.principalId, principalA));
      expect(await tokensOfA()).toHaveLength(1);
      const deactivate = await t.asCaller(
        `/api/tenants/${TENANT_ID}/principals/${principalA}`,
        { method: "PATCH", body: JSON.stringify({ status: "deactivated" }) },
      );
      expect(deactivate.status).toBe(200);
      expect(await tokensOfA()).toEqual([]);
      const replay = await fetch(`${t.hubOrigin}${runs}/${RUN_A}`, {
        headers: { authorization: bearerA },
      });
      expect(replay.status).toBe(401);
    });
  },
);
