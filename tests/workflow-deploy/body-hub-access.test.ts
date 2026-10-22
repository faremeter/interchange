// Actions inside a `childWorkflow` body and a loop body call the hub REST API
// as the top-level run.
//
// A body run shares the top-level run's principal. The deploy check collects
// the hub consumers of every body, so the top-level run's principal holds one
// token for each body action. The workflow requires `workflow-run:self/read`
// from its creator, and `self` binds to the top-level run. Each body action
// reads a different route of the top-level run, which works only when the
// body acts as the top-level run, and the test matches each request's bearer
// to the token row of the action that sent it.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";

import { actionConsumer } from "@intx/authz";
import { sha256 } from "@intx/crypto";
import { runToken } from "@intx/db/schema";
import { deriveRunPrincipalId, RUN_TOKEN_PREFIX } from "@intx/hub-common";
import { loopBodyRunId } from "@intx/workflow";
import { harnessDbEnvAvailable } from "@intx/test-harness/db-harness";

import { readWorkflowRunEvents } from "../hub-agent/lib/deploy-flow-env";
import {
  CHILD_HUB_HANDLER,
  HUB_LOOP_STEP_ID,
  LOOP_HUB_HANDLER,
  hubAccessBodiesWorkflowEntry,
} from "./fixtures/hub-access-workflow";
import {
  SELF_READ,
  hubCallOutcomes,
  startHubAccessEnv,
  type HubAccessEnv,
} from "./hub-access-env";

const TENANT_ID = "tnt_body_hub";
const OTHER_TENANT_ID = "tnt_body_hub_other";
const RUN_ID = "run_body-hub";

let t: HubAccessEnv;

// The route of the top-level run each body action reads.
const CHILD_PATH = () => `${t.runsPath}/${RUN_ID}`;
const LOOP_PATH = () => `${t.runsPath}/${RUN_ID}/events`;

describe.skipIf(!harnessDbEnvAvailable())(
  "body actions call the hub REST API as the top-level run",
  () => {
    beforeAll(async () => {
      t = await startHubAccessEnv({
        tenantId: TENANT_ID,
        callerUserId: "usr_body_hub_caller",
        callerPrincipalId: "prn_body_hub_caller",
      });
    });

    afterAll(async () => {
      if (t !== undefined) await t.teardown();
    });

    test("a childWorkflow body and a loop body each read the top-level run", async () => {
      await t.seedTenancy(OTHER_TENANT_ID);
      await t.deploy({
        anchorRunId: RUN_ID,
        actions: "./workflow.mjs",
        loops: "./workflow.mjs",
        entryModule: (address) =>
          hubAccessBodiesWorkflowEntry({
            address,
            workflowId: "wf_body_hub",
            childCalls: { run: CHILD_PATH() },
            loopCalls: { events: LOOP_PATH() },
            grantRequirements: [SELF_READ],
          }),
      });
      const events = await t.runOnce(RUN_ID);

      const spawned = events.find((e) => e.type === "ChildSpawned");
      const childRunId = spawned?.body["childRunId"];
      if (typeof childRunId !== "string") {
        throw new Error("expected a ChildSpawned event with a childRunId");
      }
      const childEvents = await readWorkflowRunEvents(
        t.env,
        RUN_ID,
        childRunId,
      );
      const loopEvents = await readWorkflowRunEvents(
        t.env,
        RUN_ID,
        loopBodyRunId(RUN_ID, HUB_LOOP_STEP_ID, 0),
      );
      expect(hubCallOutcomes(childEvents, CHILD_HUB_HANDLER)).toEqual({
        run: { status: 200, id: RUN_ID, message: null },
      });
      expect(hubCallOutcomes(loopEvents, LOOP_HUB_HANDLER)).toMatchObject({
        events: { status: 200, message: null },
      });

      // The top-level run's principal holds one token per body action.
      const principalId = await deriveRunPrincipalId(TENANT_ID, RUN_ID);
      const tokens = await t.h.db
        .select({ consumer: runToken.consumer })
        .from(runToken)
        .where(eq(runToken.principalId, principalId))
        .orderBy(runToken.consumer);
      expect(tokens.map((row) => row.consumer)).toEqual([
        actionConsumer(CHILD_HUB_HANDLER),
        actionConsumer(LOOP_HUB_HANDLER),
      ]);
      // Each body action presented the token minted for it, under the
      // top-level run's principal.
      const consumerOf = async (authorization: string | null) => {
        const bearer = authorization ?? "";
        expect(bearer.startsWith(`Bearer ${RUN_TOKEN_PREFIX}`)).toBe(true);
        const [row] = await t.h.db
          .select({
            consumer: runToken.consumer,
            principalId: runToken.principalId,
          })
          .from(runToken)
          .where(
            eq(
              runToken.tokenHashSha256,
              await sha256(bearer.slice("Bearer ".length)),
            ),
          );
        expect(row?.principalId).toBe(principalId);
        return row?.consumer;
      };
      expect(t.received.map((call) => call.path).sort()).toEqual(
        [CHILD_PATH(), LOOP_PATH()].sort(),
      );
      for (const call of t.received) {
        expect(await consumerOf(call.authorization)).toBe(
          call.path === CHILD_PATH()
            ? actionConsumer(CHILD_HUB_HANDLER)
            : actionConsumer(LOOP_HUB_HANDLER),
        );
      }
    });
  },
);
