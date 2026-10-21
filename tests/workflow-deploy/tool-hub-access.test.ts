// A deployed workflow's tool calls the hub REST API as its run principal.
//
// A single-step agent workflow carries `hubProbe` from
// `fixtures/credential-tool-bundle.ts`, a tool that declares the reserved hub
// handle, and requires `workflow-run:self/read` from its creator. The mock
// model calls the probe once, and the probe fetches the run's own detail route
// through the handle. The hub here is the real REST app on a real port, and
// the test triggers the run through the real `/mail` route, as in
// `action-hub-access.test.ts`, which also covers the refusals.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";

import { toolConsumer } from "@intx/authz";
import { runToken } from "@intx/db/schema";
import { deriveRunPrincipalId, RUN_TOKEN_PREFIX } from "@intx/hub-common";
import { harnessDbEnvAvailable } from "@intx/test-harness/db-harness";

import {
  BUNDLE_ID,
  PROBE_DEFINITION_NAME,
} from "./fixtures/credential-tool-bundle";
import { credentialToolWorkflowEntry } from "./fixtures/credential-tool-workflow";
import {
  SELF_READ,
  startHubAccessEnv,
  type HubAccessEnv,
} from "./hub-access-env";

const TENANT_ID = "tnt_tool_hub_access";
const RUN_ID = "run_tool-hub-access";
const STEP_ID = "step1";
const OWN_RUN_PATH = `/api/tenants/${TENANT_ID}/workflows/runs/${RUN_ID}`;

let t: HubAccessEnv;

describe.skipIf(!harnessDbEnvAvailable())(
  "a tool calls the hub REST API as its run",
  () => {
    beforeAll(async () => {
      t = await startHubAccessEnv({
        tenantId: TENANT_ID,
        callerUserId: "usr_tool_hub_access_caller",
        callerPrincipalId: "prn_tool_hub_access_caller",
        envOpts: {
          inferenceToolCall: {
            toolName: PROBE_DEFINITION_NAME,
            input: { path: OWN_RUN_PATH, sentinel: "hub-probe-ran.json" },
          },
        },
      });
    });

    afterAll(async () => {
      if (t !== undefined) await t.teardown();
    });

    test("the tool reads its own run with the token minted for it", async () => {
      await t.seedTenancy("tnt_tool_hub_access_other");
      await t.deploy({
        anchorRunId: RUN_ID,
        agentStepId: STEP_ID,
        entryModule: (address) =>
          credentialToolWorkflowEntry({
            stepId: STEP_ID,
            systemPrompt: "You are the hub probe agent.",
            address,
            workflowId: "wf_tool_hub_access",
            hubProbe: true,
            grantRequirements: [SELF_READ],
          }),
      });
      await t.runOnce(RUN_ID);

      // The run principal holds one token, for the probe's consumer.
      const principalId = await deriveRunPrincipalId(TENANT_ID, RUN_ID);
      const tokens = await t.h.db
        .select({ consumer: runToken.consumer })
        .from(runToken)
        .where(eq(runToken.principalId, principalId));
      expect(tokens).toEqual([{ consumer: toolConsumer(BUNDLE_ID) }]);

      expect(t.received).toHaveLength(1);
      const [call] = t.received;
      expect(call?.path).toBe(OWN_RUN_PATH);
      expect(call?.status).toBe(200);
      expect(
        call?.authorization?.startsWith(`Bearer ${RUN_TOKEN_PREFIX}`),
      ).toBe(true);
    });
  },
);
