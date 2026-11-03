// A run grant whose expiry passes while a run is live stops working in the
// run's child.
//
// The workflow runs `before`, an agent step with the `hubProbe` tool, then
// parks on the `resume` signal, then runs `after`, a second agent step with the
// same tool. On the source-ref deploy the run principal's `tool:probe/invoke`
// row is the tool's only authority in the child. The mock model calls the probe once in each step.
//
// While the run is parked, the test moves that row's expiry into the past. The
// grants the hub sends ahead of the resume signal still carry the row, now with
// a past expiry, so it is the child's own check that refuses the second call:
// the probe never runs, so the hub sees one request, and the model gets the
// refusal as the tool result.

import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { and, eq } from "drizzle-orm";

import { grant as grantTable } from "@intx/db/schema";
import { deriveRunPrincipalId } from "@intx/hub-common";
import { createRunGrantsLoader } from "@intx/hub-api";
import { harnessDbEnvAvailable } from "@intx/test-harness/db-harness";

import {
  injectSignal,
  readWorkflowRunEvents,
  waitFor,
  waitForWorkflowRunComplete,
} from "../hub-agent/lib/deploy-flow-env";
import { PROBE_DEFINITION_NAME } from "./fixtures/credential-tool-bundle";
import { failureMessages, toolResultTexts } from "./nested-tool-invoke-helpers";
import {
  SELF_READ,
  startHubAccessEnv,
  type HubAccessEnv,
} from "./hub-access-env";

const TENANT_ID = "tnt_expired_run_grant";
const RUN_ID = "run_expired-run-grant";
const OWN_RUN_PATH = `/api/tenants/${TENANT_ID}/workflows/runs/${RUN_ID}`;
const SIGNAL_NAME = "resume";
const PROBE_RESOURCE = `tool:${PROBE_DEFINITION_NAME}`;

const PROBE_MODULE = path.join(
  import.meta.dir,
  "fixtures",
  "credential-tool-bundle.ts",
);

function probeTwiceEntry(address: string): string {
  const agent = (id: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: "You are the hub probe agent.",
  tools: [hubProbe],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
})`;
  return `
import { awaitSignal, defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { hubProbe } from ${JSON.stringify(PROBE_MODULE)};

export const workflow = defineWorkflow({
  id: "wf_expired_run_grant",
  trigger: { type: "mail", to: ${JSON.stringify(address)} },
  grantRequirements: ${JSON.stringify([SELF_READ])},
  steps: {
    before: step({ agent: ${agent("expired-grant-before")} }),
    gate: awaitSignal({ name: ${JSON.stringify(SIGNAL_NAME)}, after: ["before"] }),
    after: step({ agent: ${agent("expired-grant-after")}, after: ["gate"] }),
  },
});
`;
}

let t: HubAccessEnv;

describe.skipIf(!harnessDbEnvAvailable())(
  "a run grant that expires during a run",
  () => {
    beforeAll(async () => {
      t = await startHubAccessEnv({
        tenantId: TENANT_ID,
        callerUserId: "usr_expired_run_grant_caller",
        callerPrincipalId: "prn_expired_run_grant_caller",
        envOpts: {
          inferenceToolCall: {
            toolName: PROBE_DEFINITION_NAME,
            input: { path: OWN_RUN_PATH, sentinel: "hub-probe-ran.json" },
          },
          inferenceToolCallEachRun: true,
          // The env builds the database after the router, so the loader
          // reads it at call time, as the production hub's loader does.
          loadRunGrants: (runId) =>
            createRunGrantsLoader({ db: t.h.db })(runId),
        },
      });
    });

    afterAll(async () => {
      if (t !== undefined) await t.teardown();
    });

    test("the child refuses the next tool call", async () => {
      const { env } = t;
      await t.seedTenancy("tnt_expired_run_grant_other");
      await t.deploy({ anchorRunId: RUN_ID, entryModule: probeTwiceEntry });

      const principalId = await deriveRunPrincipalId(TENANT_ID, RUN_ID);
      const probeRow = and(
        eq(grantTable.principalId, principalId),
        eq(grantTable.resource, PROBE_RESOURCE),
        eq(grantTable.action, "invoke"),
      );

      await t.trigger(RUN_ID);
      await waitFor(
        async () =>
          (await readWorkflowRunEvents(env, RUN_ID, RUN_ID)).some(
            (event) =>
              event.type === "SignalAwaited" &&
              event.body["signalName"] === SIGNAL_NAME,
          ),
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(t.received.map((call) => [call.path, call.status])).toEqual([
        [OWN_RUN_PATH, 200],
      ]);

      const expired = await t.h.db
        .update(grantTable)
        .set({ expiresAt: new Date(Date.now() - 60_000) })
        .where(probeRow)
        .returning({ id: grantTable.id });
      expect(expired).toHaveLength(1);

      // The router sends the run its grants, the expired row included, ahead
      // of the signal on the same connection.
      await injectSignal(env, RUN_ID, RUN_ID, SIGNAL_NAME, null);
      const terminal = await waitForWorkflowRunComplete(env, RUN_ID, RUN_ID, {
        diagnostics: env.sidecarDiagnostics,
      });
      if (terminal.type !== "RunCompleted") {
        const events = await readWorkflowRunEvents(env, RUN_ID, RUN_ID);
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failureMessages(events))}`,
        );
      }

      expect(t.received.map((call) => [call.path, call.status])).toEqual([
        [OWN_RUN_PATH, 200],
      ]);
      expect(env.inference.requests.flatMap(toolResultTexts)).toContain(
        `No matching grants for ${PROBE_RESOURCE}/invoke`,
      );
      expect(
        await t.h.db.select().from(grantTable).where(probeRow),
      ).toHaveLength(1);
    }, 180_000);
  },
);
