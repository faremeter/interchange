// A creator grant revoked while a run is live takes effect in that run.
//
// The workflow runs `before`, an agent step with the `hubProbe` tool, then
// parks on the `resume` signal, then runs `after`, a second agent step with the
// same tool. It requires `workflow-run:self/read` from its creator, and the
// creator holds `workflow-run:*/read`. The mock model calls the probe once in
// each step, and the probe reads the run's own detail route through the
// reserved hub handle. The hub is the real REST app on a real port, as in
// `tool-hub-access.test.ts`.
//
// While the run is parked, the test revokes the creator's grant through the
// real grants route. The route deletes the run's `workflow-run:<runId>/read`
// row and sends the run its grants without it. After the signal, the second
// probe call presents the same run token and the hub refuses it with a 403.

import path from "node:path";

import { afterAll, beforeAll, describe, expect, spyOn, test } from "bun:test";
import { and, eq } from "drizzle-orm";

import { grant as grantTable } from "@intx/db/schema";
import { deriveRunPrincipalId, RUN_TOKEN_PREFIX } from "@intx/hub-common";
import { createRunGrantsLoader } from "@intx/hub-api";
import type { WireGrantRule } from "@intx/types/grant-wire";
import { harnessDbEnvAvailable } from "@intx/test-harness/db-harness";

import {
  injectSignal,
  readWorkflowRunEvents,
  waitFor,
  waitForWorkflowRunComplete,
} from "../hub-agent/lib/deploy-flow-env";
import { PROBE_DEFINITION_NAME } from "./fixtures/credential-tool-bundle";
import { failureMessages } from "./nested-tool-invoke-helpers";
import {
  SELF_READ,
  startHubAccessEnv,
  type HubAccessEnv,
} from "./hub-access-env";

const TENANT_ID = "tnt_revoked_creator_grant";
const RUN_ID = "run_revoked-creator-grant";
const OWN_RUN = `workflow-run:${RUN_ID}`;
const OWN_RUN_PATH = `/api/tenants/${TENANT_ID}/workflows/runs/${RUN_ID}`;
const SIGNAL_NAME = "resume";
// The grant `seedTenancy` gives the creator, by the id `grantCaller` assigns.
const CREATOR_READ_GRANT_ID = "grant-caller-read-workflow-run:*";

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
  id: "wf_revoked_creator_grant",
  trigger: { type: "mail", to: ${JSON.stringify(address)} },
  grantRequirements: ${JSON.stringify([SELF_READ])},
  steps: {
    before: step({ agent: ${agent("revoked-grant-before")} }),
    gate: awaitSignal({ name: ${JSON.stringify(SIGNAL_NAME)}, after: ["before"] }),
    after: step({ agent: ${agent("revoked-grant-after")}, after: ["gate"] }),
  },
});
`;
}

const holdsOwnRunRead = (grants: readonly WireGrantRule[]) =>
  grants.some(
    (rule) =>
      rule.resource === OWN_RUN &&
      rule.action === "read" &&
      rule.effect === "allow",
  );

let t: HubAccessEnv;
// Each grant set the sidecar router loaded to send in a `run.grants` frame.
const loadedGrants: (readonly WireGrantRule[] | null)[] = [];

describe.skipIf(!harnessDbEnvAvailable())(
  "a creator grant revoked during a run",
  () => {
    beforeAll(async () => {
      t = await startHubAccessEnv({
        tenantId: TENANT_ID,
        callerUserId: "usr_revoked_creator_grant_caller",
        callerPrincipalId: "prn_revoked_creator_grant_caller",
        envOpts: {
          inferenceToolCall: {
            toolName: PROBE_DEFINITION_NAME,
            input: { path: OWN_RUN_PATH, sentinel: "hub-probe-ran.json" },
          },
          inferenceToolCallEachRun: true,
          // The env builds the database after the router, so the loader
          // reads it at call time, as the production hub's loader does.
          loadRunGrants: async (runId) => {
            const grants = await createRunGrantsLoader({ db: t.h.db })(runId);
            loadedGrants.push(grants);
            return grants;
          },
        },
      });
    });

    afterAll(async () => {
      if (t !== undefined) await t.teardown();
    });

    test("the run's next hub call fails closed", async () => {
      const { env } = t;
      await t.seedTenancy("tnt_revoked_creator_grant_other");
      await t.grantCaller("grant:*", "manage");
      await t.deploy({ anchorRunId: RUN_ID, entryModule: probeTwiceEntry });
      const address = env.deployments.get(RUN_ID)?.mailAddress;
      if (address === undefined) throw new Error("no deployment handle");

      const principalId = await deriveRunPrincipalId(TENANT_ID, RUN_ID);
      const ownRunReadRows = () =>
        t.h.db
          .select({ id: grantTable.id })
          .from(grantTable)
          .where(
            and(
              eq(grantTable.principalId, principalId),
              eq(grantTable.resource, OWN_RUN),
              eq(grantTable.action, "read"),
            ),
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
      expect(await ownRunReadRows()).toHaveLength(1);

      const pushes = spyOn(env.hub.router, "sendRunGrants");
      const loadsBeforeRevoke = loadedGrants.length;
      const revoke = await t.asCaller(
        `/api/tenants/${TENANT_ID}/grants/${encodeURIComponent(CREATOR_READ_GRANT_ID)}`,
        { method: "DELETE" },
      );
      expect(revoke.status).toBe(204);
      expect(await ownRunReadRows()).toEqual([]);

      // The route pushes after it returns. The router resolves true once it
      // sent the run a `run.grants` frame, built from the grants it loaded.
      await waitFor(() => pushes.mock.calls.some(([to]) => to === address), {
        diagnostics: env.sidecarDiagnostics,
      });
      const sent = pushes.mock.results.flatMap((result, index) =>
        pushes.mock.calls[index]?.[0] === address && result.type === "return"
          ? [result.value]
          : [],
      );
      expect(await Promise.all(sent)).toEqual([true]);
      pushes.mockRestore();
      const pushed = loadedGrants.slice(loadsBeforeRevoke);
      expect(pushed).toHaveLength(1);
      const [pushedGrants] = pushed;
      if (pushedGrants === undefined || pushedGrants === null) {
        throw new Error("the push loaded no grants for the run");
      }
      expect(holdsOwnRunRead(pushedGrants)).toBe(false);

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

      // Both calls carried the run's one token, so the refusal is the grant
      // check's on the re-derived rows, not a lost or replaced credential.
      expect(t.received.map((call) => [call.path, call.status])).toEqual([
        [OWN_RUN_PATH, 200],
        [OWN_RUN_PATH, 403],
      ]);
      const bearers = new Set(t.received.map((call) => call.authorization));
      expect(bearers.size).toBe(1);
      const [bearer] = bearers;
      expect(bearer?.startsWith(`Bearer ${RUN_TOKEN_PREFIX}`)).toBe(true);
    }, 180_000);
  },
);
