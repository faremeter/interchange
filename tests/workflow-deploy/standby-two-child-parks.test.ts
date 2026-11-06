// A deployment whose child workflow holds two open parks is put in standby, and
// each park is answered in turn.
//
// The child workflow waits on two author signals at once. Its parent relays one,
// and the other stays queued in the child, since the spawn step awaits one signal
// at a time. Standby stops the child process with the second park relayed by
// nobody. The signal for the first wakes it; the new child resumes the child
// workflow and its parent relays the second, and that signal finishes the run.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { tenant as tenantTable } from "@intx/db/schema";
import { type RepoId, type SidecarEventMap } from "@intx/hub-sessions";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  injectSignal,
  listRunIds,
  listWorkflowHostChildren,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import { putInStandby } from "./standby-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_standby-two-child-parks";
const TENANT_ID = "tnt_standby_two_child_parks";
const CALLER_PRINCIPAL_ID = "prn_standby_two_child_parks";
const DEFINITION_ASSET_ID = "ast_standby_two_child_parks_wf";
const SIGNAL_NAMES = ["first", "second"];

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: tenantSlugFromId(TENANT_ID),
    domain: DEPLOYMENT_DOMAIN,
    parentId: null,
  });
  await seedPrincipal(h.db, {
    id: CALLER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
  });
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "standby-two-child-parks-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

function entry(address: string): string {
  return `
import { awaitSignal, childWorkflow, defineWorkflow } from "@intx/workflow/definition";

const child = defineWorkflow({
  id: ${JSON.stringify(`wf_child_${DEPLOYMENT_ID}`)},
  trigger: { type: "manual" },
  steps: {
    first: awaitSignal({ name: ${JSON.stringify(SIGNAL_NAMES[0])} }),
    second: awaitSignal({ name: ${JSON.stringify(SIGNAL_NAMES[1])} }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(`wf_${DEPLOYMENT_ID}`)},
  trigger: { type: "mail", to: ${JSON.stringify(address)} },
  steps: {
    spawn: childWorkflow({ definition: child }),
  },
});
`;
}

// The signals the parent has relayed so far, in the order it relayed them.
function relayed(events: WorkflowRunEvent[]): unknown[] {
  return events.flatMap((e) =>
    e.type === "SignalAwaited" && e.body["stepId"] === "spawn"
      ? [e.body["signalName"]]
      : [],
  );
}

async function findParentRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  for (const id of await listRunIds(env, workflowRunRepoId)) {
    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, id);
    if (relayed(events).length > 0) return id;
  }
  return undefined;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a deployment in standby with a child workflow holding two open parks",
  () => {
    test("takes each park's signal in turn, and the run completes", async () => {
      const address = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentAddress: address,
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: entry(address),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: address,
        approvals: "approve-probed",
        config,
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, address, {
        messageId: "<standby-two-child-parks@integration.interchange>",
      });
      let found: string | undefined;
      await waitFor(
        async () => {
          found = await findParentRunId(handle.workflowRunRepoId);
          return found !== undefined;
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      if (found === undefined) throw new Error("no parent run on the spawn");
      const parentRunId = found;
      const relays = async () =>
        relayed(await readWorkflowRunEvents(env, DEPLOYMENT_ID, parentRunId));

      const target = env.hub.prepareAllocationIdentity(
        DEPLOYMENT_ID,
        address,
        SIDECAR_ID,
      );
      await putInStandby(env, target);
      expect(listWorkflowHostChildren(env)).toEqual([]);

      const reported =
        Promise.withResolvers<SidecarEventMap["deployment.standby"]>();
      const unsubscribe = env.hub.router.events.on(
        "deployment.standby",
        (report) => {
          reported.resolve(report);
        },
      );
      try {
        // Only one park is on the parent while the child is stopped.
        const before = await relays();
        expect(before).toHaveLength(1);
        const firstName = before[0];
        if (typeof firstName !== "string")
          throw new Error("the parent relayed no signal name");
        const secondName = SIGNAL_NAMES.find((name) => name !== firstName);
        if (secondName === undefined) throw new Error("no second signal");

        await injectSignal(env, DEPLOYMENT_ID, parentRunId, firstName, {
          answered: firstName,
        });
        expect(await reported.promise).toEqual({
          ...target,
          inStandby: false,
          reason: "Woken by signal",
        });
        await waitFor(async () => (await relays()).includes(secondName), {
          diagnostics: env.sidecarDiagnostics,
        });
        await injectSignal(env, DEPLOYMENT_ID, parentRunId, secondName, {
          answered: secondName,
        });
        const terminal = await waitForWorkflowRunComplete(
          env,
          DEPLOYMENT_ID,
          parentRunId,
          { diagnostics: env.sidecarDiagnostics },
        );
        expect(terminal.type).toBe("RunCompleted");

        const parent = await readWorkflowRunEvents(
          env,
          DEPLOYMENT_ID,
          parentRunId,
        );
        expect(parent.filter((e) => e.type === "RunStarted")).toHaveLength(1);
        expect(parent.filter((e) => e.type === "ChildSpawned")).toHaveLength(1);
      } finally {
        unsubscribe();
      }
    });
  },
);
