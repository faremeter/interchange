// A deployment whose top-level run waits on an author signal is put in standby,
// and that signal wakes it on the same sidecar.
//
// The run's own `awaitSignal` is the park, with no child workflow to relay it.
// The deployment is put in standby, which stops the child process. The signal
// then wakes it, the new child resumes the run, the gate takes the signal and
// the run completes.
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
} from "../hub-agent/lib/deploy-flow-env";
import { putInStandby } from "./standby-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_standby-signal-wake";
const TENANT_ID = "tnt_standby_signal_wake";
const CALLER_PRINCIPAL_ID = "prn_standby_signal_wake";
const DEFINITION_ASSET_ID = "ast_standby_signal_wake_wf";
const SIGNAL_NAME = "go";

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
    name: "standby-signal-wake-wf",
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
import { awaitSignal, defineWorkflow } from "@intx/workflow/definition";

export const workflow = defineWorkflow({
  id: ${JSON.stringify(`wf_${DEPLOYMENT_ID}`)},
  trigger: { type: "mail", to: ${JSON.stringify(address)} },
  steps: {
    hold: awaitSignal({ name: ${JSON.stringify(SIGNAL_NAME)} }),
  },
});
`;
}

async function findRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  for (const id of await listRunIds(env, workflowRunRepoId)) {
    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, id);
    if (
      events.some(
        (e) =>
          e.type === "SignalAwaited" &&
          e.body["stepId"] === "hold" &&
          e.body["signalName"] === SIGNAL_NAME,
      )
    )
      return id;
  }
  return undefined;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a deployment in standby on a top-level awaitSignal",
  () => {
    test("is woken by that signal, and the run completes", async () => {
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
        messageId: "<standby-signal-wake@integration.interchange>",
      });
      let found: string | undefined;
      await waitFor(
        async () => {
          found = await findRunId(handle.workflowRunRepoId);
          return found !== undefined;
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      if (found === undefined) throw new Error("no run on the gate");
      const runId = found;

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
        const injected = await injectSignal(
          env,
          DEPLOYMENT_ID,
          runId,
          SIGNAL_NAME,
          { resumed: true },
        );
        const terminal = await waitForWorkflowRunComplete(
          env,
          DEPLOYMENT_ID,
          runId,
          { diagnostics: env.sidecarDiagnostics },
        );
        expect(terminal.type).toBe("RunCompleted");
        expect(await reported.promise).toEqual({
          ...target,
          inStandby: false,
          reason: "Woken by signal",
        });

        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        expect(events.filter((e) => e.type === "RunStarted")).toHaveLength(1);
        expect(
          events.filter(
            (e) =>
              e.type === "SignalReceived" &&
              e.body["signalName"] === SIGNAL_NAME,
          ),
        ).toMatchObject([
          { body: { signalId: injected.signalId, payload: { resumed: true } } },
        ]);
      } finally {
        unsubscribe();
      }
    });
  },
);
