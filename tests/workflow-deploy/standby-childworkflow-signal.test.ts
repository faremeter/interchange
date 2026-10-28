// A deployment whose run has a childWorkflow parked inside it is put in
// standby, and the signal the child workflow waits on wakes it.
//
// The child workflow waits on an author signal, which its parent relays onto
// the deployment run. The deployment is put in standby, which stops the child
// process. The signal then wakes it: the new child resumes the parent and the
// child workflow it spawned, the child workflow takes the signal and
// completes, and so does the parent.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { tenant as tenantTable } from "@intx/db/schema";
import {
  WorkflowControlHistoryPendingError,
  type RepoId,
  type SidecarEventMap,
} from "@intx/hub-sessions";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { createApprovalSet, deriveRunAddress } from "@intx/workflow-deploy";
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
import { childWorkflowUnanswerableGateEntry } from "./fixtures/childworkflow-unanswerable-gate";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_standby-childworkflow-signal";
const TENANT_ID = "tnt_standby_childworkflow";
const CALLER_PRINCIPAL_ID = "prn_standby_childworkflow";
const DEFINITION_ASSET_ID = "ast_standby_childworkflow_wf";
const SIGNAL_NAME = "go";
// Longer than the test's budget, so the worker's acknowledgement, not the
// clock, settles the control request.
const CONTROL_TIMEOUT_MS = 180_000;

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
    name: "standby-childworkflow-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

// The parent is the run whose relay of the child workflow's gate sits on the
// spawn step.
async function findParentRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  for (const id of await listRunIds(env, workflowRunRepoId)) {
    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, id);
    if (
      events.some(
        (e) =>
          e.type === "SignalAwaited" &&
          e.body["stepId"] === "spawn" &&
          e.body["signalName"] === SIGNAL_NAME,
      )
    )
      return id;
  }
  return undefined;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a deployment in standby with a childWorkflow parked in its run",
  () => {
    test("is woken by the child workflow's signal, and the child workflow and its parent complete", async () => {
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
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_standby-childworkflow",
        agentAddress: address,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: childWorkflowUnanswerableGateEntry({
          address,
          workflowId: `wf_${DEPLOYMENT_ID}`,
          childWorkflowId: `wf_child_${DEPLOYMENT_ID}`,
          signalName: SIGNAL_NAME,
        }),
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: address,
        approvals: createApprovalSet([
          "inference.source:anthropic:mock-model",
          "director:@intx/agent/default",
          `mail.address:${address}`,
          `mail.send:${DEPLOYMENT_DOMAIN}`,
        ]),
        config,
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, address, {
        messageId: "<standby-childworkflow-signal@integration.interchange>",
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
      const spawned = (
        await readWorkflowRunEvents(env, DEPLOYMENT_ID, parentRunId)
      ).flatMap((e) =>
        e.type === "ChildSpawned" ? [e.body["childRunId"]] : [],
      );
      expect(spawned).toHaveLength(1);
      const childRunId = spawned[0];
      if (typeof childRunId !== "string")
        throw new Error("the spawn named no child run");

      // The standby attempt can overtake the child's last pushes; a repeated
      // one answers again once that history has landed.
      const target = env.hub.prepareAllocationIdentity(
        DEPLOYMENT_ID,
        address,
        SIDECAR_ID,
      );
      await waitFor(
        async () => {
          try {
            await env.hub.router.putAllocationInStandby(
              target,
              CONTROL_TIMEOUT_MS,
            );
            return true;
          } catch (error) {
            if (error instanceof WorkflowControlHistoryPendingError)
              return false;
            throw error;
          }
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      expect(listWorkflowHostChildren(env)).toEqual([]);
      const recordFile = path.join(
        env.sidecar.dataDir,
        "workflow-runs",
        handle.workflowRunRepoId.id,
        "deployment.json",
      );
      const standbyMark = async (): Promise<unknown> => {
        const record: unknown = JSON.parse(
          await fs.promises.readFile(recordFile, "utf8"),
        );
        return typeof record === "object" &&
          record !== null &&
          "standby" in record
          ? record.standby
          : undefined;
      };
      // The child workflow's gate has no deadline, so only the signal wakes it.
      expect(await standbyMark()).toEqual({
        parked: [],
        tip: expect.any(String),
      });

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
          parentRunId,
          SIGNAL_NAME,
          { resumed: true },
        );
        const terminal = await waitForWorkflowRunComplete(
          env,
          DEPLOYMENT_ID,
          parentRunId,
          { diagnostics: env.sidecarDiagnostics },
        );
        expect(terminal.type).toBe("RunCompleted");
        expect(await reported.promise).toEqual({
          ...target,
          inStandby: false,
          reason: "Woken by signal",
        });
        expect(await standbyMark()).toBeUndefined();

        const parent = await readWorkflowRunEvents(
          env,
          DEPLOYMENT_ID,
          parentRunId,
        );
        const onParent = (type: string): number =>
          parent.filter((e) => e.type === type).length;
        expect(onParent("RunStarted")).toBe(1);
        expect(onParent("ChildSpawned")).toBe(1);
        expect(onParent("RunCompleted")).toBe(1);
        expect(
          parent.filter(
            (e) =>
              e.type === "SignalReceived" &&
              e.body["signalName"] === SIGNAL_NAME,
          ),
        ).toMatchObject([
          { body: { signalId: injected.signalId, payload: { resumed: true } } },
        ]);
        expect(
          parent.some(
            (e) =>
              e.type === "ChildCompleted" &&
              e.body["childRunId"] === childRunId,
          ),
        ).toBe(true);
      } finally {
        unsubscribe();
      }

      // The new child resumed the child workflow it found parked, rather than
      // spawning it again.
      const child = await readWorkflowRunEvents(env, DEPLOYMENT_ID, childRunId);
      const onChild = (type: string): number =>
        child.filter((e) => e.type === type).length;
      expect(onChild("RunStarted")).toBe(1);
      expect(onChild("RunCompleted")).toBe(1);
    });
  },
);
