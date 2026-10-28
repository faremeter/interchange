// An onTrigger section idle between events is put in standby, and the next
// event wakes it and is serviced by the new child.
//
// Mail 1 starts the container run, whose section runs event 0's body (a short
// sleep) and re-arms on its input park. The deployment is put in standby, which
// stops the child. Mail 2 then wakes it: the new child resumes the container
// on its input park, and the section runs event 1's body to completion.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child driving `runOnTrigger`, mock inference.

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
import { sectionBodyRunId } from "@intx/workflow";
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
  listRunIds,
  listWorkflowHostChildren,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import { onTriggerBodyEntry } from "./fixtures/on-trigger-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_standby-on-trigger-between-events";
const TENANT_ID = "tnt_standby_on_trigger";
const CALLER_PRINCIPAL_ID = "prn_standby_on_trigger";
const DEFINITION_ASSET_ID = "ast_standby_on_trigger_wf";
const SECTION_ID = "section";
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
    name: "standby-on-trigger-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

// The container is the one run whose id is not a minted section body's.
async function findContainerRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(env, workflowRunRepoId);
  return ids.find((id) => !id.startsWith(`${SECTION_ID}__`));
}

function childCompletedAt(
  events: readonly WorkflowRunEvent[],
  childRunId: string,
): number {
  return events.findIndex(
    (e) => e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
  );
}

describe.skipIf(!harnessDbEnvAvailable())(
  "an onTrigger section in standby between events",
  () => {
    test("is woken by the next event, whose body the new child runs to completion", async () => {
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
        principalId: "prin_standby-on-trigger",
        agentAddress: address,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };
      const handle = await deployWorkflowSourceForTest(env, {
        // A body that completes on its own, so the section re-arms on its
        // input after each event.
        entryModule: onTriggerBodyEntry({
          address,
          sectionId: SECTION_ID,
          body: { variant: "sleep", stepId: "wait", duration: 10 },
          workflowId: `wf_${DEPLOYMENT_ID}`,
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
        sources: { [SECTION_ID]: [inferenceSource] },
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(address),
        { diagnostics: env.sidecarDiagnostics },
      );

      await fireMailTrigger(env, address, {
        messageId: "<standby-on-trigger-1@integration.interchange>",
        content: "First event.",
      });
      // Event 0's body completed and the section re-armed on its input after.
      let found: string | undefined;
      await waitFor(
        async () => {
          found = await findContainerRunId(handle.workflowRunRepoId);
          if (found === undefined) return false;
          const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, found);
          const completed = childCompletedAt(
            events,
            sectionBodyRunId(found, SECTION_ID, 0),
          );
          return (
            completed >= 0 &&
            events
              .slice(completed)
              .some(
                (e) =>
                  e.type === "SignalAwaited" && e.body["parkKind"] === "input",
              )
          );
        },
        { diagnostics: env.sidecarDiagnostics },
      );
      if (found === undefined) throw new Error("no container run");
      const containerRunId = found;
      const event0 = sectionBodyRunId(containerRunId, SECTION_ID, 0);
      const event1 = sectionBodyRunId(containerRunId, SECTION_ID, 1);

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
      // The container waits on its input alone: event 0's sleep has fired, so
      // no timer wakes the section on its own.
      expect(await standbyMark()).toEqual({
        parked: [
          {
            correlationId: expect.any(String),
            parkKind: "input",
            runId: containerRunId,
          },
        ],
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
        await fireMailTrigger(env, address, {
          messageId: "<standby-on-trigger-2@integration.interchange>",
          content: "Second event.",
        });
        await waitFor(
          async () =>
            childCompletedAt(
              await readWorkflowRunEvents(env, DEPLOYMENT_ID, containerRunId),
              event1,
            ) >= 0,
          { diagnostics: env.sidecarDiagnostics },
        );
        expect(await reported.promise).toEqual({
          ...target,
          inStandby: false,
          reason: "Woken by mail",
        });
        expect(await standbyMark()).toBeUndefined();
      } finally {
        unsubscribe();
      }

      const events = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        containerRunId,
      );
      const types = events.map((e) => e.type);
      // Mail 2 was the section's next event, not a run of its own.
      expect(types.filter((t) => t === "RunStarted")).toHaveLength(1);
      expect(
        events.flatMap((e) =>
          e.type === "ChildSpawned" ? [e.body["childRunId"]] : [],
        ),
      ).toEqual([event0, event1]);
      expect(types).not.toContain("RunCompleted");
      expect(types).not.toContain("RunFailed");
      expect(types).not.toContain("RunCancelled");
    });
  },
);
