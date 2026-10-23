// The environment the body-action restart tests share: a deployment of the
// `body-action-restart-workflow` fixture on a real hub and sidecar, fired once
// with the run grants its approved snapshot derives, and the readers the tests
// assert with. Each body action appends its handler name to a marker file in a
// fresh temp directory, so the marker lines count handler effects from outside
// the runtime.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { expect } from "bun:test";

import { tenant as tenantTable } from "@intx/db/schema";
import type { RepoId } from "@intx/hub-sessions";
import type { HarnessConfig } from "@intx/types/runtime";
import { loopBodyRunId, sectionBodyRunId } from "@intx/workflow";
import { deriveRunAddress } from "@intx/workflow-deploy";
import { createTestDb, type TestDb } from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import {
  CHILD_MARK_HANDLER,
  CHILD_STEP_ID,
  LOOP_MARK_HANDLER,
  LOOP_STEP_ID,
  MARK_CAPABILITY,
  SECTION_MARK_HANDLER,
  SECTION_STEP_ID,
  bodyActionRestartEntry,
  type MARK_HANDLERS,
} from "./fixtures/body-action-restart-workflow";
import { deriveWireRunGrants } from "./nested-tool-invoke-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";

export type MarkHandler = (typeof MARK_HANDLERS)[number];

export type BodyActionRestartEnv = {
  env: DeployFlowEnv;
  deploymentId: string;
  mailAddress: string;
  /** The top-level run, the one that spawns the `childWorkflow` body. */
  runId: string;
  /** The handler names the effects appended, one per line, in order. */
  markerLines(): string[];
  /** The log of the body run whose action `handler` names. */
  bodyEvents(handler: MarkHandler): Promise<WorkflowRunEvent[]>;
  teardown(): Promise<void>;
};

/**
 * Deploy the fixture, fire its trigger once, and wait for the top-level run to
 * spawn its `childWorkflow` body. `name` keeps each test file's tenant, asset,
 * and deployment ids apart.
 */
export async function startBodyActionRestartEnv(opts: {
  name: string;
  block: boolean;
}): Promise<BodyActionRestartEnv> {
  const tenantId = `tnt_body_action_${opts.name}`;
  const callerPrincipalId = `prn_body_action_${opts.name}`;
  const definitionAssetId = `ast_body_action_${opts.name}`;
  const deploymentId = `run_body-action-${opts.name}`;

  const markerDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "body-action-marker-"),
  );
  const markerPath = path.join(markerDir, "marker");

  let h: TestDb | undefined;
  let env: DeployFlowEnv | undefined;
  const teardown = async () => {
    if (env !== undefined) await env.teardown();
    if (h !== undefined) await h.close();
    await fs.promises.rm(markerDir, { recursive: true, force: true });
  };

  try {
    h = await createTestDb();
    await h.db.insert(tenantTable).values({
      id: tenantId,
      name: tenantId,
      slug: tenantSlugFromId(tenantId),
      domain: DEPLOYMENT_DOMAIN,
      parentId: null,
    });
    await seedPrincipal(h.db, {
      id: callerPrincipalId,
      tenantId,
      kind: "user",
    });
    await seedAsset(h.db, {
      id: definitionAssetId,
      tenantId,
      kind: "workflow",
      name: definitionAssetId,
      creatorPrincipalId: callerPrincipalId,
    });

    const startedEnv = await startDeployFlowEnv();
    env = startedEnv;

    const mailAddress = deriveRunAddress({
      runId: deploymentId,
      domain: DEPLOYMENT_DOMAIN,
    });
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: deploymentId,
      tenantId: "tenant-1",
      principalId: `prin_${deploymentId}`,
      agentAddress: mailAddress,
      systemPrompt: "unused (no agent step)",
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${String(startedEnv.inference.server.port)}`,
          credentialId: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:mock-model",
    };

    const handle = await deployWorkflowSourceForTest(startedEnv, {
      entryModule: bodyActionRestartEntry({
        address: mailAddress,
        workflowId: `wf_body_action_${opts.name}`,
        markerPath,
        block: opts.block,
      }),
      actions: "./workflow.mjs",
      loops: "./workflow.mjs",
      db: h.db,
      tenantId,
      definitionAssetId,
      anchorRunId: deploymentId,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      agentAddress: mailAddress,
      approvals: "approve-probed",
      config,
    });
    const snapshot = handle.approved.grantSnapshot;
    // Without the effect grant each body's capped grants allow no effect, and
    // `perform` fails closed before the marker line is written.
    expect(snapshot.perStep.flatMap((step) => step.grants)).toContain(
      `effect:${MARK_CAPABILITY}`,
    );
    const workflowRunRepoId = handle.workflowRunRepoId;

    await waitFor(
      () => startedEnv.hub.router.getRoutableAddresses().includes(mailAddress),
      { diagnostics: startedEnv.sidecarDiagnostics },
    );

    await fireMailTrigger(startedEnv, mailAddress, {
      messageId: `<${deploymentId}@integration.interchange>`,
      grants: deriveWireRunGrants(snapshot),
    });

    const spawned = await waitForChildSpawned(
      startedEnv,
      deploymentId,
      workflowRunRepoId,
    );

    const bodyRunIds: Record<MarkHandler, string> = {
      [CHILD_MARK_HANDLER]: spawned.childRunId,
      [LOOP_MARK_HANDLER]: loopBodyRunId(spawned.runId, LOOP_STEP_ID, 0),
      [SECTION_MARK_HANDLER]: sectionBodyRunId(
        spawned.runId,
        SECTION_STEP_ID,
        0,
      ),
    };
    return {
      env: startedEnv,
      deploymentId,
      mailAddress,
      runId: spawned.runId,
      markerLines: () => readMarkerLines(markerPath),
      bodyEvents: (handler) =>
        readWorkflowRunEvents(startedEnv, deploymentId, bodyRunIds[handler]),
      teardown,
    };
  } catch (cause) {
    await teardown();
    throw cause;
  }
}

/** The events of `stepId` with the type `type` in a body run's log. */
export function stepEvents(
  events: readonly WorkflowRunEvent[],
  type: string,
  stepId: string,
): WorkflowRunEvent[] {
  return events.filter(
    (event) => event.type === type && event.body["stepId"] === stepId,
  );
}

function readMarkerLines(markerPath: string): string[] {
  if (!fs.existsSync(markerPath)) return [];
  return fs
    .readFileSync(markerPath, "utf8")
    .split("\n")
    .filter((line) => line !== "");
}

// The `childWorkflow` body run id is random, and only the top-level run's
// `ChildSpawned` for the spawn step names it. The loop and the section record
// their bodies' `ChildSpawned` on the same run under their own steps, so the
// spawn step id picks the `childWorkflow` body out of the three.
async function waitForChildSpawned(
  env: DeployFlowEnv,
  deploymentId: string,
  workflowRunRepoId: RepoId,
): Promise<{ runId: string; childRunId: string }> {
  let found: { runId: string; childRunId: string } | undefined;
  await waitFor(
    async () => {
      for (const id of await listRunIds(env, workflowRunRepoId)) {
        const events = await readWorkflowRunEvents(env, deploymentId, id);
        const childRunId = events.find(
          (event) =>
            event.type === "ChildSpawned" &&
            event.body["stepId"] === CHILD_STEP_ID,
        )?.body["childRunId"];
        if (typeof childRunId === "string") {
          found = { runId: id, childRunId };
          return true;
        }
      }
      return false;
    },
    { diagnostics: env.sidecarDiagnostics },
  );
  if (found === undefined) {
    throw new Error("no run spawned the childWorkflow body");
  }
  return found;
}
