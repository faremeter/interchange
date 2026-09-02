// A deployed onTrigger section whose inline body contains a `loop` with an agent
// body runs to completion (INTR-477).
//
// This proves both halves of the fix end to end. The deploy pin recurses into a
// loop nested inside a spawned (onTrigger) body to pin the loop-body agent
// step's inference source, and the runtime wires loop support (`spawnLoopIteration`
// + `loopFns`) into the spawned-body child env. Two failure modes are ruled out:
// pre-fix the deploy was rejected outright by the fail-closed guard, and even
// without that guard the body child crashed at the first iteration -- first with
// no source pinned, then with "this host does not support loops". Firing the
// trigger spawns the body child, whose loop drives its agent body against the
// mock inference fixture and converges, so the body child completes.
//
// The second case adds a `childWorkflow` grandchild to the loop body, exercising
// the spawned-body env wiring that merges a loop body's childWorkflow
// grandchildren into the body's spawn map and caps the grandchild's grants per
// iteration.
//
// Harness justification: SPAWN-REAL. Real hub, real sidecar subprocess, and a
// real workflow-process child running the onTrigger body child, whose loop
// spawns per-iteration child runs each executing the body agent through the body
// invoker.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { onTriggerLoopBodyEntry } from "./fixtures/on-trigger-loop-body";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const SECTION_ID = "section";
const BODY_CHILD_RUN_ID = `${SECTION_ID}__0`;

const TENANT_ID = "tnt_on_trigger_loop_body";
const CALLER_PRINCIPAL_ID = "prn_on_trigger_loop_body";
const PLAIN_ASSET_ID = "ast_on_trigger_loop_body_plain";
const GRANDCHILD_ASSET_ID = "ast_on_trigger_loop_body_grandchild";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: TENANT_ID,
    domain: DEPLOYMENT_DOMAIN,
    parentId: null,
  });
  await seedPrincipal(h.db, {
    id: CALLER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
  });
  for (const assetId of [PLAIN_ASSET_ID, GRANDCHILD_ASSET_ID]) {
    await seedAsset(h.db, {
      id: assetId,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: assetId,
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });
  }

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

/**
 * The container run is the single run under the deployment's workflow-run repo
 * that is NOT a body child (body children are `${SECTION_ID}__<n>`).
 */
async function findContainerRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(env, workflowRunRepoId);
  return ids.find((id) => !id.startsWith(`${SECTION_ID}__`));
}

const hasChildCompleted = (
  events: { type: string; body: Record<string, unknown> }[],
  childRunId: string,
): boolean =>
  events.some(
    (e) => e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
  );

/**
 * Deploy an onTrigger-loop-body workflow, fire its trigger, and assert the body
 * child runs its loop to completion (with real inference) while the container
 * re-arms. Pre-fix the body child would crash at the first loop iteration.
 */
async function deployAndAssertBodyCompletes(opts: {
  deploymentId: string;
  assetId: string;
  loopBodySpawnsGrandchild: boolean;
}): Promise<void> {
  const inferenceBefore = env.inference.requests.length;
  const deploymentMailAddress = deriveRunAddress({
    runId: opts.deploymentId,
    domain: DEPLOYMENT_DOMAIN,
  });

  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: opts.deploymentId,
    tenantId: "tenant-1",
    principalId: `prin_${opts.deploymentId}`,
    agentAddress: deploymentMailAddress,
    systemPrompt: "Fallback prompt (overridden per step by the definition)",
    tools: [],
    grants: [],
    sources: [
      {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
        model: "mock-model",
      },
    ],
    defaultSource: "anthropic:mock-model",
  };

  const handle = await deployWorkflowSourceForTest(env, {
    entryModule: onTriggerLoopBodyEntry({
      address: deploymentMailAddress,
      workflowId: `wf_${opts.deploymentId}`,
      loopBodySpawnsGrandchild: opts.loopBodySpawnsGrandchild,
    }),
    // The entry exports the loop fns, so point interchange.loops at the same
    // bundled entry.
    loops: "./workflow.mjs",
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId: opts.assetId,
    anchorRunId: opts.deploymentId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: deploymentMailAddress,
    approvals: "approve-probed",
    config,
    // Omit sources so the harness computes them via the real deploy pin, which
    // recurses into the loop nested in the onTrigger body -- exactly the path
    // under test.
  });
  // The deploy itself succeeding is part of the proof: pre-fix, a loop nested in
  // a spawned body was rejected at deploy by the fail-closed guard.
  expect(handle.publicKey).toBeTruthy();

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
    { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
  );

  await fireMailTrigger(env, deploymentMailAddress, {
    messageId: `<${opts.deploymentId}@integration.interchange>`,
    content: "trigger the loop body",
  });

  const containerRunId = await (async () => {
    await waitFor(
      async () =>
        (await findContainerRunId(handle.workflowRunRepoId)) !== undefined,
      { diagnostics: env.sidecarDiagnostics, timeoutMs: 30_000 },
    );
    const id = await findContainerRunId(handle.workflowRunRepoId);
    if (id === undefined) throw new Error("no container run");
    return id;
  })();

  // The body child runs the loop to completion (it converges, `settle` runs) and
  // completes. Pre-fix its loop-body agent step would crash at the first
  // iteration with no pinned source or no loop support, so it never would.
  await waitFor(
    async () => {
      const events = await readWorkflowRunEvents(
        env,
        opts.deploymentId,
        containerRunId,
      );
      return hasChildCompleted(events, BODY_CHILD_RUN_ID);
    },
    { diagnostics: env.sidecarDiagnostics, timeoutMs: 60_000 },
  );

  // The loop body's agent step actually reached the mock provider: the reply is
  // real model output resolved against the pinned source, not a crash.
  expect(env.inference.requests.length).toBeGreaterThan(inferenceBefore);

  // The section re-arms for the next event and never self-completes -- the
  // nested loop running didn't disturb the long-lived container.
  const containerEvents = await readWorkflowRunEvents(
    env,
    opts.deploymentId,
    containerRunId,
  );
  const containerTypes = containerEvents.map((e) => e.type);
  expect(containerTypes.filter((t) => t === "RunStarted").length).toBe(1);
  expect(containerTypes).not.toContain("RunCompleted");
  expect(containerTypes).not.toContain("RunFailed");
  expect(containerTypes).not.toContain("RunCancelled");
}

describe.skipIf(!harnessDbEnvAvailable())(
  "an onTrigger body containing a loop with an agent body runs to completion",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a loop with an agent body pins its source and the body child completes", async () => {
      await deployAndAssertBodyCompletes({
        deploymentId: "run_on-trigger-loop-body-1",
        assetId: PLAIN_ASSET_ID,
        loopBodySpawnsGrandchild: false,
      });
    }, 120_000);

    test("a loop body that spawns a childWorkflow grandchild runs to completion", async () => {
      await deployAndAssertBodyCompletes({
        deploymentId: "run_on-trigger-loop-body-2",
        assetId: GRANDCHILD_ASSET_ID,
        loopBodySpawnsGrandchild: true,
      });
    }, 120_000);
  },
);
