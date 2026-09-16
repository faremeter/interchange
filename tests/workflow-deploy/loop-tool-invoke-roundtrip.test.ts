// An agent step inside a `loop` body INVOKES a real tool, and the tool's real
// output lands in its turn.
//
// The sibling `loop-roundtrip.test.ts` proves a deployed loop runs its body end
// to end, but its body agent declares `tools: []` -- as does every other
// deployed loop fixture -- so the body never reaches the tool-invocation
// authorize seam. This proves the stronger property, the one
// `child-workflow-tool-invoke-roundtrip.test.ts` already proves for a spawned
// child: the body's real agent CALLS the tool and feeds the result back into a
// follow-up turn -- the real `tool_use` -> execute -> `tool_result` -> reply
// round-trip. The mock provider is configured to drive a `tool_use` on the
// first request of every run that exposes the tool, so the body runs the inline
// `mail_send` tool for real and re-inferences with its result.
//
// The assertion reads the tool_result's TEXT, not merely its presence. A tool
// call the authorize seam blocks still appends a tool_result -- an error one,
// carrying the throw's message -- and the agent then replies normally, so the
// run completes and a presence-only check passes while nothing ever ran. Only
// the tool's own return value (`wrote <filename>`) proves the body executed it.
//
// The env runs exactly one workflow, and only the loop body's step carries a
// tool. The two top-level agent steps can produce no tool_result at all, so a
// tool_result in ANY captured request can only have originated in the loop body.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, and a real workflow-process child evaluating the deployed source.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { loopBodyRunId } from "@intx/workflow";
import { loadFrozenGrantSnapshot } from "@intx/db";
import type { GrantEffect, GrantWalkSnapshot } from "@intx/types";
import type { WireGrantRule } from "@intx/types/grant-wire";
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
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type InferenceRequest,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import {
  LOOP_BODY_STEP_ID,
  LOOP_STEP_ID,
  TOP_LEVEL_STEP_IDS,
  loopToolInvokeWorkflowEntry,
} from "./fixtures/loop-tool-invoke-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_loop-tool-invoke-1";
const WORKFLOW_ID = `wf_${DEPLOYMENT_ID}`;

const TENANT_ID = "tnt_loop_tool_invoke";
const CALLER_PRINCIPAL_ID = "prn_loop_tool_invoke";
const DEFINITION_ASSET_ID = "ast_loop_tool_invoke_wf";

// The filename the driven tool call writes under the step workdir, and the
// content the "fs" variant of `mail_send` returns for it. Matching this exact
// string is what distinguishes a real execution from an error tool_result.
const TOOL_OUTPUT_FILENAME = "loop-body-invoked.txt";
const EXPECTED_TOOL_RESULT = `wrote ${TOOL_OUTPUT_FILENAME}`;

// The loop converges after exactly two iterations (see the fixture), so the
// container run log carries one ChildSpawned per iteration.
const EXPECTED_ITERATIONS = 2;

/**
 * Project a frozen grant-walk snapshot into the run's runtime `tool:`/`effect:`
 * grant rows, in the `run.grants` wire shape the trigger delivers. Mirrors the
 * production `deriveRunRuntimeGrantRows`/`runGrantToWire` tail (not exported
 * from `@intx/hub-api`): one row per distinct grant across steps, tool effect
 * taken from the step's `grantEffects` with `ask` winning over `allow`, effect
 * grants always `allow`. The rows are principal-agnostic (`principalId: null`),
 * matched by resource + action at the child's grant evaluator.
 */
function deriveWireRunGrants(snapshot: GrantWalkSnapshot): WireGrantRule[] {
  const effectByResource = new Map<string, GrantEffect>();
  for (const step of snapshot.perStep) {
    const grantEffects = new Map<string, GrantEffect>(
      Object.entries(step.grantEffects),
    );
    for (const grant of step.grants) {
      if (grant.startsWith("tool:")) {
        const effect = grantEffects.get(grant);
        if (effect === undefined) {
          throw new Error(
            `deriveWireRunGrants: tool grant ${JSON.stringify(grant)} has no grantEffects entry`,
          );
        }
        const existing = effectByResource.get(grant);
        if (existing === "ask" || effect === "ask") {
          effectByResource.set(grant, "ask");
        } else if (existing === undefined) {
          effectByResource.set(grant, effect);
        }
      } else if (grant.startsWith("effect:") && !effectByResource.has(grant)) {
        effectByResource.set(grant, "allow");
      }
    }
  }
  return [...effectByResource].map(([resource, effect]) => ({
    id: `run-grant:${resource}`,
    resource,
    action: "invoke",
    effect,
    origin: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
  }));
}

// Flatten the text of every `tool_result` block in an inference request. The
// Anthropic adapter serializes a result as a user-turn content block whose own
// `content` is either a bare string or an array of `{ type: "text", text }`.
function toolResultTexts(req: InferenceRequest): string[] {
  const texts: string[] = [];
  for (const message of req.messages ?? []) {
    const content = message.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (block.type !== "tool_result") continue;
      const inner = block.content;
      if (typeof inner === "string") {
        texts.push(inner);
        continue;
      }
      if (Array.isArray(inner)) {
        texts.push(
          inner
            .filter((b) => b.type === "text" && b.text !== undefined)
            .map((b) => b.text ?? "")
            .join(""),
        );
      }
    }
  }
  return texts;
}

// Every failure event in a run log, rendered with its full body. A step that
// throws records the thrown message under `error.message`, so an assertion on
// this list reports the underlying failure rather than a bare count mismatch.
function failureMessages(events: readonly WorkflowRunEvent[]): string[] {
  return events
    .filter((e) => e.type === "StepFailed" || e.type === "RunFailed")
    .map((e) => `${e.type} ${JSON.stringify(e.body)}`);
}

// The top-level container run: the loop's per-iteration body runs are keyed
// `<runId>__<loopStepId>__<index>`, so the container is the only run id with no
// `__` separator.
async function findContainerRunId(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  const ids = await listRunIds(env, workflowRunRepoId);
  return ids.find((id) => !id.includes("__"));
}

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  // A file-scope beforeAll fires even when describe.skipIf skips the suite
  // bodies, so it needs its own guard or a missing DB env throws here.
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
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "loop-tool-invoke-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  // Drive the inline `mail_send` tool on the first request that exposes it, on
  // every run. Each loop iteration is its own agent conversation, so
  // `inferenceToolCallEachRun` is what makes every iteration drive the tool
  // rather than only the first. The "fs" variant writes a file under the step
  // workdir with no env requirement, so the call runs for real in the body.
  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: "loop body tool ran", body: TOOL_OUTPUT_FILENAME },
    },
    inferenceToolCallEachRun: true,
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "a loop body's agent step invokes a real tool",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    // The per-step credentials, source, and deploy-tree tables are keyed by
    // base step id in one namespace shared by the top level and the loop body,
    // so a colliding body step id would resolve to the top-level step's entry
    // and make the round-trip below pass against the wrong step.
    test("the loop body step id collides with no top-level step id", () => {
      expect(TOP_LEVEL_STEP_IDS).not.toContain(LOOP_BODY_STEP_ID);
    });

    test("the body agent calls its inline tool and the real result lands in a turn", async () => {
      const deploymentMailAddress = deriveRunAddress({
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
        principalId: `prin_${DEPLOYMENT_ID}`,
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        // The loop body's step declares the inline tool, so the deploy walk
        // folds its grant into the top level; the operator must approve it.
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: loopToolInvokeWorkflowEntry({
          address: deploymentMailAddress,
          workflowId: WORKFLOW_ID,
        }),
        // The entry module exports both `workflow` and the loop fns, so point
        // interchange.loops at the same bundled entry.
        loops: "./workflow.mjs",
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        // Omit `sources` so the harness computes them via the real source pin,
        // which recurses into the loop body and pins the body step too.
      });
      expect(handle.publicKey).toBeTruthy();

      const workflowRunRepoId: RepoId = handle.workflowRunRepoId;

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // The loop body's tool grant reaches the run the way production delivers
      // it: the deploy walk folds the body step's `tool:` grant into the frozen
      // snapshot, and the trigger route projects that snapshot into the run's
      // grant rows. `fireMailTrigger` is the router-level helper, so unlike the
      // production route it does not materialize the rows itself -- deriving
      // them from the frozen snapshot here reproduces the delivery, and keeps
      // the grant the body authorizes against sourced from the probe walk
      // rather than a hand-authored constant. A source-ref deploy has no
      // tool-mark floor to fall back on (a source tool's runtime name is the
      // bare `definition.name` the walk already emitted a grant for), so this
      // IS the body's only authority.
      if (!handle.approved.approval.ok) {
        throw new Error("expected an approved definition");
      }
      const snapshot = await loadFrozenGrantSnapshot(
        h.db,
        handle.approved.approval.definitionId,
      );
      if (snapshot === null) {
        throw new Error("expected a frozen grant snapshot for the definition");
      }
      expect(
        snapshot.perStep.flatMap((s) =>
          s.grants.filter((g) => g.startsWith("tool:")),
        ),
      ).toContain(`tool:${MAIL_TOOL_NAME}`);

      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: `<${DEPLOYMENT_ID}@integration.interchange>`,
        content: "run the tool-bearing loop body",
        grants: deriveWireRunGrants(snapshot),
      });

      await waitFor(
        async () => (await findContainerRunId(workflowRunRepoId)) !== undefined,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      const runId = await findContainerRunId(workflowRunRepoId);
      if (runId === undefined) throw new Error("unreachable");

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
      );

      // Read the body run's own log first: a body step's failure is recorded
      // there, and the container only sees the iteration's terminal status.
      const bodyRunId = loopBodyRunId(runId, LOOP_STEP_ID, 0);
      expect(
        failureMessages(
          await readWorkflowRunEvents(env, DEPLOYMENT_ID, bodyRunId),
        ),
      ).toEqual([]);

      const containerEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        runId,
      );
      expect(failureMessages(containerEvents)).toEqual([]);
      expect(terminal.type).toBe("RunCompleted");

      // The loop ran its body once per iteration. All child spawns in this run
      // are loop iterations: the workflow has no onTrigger or childWorkflow
      // primitive.
      const loopSpawns = containerEvents.filter(
        (e) => e.type === "ChildSpawned",
      ).length;
      expect(loopSpawns).toBe(EXPECTED_ITERATIONS);

      // The body's agent executed the tool and re-inferenced with its REAL
      // output. Every top-level step is toolless, so this result can only be
      // the loop body's invocation landing in a turn. Asserting on the text
      // rather than the block's presence is what rejects an error tool_result
      // carrying an authorize failure, which the agent would answer normally
      // and let the run complete.
      const results = env.inference.requests.flatMap(toolResultTexts);
      expect(results).toContain(EXPECTED_TOOL_RESULT);
    }, 180_000);
  },
);
