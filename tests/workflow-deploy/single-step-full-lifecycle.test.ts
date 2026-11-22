// Phase 4.6 END-TO-END MILESTONE -- the full single-agent lifecycle on
// the unified child path, composing everything 4.1-4.5 built in ONE
// spawned-child flow.
//
// This is the integration proof that the unified single-agent path runs
// end-to-end: no new production code beyond wiring the prior sub-steps
// together behind the source-ref deploy path, and the in-process runtime
// stays present and untouched.
//
// The lifecycle stages, each asserting a prior sub-step composes with the
// rest:
//
//   1. DEPLOY (4.1): a single agent (a `run_<hex>` run identity) with
//      grants + a mail-send tool, through the spawned workflow-process
//      child (the source-ref deploy path). Identity preserved (deploy-ack
//      for the run's `run_<hex>` address); grants enforced (the granted tool
//      runs in-child).
//   2. MAIL IN (4.2): an inbound mail with a KNOWN body reaches the step
//      agent's `agent.send` as the step input -- proven by the
//      tool-driven reply round-trip completing for that message's run.
//   3. TOOL-DRIVEN SIGNED REPLY OUT (4.3): the agent's model turn calls
//      the `mail_send` tool; the tool's `env.transport.send` routes
//      through the supervisor-backed transport -> outbound bridge ->
//      `outbound.message` IPC -> supervisor `sendOutbound` -> host
//      transport SIGNED send -> `SendReceipt`. The tool writes its
//      workspace sentinel ONLY on a successful receipt, so the sentinel is
//      a load-bearing proof the outbound signed-send composed end-to-end
//      across the real OS process boundary. (The fixture's inline `mail_send`
//      tool in its transport variant replaces the filesystem-only variant for
//      this test.)
//   4. DURABLE CONVERSATION SNAPSHOT (4.4): the completed turn is committed
//      from the step's conversation store into the workflow-run substrate
//      once the turn settles.
//   5. KILL + RESPAWN (4.5): the state a respawn restores is durable. A
//      FRESH process reads the step's committed state from the
//      subprocess's on-disk substrate with the production `readStepState`,
//      which every turn's build restores from. The state carries the turn
//      and the stamp of the invocation that committed it. This is a
//      CROSS-PROCESS durability proof: the durable substrate the real
//      subprocess wrote is read back by a genuinely separate process. The
//      stage runs no second turn, so it does not prove a resume; the
//      conversation-durability test does.
//   6. EVENTS (4.1 sessionId wiring): inference events reached the hub's
//      `agent.event` sink keyed to the deploy's sessionId, across the
//      lifecycle.
//
// Harness justification. Stages 1-4 and 6 are SPAWN-REAL: a real hub, a
// real sidecar subprocess, a real workflow-process child, and a test
// inference provider. Stage 5's respawn read is exercised in-process
// against the SAME on-disk substrate the subprocess wrote: a respawn IS a
// fresh process reading the durable substrate, and reading the state the
// prior process committed proves continuity without the OS kill-timing
// nondeterminism a hard subprocess kill would inject into the durability
// assertion.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto";
import { isRunAddress } from "@intx/types";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { WireGrantRule } from "@intx/types/grant-wire";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import { readStepState } from "@intx/workflow-host";
import { stepWorkspaceDir } from "@intx/workflow-host/child";
import {
  createAgentRepoStore,
  type RepoId,
  type RepoStore,
} from "@intx/hub-sessions";
import { tenant as tenantTable } from "@intx/db/schema";
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
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { MAIL_TOOL_NAME } from "./fixtures/mail-tool";
import { singleStepMailToolEntry } from "./fixtures/single-step-mail-tool";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A single-agent run id: `run_` + a hex-shaped local part. The
// deploy address `run_<id>@<domain>` is the run's own top-level
// address, not a per-step derived address.
const INSTANCE_LOCAL = "run_cafef00dba5eba11cafef00dba5eba11";
const DEPLOYMENT_ID = INSTANCE_LOCAL;
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";
const AGENT_ID = "agent-lifecycle";

const FIRST_BODY = "First inbound body lifecycle-alpha-7731.";

const GRANTED_RESOURCE = `tool:${MAIL_TOOL_NAME}`;

const SENTINEL_FILENAME_FIRST = "lifecycle-reply-1.txt";

// The run's tool grant, delivered per run via the `run.grants` frame the
// trigger sends. `fireMailTrigger` does not materialize run grants itself the
// way the production route does, so feeding the tool grant here reproduces the
// per-run delivery; the child's authorize resolves the tool call against it.
const GRANTED_RULE: WireGrantRule = {
  id: "grant-tool-invoke",
  resource: GRANTED_RESOURCE,
  action: "invoke",
  effect: "allow",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition
// projects over. The install/approve freeze and the anchor `workflow_run`
// insert both write against these, so they must exist in the real DB before
// the deploy runs.
const TENANT_ID = "tnt_single_step_full_lifecycle";
const CALLER_PRINCIPAL_ID = "prn_single_step_full_lifecycle";
const DEFINITION_ASSET_ID = "ast_single_step_full_lifecycle_wf";

let env: DeployFlowEnv;
let h: TestDb;
let deploymentMailAddress: string;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  // The inline `mail_send` tool in its transport variant calls
  // `env.transport.send` (the supervisor-backed transport) and sentinels only
  // on a successful receipt -- the OUTBOUND signed-send proof.
  // `inferenceToolCall` drives the model to call `mail_send` on the first
  // request that exposes it; the tool replies to the agent's own deployment
  // address (a local, registered recipient on the sidecar's transport) so the
  // supervisor's signed send delivers without a remote leg.
  deploymentMailAddress = deriveRunAddress({
    runId: DEPLOYMENT_ID,
    domain: DEPLOYMENT_DOMAIN,
  });

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
    name: "single-step-full-lifecycle-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: MAIL_TOOL_NAME,
      input: { to: deploymentMailAddress, body: SENTINEL_FILENAME_FIRST },
    },
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "single-step full lifecycle on the unified child path (Phase 4.6)",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("deploy -> mail -> signed tool reply -> durable snapshot -> respawn restore -> events", async () => {
      // ---- STAGE 1: DEPLOY (4.1: identity + grants) ----

      // Precondition: the deployment address is the launched-agent identity
      // shape, not a workflow-derived address.
      expect(isRunAddress(deploymentMailAddress)).toBe(true);

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${env.inference.server.port}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentAddress: deploymentMailAddress,
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const operatorApprovals: ApprovalSet = createApprovalSet([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `tool:${MAIL_TOOL_NAME}`,
      ]);

      const entryModule = singleStepMailToolEntry({
        variant: "transport",
        stepId: STEP_ID,
        systemPrompt: "You are the single-step launched agent.",
        address: deploymentMailAddress,
        agentId: AGENT_ID,
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: operatorApprovals,
        config,
        sources: { [STEP_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      // 4.1 identity preserved: the deploy-ack fired for the run's
      // `run_<hex>` address and persisted a public key.
      await waitFor(() => env.hub.deployAcks.has(deploymentMailAddress), {
        diagnostics: env.sidecarDiagnostics,
      });
      const ackKey = env.hub.deployAcks.get(deploymentMailAddress);
      expect(typeof ackKey).toBe("string");
      expect((ackKey ?? "").length).toBeGreaterThan(0);

      const workflowRunRepoId = handle.workflowRunRepoId;

      // The source-ref frame round-trips through the real sidecar subprocess
      // (index the pack, check out the pinned subtree, register the address),
      // so routability is asynchronous. Wait for it before firing the trigger.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );

      // ---- STAGE 2 + 3: MAIL IN + TOOL-DRIVEN SIGNED REPLY (4.2 + 4.3) ----

      const first = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<full-lifecycle-1@integration.interchange>",
        content: FIRST_BODY,
        grants: [GRANTED_RULE],
      });

      const firstRunId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
      });

      const firstTerminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        firstRunId,
        { diagnostics: env.sidecarDiagnostics },
      );
      if (firstTerminal.type !== "RunCompleted") {
        const events = await readWorkflowRunEvents(
          env,
          DEPLOYMENT_ID,
          firstRunId,
        );
        const failed = events.find(
          (e) => e.type === "StepFailed" || e.type === "RunFailed",
        );
        throw new Error(
          `stage 2/3: expected RunCompleted, got ${firstTerminal.type}: ${JSON.stringify(failed?.body)}\n${env.sidecarDiagnostics()}`,
        );
      }

      const firstEvents = await readWorkflowRunEvents(
        env,
        DEPLOYMENT_ID,
        firstRunId,
      );
      const firstStartedBody = firstEvents.find(
        (e) => e.type === "RunStarted",
      )?.body;
      if (firstStartedBody === undefined) throw new Error("missing RunStarted");
      // 4.2: the run consumed the mail this test fired.
      expect(firstStartedBody["consumedDeliveryKey"]).toBe(first.deliveryKey);

      // 4.3: the granted `mail_send` tool ran in-child and its
      // `env.transport.send` produced a SUCCESSFUL `SendReceipt` -- the tool
      // writes the sentinel ONLY after the supervisor's signed send returns
      // a receipt. A broken outbound IPC/supervisor/host-transport chain
      // would reject inside `send`, leaving no sentinel and failing the run.
      // The sentinel content is the receipt JSON, so a non-empty messageId
      // proves the signed send completed.
      // The step's first attempt in the run wrote it into that attempt's
      // workspace.
      const firstSentinelPath = path.join(
        stepWorkspaceDir({
          dataDir: env.sidecar.dataDir,
          workflowRunRepoId,
          runId: firstRunId,
          stepId: STEP_ID,
          attempt: 1,
        }),
        SENTINEL_FILENAME_FIRST,
      );
      if (!fs.existsSync(firstSentinelPath)) {
        throw new Error(
          `stage 3: outbound signed-send sentinel ${firstSentinelPath} was not written; the tool's env.transport.send did not produce a receipt\n${env.sidecarDiagnostics()}`,
        );
      }
      const firstReceipt = readReceiptSentinel(firstSentinelPath);
      expect(firstReceipt.messageId.length).toBeGreaterThan(0);

      // ---- STAGE 4: DURABLE CONVERSATION SNAPSHOT (4.4) ----

      // The turn's settle commits the step's conversation into the step's
      // state directory under the deployment's top-level run, keyed by
      // stepId rather than by the attempt. Read it from the sidecar's
      // on-disk workflow-run substrate (the supervisor's single-writer
      // substrate): deterministic, no hub pack-push timing dependency.
      const sidecarSubstrate = createAgentRepoStore({
        dataDir: env.sidecar.dataDir,
        signingKey: await generateKeyPair(),
      }).repoStore;
      await waitFor(
        async () =>
          (await readSnapshotUserTexts(sidecarSubstrate, workflowRunRepoId))
            .length >= 1,
        { diagnostics: env.sidecarDiagnostics },
      );
      const afterBoundary = await readSnapshotUserTexts(
        sidecarSubstrate,
        workflowRunRepoId,
      );
      expect(afterBoundary.some((t) => t.includes(FIRST_BODY))).toBe(true);

      // ---- STAGE 5: KILL + RESPAWN -> resume from substrate (4.5) ----
      //
      // Model a fresh-process respawn: reopen the subprocess's on-disk
      // workflow-run substrate in-process and read the step's committed
      // state the way every turn's build restores it. Only the substrate
      // holds it, so the read is a cross-process durability proof. A
      // throwaway signing key is enough, because the read writes nothing.
      const respawnSubstrate = createAgentRepoStore({
        dataDir: env.sidecar.dataDir,
        signingKey: await generateKeyPair(),
      }).repoStore;
      const restored = await readStepState({
        substrate: respawnSubstrate,
        workflowRunRepoId,
        workflowRunRef: WORKFLOW_RUN_REF,
        principal: { kind: "hub" },
        runId: DEPLOYMENT_ID,
        stepId: STEP_ID,
      });
      if (restored === null) {
        throw new Error("stage 5: the step committed no state");
      }
      const restoredUserTexts = restored.turns
        .filter((t) => t.role === "user")
        .flatMap((t) =>
          t.content.map((c) => (c.type === "text" ? c.text : "")),
        );

      // THE durability dividend: the substrate holds the PRE-respawn turn
      // that the next turn's build restores.
      expect(restoredUserTexts.some((t) => t.includes(FIRST_BODY))).toBe(true);
      // The invocation that committed the turn stamped the state, so the
      // next invocation of the step continues it.
      expect(restored.invocation).toEqual({
        runId: firstRunId,
        stepId: STEP_ID,
        attempt: 1,
      });

      // ---- STAGE 6: EVENTS reached the sink with the sessionId (4.1) ----

      await waitFor(
        () =>
          env.hub.agentEvents.some(
            (e) =>
              e.addr === deploymentMailAddress &&
              e.sid === SESSION_ID &&
              isInferenceStart(e.event),
          ),
        { diagnostics: env.sidecarDiagnostics },
      );
      const inferenceStart = env.hub.agentEvents.find(
        (e) =>
          e.addr === deploymentMailAddress &&
          e.sid === SESSION_ID &&
          isInferenceStart(e.event),
      );
      expect(inferenceStart).toBeDefined();
      expect(inferenceStart?.sid).toBe(SESSION_ID);
    });
  },
);

const ReceiptSentinel = type({
  messageId: "string",
  status: "string",
});

/** The receipt JSON the transport-backed tool writes on a successful send. */
function readReceiptSentinel(p: string): {
  messageId: string;
  status: string;
} {
  const raw = fs.readFileSync(p, "utf8");
  const parsed: unknown = JSON.parse(raw);
  const validated = ReceiptSentinel(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `receipt sentinel failed validation: ${validated.summary}; raw=${raw}`,
    );
  }
  return validated;
}

const TurnShape = type({
  role: "string",
  content: type({ type: "string", "text?": "string" }).array(),
});

/**
 * Reconstruct the durable conversation from the two-tier substrate layout
 * (checkpoint + WAL) in the step's state directory and return the
 * user-turn texts. Goes through the production `readStepState` so the test
 * reads the conversation the same way a step's restore does.
 */
async function readSnapshotUserTexts(
  substrate: RepoStore,
  workflowRunRepoId: RepoId,
): Promise<string[]> {
  const reconstructed = await readStepState({
    substrate,
    workflowRunRepoId,
    workflowRunRef: WORKFLOW_RUN_REF,
    principal: { kind: "hub" },
    runId: DEPLOYMENT_ID,
    stepId: STEP_ID,
  });
  if (reconstructed === null) return [];
  const texts: string[] = [];
  for (const rawTurn of reconstructed.turns) {
    const turn = TurnShape(rawTurn);
    if (turn instanceof type.errors) {
      throw new Error(`reconstructed turn failed validation: ${turn.summary}`);
    }
    if (turn.role !== "user") continue;
    texts.push(turn.content.map((c) => c.text ?? "").join(""));
  }
  return texts;
}

function isInferenceStart(event: unknown): boolean {
  return (
    typeof event === "object" &&
    event !== null &&
    "type" in event &&
    (event as { type: unknown }).type === "inference.start"
  );
}
