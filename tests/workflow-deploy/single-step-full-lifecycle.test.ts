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
//   2. MAIL IN (4.2): an inbound mail with a KNOWN body reaches the warm
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
//   4. DURABLE CONVERSATION SNAPSHOT (4.4): the completed turn is mirrored
//      from the warm agent's conversation store into the workflow-run
//      substrate at the run boundary.
//   5. KILL + RESPAWN (4.5): the conversation RESUMES from the substrate.
//      A FRESH process (a `createDurableConversationRegistry` built
//      in-process against the subprocess's on-disk substrate, with a fresh
//      local store dir -- the strengthened 4.5 pattern) restores the warm
//      agent's prior conversation from the workflow-run substrate the
//      spawned child committed. The fresh local store starts empty, so the
//      restored turns can come ONLY from the substrate -- closing the hole
//      where a surviving local store would mask a broken restore. This is
//      a CROSS-PROCESS durability proof: the durable substrate the real
//      subprocess wrote is read back by a genuinely separate process.
//   6. EVENTS (4.1 sessionId wiring): inference events reached the hub's
//      `agent.event` sink keyed to the deploy's sessionId, across the
//      lifecycle.
//
// Harness justification. Stages 1-4 and 6 are SPAWN-REAL: a real hub, a
// real sidecar subprocess, a real workflow-process child, and a test
// inference provider. Stage 5's respawn restore is exercised in-process
// against the SAME on-disk substrate the subprocess wrote, mirroring the
// 4.5 conversation-durability test's deliberate choice: a respawn IS a
// fresh process building a fresh `createDurableConversationRegistry`
// against the durable substrate, and reading the snapshot the prior
// process committed proves continuity without the OS kill-timing
// nondeterminism a hard subprocess kill would inject into the durability
// assertion. The restore path is the REAL production wiring
// (`createDurableConversationRegistry.acquire` -> `restoreFromSubstrate`).

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto";
import { createSSHSignature } from "@intx/crypto";
import { isRunAddress } from "@intx/types";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { WireGrantRule } from "@intx/types/grant-wire";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import {
  createDurableConversationRegistry,
  reconstructDurableConversation,
} from "@intx/sidecar-app/src/conversation-state";
import {
  createAgentRepoStore,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

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
        apiKey: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
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
        timeoutMs: 20_000,
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
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // ---- STAGE 2 + 3: MAIL IN + TOOL-DRIVEN SIGNED REPLY (4.2 + 4.3) ----

      const first = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<full-lifecycle-1@integration.interchange>",
        content: FIRST_BODY,
        grants: [GRANTED_RULE],
      });

      const firstRunId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      const firstTerminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        firstRunId,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
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
      expect(firstStartedBody["consumedMessageId"]).toBe(first.messageId);

      // 4.3: the granted `mail_send` tool ran in-child and its
      // `env.transport.send` produced a SUCCESSFUL `SendReceipt` -- the tool
      // writes the sentinel ONLY after the supervisor's signed send returns
      // a receipt. A broken outbound IPC/supervisor/host-transport chain
      // would reject inside `send`, leaving no sentinel and failing the run.
      // The sentinel content is the receipt JSON, so a non-empty messageId
      // proves the signed send completed.
      const firstSentinelPath = stepWorkspaceSentinelPath(
        env,
        workflowRunRepoId.id,
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

      // The run-boundary hook mirrors the warm agent's conversation into the
      // workflow-run substrate. The stable per-agent store is keyed by stepId,
      // not by the terminal run or its attempt.
      const agentStateDir = substrateAgentStateDir(
        env,
        workflowRunRepoId.id,
        STEP_ID,
      );
      await waitFor(
        async () => (await readSnapshotUserTexts(agentStateDir)).length >= 1,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );
      const afterBoundary = await readSnapshotUserTexts(agentStateDir);
      expect(afterBoundary.some((t) => t.includes(FIRST_BODY))).toBe(true);

      // The warm agent's conversation `.git` lives at the stable per-agent
      // durable store root, NOT under any per-run `attempt-*` dir -- the
      // store that survives respawn.
      const durableConversationDir = path.join(
        env.sidecar.dataDir,
        "agent-conversation-state",
        workflowRunRepoId.id,
        encodeURIComponent(STEP_ID),
      );
      expect(fs.existsSync(path.join(durableConversationDir, ".git"))).toBe(
        true,
      );

      // ---- STAGE 5: KILL + RESPAWN -> resume from substrate (4.5) ----
      //
      // Model a fresh-process respawn: build a fresh
      // `createDurableConversationRegistry` (the REAL production wiring)
      // against the subprocess's on-disk workflow-run substrate, with a
      // BRAND-NEW local store dir. The fresh local store starts empty, so the
      // restored turns can come ONLY from the durable substrate the spawned
      // child committed -- a cross-process durability proof.
      const freshLocalDataDir = await fs.promises.mkdtemp(
        path.join(env.sidecar.dataDir, "respawn-local-"),
      );
      const freshLocalStoreDir = path.join(
        freshLocalDataDir,
        "agent-conversation-state",
        workflowRunRepoId.id,
        encodeURIComponent(STEP_ID),
      );
      // The fresh local store genuinely has no conversation on disk yet.
      expect(fs.existsSync(freshLocalStoreDir)).toBe(false);

      // Reopen the subprocess's substrate in-process. The durable registry's
      // restore path only reads the substrate working tree (`getRepoDir` +
      // `fs.readFile`), so a throwaway signing key is sufficient.
      const respawnSubstrate = createAgentRepoStore({
        dataDir: env.sidecar.dataDir,
        signingKey: await generateKeyPair(),
      }).repoStore;
      const respawnSigner = await sshSigner();
      const respawnPrincipal: WorkflowRunWorkflowProcessPrincipal = {
        kind: "workflow-process",
        anchorRunId: workflowRunRepoId.id,
      };
      const respawnRegistry = createDurableConversationRegistry({
        dataDir: freshLocalDataDir,
        workflowRunRepoId,
        workflowRunRef: WORKFLOW_RUN_REF,
        substrate: respawnSubstrate,
        principal: respawnPrincipal,
        signer: respawnSigner,
      });

      // The first acquire builds the store and restores its prior
      // conversation snapshot from the substrate -- the exact lazy-rebuild /
      // respawn-restore path the warm agent takes after a child respawn.
      const respawnStore = await respawnRegistry.acquire(STEP_ID);
      const restored = await respawnStore.storage.load();
      const restoredUserTexts = restored.turns
        .filter((t) => t.role === "user")
        .flatMap((t) =>
          t.content.map((c) => (c.type === "text" ? c.text : "")),
        );

      // THE durability dividend: the post-respawn agent's conversation
      // reflects the PRE-respawn turn. The fresh local
      // store started EMPTY, so the only possible source is the substrate
      // restore. A broken restore would have loaded zero turns.
      expect(restoredUserTexts.some((t) => t.includes(FIRST_BODY))).toBe(true);

      // The restore wrote the prior conversation into the previously-empty
      // fresh local store, confirming the restore path (not a surviving local
      // store) reconstructed continuity.
      expect(fs.existsSync(path.join(freshLocalStoreDir, ".git"))).toBe(true);

      await fs.promises.rm(freshLocalDataDir, { recursive: true, force: true });

      // ---- STAGE 6: EVENTS reached the sink with the sessionId (4.1) ----

      await waitFor(
        () =>
          env.hub.agentEvents.some(
            (e) =>
              e.addr === deploymentMailAddress &&
              e.sid === SESSION_ID &&
              isInferenceStart(e.event),
          ),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
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

/**
 * Warm single-step workspace path under the sidecar data dir where the
 * transport-backed `mail_send` tool writes its receipt sentinel. The warm
 * agent's workspace is keyed STABLY per agent
 * (`workflow-step-state/<repoId>/warm/<stepId>/workspace`), not per
 * message, so the path is independent of the run that wrote the sentinel.
 */
function stepWorkspaceSentinelPath(
  deployEnv: DeployFlowEnv,
  workflowRunRepoSlug: string,
  filename: string,
): string {
  return path.join(
    deployEnv.sidecar.dataDir,
    "workflow-step-state",
    workflowRunRepoSlug,
    "warm",
    encodeURIComponent(STEP_ID),
    "workspace",
    filename,
  );
}

/**
 * Path of the per-agent conversation snapshot the durable store mirrors to
 * the sidecar's on-disk workflow-run substrate (the supervisor's
 * single-writer substrate). Deterministic, no hub pack-push timing
 * dependency.
 */
function substrateAgentStateDir(
  deployEnv: DeployFlowEnv,
  workflowRunRepoSlug: string,
  stepId: string,
): string {
  return path.join(
    deployEnv.sidecar.dataDir,
    "workflow-runs",
    workflowRunRepoSlug,
    "agent-state",
    encodeURIComponent(stepId),
  );
}

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
 * (checkpoint + WAL) at the per-agent `agent-state/<stepId>/` dir and
 * return the user-turn texts. Goes through the production
 * `reconstructDurableConversation` so the test reads the conversation the
 * same way the warm agent's restore does.
 */
async function readSnapshotUserTexts(agentStateDir: string): Promise<string[]> {
  const reconstructed = await reconstructDurableConversation(
    agentStateDir,
    STEP_ID,
  );
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

/**
 * SSH-shaped commit signer for the in-process durable store's local isogit
 * repo. The respawn restore writes the restored turns into a fresh local
 * store and commits them; the signer attributes that commit.
 */
async function sshSigner(): Promise<(payload: string) => Promise<string>> {
  const keyPair = await generateKeyPair();
  return (payload: string) =>
    Promise.resolve(
      createSSHSignature(payload, keyPair.privateKey, keyPair.publicKey),
    );
}

function isInferenceStart(event: unknown): boolean {
  return (
    typeof event === "object" &&
    event !== null &&
    "type" in event &&
    (event as { type: unknown }).type === "inference.start"
  );
}
