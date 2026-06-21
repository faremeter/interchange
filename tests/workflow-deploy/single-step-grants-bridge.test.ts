// Phase 4.1 lock: a single-step launched agent routed through the
// spawned workflow-process child resolves its GRANTS from the legacy
// agent-state repo, preserves its `ins_<hex>` identity, and threads its
// inference events to the hub timeline keyed to the deploy's session.
//
// This is the foundation sub-step every later Phase 4 step keys off: if
// grants resolve EMPTY the child's authorize fails closed on every
// resource and every tool-using agent silently stops working. The test
// is therefore written to FAIL if the grants bridge is removed (the
// granted tool's authorize would deny, the run would not complete, and
// the credentials snapshot would read back empty).
//
// The deploy is a one-step workflow whose deployment mail address is a
// legacy launched-agent address (`ins_<id>@<domain>`). The sidecar's
// deploy router recognizes the single-step projection
// (`stepOrder.length === 1`) and applies the launched-agent identity
// strategy: the sole step's grants live in the legacy agent-state repo
// keyed by `parseAgentId(legacyAddress)`, and the grants bridge writes
// `config.grants` there before the child spawns.
//
// Assertions:
//   (a) identity: the deploy-ack persisted the public key for the legacy
//       `ins_<hex>` address, and `isWorkflowDerivedAddress(legacy)` is
//       false (the address never collapses to the `ins_dep_` family).
//   (b) grants resolve: `assembleCredentialsSnapshot` (the exact call the
//       supervisor runs) reads the granted rule back from the legacy
//       agent-state repo's `state/grants.json`.
//   (c) authorize round-trip: `evaluateGrants` (the exact evaluator the
//       child's authorize adapter uses) ALLOWS the granted resource and
//       FAILS CLOSED on an ungranted one. The behavioral half drives a
//       mail message whose model turn calls the granted tool: the tool's
//       authorize succeeds in the child, the tool runs, and the run
//       reaches `RunCompleted`.
//   (d) events: an `inference.start` reaches the hub's `agent.event` sink
//       carrying the deploy's sessionId.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import { type } from "arktype";

import { evaluateGrants } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import { WireGrantRule } from "@intx/types/grant-wire";
import type { HarnessConfig } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  isWorkflowDerivedAddress,
  type ApprovalSet,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveTrivialDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import { generateKeyPair } from "@intx/crypto-node";
import {
  assembleCredentialsSnapshot,
  type CredentialsSnapshot,
} from "@intx/workflow-host";
import {
  createAgentRepoStore,
  parseAgentId,
  type RepoId,
  type WorkflowRunHubPrincipal,
} from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import {
  SESSION_ID,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A launched-agent instance id: `ins_` + a hex-shaped local part. The
// deployment address `ins_<id>@<domain>` is the legacy `ins_<hex>`
// identity the agent-launch path mints; it is NOT a workflow-derived
// `ins_dep_<...>` address, so identity preservation is exercised.
const INSTANCE_LOCAL = "deadbeefcafe0001deadbeefcafe0002";
const DEPLOYMENT_ID = INSTANCE_LOCAL;
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

// The tool the model is told to call. The granted resource is
// `tool:<TOOL_NAME>` with action `invoke`; the agent's authorize gate
// fires that exact query when the model calls the tool.
const TOOL_NAME = "@intx/tools-mail/sidecar-bundle:mail_send";
const GRANTED_RESOURCE = `tool:${TOOL_NAME}`;
const UNGRANTED_RESOURCE = "tool:@intx/some-other/bundle:forbidden_tool";

const SENTINEL_FILENAME = "grants-bridge-ran.txt";
const SENTINEL_CONTENT = "authorized-in-child";

const TOOL_PINS: readonly ToolPackagePin[] = [
  { name: "@intx/tools-mail", version: "0.1.2" },
];

// The operator-approved grant the hub ships in-band on the deploy frame.
// The grants bridge writes this verbatim into the legacy agent-state
// repo's `state/grants.json`; the child reads it back to authorize the
// tool. `expiresAt: null` keeps the rule non-expiring so the evaluator
// never compares a wire-serialized date.
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

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: TOOL_NAME,
      input: { to: SENTINEL_CONTENT, body: SENTINEL_FILENAME },
    },
  });
});

afterAll(async () => {
  await env.teardown();
});

describe("single-step launched-agent grants bridge via spawned child", () => {
  test("grants resolve from the legacy agent-state repo, identity is preserved, and events carry the sessionId", async () => {
    const deploymentMailAddress = deriveDeploymentAddress({
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });

    // (a) precondition: the deployment address is the launched-agent
    // identity shape, not a workflow-derived address.
    expect(isWorkflowDerivedAddress(deploymentMailAddress)).toBe(false);

    const agent = defineAgent({
      id: "agent-launched-grants",
      systemPrompt: "You are the single-step launched agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    });

    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: {
        [STEP_ID]: step({ agent }),
      },
    });

    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the orchestrator)",
      tools: [],
      // The non-empty grant set is the whole point: it must resolve in
      // the child for the granted tool's authorize to allow.
      grants: [GRANTED_RULE],
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${env.inference.server.port}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:mock-model",
    };

    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    const launchSession: LaunchSessionFn = async (orchestratorParams) => {
      await env.hub.sessionService.launchSession({
        agentAddress: orchestratorParams.agentAddress,
        agentId: orchestratorParams.agentId,
        instanceId: orchestratorParams.instanceId,
        config: orchestratorParams.config,
        deployContent: toLaunchDeployContent(orchestratorParams.deployContent),
        ...(orchestratorParams.toolPackagePins !== undefined
          ? { toolPackagePins: orchestratorParams.toolPackagePins }
          : {}),
      });
    };

    const sendMultiStepDeploy: SendMultiStepDeployFn = async (params) =>
      env.hub.router.sendAgentDeploy(params.agentAddress, params.config, {
        definition: {
          id: params.definition.id,
          triggers: [...params.definition.triggers],
          stepOrder: [...params.definition.stepOrder],
          steps: params.definition.steps as Record<string, unknown>,
          ...(params.definition.state !== undefined
            ? { state: params.definition.state }
            : {}),
        },
        sources: params.sources,
      });

    const workflowRepo: WorkflowRepoWriter = {
      async writeWorkflowRepo(args) {
        const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
        const principal: WorkflowRunHubPrincipal = { kind: "hub" };
        const files: Record<string, string> = {};
        for (const [k, v] of args.files) {
          files[k] = v;
        }
        await env.hub.agentRepoStore.repoStore.writeTree(
          principal,
          repoId,
          DEFAULT_ASSET_REF,
          {
            files,
            message: `single-step-grants-bridge test: write workflow repo ${args.workflowRepoId}`,
          },
        );
      },
    };

    const orchestrator = createWorkflowDeployOrchestrator({
      directorRegistry: createDefaultDirectorRegistry(),
      workflowRepo,
      launchSession,
      sendMultiStepDeploy,
    });

    let result: Awaited<ReturnType<typeof orchestrator.deployWorkflow>>;
    try {
      result = await orchestrator.deployWorkflow({
        workflow,
        config,
        deployContent: { systemPrompt: config.systemPrompt },
        operatorApprovals,
        deploymentId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        hubPublicKey: "00".repeat(32),
        toolPackagePins: TOOL_PINS,
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      const diag = env.sidecarDiagnostics();
      throw new Error(
        `deployWorkflow failed: ${message}\n${diag.length > 0 ? diag : "<no sidecar diagnostics>"}`,
        { cause },
      );
    }
    expect(result.kind).toBe("multi-step");

    // (a) identity: the deploy-ack fired for the legacy `ins_<hex>`
    // address and persisted a public key. The ack listener keys on the
    // legacy address (workflow-derived addresses are a no-op), so a
    // captured ack for this address proves the identity survived the
    // child re-route.
    await waitFor(() => env.hub.deployAcks.has(deploymentMailAddress), {
      timeoutMs: 20_000,
      diagnostics: env.sidecarDiagnostics,
    });
    const ackKey = env.hub.deployAcks.get(deploymentMailAddress);
    expect(ackKey).toBeDefined();
    expect(typeof ackKey).toBe("string");
    expect((ackKey ?? "").length).toBeGreaterThan(0);

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveTrivialDeploymentId(deploymentMailAddress),
    };
    env.registerDeployment({
      deploymentId: DEPLOYMENT_ID,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: deploymentMailAddress,
    });

    // (b) grants resolve: the grants bridge wrote `config.grants` into the
    // legacy agent-state repo at `parseAgentId(legacyAddress)`. Read it
    // back through `assembleCredentialsSnapshot` -- the EXACT call the
    // supervisor runs at spawn -- against the sidecar's on-disk substrate,
    // using the same single-step `deriveStepRepoId` the deploy router
    // applied. An empty snapshot here is the silent zero-grants failure
    // this sub-step exists to prevent.
    const legacyAgentStateRepoId: RepoId = {
      kind: "agent-state",
      id: parseAgentId(deploymentMailAddress),
    };

    // Read the grants back off the SIDECAR's on-disk substrate. The
    // sidecar runs in a subprocess, so reconstruct a RepoStore pointed at
    // the same data dir; only `getRepoDir` (a pure path computation that
    // honors the agent-state kind's directory layout) is exercised by
    // `assembleCredentialsSnapshot`, so a throwaway signing key is fine.
    const readbackRepoStore = createAgentRepoStore({
      dataDir: env.sidecar.dataDir,
      signingKey: await generateKeyPair(),
    }).repoStore;
    const grantsFilePath = path.join(
      readbackRepoStore.getRepoDir(legacyAgentStateRepoId),
      "state",
      "grants.json",
    );
    await waitFor(() => fs.existsSync(grantsFilePath), {
      timeoutMs: 20_000,
      diagnostics: env.sidecarDiagnostics,
    });

    const snapshot: CredentialsSnapshot = await assembleCredentialsSnapshot({
      repoStore: readbackRepoStore,
      principal: { kind: "hub" },
      stepOrder: [STEP_ID],
      deploymentId: deriveTrivialDeploymentId(deploymentMailAddress),
      deriveStepAddress: () => deploymentMailAddress,
      deriveStepRepoId: () => legacyAgentStateRepoId,
    });
    const stepSnapshot = snapshot.steps.find((s) => s.stepId === STEP_ID);
    if (stepSnapshot === undefined) {
      throw new Error(
        `credentials snapshot has no entry for step ${STEP_ID}; steps=${JSON.stringify(
          snapshot.steps.map((s) => s.stepId),
        )}`,
      );
    }
    // The granted rule must be present -- the lock against silent
    // zero-grants. If the grants bridge were removed this array would be
    // empty and the find would fail. Validate the snapshot's
    // `readonly unknown[]` grants through the same `WireGrantRule`
    // validator the wire boundary uses; the validator coerces
    // `expiresAt` back to a `Date`, yielding `GrantRule`-shaped entries
    // the evaluator accepts.
    const validatedGrants = WireGrantRule.array()([...stepSnapshot.grants]);
    if (validatedGrants instanceof type.errors) {
      throw new Error(
        `read-back grants failed WireGrantRule validation: ${validatedGrants.summary}`,
      );
    }
    const readBackGrants: GrantRule[] = validatedGrants;
    expect(readBackGrants.length).toBeGreaterThan(0);
    const granted = readBackGrants.find(
      (g) => g.resource === GRANTED_RESOURCE && g.effect === "allow",
    );
    expect(granted).toBeDefined();

    // (c) authorize round-trip against the read-back grants -- the same
    // evaluator the child's authorize adapter uses. Granted allows;
    // ungranted fails closed (no allow effect).
    const allowed = await evaluateGrants(
      [...readBackGrants],
      GRANTED_RESOURCE,
      "invoke",
    );
    expect(allowed.effect).toBe("allow");

    const denied = await evaluateGrants(
      [...readBackGrants],
      UNGRANTED_RESOURCE,
      "invoke",
    );
    // Fail closed: no grant matches, so the resolved effect is null (the
    // authorize layer treats a null effect as deny).
    expect(denied.effect).not.toBe("allow");

    // (c) behavioral: drive a mail message. The model turn calls the
    // granted tool; the tool's authorize succeeds in the child, the tool
    // runs (writes the sentinel), and the run reaches RunCompleted. If
    // grants had resolved empty the tool authorize would deny and the run
    // would not complete.
    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<single-step-grants-bridge-1@integration.interchange>",
    });

    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      diagnostics: env.sidecarDiagnostics,
      timeoutMs: 20_000,
    });

    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      },
    );
    if (terminal.type !== "RunCompleted") {
      const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
      const failed = events.find(
        (e) => e.type === "StepFailed" || e.type === "RunFailed",
      );
      throw new Error(
        `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failed?.body)}\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(terminal.type).toBe("RunCompleted");

    // The granted tool actually executed in the child (proof the
    // authorize allowed it): the tool wrote a sentinel into the per-step
    // workspace.
    const sentinelPath = path.join(
      env.sidecar.dataDir,
      "workflow-step-state",
      workflowRunRepoId.id,
      "runs",
      runId,
      "steps",
      STEP_ID,
      "attempt-1",
      "workspace",
      SENTINEL_FILENAME,
    );
    if (!fs.existsSync(sentinelPath)) {
      throw new Error(
        `granted tool sentinel ${sentinelPath} was not written; the tool authorize did not allow in the child\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(fs.readFileSync(sentinelPath, "utf-8")).toBe(SENTINEL_CONTENT);

    // (d) events: an inference.start reached the hub's agent.event sink
    // carrying the deploy's sessionId. The sink is keyed by
    // (agentAddress, sessionId); the production wiring threads
    // config.sessionId through publishWorkflowInferenceEvent.
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
});

function isInferenceStart(event: unknown): boolean {
  return (
    typeof event === "object" &&
    event !== null &&
    "type" in event &&
    (event as { type: unknown }).type === "inference.start"
  );
}
