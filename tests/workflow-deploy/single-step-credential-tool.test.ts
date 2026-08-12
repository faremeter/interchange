// End-to-end acceptance for the mediated-credential delivery + authorization
// rail, driven through a REAL credential-consuming tool.
//
// A single-step workflow pins `@intx/tools-credential-probe` (compiled from
// `fixtures/credential-tool-bundle.ts`). The deploy carries a `CredentialDelivery`
// on the deploy frame; the supervisor pushes it into the child's material cell
// on the pre-trigger barrier, so it is resident before the step agent builds.
// When the model drives the `probe` tool, the tool resolves its declared handle
// through the consumer-scoped `credentials` capability (Gate 2 authorizes it
// against a `credential:{id}` / `use` grant), obtains an http mediated
// credential, and fetches a path on the credential's pinned origin -- an
// in-process mock server that records the Authorization header it sees.
//
// The proofs:
//   * Positive: the exact delivered secret arrives at the pinned origin as a
//     bearer -- the whole rail (deploy-frame delivery -> child cell -> Gate 2
//     -> provider shape -> authed fetch) carried it, without the secret ever
//     touching disk or the tool's own API.
//   * Rotation: a live `credentials.update` push swaps the material; the next
//     run's fetch carries the NEW secret, proving the delivery channel reaches
//     a running child and the shaped handle reads material live.
//   * Negative: a run whose grant does not authorize the consumer fails the
//     resolve closed -- no request ever reaches the origin.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import { toolConsumer } from "@intx/authz";
import type { HarnessConfig } from "@intx/types/runtime";
import type { CredentialDelivery } from "@intx/types/sidecar";
import type { WireGrantRule } from "@intx/types/grant-wire";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveRunAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { RepoId, WorkflowRunHubPrincipal } from "@intx/hub-sessions";
import { DEFAULT_ASSET_REF } from "@intx/hub-sessions";

import path from "node:path";

import {
  SESSION_ID,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";
import {
  CREDENTIAL_HANDLE,
  PACKAGE_NAME,
  TOOL_NAME,
} from "./fixtures/credential-tool-bundle";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

const PACKAGE_VERSION = "0.1.0";
const TOOL_PINS: readonly ToolPackagePin[] = [
  { name: PACKAGE_NAME, version: PACKAGE_VERSION },
];

// The consumer identity Gate 2 checks: the delivered descriptor's `consumer`
// and the credential-use grant's `{ tool }` condition must both equal this.
const CONSUMER = toolConsumer(PACKAGE_NAME);
const CREDENTIAL_ID = "cred-probe-1";
const SECRET_INITIAL = "sk-probe-alpha-9d2f";
const SECRET_ROTATED = "sk-probe-bravo-7a41";

// The path the model drives `probe` to fetch, and where the tool writes its
// outcome in the step workspace.
const PROBE_PATH = "/whoami";
const PROBE_SENTINEL = "credential-probe-ran.json";

let env: DeployFlowEnv;

// In-process mock origin the mediated http credential authenticates to. Every
// request's Authorization header is recorded so the test can assert the exact
// delivered secret arrived as a bearer.
let origin: ReturnType<typeof Bun.serve>;
const originRequests: { path: string; authorization: string | null }[] = [];

beforeAll(async () => {
  origin = Bun.serve({
    port: 0,
    fetch(req) {
      const url = new URL(req.url);
      originRequests.push({
        path: url.pathname,
        authorization: req.headers.get("authorization"),
      });
      return new Response(
        JSON.stringify({ ok: true, seenPath: url.pathname }),
        {
          headers: { "content-type": "application/json" },
        },
      );
    },
  });

  env = await startDeployFlowEnv({
    credentialTool: {
      entryPath: path.join(
        import.meta.dir,
        "fixtures",
        "credential-tool-bundle.ts",
      ),
      packageName: PACKAGE_NAME,
      version: PACKAGE_VERSION,
      handle: CREDENTIAL_HANDLE,
    },
    inferenceToolCall: {
      toolName: TOOL_NAME,
      input: { path: PROBE_PATH, sentinel: PROBE_SENTINEL },
    },
    // The rail is exercised across more than one run (before and after a
    // rotation), so the tool must be driven on every run, not just the first.
    inferenceToolCallEachRun: true,
  });
});

afterAll(async () => {
  await origin.stop(true);
  await env.teardown();
});

/** Build a delivery binding the probe's handle to a credential whose http
 *  material carries `secret`, pinned to the mock origin. */
function deliveryWithSecret(secret: string): CredentialDelivery {
  return {
    bindings: [
      {
        handle: CREDENTIAL_HANDLE,
        credentialId: CREDENTIAL_ID,
        consumer: CONSUMER,
      },
    ],
    materials: [
      {
        credentialId: CREDENTIAL_ID,
        providerKey: "http",
        origin: origin.url.origin,
        secret,
      },
    ],
  };
}

/** The run grant that authorizes the probe's consumer to use the credential
 *  (Gate 2). Its `{ tool }` condition scopes it to exactly this package. */
const CREDENTIAL_USE_GRANT: WireGrantRule = {
  id: "grant-credential-probe-use",
  resource: `credential:${CREDENTIAL_ID}`,
  action: "use",
  effect: "allow",
  origin: "creator",
  conditions: { tool: CONSUMER },
  expiresAt: null,
  roleId: null,
  principalId: null,
};

/**
 * Deploy the single-step credential-probe workflow at head, threading a
 * `CredentialDelivery` onto the deploy frame (the production launch path). The
 * supervisor delivers it to the child before the first run's trigger fires.
 */
async function deployProbe(
  deploymentId: string,
  delivery: CredentialDelivery | undefined,
): Promise<{ mailAddress: string; workflowRunRepoId: RepoId }> {
  const agent = defineAgent({
    id: "agent-step1",
    systemPrompt: "You are the single-step credential-probe agent.",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
  });

  const mailAddress = deriveRunAddress({
    runId: deploymentId,
    domain: DEPLOYMENT_DOMAIN,
  });

  const workflow: WorkflowDefinition = defineWorkflow({
    id: `wf_${deploymentId}`,
    trigger: { type: "mail", to: mailAddress },
    steps: { [STEP_ID]: step({ agent }) },
  });

  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: `ins_${deploymentId}`,
    tenantId: "tenant-1",
    principalId: "prin_integration-1",
    agentAddress: mailAddress,
    systemPrompt: "Fallback prompt (overridden per step by the orchestrator)",
    tools: [],
    // No inline tool grant: the pinned probe loads sidecar-side and authorizes
    // on its own unmarked floor. Credential USE is a separate gate, satisfied
    // by the per-run grant delivered at trigger time.
    grants: [],
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
    `mail.address:${mailAddress}`,
    `mail.send:${DEPLOYMENT_DOMAIN}`,
  ]);

  const launchSession: LaunchSessionFn = async (orchestratorParams) => {
    await env.hub.sessionService.stageWorkflowStep({
      agentAddress: orchestratorParams.agentAddress,
      agentId: orchestratorParams.agentId,
      runId: orchestratorParams.instanceId,
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

  // The single-step head deploy: inject the credential delivery onto the
  // workflow frame the orchestrator asks the session service to send. A test
  // that delivers over the live channel instead deploys with none here.
  const deploySingleStepAtHead: DeploySingleStepFn = (params) =>
    env.hub.sessionService.deploySingleStepAtHead({
      ...params,
      ...(delivery !== undefined ? { credentials: delivery } : {}),
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
          message: `single-step-credential-tool: write workflow repo ${args.workflowRepoId}`,
        },
      );
    },
  };

  const orchestrator = createWorkflowDeployOrchestrator({
    directorRegistry: createDefaultDirectorRegistry(),
    workflowRepo,
    launchSession,
    sendMultiStepDeploy,
    deploySingleStepAtHead,
  });

  try {
    const result = await orchestrator.deployWorkflow({
      workflow,
      config,
      deployContent: { systemPrompt: config.systemPrompt },
      operatorApprovals,
      deploymentId,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
      toolPackagePins: TOOL_PINS,
    });
    expect(result.publicKey).toBeTruthy();
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    throw new Error(
      `deployWorkflow failed: ${message}\n${env.sidecarDiagnostics()}`,
      { cause },
    );
  }

  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: deriveDeploymentId(mailAddress),
  };
  env.registerDeployment({
    deploymentId,
    workflowDefinition: workflow,
    workflowRunRepoId,
    workflowRunRef: WORKFLOW_RUN_REF,
    mailAddress,
  });

  return { mailAddress, workflowRunRepoId };
}

/** Fire a run and wait for it to complete, returning its id and terminal. */
async function runOnce(
  deploymentId: string,
  mailAddress: string,
  workflowRunRepoId: RepoId,
  grants: WireGrantRule[],
  messageId: string,
): Promise<{
  runId: string;
  terminal: Awaited<ReturnType<typeof waitForWorkflowRunComplete>>;
}> {
  await fireMailTrigger(env, mailAddress, { messageId, grants });
  const runId = await waitForFirstRunId(env, workflowRunRepoId, {
    diagnostics: env.sidecarDiagnostics,
    timeoutMs: 20_000,
  });
  const terminal = await waitForWorkflowRunComplete(env, deploymentId, runId, {
    timeoutMs: 20_000,
    diagnostics: env.sidecarDiagnostics,
  });
  return { runId, terminal };
}

describe("single-step credential-consuming tool end-to-end", () => {
  test("delivers the credential and authenticates to its pinned origin", async () => {
    const deploymentId = "single-step-credential-tool-pos";
    const { mailAddress, workflowRunRepoId } = await deployProbe(
      deploymentId,
      deliveryWithSecret(SECRET_INITIAL),
    );

    const before = originRequests.length;
    const { runId, terminal } = await runOnce(
      deploymentId,
      mailAddress,
      workflowRunRepoId,
      [CREDENTIAL_USE_GRANT],
      "<credential-tool-pos-1@integration.interchange>",
    );
    if (terminal.type !== "RunCompleted") {
      const events = await readWorkflowRunEvents(env, deploymentId, runId);
      throw new Error(
        `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(events.find((e) => e.type === "StepFailed" || e.type === "RunFailed")?.body)}\n${env.sidecarDiagnostics()}`,
      );
    }

    // THE PROOF: the exact delivered secret reached the pinned origin as a
    // bearer, and nothing else did.
    const fresh = originRequests.slice(before);
    expect(fresh.length).toBeGreaterThanOrEqual(1);
    const probeReq = fresh.find((r) => r.path === PROBE_PATH);
    expect(probeReq).toBeDefined();
    expect(probeReq?.authorization).toBe(`Bearer ${SECRET_INITIAL}`);
  });

  test("delivers over the live credentials.update channel to a running child", async () => {
    const deploymentId = "single-step-credential-tool-chan";
    // Deploy with NO deploy-frame delivery: the credential reaches the child
    // ONLY over the live credentials.update channel. Push it and wait for the
    // child to ack, so the material is resident before the run's agent builds.
    const { mailAddress, workflowRunRepoId } = await deployProbe(
      deploymentId,
      undefined,
    );
    await env.hub.router.sendCredentialsUpdate(
      mailAddress,
      deliveryWithSecret(SECRET_ROTATED),
    );

    const before = originRequests.length;
    const { runId, terminal } = await runOnce(
      deploymentId,
      mailAddress,
      workflowRunRepoId,
      [CREDENTIAL_USE_GRANT],
      "<credential-tool-chan-1@integration.interchange>",
    );
    if (terminal.type !== "RunCompleted") {
      const events = await readWorkflowRunEvents(env, deploymentId, runId);
      throw new Error(
        `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(events.find((e) => e.type === "StepFailed" || e.type === "RunFailed")?.body)}\n${env.sidecarDiagnostics()}`,
      );
    }

    // The channel-delivered secret is what the tool sent -- the live push
    // reached the running child's cell and the shaped handle read it.
    const fresh = originRequests.slice(before);
    const probeReq = fresh.find((r) => r.path === PROBE_PATH);
    expect(probeReq).toBeDefined();
    expect(probeReq?.authorization).toBe(`Bearer ${SECRET_ROTATED}`);
  });

  test("fails closed when no grant authorizes the credential's use", async () => {
    const deploymentId = "single-step-credential-tool-neg";
    const { mailAddress, workflowRunRepoId } = await deployProbe(
      deploymentId,
      deliveryWithSecret(SECRET_INITIAL),
    );

    const before = originRequests.length;
    const inferBefore = env.inference.requests.length;
    // Fire the run WITHOUT the credential-use grant: Gate 2 denies the resolve
    // inside the tool, which throws before any request is shaped.
    const { terminal } = await runOnce(
      deploymentId,
      mailAddress,
      workflowRunRepoId,
      [],
      "<credential-tool-neg-1@integration.interchange>",
    );

    // THE PROOF of fail-closed: the model DID drive the tool (a tool_use turn
    // plus a follow-up turn once its error result landed, so >= 2 inference
    // requests), yet the denied credential never authenticated -- nothing
    // reached the origin. The gate refused the use; the tool did not silently
    // skip it.
    expect(env.inference.requests.length - inferBefore).toBeGreaterThanOrEqual(
      2,
    );
    expect(originRequests.length).toBe(before);
    expect(terminal.type).toBe("RunCompleted");
  });
});
