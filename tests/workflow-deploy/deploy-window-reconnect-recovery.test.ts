// A reconnect racing the deploy window recovers cleanly instead of wedging.
//
// The deploy window is the interval where the sidecar's supervisor is LIVE
// (so it re-announces its deployment address on reconnect) but the hub has
// not yet recorded the deployment's public key. A reconnect that lands in
// that window fails the ownership challenge: the hub answers
// `challenge.failed` "Unknown agent address", and the sidecar's
// `handleChallengeFailed` calls `keyStore.forgetAgent`, wiping the
// in-memory signing key.
//
// The regression this guards: once the key is landed and a later reconnect
// is challenged, the sidecar must still be able to sign. `signChallenge`
// reloads the durable on-disk key on a cache miss, so the wiped in-memory
// key is not fatal -- the address becomes routable again and mail runs to
// completion. Before that reload, the wiped cache stranded the address
// permanently (signChallenge returned null on every subsequent challenge).
//
// Gating technique: the hub's `lookupPublicKey` reads `env.hub.deployAcks`,
// which the `agent.deploy.ack` listener populates at deploy time. The test
// stashes the real acked key, then holds the deploy window open by deleting
// the entry from `deployAcks` (so `lookupPublicKey` returns null), forces a
// reconnect there to fail the first challenge, then "lands" the key by
// restoring the entry and forces a further reconnect. This makes the racing
// reconnect deterministic rather than timing-dependent.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, a real workflow-process child, and a test inference provider.
// The drops are genuine server-side WebSocket closes; the recovery is the
// sidecar's real `hub-link` reconnect path passing the hub's ownership
// challenge after the key lands.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveDeploymentAddress,
  isWorkflowDerivedAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import {
  DEFAULT_ASSET_REF,
  type RepoId,
  type WorkflowRunHubPrincipal,
} from "@intx/hub-sessions";

import {
  SESSION_ID,
  SIDECAR_ID,
  dropHubLink,
  fireMailTrigger,
  listRunIds,
  startDeployFlowEnv,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// An `ins_` + hex-shaped local part, so the deployment address is the
// legacy `ins_<hex>` identity rather than a workflow-derived
// `ins_dep_<...>` address, matching the reconnect-survival fixture.
const DEPLOYMENT_ID = "dep10ec0ffee0ec0ffee0ec0ffee0ec0";
const WORKFLOW_RUN_REF = "refs/heads/main";
const STEP_ID = "step1";

let env: DeployFlowEnv;
let deploymentMailAddress: string;

beforeAll(async () => {
  deploymentMailAddress = deriveDeploymentAddress({
    deploymentId: DEPLOYMENT_ID,
    deploymentDomain: DEPLOYMENT_DOMAIN,
  });
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("deploy-window reconnect recovers cleanly", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("a reconnect that fails in the deploy window recovers once the key lands", async () => {
    expect(isWorkflowDerivedAddress(deploymentMailAddress)).toBe(false);

    // ---- deploy a single-step workflow ----
    const agent = defineAgent({
      id: "agent-deploy-window-recovery",
      systemPrompt: "You are the deploy-window recovery test agent.",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const workflow: WorkflowDefinition = defineWorkflow({
      id: `wf_${DEPLOYMENT_ID}`,
      trigger: { type: "mail", to: deploymentMailAddress },
      steps: { [STEP_ID]: step({ agent }) },
    });
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_deploy-window-recovery-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the orchestrator)",
      tools: [],
      grants: [],
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${String(env.inference.server.port)}`,
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

    const launchSession: LaunchSessionFn = async (p) => {
      await env.hub.sessionService.stageWorkflowStep({
        agentAddress: p.agentAddress,
        agentId: p.agentId,
        instanceId: p.instanceId,
        config: p.config,
        deployContent: toLaunchDeployContent(p.deployContent),
        ...(p.toolPackagePins !== undefined
          ? { toolPackagePins: p.toolPackagePins }
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
    const deploySingleStepAtHead: DeploySingleStepFn = (params) =>
      env.hub.sessionService.deploySingleStepAtHead(params);
    const workflowRepo: WorkflowRepoWriter = {
      async writeWorkflowRepo(args) {
        const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
        const principal: WorkflowRunHubPrincipal = { kind: "hub" };
        const files: Record<string, string> = {};
        for (const [k, v] of args.files) files[k] = v;
        await env.hub.agentRepoStore.repoStore.writeTree(
          principal,
          repoId,
          DEFAULT_ASSET_REF,
          { files, message: "deploy-window recovery: write workflow repo" },
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

    const result = await orchestrator.deployWorkflow({
      workflow,
      config,
      deployContent: { systemPrompt: config.systemPrompt },
      operatorApprovals,
      deploymentId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
      hubPublicKey: "00".repeat(32),
    });
    expect(result.publicKey).toBeTruthy();

    // Wait for the deployment to ack its key so the sidecar is fully live.
    await waitFor(() => env.hub.deployAcks.has(deploymentMailAddress), {
      timeoutMs: 20_000,
      diagnostics: env.sidecarDiagnostics,
    });

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(deploymentMailAddress),
    };
    env.registerDeployment({
      deploymentId: DEPLOYMENT_ID,
      workflowDefinition: workflow,
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: deploymentMailAddress,
    });

    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    // Capture the real acked key, then simulate the deploy window: the
    // sidecar is live, but the hub has not yet recorded the key. Deleting
    // the entry makes `lookupPublicKey` (which reads `deployAcks`) return
    // null for the racing reconnect while the sidecar deployment stays live.
    const ackedKey = env.hub.deployAcks.get(deploymentMailAddress);
    if (ackedKey === undefined) {
      throw new Error(
        `expected an acked key for ${deploymentMailAddress} after deploy`,
      );
    }
    env.hub.deployAcks.delete(deploymentMailAddress);

    // ---- racing reconnect in the deploy window (key not recorded) ----
    dropHubLink(env);
    await waitFor(
      () =>
        !env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
      { timeoutMs: 5_000, diagnostics: env.sidecarDiagnostics },
    );

    // The sidecar reconnects (~3s) and re-announces the address. The hub's
    // `lookupPublicKey` returns null, so it answers `challenge.failed`
    // "Unknown agent address"; the sidecar's `handleChallengeFailed` wipes
    // the in-memory signing key via `forgetAgent`. Wait past several 3s
    // reconnect cycles and confirm the address stays out of routing: with
    // the key still gated, the challenge cannot pass.
    let routableInWindow = false;
    const windowStart = Date.now();
    while (Date.now() - windowStart < 8_000) {
      if (
        env.hub.router.getRoutableAddresses().includes(deploymentMailAddress)
      ) {
        routableInWindow = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    expect(routableInWindow).toBe(false);

    // ---- the key lands: restore the entry and force a fresh reconnect ----
    // `lookupPublicKey` now returns the acked key. The sidecar's in-memory
    // key was wiped by the earlier `challenge.failed`, so recovery depends
    // on `signChallenge` reloading the durable on-disk key on the cache
    // miss. Force a reconnect so the challenge is re-issued now that the
    // key is landed.
    env.hub.deployAcks.set(deploymentMailAddress, ackedKey);
    dropHubLink(env);

    // ---- clean recovery: the address becomes routable again ----
    const reconnectMs = await waitForReconnect(env, deploymentMailAddress, {
      timeoutMs: 30_000,
    });
    // A generous lower bound guards against a false "already routable" pass
    // that never actually dropped; the upper bound catches a hung link.
    expect(reconnectMs).toBeGreaterThan(1_000);
    expect(reconnectMs).toBeLessThan(30_000);
    expect(env.hub.router.getRoutableAddresses()).toContain(
      deploymentMailAddress,
    );

    // Let the recovered link settle before firing mail. The wedge window
    // left a backlog of failed workflow-run pack pushes that retry on the
    // fresh link; firing mail on top of that backlog can race a residual
    // "Connection lost" on the supervisor's inbox enqueue. Require the
    // address to stay continuously routable across a quiet window so the
    // trigger lands on a stable link.
    const settleStart = Date.now();
    let stableSince = Date.now();
    while (Date.now() - stableSince < 2_000) {
      if (
        !env.hub.router.getRoutableAddresses().includes(deploymentMailAddress)
      ) {
        stableSince = Date.now();
      }
      if (Date.now() - settleStart > 30_000) {
        throw new Error(
          `recovered link never held routable for 2s within 30s\n${env.sidecarDiagnostics()}`,
        );
      }
      await new Promise((r) => setTimeout(r, 100));
    }

    // ---- and a mail trigger runs to completion on the recovered link ----
    // Fire with retry: a trigger that lands while a residual reconnect is
    // in flight can be dropped before the supervisor enqueues it, producing
    // no run. Retry with a fresh message id (each keyed on the attempt so
    // the dedup index never collides) until a run appears, capping attempts
    // so a genuine wedge still fails loudly rather than looping forever.
    const runId = await (async () => {
      const start = Date.now();
      let attempt = 0;
      for (;;) {
        attempt += 1;
        await fireMailTrigger(env, deploymentMailAddress, {
          messageId: `<deploy-window-recovery-${String(attempt)}@integration.interchange>`,
          content: "recovered",
        });
        const deadline = Date.now() + 10_000;
        while (Date.now() < deadline) {
          const ids = await listRunIds(env, workflowRunRepoId);
          const first = ids[0];
          if (first !== undefined) return first;
          await new Promise((r) => setTimeout(r, 100));
        }
        if (Date.now() - start > 60_000) {
          throw new Error(
            `no run produced on the recovered link after ${String(attempt)} mail triggers\n${env.sidecarDiagnostics()}`,
          );
        }
      }
    })();
    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
    );
    expect(terminal.type).toBe("RunCompleted");
  }, 240_000);
});
