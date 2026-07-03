// Unresolvable-director deploy-rejection integration test.
//
// Constructs a workflow whose step's agent declares a `director` ref
// neither the sidecar's built-in registry nor any pinned tool package's
// `interchange.directors` field can resolve, attempts to deploy via the
// workflow-deploy orchestrator (wired against the env's hub substrate),
// and asserts the deploy fails with a `CapabilityApprovalDeniedError`
// whose message names the unresolvable director ref and whose
// `unresolvedDirectors` field carries the same id.
//
// This is the defensive case of the capability walk: the walk surfaces
// every unresolved director on `unresolvedDirectors`; the orchestrator's
// approval gate rejects the deploy and formats the message as
// `unresolvable director: <id>`. The test also asserts no agent-state
// repo or workflow-run repo was created for the rejected deployment --
// the deploy must fail without partial state.
//
// The pre-landed `deploy-flow-env` fixture supplies the hub, sidecar,
// mock inference, and the `deployWorkflow` helper that composes the
// orchestrator against the env's production paths; this file does not
// modify the fixture.
//
// `deployWorkflow` (the orchestrator's entry point) throws
// `CapabilityApprovalDeniedError` directly when the capability walk
// surfaces unresolvable directors; the test imports the class symbol
// so the assertion narrows on the structural contract rather than the
// stringly-typed `name` field alone.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { defineAgent } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow } from "@intx/workflow";
import { CapabilityApprovalDeniedError } from "@intx/workflow-deploy";

import {
  AGENT_ADDRESS,
  AGENT_ID,
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflow,
  startDeployFlowEnv,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const UNRESOLVABLE_DIRECTOR_ID = "@vendor/missing/director";

let env: DeployFlowEnv;

beforeAll(async () => {
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  await env.teardown();
});

describe("unresolvable-director deploy rejection", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("deploy fails cleanly when the agent references a director the registry cannot resolve", async () => {
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: AGENT_ID,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: AGENT_ADDRESS,
      systemPrompt: "Fallback prompt (overridden by deploy tree)",
      tools: [],
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

    // The agent's `director` ref is intentionally absent from the
    // default registry and from any pinned tool package's
    // `interchange.directors` field; the capability walk surfaces it
    // on `unresolvedDirectors`, the approval gate rejects the deploy,
    // and the orchestrator translates the rejection into a
    // `CapabilityApprovalDeniedError` whose message starts with
    // `unresolvable director:`.
    const agent = defineAgent({
      id: AGENT_ID,
      systemPrompt: "You are an integration test agent.",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
      director: { id: UNRESOLVABLE_DIRECTOR_ID, config: {} },
    });

    const workflow = defineWorkflow({
      id: `wf_${AGENT_ID}`,
      agent,
      trigger: { type: "mail", to: AGENT_ADDRESS },
    });

    // The operator approves a broad surface so the deploy cannot fail
    // on a missing inference/mail/tool grant: the only remaining
    // rejection vector is the unresolvable director ref. Pre-approving
    // the would-be director grant (`director:<id>`) defensively is
    // moot -- the orchestrator's `formatApprovalDeniedMessage` prefers
    // the unresolvable-director branch even when the pending set is
    // empty.
    const mailDomain = AGENT_ADDRESS.slice(AGENT_ADDRESS.lastIndexOf("@") + 1);
    const operatorApprovals = new Set<string>([
      `inference.source:anthropic:mock-model`,
      `mail.address:${AGENT_ADDRESS}`,
      `mail.send:${mailDomain}`,
      `director:${UNRESOLVABLE_DIRECTOR_ID}`,
    ]);

    const initialDeploymentCount = env.deployments.size;
    const initialDeployAckCount = env.hub.deployAcks.size;
    const initialStatePackCount = env.hub.statePacks.length;

    let captured: unknown = undefined;
    try {
      await deployWorkflow(env, workflow, {
        config,
        deployContent: { systemPrompt: "You are an integration test agent." },
        deploymentId: AGENT_ID,
        operatorApprovals,
      });
    } catch (err) {
      captured = err;
    }

    expect(captured).toBeInstanceOf(CapabilityApprovalDeniedError);
    if (!(captured instanceof CapabilityApprovalDeniedError)) {
      throw new Error("unreachable");
    }
    expect(captured.name).toBe("CapabilityApprovalDeniedError");
    expect(captured.message).toContain("unresolvable director");
    expect(captured.message).toContain(UNRESOLVABLE_DIRECTOR_ID);

    // `CapabilityApprovalDeniedError` carries the unresolvable ids on a
    // structured field so callers do not need to scrape the message
    // string.
    expect(captured.unresolvedDirectors).toEqual([UNRESOLVABLE_DIRECTOR_ID]);

    // No partial state: the orchestrator runs the capability walk and
    // approval gate before writing the workflow repo or invoking
    // `launchSession`, so a rejected deploy must not have produced a
    // deployment handle, a deploy-ack, or a state-pack write.
    expect(env.deployments.size).toBe(initialDeploymentCount);
    expect(env.hub.deployAcks.size).toBe(initialDeployAckCount);
    expect(env.hub.statePacks.length).toBe(initialStatePackCount);
  });
});
