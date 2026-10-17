// runLocal with the real step invoker from `@intx/workflow-host/local`,
// authorizing with the same grants and evaluator as a deployed source-ref run.
//
// Each run builds a real agent through the tool-bearing agent factory, drives
// it with deterministic mock inference from `@intx/inference-testing`, and
// authorizes every tool call through `createCredentialsBackedAuthorize` over
// the local credentials snapshot of the definition, with the deployed grant
// evaluator. A tool the definition declares runs; a tool name it does not
// declare is refused with the grant evaluator's reason; a tool in a
// childWorkflow child runs; a credential tool authenticates through the
// consumer-scoped credentials capability, or fails closed with no use grant;
// and an approval-gated tool parks the step, which a run that cannot be
// answered fails rather than waits on.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  createDefaultDirectorRegistry,
  defineAgent,
  defineTool,
  type BaseEnv,
} from "@intx/agent";
import { toolConsumer } from "@intx/authz";
import {
  createCredentialProviderRegistry,
  createHttpCredentialProvider,
} from "@intx/harness";
import { setupHarness, type Harness } from "@intx/inference-testing";
import type { CredentialDelivery } from "@intx/types/credential-delivery";
import {
  createRuntimeCapabilities,
  type RuntimeCapabilities,
} from "@intx/types/runtime-capabilities";
import type { InferenceSource } from "@intx/types/runtime-core";
import {
  childWorkflow,
  defineWorkflow,
  runLocal,
  step,
  type WorkflowDefinition,
} from "@intx/workflow";
import {
  attachStepCredentialWiring,
  attachStepTools,
  createCredentialsBackedAuthorize,
  createGrantEvaluator,
  createLocalCredentialsSnapshot,
  createToolBearingAgentFactory,
  createWorkflowStepInvoker,
  snapshotStepGrants,
} from "@intx/workflow-host/local";

import { driveUntilSettled, localStepEnvBase } from "./local-step-env";

const TOOL_ID = "@acme/tools/local";
const ORIGIN = "https://api.example.com";
const SECRET = "sk-local-echo-2f71";

const source: InferenceSource = {
  id: "anthropic:mock-model",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-mock",
  model: "mock-model",
};

interface LocalToolEnv extends BaseEnv {
  capabilities: RuntimeCapabilities;
}

const noArgs = { type: "object", properties: {} } as const;

// One tool package with a plain tool and a credential tool. Each run records
// its calls so a test can tell an executed tool from a refused one.
function localTools(calls: string[]) {
  return defineTool<LocalToolEnv>({
    id: TOOL_ID,
    requires: ["capabilities"],
    definitions: [
      { name: "read" },
      { name: "probe" },
      { name: "send", approval: "ask" },
    ],
    factory: (env) => ({
      definitions: [
        { name: "read", description: "Read a value.", inputSchema: noArgs },
        {
          name: "probe",
          description: "Fetch /whoami through the bound credential.",
          inputSchema: noArgs,
        },
        { name: "send", description: "Send a value.", inputSchema: noArgs },
      ],
      run: async (call) => {
        calls.push(call.name);
        if (call.name === "read") {
          return { callId: call.id, content: "read-ok" };
        }
        if (call.name === "send") {
          return { callId: call.id, content: "send-ok" };
        }
        const credentials = env.capabilities.resolve("credentials");
        const mediated = await credentials.resolve("api");
        const response = await mediated.fetch("/whoami");
        return { callId: call.id, content: `probe ${response.status}` };
      },
    }),
  });
}

function agentStep(calls: string[]) {
  return step({
    agent: defineAgent({
      id: "local-agent",
      systemPrompt: "You are the local real-step-invoker agent.",
      tools: [localTools(calls)],
      capabilities: [],
      inference: {
        sources: [{ provider: source.provider, model: source.model }],
      },
    }),
  });
}

describe("runLocal with the real step invoker", () => {
  let harness: Harness;
  let baseDir: string;
  let originRequests: { url: string; authorization: string | null }[];

  beforeEach(() => {
    harness = setupHarness();
    baseDir = mkdtempSync(join(tmpdir(), "run-local-real-step-invoker-"));
    originRequests = [];
  });

  afterEach(() => {
    harness.dispose();
    rmSync(baseDir, { recursive: true, force: true });
  });

  // Run `definition` with the local snapshot as its grants. With `bound`, the
  // material cell binds the credential tool's `api` handle and the snapshot
  // carries its use grant.
  async function run(
    definition: WorkflowDefinition,
    bound: boolean,
    hasUpstreamSignalResolver = true,
  ) {
    const delivery: CredentialDelivery = {
      bindings: [
        { handle: "api", credentialId: "c1", consumer: toolConsumer(TOOL_ID) },
      ],
      materials: [
        {
          credentialId: "c1",
          providerKey: "http",
          origin: ORIGIN,
          secret: SECRET,
        },
      ],
    };
    const materialCell = { current: delivery };
    const providers = createCredentialProviderRegistry([
      createHttpCredentialProvider({
        fetch: async (input, init) => {
          const request =
            input instanceof Request
              ? new Request(input, init)
              : new Request(String(input), init);
          originRequests.push({
            url: request.url,
            authorization: request.headers.get("authorization"),
          });
          return new Response("ok");
        },
      }),
    ]);
    const snapshotRef = {
      current: createLocalCredentialsSnapshot({
        definition,
        directors: createDefaultDirectorRegistry(),
        ...(bound ? { delivery } : {}),
      }),
    };
    const authorize = createCredentialsBackedAuthorize(
      snapshotRef,
      createGrantEvaluator({ floorFor: () => [] }),
    );
    const invokeStep = createWorkflowStepInvoker({
      workflowAuthorize: authorize,
      agentFactory: createToolBearingAgentFactory(),
      buildEnv: async (req) => {
        const stepId = req.authzContext.stepId;
        if (stepId === undefined) throw new Error("step has no id");
        const env = {
          ...(await localStepEnvBase({
            baseDir,
            name: `${req.authzContext.runId ?? "run"}-${stepId}`,
            source,
            harness,
          })),
          capabilities: createRuntimeCapabilities({}),
        };
        attachStepTools(env, {
          factories: req.agent.toolFactories.map((factory) => ({
            packageName: factory.id,
            declaredCredentials: [],
            factory,
          })),
          pluginFactories: [],
        });
        attachStepCredentialWiring(env, {
          materialCell,
          resolveGrants: () => snapshotStepGrants(snapshotRef, stepId),
          providers,
        });
        return env;
      },
    });
    const result = await driveUntilSettled(
      harness,
      runLocal(definition, {
        authorize,
        hasUpstreamSignalResolver,
        invokeStep,
      }),
    );
    const bodies = await Promise.all(
      harness.scenario.matchedRequests().map((r) => r.text()),
    );
    return { result, bodies };
  }

  function replyWithToolCalls(...names: string[]) {
    harness.scenario.replyOnce("anthropic", {
      toolCalls: names.map((name) => ({ name, args: {} })),
    });
    harness.scenario.replyOnce("anthropic", { text: "done" });
  }

  test("a declared tool runs and an undeclared tool is refused by the grants", async () => {
    const calls: string[] = [];
    replyWithToolCalls("read", "rogue");
    const { result, bodies } = await run(
      defineWorkflow({
        id: "local-tools",
        trigger: { type: "manual" },
        steps: { work: agentStep(calls) },
      }),
      false,
    );
    expect(result.terminalStatus).toBe("completed");
    expect(calls).toEqual(["read"]);
    expect(bodies[1]).toContain("read-ok");
    expect(bodies[1]).toContain("No matching grants for tool:rogue/invoke");
  });

  test("a tool in a childWorkflow child runs", async () => {
    const calls: string[] = [];
    replyWithToolCalls("read");
    const { result, bodies } = await run(
      defineWorkflow({
        id: "local-child",
        trigger: { type: "manual" },
        steps: {
          spawn: childWorkflow({
            definition: defineWorkflow({
              id: "local-child-body",
              trigger: { type: "manual" },
              steps: { inChild: agentStep(calls) },
            }),
          }),
        },
      }),
      false,
    );
    expect(result.terminalStatus).toBe("completed");
    expect(calls).toEqual(["read"]);
    expect(bodies[1]).toContain("read-ok");
  });

  test("a credential tool authenticates with the bound secret", async () => {
    const calls: string[] = [];
    replyWithToolCalls("probe");
    const { result, bodies } = await run(
      defineWorkflow({
        id: "local-credential",
        trigger: { type: "manual" },
        steps: { work: agentStep(calls) },
      }),
      true,
    );
    expect(result.terminalStatus).toBe("completed");
    expect(originRequests).toEqual([
      { url: `${ORIGIN}/whoami`, authorization: `Bearer ${SECRET}` },
    ]);
    expect(bodies[1]).toContain("probe 200");
  });

  test("a credential tool fails closed when the snapshot has no use grant", async () => {
    const calls: string[] = [];
    replyWithToolCalls("probe");
    const { result, bodies } = await run(
      defineWorkflow({
        id: "local-credential-denied",
        trigger: { type: "manual" },
        steps: { work: agentStep(calls) },
      }),
      false,
    );
    expect(result.terminalStatus).toBe("completed");
    expect(calls).toEqual(["probe"]);
    expect(originRequests).toEqual([]);
    expect(bodies[1]).toContain(
      `consumer ${toolConsumer(TOOL_ID)} is not authorized to use credential c1`,
    );
  });

  test("an approval-gated tool parks the step, and an unanswerable run fails it", async () => {
    const calls: string[] = [];
    replyWithToolCalls("send");
    const { result } = await run(
      defineWorkflow({
        id: "local-ask",
        trigger: { type: "manual" },
        steps: { work: agentStep(calls) },
      }),
      false,
      false,
    );
    expect(result.terminalStatus).toBe("failed");
    expect(calls).toEqual([]);
    const failure = result.events.find((event) => event.kind === "StepFailed");
    if (failure === undefined || failure.kind !== "StepFailed") {
      throw new Error("expected the work step to fail");
    }
    expect(failure.error.message).toContain("no upstream resolver");
  });
});
