import { describe, test, expect } from "bun:test";

import {
  createDefaultDirectorRegistry,
  createDirectorRegistry,
  defaultDirectorFactory,
  defineAgent,
  effectiveDirectorRef,
  type AgentDefinition,
  type AnnotatedToolFactory,
  type BaseEnv,
} from "@intx/agent";
import {
  defineWorkflow,
  type WorkflowDefinition,
} from "@intx/workflow/definition";

import { walkCapabilities } from "./capability-walk";

function makeMailFactory(): AnnotatedToolFactory<BaseEnv> {
  const factory = (_env: BaseEnv) => ({
    definitions: [],
    run: () =>
      Promise.resolve({ callId: "", content: "", isError: false as const }),
  });
  return Object.assign(factory, {
    id: "@intx/tools-mail/sidecar-bundle",
    requires: [] as readonly string[],
  });
}

function makeTrivialAgent(): AgentDefinition<BaseEnv> {
  return defineAgent({
    id: "ins_test-agent",
    systemPrompt: "You are an integration test agent.",
    tools: [makeMailFactory()],
    capabilities: [],
    inference: {
      sources: [{ provider: "anthropic", model: "mock-model" }],
    },
  });
}

function makeSingleStepWorkflow(
  agent: AgentDefinition<BaseEnv>,
): WorkflowDefinition {
  return defineWorkflow({
    id: "wf_integration",
    agent,
    trigger: { type: "mail", to: "ins_test-agent@integration.interchange" },
  });
}

function computeImplicitGrants(
  workflow: WorkflowDefinition,
  agent: AgentDefinition<BaseEnv>,
  registry: ReturnType<typeof createDefaultDirectorRegistry>,
): Set<string> {
  const grants = new Set<string>();
  for (const factory of agent.toolFactories) {
    grants.add(`tool:${factory.id}`);
  }
  for (const capability of agent.capabilities) {
    grants.add(`capability:${capability}`);
  }
  for (const source of agent.inference.sources) {
    grants.add(`inference.source:${source.provider}:${source.model}`);
  }
  const directorRef = effectiveDirectorRef(agent, registry);
  const directorFactory = registry.resolve(directorRef);
  grants.add(`director:${directorFactory.id}`);
  for (const trigger of workflow.triggers) {
    if (trigger.type !== "mail") continue;
    grants.add(`mail.address:${trigger.to}`);
    const at = trigger.to.lastIndexOf("@");
    if (at >= 0 && at < trigger.to.length - 1) {
      grants.add(`mail.send:${trigger.to.slice(at + 1)}`);
    }
  }
  return grants;
}

describe("walkCapabilities", () => {
  test("trivial workflow grants match the implicit agent-deploy grants", () => {
    const registry = createDefaultDirectorRegistry();
    const agent = makeTrivialAgent();
    const workflow = makeSingleStepWorkflow(agent);

    const walk = walkCapabilities(workflow, registry);

    expect(walk.unresolvedDirectors).toEqual([]);
    expect(walk.perStep.size).toBe(1);
    const stepId = workflow.stepOrder[0];
    if (stepId === undefined) {
      throw new Error("trivial workflow must have a single step");
    }
    const declarations = walk.perStep.get(stepId);
    if (declarations === undefined) {
      throw new Error(`walk produced no declarations for step ${stepId}`);
    }

    const implicitGrants = computeImplicitGrants(workflow, agent, registry);
    const walkGrants = new Set(declarations.grants);

    expect(walkGrants).toEqual(implicitGrants);
    expect(declarations.grants.length).toBe(implicitGrants.size);
  });

  test("emits tool, director, capability, and inference-source grants", () => {
    const registry = createDefaultDirectorRegistry();
    const agent = defineAgent({
      id: "ag_multi",
      systemPrompt: "multi-shape agent",
      tools: [makeMailFactory()],
      capabilities: ["reply", "summarize"],
      inference: {
        sources: [
          { provider: "anthropic", model: "claude-3" },
          { provider: "openai", model: "gpt-4" },
        ],
      },
    });
    const workflow = defineWorkflow({
      id: "wf_multi",
      agent,
      trigger: { type: "manual" },
    });

    const walk = walkCapabilities(workflow, registry);
    const stepId = workflow.stepOrder[0];
    if (stepId === undefined) throw new Error("missing step");
    const declarations = walk.perStep.get(stepId);
    if (declarations === undefined) throw new Error("missing declarations");
    const grants = new Set(declarations.grants);

    expect(grants.has("tool:@intx/tools-mail/sidecar-bundle")).toBe(true);
    expect(grants.has(`director:${defaultDirectorFactory.id}`)).toBe(true);
    expect(grants.has("capability:reply")).toBe(true);
    expect(grants.has("capability:summarize")).toBe(true);
    expect(grants.has("inference.source:anthropic:claude-3")).toBe(true);
    expect(grants.has("inference.source:openai:gpt-4")).toBe(true);
  });

  test("mail trigger emits both address and send-domain grants", () => {
    const registry = createDefaultDirectorRegistry();
    const agent = makeTrivialAgent();
    const workflow = defineWorkflow({
      id: "wf_mail",
      agent,
      trigger: { type: "mail", to: "support@example.com" },
    });

    const walk = walkCapabilities(workflow, registry);
    const stepId = workflow.stepOrder[0];
    if (stepId === undefined) throw new Error("missing step");
    const declarations = walk.perStep.get(stepId);
    if (declarations === undefined) throw new Error("missing declarations");
    const grants = new Set(declarations.grants);

    expect(grants.has("mail.address:support@example.com")).toBe(true);
    expect(grants.has("mail.send:example.com")).toBe(true);
  });

  test("unresolved director surfaces on the result without raising", () => {
    const emptyRegistry = createDirectorRegistry({
      factories: [defaultDirectorFactory],
      defaultId: defaultDirectorFactory.id,
    });
    const agent: AgentDefinition<BaseEnv> = {
      id: "ag_unresolved",
      systemPrompt: "agent with missing director",
      director: { id: "@vendor/missing/director", config: {} },
      toolFactories: [makeMailFactory()],
      capabilities: [],
      inference: {
        sources: [{ provider: "anthropic", model: "mock-model" }],
      },
    };
    const workflow = defineWorkflow({
      id: "wf_unresolved",
      agent,
      trigger: { type: "manual" },
    });

    const walk = walkCapabilities(workflow, emptyRegistry);

    expect(walk.unresolvedDirectors).toEqual(["@vendor/missing/director"]);
    const stepId = workflow.stepOrder[0];
    if (stepId === undefined) throw new Error("missing step");
    const declarations = walk.perStep.get(stepId);
    if (declarations === undefined) throw new Error("missing declarations");
    expect(declarations.grants).toContain(
      "tool:@intx/tools-mail/sidecar-bundle",
    );
    expect(declarations.grants.some((g) => g.startsWith("director:"))).toBe(
      false,
    );
  });

  test("non-mail triggers contribute no mail grants", () => {
    const registry = createDefaultDirectorRegistry();
    const agent = makeTrivialAgent();
    const workflow = defineWorkflow({
      id: "wf_manual",
      agent,
      trigger: { type: "manual" },
    });

    const walk = walkCapabilities(workflow, registry);
    const stepId = workflow.stepOrder[0];
    if (stepId === undefined) throw new Error("missing step");
    const declarations = walk.perStep.get(stepId);
    if (declarations === undefined) throw new Error("missing declarations");

    expect(
      declarations.grants.some(
        (g) => g.startsWith("mail.address:") || g.startsWith("mail.send:"),
      ),
    ).toBe(false);
  });
});
