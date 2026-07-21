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
  type ToolDeclaration,
} from "@intx/agent";
import {
  action,
  defineWorkflow,
  loop,
  step,
  type WorkflowDefinition,
} from "@intx/workflow/definition";

import { DuplicateWalkToolError, walkCapabilities } from "./capability-walk";

// A synthetic mail factory that declares one tool, `mail_send`. The walk
// keys `tool:` grants on each declared definition name (not on the
// factory id), so the fixture must carry a real definition or the walk
// emits zero tool grants and every `tool:` assertion passes vacuously.
function makeMailFactory(): AnnotatedToolFactory<BaseEnv> {
  const factory = (_env: BaseEnv) => ({
    definitions: [],
    run: () =>
      Promise.resolve({ callId: "", content: "", isError: false as const }),
  });
  return Object.assign(factory, {
    id: "@intx/tools-mail/sidecar-bundle",
    requires: [] as readonly string[],
    definitions: [{ name: "mail_send" }],
  });
}

// A synthetic factory whose declared definitions are supplied by the
// caller, so a test can exercise gated tools, ungated tools, and
// intra-factory duplicate names.
function makeFactory(
  id: string,
  definitions: readonly ToolDeclaration[],
): AnnotatedToolFactory<BaseEnv> {
  const factory = (_env: BaseEnv) => ({
    definitions: [],
    run: () =>
      Promise.resolve({ callId: "", content: "", isError: false as const }),
  });
  return Object.assign(factory, {
    id,
    requires: [] as readonly string[],
    definitions,
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
  // Mirror the real walk: one `tool:<def.name>` per declared definition,
  // NOT one per factory id.
  for (const factory of agent.toolFactories) {
    for (const definition of factory.definitions) {
      grants.add(`tool:${definition.name}`);
    }
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

    expect(grants.has("tool:mail_send")).toBe(true);
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
    expect(declarations.grants).toContain("tool:mail_send");
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

  test("emits an effect grant for each of an action's declared requires", () => {
    const registry = createDefaultDirectorRegistry();
    const workflow = defineWorkflow({
      id: "wf_action",
      trigger: { type: "manual" },
      steps: {
        commit: action({
          handler: "commit",
          effect: { requires: ["git:commit", "shell:run"] },
        }),
      },
    });

    const walk = walkCapabilities(workflow, registry);
    const declarations = walk.perStep.get("commit");
    if (declarations === undefined) throw new Error("missing declarations");
    const grants = new Set(declarations.grants);

    expect(grants.has("effect:git:commit")).toBe(true);
    expect(grants.has("effect:shell:run")).toBe(true);
    // An action carries no agent, so it contributes no agent-shaped grants.
    expect(declarations.grants.some((g) => g.startsWith("tool:"))).toBe(false);
    expect(declarations.grants.some((g) => g.startsWith("director:"))).toBe(
      false,
    );
  });

  test("a loop node carries the union of its body's grants", () => {
    const registry = createDefaultDirectorRegistry();
    const body = defineWorkflow({
      id: "loop-body",
      trigger: { type: "manual" },
      steps: {
        work: step({ agent: makeTrivialAgent() }),
        commit: action({
          handler: "c",
          effect: { requires: ["git:commit"] },
          after: ["work"],
        }),
      },
    });
    const workflow = defineWorkflow({
      id: "wf_loop",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body,
          while: "w",
          carry: "c",
          maxIterations: 3,
          onExhausted: "esc",
        }),
        esc: step({ agent: makeTrivialAgent(), after: ["rework"] }),
      },
    });

    const walk = walkCapabilities(workflow, registry);
    const declarations = walk.perStep.get("rework");
    if (declarations === undefined) throw new Error("missing declarations");
    const grants = new Set(declarations.grants);

    // The body agent's tool grant and the body action's effect grant both
    // surface on the loop node so the approval gate sees them.
    expect(grants.has("tool:mail_send")).toBe(true);
    expect(grants.has("effect:git:commit")).toBe(true);
  });

  test("actions and agent steps each carry their own grants", () => {
    const registry = createDefaultDirectorRegistry();
    const agent = makeTrivialAgent();
    const workflow = defineWorkflow({
      id: "wf_mixed",
      trigger: { type: "manual" },
      steps: {
        plan: step({ agent }),
        commit: action({
          handler: "commit",
          effect: { requires: ["git:commit"] },
          after: ["plan"],
        }),
      },
    });

    const walk = walkCapabilities(workflow, registry);
    const planGrants = walk.perStep.get("plan");
    const commitGrants = walk.perStep.get("commit");
    if (planGrants === undefined || commitGrants === undefined) {
      throw new Error("missing declarations");
    }

    // The agent step carries tool grants but no effect grants.
    expect(planGrants.grants.some((g) => g.startsWith("tool:"))).toBe(true);
    expect(planGrants.grants.some((g) => g.startsWith("effect:"))).toBe(false);
    // The action carries its effect grant but no agent grants.
    expect(commitGrants.grants).toContain("effect:git:commit");
    expect(commitGrants.grants.some((g) => g.startsWith("tool:"))).toBe(false);
  });

  test("carries per-tool effects: gated -> ask, ungated -> allow", () => {
    const registry = createDefaultDirectorRegistry();
    const agent = defineAgent({
      id: "ag_effects",
      systemPrompt: "gated + ungated tools",
      tools: [
        makeFactory("@intx/tools-posix/sidecar-bundle", [
          { name: "run_shell", approval: "ask" },
          { name: "list_dir" },
        ]),
      ],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const workflow = defineWorkflow({
      id: "wf_effects",
      agent,
      trigger: { type: "manual" },
    });

    const walk = walkCapabilities(workflow, registry);
    const stepId = workflow.stepOrder[0];
    if (stepId === undefined) throw new Error("missing step");
    const declarations = walk.perStep.get(stepId);
    if (declarations === undefined) throw new Error("missing declarations");

    expect(declarations.grants).toContain("tool:run_shell");
    expect(declarations.grants).toContain("tool:list_dir");
    expect(declarations.grantEffects.get("tool:run_shell")).toBe("ask");
    expect(declarations.grantEffects.get("tool:list_dir")).toBe("allow");
    // grantEffects covers TOOL grants only: no director/capability/etc.
    // key leaks in.
    for (const key of declarations.grantEffects.keys()) {
      expect(key.startsWith("tool:")).toBe(true);
    }
  });

  test("ask wins when loop body siblings share a tool name", () => {
    // Two agent steps in one loop body declare the same bare tool name:
    // the first gates it (`approval: "ask"`), the second (later in order)
    // leaves it ungated. The loop threads one GrantSet across its body, so
    // the two write the same `tool:run_shell` key; the ask mark must not be
    // downgraded to `allow` by the later sibling.
    const registry = createDefaultDirectorRegistry();
    const gatedAgent = defineAgent({
      id: "ag_gated",
      systemPrompt: "gates run_shell",
      tools: [
        makeFactory("@vendor/a/main", [{ name: "run_shell", approval: "ask" }]),
      ],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const ungatedAgent = defineAgent({
      id: "ag_ungated",
      systemPrompt: "leaves run_shell ungated",
      tools: [makeFactory("@vendor/b/main", [{ name: "run_shell" }])],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const body = defineWorkflow({
      id: "loop-body",
      trigger: { type: "manual" },
      steps: {
        gate: step({ agent: gatedAgent }),
        pass: step({ agent: ungatedAgent, after: ["gate"] }),
      },
    });
    const workflow = defineWorkflow({
      id: "wf_ask_wins",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body,
          while: "w",
          carry: "c",
          maxIterations: 3,
          onExhausted: "esc",
        }),
        esc: step({ agent: makeTrivialAgent(), after: ["rework"] }),
      },
    });

    const walk = walkCapabilities(workflow, registry);
    const declarations = walk.perStep.get("rework");
    if (declarations === undefined) throw new Error("missing declarations");
    expect(declarations.grantEffects.get("tool:run_shell")).toBe("ask");
  });

  test("throws on a duplicate tool name within a single factory", () => {
    const registry = createDefaultDirectorRegistry();
    const agent = defineAgent({
      id: "ag_intra_dup",
      systemPrompt: "duplicate names in one factory",
      tools: [
        makeFactory("@vendor/dup/main", [
          { name: "search" },
          { name: "search" },
        ]),
      ],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const workflow = defineWorkflow({
      id: "wf_intra_dup",
      agent,
      trigger: { type: "manual" },
    });

    expect(() => walkCapabilities(workflow, registry)).toThrow(
      DuplicateWalkToolError,
    );
  });

  test("throws when two factories in one agent mint the same tool name", () => {
    const registry = createDefaultDirectorRegistry();
    const agent = defineAgent({
      id: "ag_cross_dup",
      systemPrompt: "same final tool name across two factories",
      tools: [
        makeFactory("@vendor/a/main", [{ name: "search" }]),
        makeFactory("@vendor/b/main", [{ name: "search" }]),
      ],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const workflow = defineWorkflow({
      id: "wf_cross_dup",
      agent,
      trigger: { type: "manual" },
    });

    expect(() => walkCapabilities(workflow, registry)).toThrow(
      DuplicateWalkToolError,
    );
  });
});
