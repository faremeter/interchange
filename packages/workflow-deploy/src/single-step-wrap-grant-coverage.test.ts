// Regression coverage for H-D1: the single-step wrap path must surface
// the HarnessConfig's tool surface to the capability walk so the
// operator-approval gate gates `tool:<name>` grants. Before the fix,
// `wrapHarnessAsSingleStepWorkflow` emitted `toolFactories: []` regardless
// of `HarnessConfig.tools`, and the walk emitted zero `tool:` grants.
// The gate then admitted every single-step deploy unconditionally,
// defeating the agent-deploy uniformity claim's substance at the
// approval layer.
//
// The fix widens the wrap to propagate `HarnessConfig.tools[i].name`
// into the synthesized agent's `toolFactories[i].id`, so the existing
// walk surfaces `tool:<name>` grants the gate can deny.

import { describe, test, expect } from "bun:test";

import {
  createDefaultDirectorRegistry,
  type AgentDefinition,
  type BaseEnv,
} from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import { defineWorkflow } from "@intx/workflow/definition";

import { walkCapabilities } from "./capability-walk";
import { wrapHarnessAsSingleStepWorkflow } from "./orchestrator";

const HARNESS_CONFIG_WITH_TOOLS: HarnessConfig = {
  sessionId: "ses-1",
  agentId: "legacy-agent",
  tenantId: "tenant-1",
  principalId: "prin-1",
  agentAddress: "ins_legacy@integration.interchange",
  systemPrompt: "legacy",
  tools: [
    {
      name: "sketchy_tool",
      description: "operator never approved this",
      inputSchema: {},
    },
    {
      name: "other_tool",
      description: "another tool the wrap must surface",
      inputSchema: {},
    },
  ],
  grants: [],
  sources: [
    {
      id: "src-1",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      apiKey: "k",
      model: "model-1",
    },
  ],
  defaultSource: "src-1",
};

describe("H-D1: single-step wrap surfaces HarnessConfig.tools to the walk", () => {
  test("walk emits a tool: grant per HarnessConfig.tools entry", () => {
    const singleStepAgent: AgentDefinition<BaseEnv> =
      wrapHarnessAsSingleStepWorkflow({
        config: HARNESS_CONFIG_WITH_TOOLS,
        deployContent: { systemPrompt: "legacy" },
      });
    expect(singleStepAgent.toolFactories.length).toBe(2);
    const factoryIds = singleStepAgent.toolFactories.map((f) => f.id);
    expect(factoryIds).toContain("sketchy_tool");
    expect(factoryIds).toContain("other_tool");

    const workflow = defineWorkflow({
      id: "wf_single_step",
      agent: singleStepAgent,
      trigger: { type: "mail", to: HARNESS_CONFIG_WITH_TOOLS.agentAddress },
    });
    const walk = walkCapabilities(workflow, createDefaultDirectorRegistry());
    const stepId = workflow.stepOrder[0];
    if (stepId === undefined) throw new Error("missing step");
    const declarations = walk.perStep.get(stepId);
    if (declarations === undefined) throw new Error("missing decls");
    const toolGrants = declarations.grants.filter((g) => g.startsWith("tool:"));
    expect(toolGrants.sort()).toEqual(
      ["tool:other_tool", "tool:sketchy_tool"].sort(),
    );
    // Director grant still surfaces (default director).
    expect(declarations.grants.some((g) => g.startsWith("director:"))).toBe(
      true,
    );
  });

  test("walk emits no tool: grants when HarnessConfig.tools is empty", () => {
    const singleStepAgent = wrapHarnessAsSingleStepWorkflow({
      config: { ...HARNESS_CONFIG_WITH_TOOLS, tools: [] },
      deployContent: { systemPrompt: "legacy" },
    });
    expect(singleStepAgent.toolFactories).toEqual([]);

    const workflow = defineWorkflow({
      id: "wf_single_step_empty",
      agent: singleStepAgent,
      trigger: { type: "mail", to: HARNESS_CONFIG_WITH_TOOLS.agentAddress },
    });
    const walk = walkCapabilities(workflow, createDefaultDirectorRegistry());
    const stepId = workflow.stepOrder[0];
    if (stepId === undefined) throw new Error("missing step");
    const declarations = walk.perStep.get(stepId);
    if (declarations === undefined) throw new Error("missing decls");
    const toolGrants = declarations.grants.filter((g) => g.startsWith("tool:"));
    expect(toolGrants).toEqual([]);
  });
});
