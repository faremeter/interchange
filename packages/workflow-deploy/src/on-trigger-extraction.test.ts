// The deploy step materializes each onTrigger section's authored inline body
// into its own workflow asset and rewrites the primitive to a ref, so the
// runtime spawns the body as a child run resolved by ref (the childWorkflow
// production path). The extraction also pins each body's per-step inference
// sources (gated against the operator-approved set) and rejects a tool-bearing
// body agent (INTR-310 defers body tool trees). This exercises the extraction
// transform in isolation with a mock workflow-asset writer.

import { describe, test, expect } from "bun:test";

import {
  createDefaultDirectorRegistry,
  defineAgent,
  type AnnotatedToolFactory,
  type BaseEnv,
} from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";

import { defineWorkflow, onTrigger, step } from "@intx/workflow/definition";

import {
  extractOnTriggerBodies,
  WorkflowDefinitionInvalidError,
  type WorkflowRepoWriter,
} from "./orchestrator";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
}

// A body agent that declares a tool, so its `toolFactories` is non-empty and
// the tool-bearing-body guard rejects it.
function makeToolFactory(): AnnotatedToolFactory<BaseEnv> {
  const factory = (_env: BaseEnv) => ({
    definitions: [],
    run: () =>
      Promise.resolve({ callId: "", content: "", isError: false as const }),
  });
  return Object.assign(factory, {
    id: "@intx/tools-demo/bundle",
    requires: [] as readonly string[],
    definitions: [{ name: "demo_tool" }],
  });
}

function makeToolAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [makeToolFactory()],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
}

// The body agent's (anthropic, m) source, both the chain head and the default,
// so `pinBodySources` resolves the body step's pin.
const BODY_SOURCE = {
  id: "src-anthropic-m",
  provider: "anthropic",
  baseURL: "https://api.example/anthropic",
  apiKey: "secret",
  model: "m",
};

const CONFIG: HarnessConfig = {
  sessionId: "ses-extract",
  agentId: "ag_extract",
  tenantId: "tenant-1",
  principalId: "prin-1",
  agentAddress: "run_extract@workflow.interchange",
  systemPrompt: "s",
  tools: [],
  grants: [],
  sources: [BODY_SOURCE],
  defaultSource: "src-anthropic-m",
};

// The operator approved the body agent's (provider, model): the top-level walk
// ran on the inline form, so the body agent's source grant is in the set.
const APPROVALS = new Set<string>(["inference.source:anthropic:m"]);

describe("extractOnTriggerBodies", () => {
  test("deploys a section body as its own asset and rewrites the primitive to a ref", async () => {
    const writes = new Map<string, ReadonlyMap<string, string>>();
    const writer: WorkflowRepoWriter = {
      writeWorkflowRepo: async ({ workflowRepoId, files }) => {
        writes.set(workflowRepoId, files);
      },
    };
    const body = defineWorkflow({
      id: "authored-body-id",
      trigger: { type: "manual" },
      steps: { work: step({ agent: makeAgent("a") }) },
    });
    const workflow = defineWorkflow({
      id: "wf",
      steps: {
        section: onTrigger({ on: { type: "mail", to: "s@x.example" }, body }),
      },
    });

    const { workflow: deployed, referencedDefinitions } =
      await extractOnTriggerBodies({
        workflow,
        registry: createDefaultDirectorRegistry(),
        workflowRepo: writer,
        config: CONFIG,
        operatorApprovals: APPROVALS,
      });

    // The body landed as its own workflow asset, keyed by the derived ref
    // (parent id + section step id), and its stored id is that ref so
    // spawnChild resolves it by ref.
    const bodyRef = "wf__section";
    expect([...writes.keys()]).toEqual([bodyRef]);
    const bodyJson = writes.get(bodyRef)?.get("workflow.json");
    expect(bodyJson).toBeDefined();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test parses its own JSON fixture
    const parsed = JSON.parse(bodyJson ?? "{}") as { id: string };
    expect(parsed.id).toBe(bodyRef);

    // The section primitive now references the asset instead of inlining it.
    const section = deployed.steps.section;
    expect(section?.kind).toBe("onTrigger");
    if (section?.kind === "onTrigger") {
      expect(section.body).toEqual({ ref: bodyRef });
    }

    // The extracted body rides back with its own per-step source pins so the
    // deploy frame can carry both to the sidecar (the hub-stored copy is not on
    // disk). The body's sources cover its own stepOrder, keyed by body step id.
    expect(referencedDefinitions.map((d) => d.definition.id)).toEqual([
      bodyRef,
    ]);
    expect(referencedDefinitions[0]?.sources).toEqual({ work: [BODY_SOURCE] });
  });

  test("rejects a body whose agent declares tools (INTR-310 defers body tool trees)", async () => {
    const writer: WorkflowRepoWriter = {
      writeWorkflowRepo: async () => {
        // The body asset write happens before the source-pin/tool guard, so a
        // rejected deploy may still have written the asset; the write itself is
        // not the assertion here.
      },
    };
    const body = defineWorkflow({
      id: "tool-body-id",
      trigger: { type: "manual" },
      steps: { work: step({ agent: makeToolAgent("a") }) },
    });
    const workflow = defineWorkflow({
      id: "wf-tools",
      steps: {
        section: onTrigger({ on: { type: "mail", to: "s@x.example" }, body }),
      },
    });

    await expect(
      extractOnTriggerBodies({
        workflow,
        registry: createDefaultDirectorRegistry(),
        workflowRepo: writer,
        config: CONFIG,
        operatorApprovals: APPROVALS,
      }),
    ).rejects.toBeInstanceOf(WorkflowDefinitionInvalidError);
  });

  test("leaves a workflow with no onTrigger section unchanged", async () => {
    const writer: WorkflowRepoWriter = {
      writeWorkflowRepo: async () => {
        throw new Error("no onTrigger body: should not write an asset");
      },
    };
    const workflow = defineWorkflow({
      id: "wf-plain",
      trigger: { type: "manual" },
      steps: { s: step({ agent: makeAgent("a") }) },
    });

    const { workflow: deployed, referencedDefinitions } =
      await extractOnTriggerBodies({
        workflow,
        registry: createDefaultDirectorRegistry(),
        workflowRepo: writer,
        config: CONFIG,
        operatorApprovals: APPROVALS,
      });

    expect(deployed).toBe(workflow);
    expect(referencedDefinitions).toEqual([]);
  });
});
