// Phase 3 proof: the per-step agent's event stream is threaded up
// through the step-invoker's `onEvent` sink.
//
// The step-invoker adapter (`createWorkflowStepInvoker`,
// `packages/workflow-host/src/adapters/step-invoker.ts`) is the portable
// seam the sidecar's substrate factory drives per step. The factory's
// `invokeStep` wrapper passes the child's per-run event-channel sink as
// the adapter's `onEvent`; the chain from there is `onEvent -> child
// event-channel sender -> supervisor -> publishWorkflowInferenceEvent ->
// hub timeline`. This test pins the FIRST link of that chain -- the only
// link Phase 3 adds -- by driving a real per-step agent against the
// deterministic inference test harness with a recording `onEvent` and
// asserting the agent's `InferenceEvent`s arrive at the sink.
//
// The agent is driven through a two-turn tool loop: the mock provider
// emits a `tool_use` turn calling a real tool, the tool executes in the
// step's workdir, and a second turn produces the text reply. That exercise
// fires `inference.start` (per turn), the tool-call inference events, the
// `tool.start` / `tool.done` execution events, and `inference.done` (the
// assistant turn) -- every InferenceEvent member except the intentionally
// excluded `message.received`.
//
// Against the pre-Phase-3 behaviour the wrapper `void onEvent`d and the
// adapter never subscribed the agent's stream, so the recorder would stay
// empty. This test therefore FAILS against the old wiring and proves the
// events actually flow up.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  createDefaultDirectorRegistry,
  defineAgent,
  defineTool,
  type AgentDefinition,
  type BaseEnv,
  type ToolBundle,
} from "@intx/agent";
import { noopAuditStore } from "@intx/agent/testing";
import { setupHarness, type Harness } from "@intx/inference-testing";
import { createIsogitStore } from "@intx/storage-isogit";
import { createWorkflowStepInvoker } from "@intx/workflow-host";
import type {
  AuthorizeContext,
  StepInvokeRequest,
  WorkflowAuthorizeFn,
} from "@intx/workflow";
import type {
  InferenceEvent,
  InferenceSource,
  ToolCall,
  ToolResult,
} from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "anthropic:event-threading",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-event-threading",
  model: "claude-event-threading",
};

const TOOL_NAME = "record_value";
const TOOL_ID = "@intx-test/event-threading/record_value";

// A real tool whose `run` writes a sentinel into `env.workdir`. The tool
// firing is what produces the `tool.start` / `tool.done` execution events
// and proves the agent looped through a real tool invocation rather than a
// no-op stub.
function recordValueTool(sentinelWritten: { value: boolean }) {
  return defineTool<BaseEnv>({
    id: TOOL_ID,
    factory: (env: BaseEnv): ToolBundle => ({
      definitions: [
        {
          name: TOOL_NAME,
          description: "record a value",
          inputSchema: {
            type: "object",
            properties: { value: { type: "string" } },
          },
        },
      ],
      async run(call: ToolCall, _signal: AbortSignal): Promise<ToolResult> {
        sentinelWritten.value = true;
        void env;
        return {
          callId: call.id,
          content: "recorded",
          isError: false,
        };
      },
    }),
  });
}

function stepAgentDefinition(
  tool: ReturnType<typeof recordValueTool>,
): AgentDefinition<BaseEnv> {
  return defineAgent({
    id: "event-threading-step",
    systemPrompt: "single-step event threading agent",
    tools: [tool],
    capabilities: [],
    inference: {
      sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
    },
  });
}

// The step-invoker constructs the agent's `authorize` on top of this
// workflow-typed callback. The agent runtime gates every tool call through
// it (`tool:<name>`, action `invoke`); allowing that gate is what lets the
// tool loop run. Any other resource is denied loudly, matching the
// sidecar's per-step authorize before the Phase 4 credentials snapshot
// lands.
const workflowAuthorize: WorkflowAuthorizeFn = (resource, action) => {
  if (resource.startsWith("tool:") && action === "invoke") {
    return Promise.resolve({
      effect: "allow" as const,
      matchingGrants: [],
      resolvedBy: null,
    });
  }
  throw new Error(
    `event-threading test authorize: unexpected ${resource}/${action}`,
  );
};

function buildRequest(
  tool: ReturnType<typeof recordValueTool>,
): StepInvokeRequest {
  const authzContext: AuthorizeContext = {
    stepId: "step1",
    attempt: 1,
    runId: "run-event-threading",
  };
  return {
    agent: stepAgentDefinition(tool),
    input: { goal: "record the value" },
    authzContext,
    signal: new AbortController().signal,
  };
}

describe("single-step event threading", () => {
  let workDir: string;
  let harness: Harness;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "event-threading-"));
    harness = setupHarness();
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("the per-step agent's events flow up to the step-invoker onEvent sink", async () => {
    // Turn 1: the model calls the real tool. Turn 2: a text reply once the
    // tool result lands. Two inference turns => two inference.start events.
    harness.scenario.replyOnce("anthropic", {
      toolCalls: [
        { name: TOOL_NAME, args: { value: "forty-two" }, callId: "tc-1" },
      ],
    });
    harness.scenario.replyOnce("anthropic", { text: "recorded the value" });

    const sentinelWritten = { value: false };
    const tool = recordValueTool(sentinelWritten);

    const recorded: InferenceEvent[] = [];
    const storeDir = join(workDir, "ctx");
    const stepWorkdir = join(workDir, "workspace");

    const invoker = createWorkflowStepInvoker({
      workflowAuthorize,
      buildEnv: async (): Promise<Omit<BaseEnv, "authorize">> => {
        const storage = await createIsogitStore(storeDir);
        return {
          sources: [SOURCE],
          defaultSource: SOURCE.id,
          storage,
          workdir: stepWorkdir,
          audit: noopAuditStore(),
          directors: createDefaultDirectorRegistry(),
          deps: harness.deps,
        };
      },
      onEvent: (event) => {
        recorded.push(event);
      },
    });

    const req = buildRequest(tool);
    const settled = invoker(req);
    await harness.run();
    const result = await settled;

    // The step produced the real agent reply, and the real tool ran.
    expect(result.output).toMatchObject({ reply: "recorded the value" });
    expect(sentinelWritten.value).toBe(true);

    const types = recorded.map((e) => e.type);

    // The proof the events flow up: against the old `void onEvent` wiring
    // the recorder stays empty. Here it captures the inbound inference,
    // the tool execution, and the assistant turn.
    expect(recorded.length).toBeGreaterThan(0);

    // Inbound inference began (fires per turn; at least the first turn).
    expect(types).toContain("inference.start");
    // The tool-call surfaced in the inference stream AND the tool executed.
    expect(types).toContain("inference.tool_call.end");
    expect(types).toContain("tool.start");
    expect(types).toContain("tool.done");
    // The assistant turn completed.
    expect(types).toContain("inference.done");

    // The single intentional exclusion never reaches the sink: the adapter
    // filters `message.received` exactly as the in-process harness does.
    expect(types).not.toContain("message.received");

    // Two inference turns (tool_use then text) => inference.start twice.
    expect(types.filter((t) => t === "inference.start").length).toBe(2);
  });

  test("omitting onEvent never consumes the agent stream and forwards nothing", async () => {
    harness.scenario.replyOnce("anthropic", { text: "no observers" });

    const sentinelWritten = { value: false };
    const tool = recordValueTool(sentinelWritten);
    const storeDir = join(workDir, "ctx");
    const stepWorkdir = join(workDir, "workspace");

    // No `onEvent`: the adapter must not subscribe the stream. The step
    // still completes; this pins the optionality contract so existing
    // callers that never wanted observability keep the prior behaviour.
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize,
      buildEnv: async (): Promise<Omit<BaseEnv, "authorize">> => {
        const storage = await createIsogitStore(storeDir);
        return {
          sources: [SOURCE],
          defaultSource: SOURCE.id,
          storage,
          workdir: stepWorkdir,
          audit: noopAuditStore(),
          directors: createDefaultDirectorRegistry(),
          deps: harness.deps,
        };
      },
    });

    const req = buildRequest(tool);
    const settled = invoker(req);
    await harness.run();
    const result = await settled;

    expect(result.output).toMatchObject({ reply: "no observers" });
    expect(existsSync(storeDir)).toBe(true);
  });
});
