// In-process end-to-end thread of the approval snapshot, no DB, no subprocess.
//
// This drives the REAL step-invoker adapter (wrapping the REAL `createAgent`)
// inside a REAL `runtimeRun`, with a real authz `ask` suspension, and asserts
// the snapshot the authz extension builds reaches `env.onPark`/`WorkflowPark`.
// It chains: authz-ask -> reactor gate.blocked -> agent SendResult -> step
// invoker StepInvokeResult -> parkOnSignal -> onPark, closing the gap between
// the agent-layer test (which stops at SendResult) and the workflow park-notify
// test (which starts from a mock suspend).
//
// CEILING: this stops at `onPark`. The remaining hops -- park.notify IPC frame
// -> supervisor registration -> sidecar->hub register frame -> the DB co-write
// -- cross the workflow-process child subprocess boundary and terminate in
// Postgres. They are covered separately (the supervisor park-notify test, the
// hub-link frame test, and the CI-gated real-DB co-write test). Do NOT extend
// this test across the subprocess boundary; reach for the deploy harness for
// that.

import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type } from "arktype";

import {
  createDirectorRegistry,
  defineAgent,
  defineDirector,
  defineTool,
  type BaseEnv,
} from "@intx/agent";
import { noopAuditStore } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit";
import type {
  ReactorCapabilities,
  ReactorDirector,
  ReactorInboundEvent,
  ReactorState,
} from "@intx/types/runtime";
import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  step,
  type WorkflowPark,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

import { createWorkflowStepInvoker, type StepEnvBase } from "./step-invoker";

const TOOL_NAME = "charge_card";
const TOOL_DESCRIPTION = "Charge the customer's card";
const TOOL_ARGS = { amount: 100 };
const INPUT_SCHEMA = {
  type: "object",
  properties: { amount: { type: "number" } },
};

// Counts how many times the tool's `run` fires within a single `runOnce`.
// `runOnce` resets it. It is the load-bearing witness that distinguishes the
// two arms: on `ask` the authz gate suspends BEFORE `run` (count stays 0); on
// `allow` the tool actually executes (count reaches 1). Without it, an empty
// `parks` is ambiguous -- a failed or tool-less run yields the same emptiness.
let toolRunCount = 0;

const chargeTool = defineTool<BaseEnv>({
  id: "@intx-test/e2e/charge_card",
  definitions: [{ name: TOOL_NAME }],
  factory: () => ({
    definitions: [
      {
        name: TOOL_NAME,
        description: TOOL_DESCRIPTION,
        inputSchema: INPUT_SCHEMA,
      },
    ],
    async run(call) {
      toolRunCount += 1;
      return { callId: call.id, content: "charged", isError: false };
    },
  }),
});

// Issues the tool call on the inbound message, before any inference -- so the
// authz decision fires and (on `ask`) the step parks with no model call.
const toolExecDirector: ReactorDirector = {
  async decide(
    event: ReactorInboundEvent,
    _state: ReactorState,
    caps: ReactorCapabilities,
  ) {
    if (event.type === "message.received") {
      return caps.executeTools([
        { id: "call-charge", name: TOOL_NAME, arguments: TOOL_ARGS },
      ]);
    }
    return caps.done();
  },
};

function directorRegistry() {
  const defined = defineDirector({
    id: "@intx-test/e2e/tool-exec",
    configSchema: type({}),
    factory: () => toolExecDirector,
  });
  return createDirectorRegistry({
    factories: [defined.factory],
    defaultId: defined.factory.id,
  });
}

const SOURCE = {
  id: "anthropic:e2e",
  provider: "anthropic",
  baseURL: "http://localhost:1",
  apiKey: "test-key",
  model: "claude-test",
};

const agentDef = defineAgent({
  id: "e2e-approval-agent",
  systemPrompt: "test",
  tools: [chargeTool],
  capabilities: [],
  inference: {
    sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
  },
});

const workflowDef = defineWorkflow({
  id: "e2e-approval",
  trigger: { type: "manual" },
  steps: { s: step({ agent: agentDef }) },
});

// Drive the real step-invoker + real agent inside a real run with the authz
// extension returning `effect` for the tool. Returns the parks captured at
// `onPark`. The run is cancelled after the park so the assertion does not
// depend on a resume/re-dispatch cycle.
async function runOnce(
  effect: "ask" | "allow",
): Promise<{ parks: WorkflowPark[]; toolRuns: number }> {
  toolRunCount = 0;
  const workdirs: string[] = [];
  const parks: WorkflowPark[] = [];

  const invokeStep = createWorkflowStepInvoker({
    workflowAuthorize: async () => ({
      effect,
      matchingGrants: [],
      resolvedBy: null,
    }),
    buildEnv: async (): Promise<StepEnvBase> => {
      const workdir = mkdtempSync(join(tmpdir(), "e2e-approval-"));
      workdirs.push(workdir);
      const storage = await createIsogitStore(workdir);
      return {
        sources: [SOURCE],
        defaultSource: SOURCE.id,
        storage,
        workdir,
        audit: noopAuditStore(),
        directors: directorRegistry(),
      };
    },
    // agentFactory defaults to the real `createAgent`.
  });

  const repoStore = createInMemoryRepoStore();
  const clock = (): Date => new Date();
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs: createInMemoryBlobSubstrate(),
    directors: directorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(workflowDef),
    onPark: (park) => parks.push(park),
  };

  const handle = runtimeRun(workflowDef, env, { runId: `run-${effect}` });
  // Let the step build the agent, dispatch the tool, hit the ask gate, and park.
  await new Promise((r) => setTimeout(r, 250));
  await handle.cancel("self", "captured park");
  await handle.complete.catch(() => undefined);
  for (const dir of workdirs) rmSync(dir, { recursive: true, force: true });
  return { parks, toolRuns: toolRunCount };
}

describe("approval snapshot in-process end-to-end thread", () => {
  test("a real ask suspension threads the tool snapshot to onPark", async () => {
    const { parks, toolRuns } = await runOnce("ask");

    // The ask gate suspends before the tool body: the call is held for
    // approval, not executed. This is what makes the park meaningful.
    expect(toolRuns).toBe(0);
    expect(parks).toHaveLength(1);
    const park = parks[0];
    if (park === undefined) throw new Error("unreachable");
    expect(park.kind).toBe("approval");
    expect(park.approvalSnapshot).toEqual({
      name: TOOL_NAME,
      description: TOOL_DESCRIPTION,
      inputSchema: INPUT_SCHEMA,
      arguments: TOOL_ARGS,
    });
  });

  test("an allowed tool runs without parking or a snapshot", async () => {
    // The counterpart to the ask arm. With an `allow` decision the authz gate
    // does not suspend, so the tool body actually runs (`toolRuns` reaches 1)
    // and no control-plane park fires -- the thread parks and snapshots only on
    // the ask rail. Asserting `toolRuns === 1` is what makes the empty `parks`
    // load-bearing: it proves the allow decision was exercised and the tool
    // reached execution, distinguishing this from a run that failed or never
    // dispatched the call (both of which would also leave `parks` empty).
    //
    // The run's own terminal outcome is deliberately not asserted: after the
    // tool returns, the real agent proceeds to an inference cycle against an
    // unreachable source and is torn down by the cancel. That is orthogonal to
    // the park/snapshot invariant under test, which is settled by the time the
    // tool has run.
    const { parks, toolRuns } = await runOnce("allow");
    expect(toolRuns).toBe(1);
    expect(parks).toEqual([]);
  });
});
