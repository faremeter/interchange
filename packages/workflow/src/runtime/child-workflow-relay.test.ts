// A childWorkflow stays unaddressable. The parent run relays each park the
// child surfaces: an author awaitSignal, an approval carrying the body's
// snapshot, and a trigger-budget input park. The relay climbs through a
// nested childWorkflow and through a loop inside the child. A loop body
// still rejects an input re-arm, and a run holds one input park. The spawn
// step awaits one signal at a time, so a second park that is already live
// stays queued until the first decision is relayed. A queued park whose
// gate has already moved on is not given a signal that arrived while it
// waited. An untimed relay is
// refused when the parent's own flag says nothing upstream can answer it,
// and that parent writes no suspension. A child that dies during an
// approval relay fails the spawn step. A parked childWorkflow does not
// survive a restart: resume rejects the step left awaiting-signal and does
// not spawn the child again.

import { describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { signalName } from "@intx/types";
import type { ApprovalSnapshot } from "@intx/types/runtime";

import {
  awaitSignal,
  childWorkflow,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  createSpawnLoopIteration,
  createSuspendableChildHandle,
  defineWorkflow,
  enumerateInlineLoopBodies,
  loop,
  rewriteInlineChildWorkflowBodies,
  runLocal,
  runtimeRun,
  sleep,
  step,
  type LoopFnRegistry,
  type RepoStore,
  type RuntimeWorkflowRun,
  type SpawnChildWorkflow,
  type SignalChannel,
  type StepInvoker,
  type WorkflowAuthorizeFn,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowPark,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { waitForEvent } from "@intx/workflow/testing";

import { createInMemorySpawnChild } from "../runlocal/run-local";
import { commit } from "./commit-chain";

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
});

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

const snapshot: ApprovalSnapshot = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

const at = new Date().toISOString();

// The parent drive pulls one park at a time. Returning the handle only after
// every named child await is durable leaves the others queued for that first
// pull, so the relay has to finish the open park before it reads the next.
function holdSpawnUntilChildAwaits(
  childBodies: ReadonlyMap<string, WorkflowDefinition>,
  invokeStep: StepInvoker,
  parkNames: readonly string[],
): SpawnChildWorkflow {
  return async (input) => {
    if (input.signal.aborted) {
      const reason: unknown = input.signal.reason;
      throw reason instanceof Error
        ? reason
        : new DOMException("aborted", "AbortError");
    }
    const body = childBodies.get(input.definitionRef);
    if (body === undefined) {
      throw new Error(
        `childWorkflow ${input.definitionRef} has no lifted definition`,
      );
    }
    const { workflow: rewritten } = rewriteInlineChildWorkflowBodies(body);
    const repoStore = createInMemoryRepoStore();
    const clock = (): Date => new Date();
    const newId = (prefix: string): string =>
      `${prefix}-${Math.random().toString(36).slice(2, 8)}`;
    const env: WorkflowRuntimeEnv = {
      repoStore,
      scheduler: createInMemoryScheduler({ repoStore, clock }),
      signalChannel: createInMemorySignalChannel({ newId: () => newId("sig") }),
      blobs: createInMemoryBlobSubstrate(),
      directors: createDefaultDirectorRegistry(),
      authorize: allowAll,
      invokeStep,
      spawnChild: () =>
        Promise.reject(new Error("this test child does not spawn")),
      clock,
      newId,
      drain: createNoopDrainController(rewritten),
      hasUpstreamSignalResolver: true,
    };
    const handle = createSuspendableChildHandle(env, {
      definition: rewritten,
      childRunId: input.childRunId,
      input: input.input,
      depth: input.depth,
      maxChildSpawnDepth: input.maxChildSpawnDepth,
      signal: input.signal,
      surfaceInputPark: true,
    });
    for (const name of parkNames) {
      await waitForEvent(
        repoStore,
        input.childRunId,
        (event) => event.kind === "SignalAwaited" && event.signalName === name,
      );
    }
    return handle;
  };
}

function loopFnsReturning(cont: boolean): LoopFnRegistry {
  return (ref) => {
    if (ref === "cont") return () => cont;
    if (ref === "next") return () => null;
    throw new Error(`unknown loop fn ${ref}`);
  };
}

function spawnOutput(output: unknown): {
  childRunId: string;
  terminalStatus: string;
} {
  if (
    typeof output === "object" &&
    output !== null &&
    "childRunId" in output &&
    typeof output.childRunId === "string" &&
    "terminalStatus" in output &&
    typeof output.terminalStatus === "string"
  ) {
    return {
      childRunId: output.childRunId,
      terminalStatus: output.terminalStatus,
    };
  }
  throw new Error("spawn output missing childRunId/terminalStatus");
}

function relayed(
  events: readonly WorkflowEvent[],
  stepId: string,
  parkKind: "approval" | "input" | "signal-relay",
  signal: string,
): WorkflowEvent | undefined {
  return events.find(
    (event) =>
      event.kind === "SignalAwaited" &&
      event.stepId === stepId &&
      event.parkKind === parkKind &&
      event.signalName === signal,
  );
}

// runLocal does not expose the repo or the park sink. An approval or input
// park mints a reserved name, so the test has to read the parent's notify
// and the parent's log. The wiring matches the local spawner: inline
// children are lifted, the child forces its resolver flag, and a loop body
// is enumerated before the run starts.
function startRelayed(args: {
  definition: WorkflowDefinition;
  runId: string;
  invokeStep: StepInvoker;
  onPark?: (park: WorkflowPark) => void;
  loopFns?: LoopFnRegistry;
  holdUntilChildAwaits?: readonly string[];
  signalChannel?: SignalChannel;
  spawnChild?: SpawnChildWorkflow;
}): { run: RuntimeWorkflowRun; repoStore: RepoStore } {
  const repoStore = createInMemoryRepoStore();
  const clock = (): Date => new Date();
  const newId = (prefix: string): string =>
    `${prefix}-${Math.random().toString(36).slice(2, 8)}`;
  const { workflow: rewritten, bodies } = rewriteInlineChildWorkflowBodies(
    args.definition,
  );
  const childBodies = new Map(
    bodies.map((body) => [body.ref, body.definition]),
  );
  const loopBodies = new Map<string, WorkflowDefinition>();
  for (const loopBody of enumerateInlineLoopBodies(rewritten)) {
    const bodyRewrite = rewriteInlineChildWorkflowBodies(loopBody.definition);
    loopBodies.set(loopBody.ref, bodyRewrite.workflow);
    for (const grandchild of bodyRewrite.bodies) {
      childBodies.set(grandchild.ref, grandchild.definition);
    }
  }
  const signalChannel =
    args.signalChannel ??
    createInMemorySignalChannel({ newId: () => newId("sig") });
  const spawnChild =
    args.spawnChild ??
    (args.holdUntilChildAwaits !== undefined
      ? holdSpawnUntilChildAwaits(
          childBodies,
          args.invokeStep,
          args.holdUntilChildAwaits,
        )
      : createInMemorySpawnChild(childBodies, {
          authorize: allowAll,
          invokeStep: args.invokeStep,
          ...(args.loopFns !== undefined ? { loopFns: args.loopFns } : {}),
        }));
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel,
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: allowAll,
    invokeStep: args.invokeStep,
    spawnChild,
    clock,
    newId,
    drain: createNoopDrainController(rewritten),
    hasUpstreamSignalResolver: true,
    ...(args.onPark !== undefined ? { onPark: args.onPark } : {}),
    ...(args.loopFns !== undefined ? { loopFns: args.loopFns } : {}),
  };
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  return {
    run: runtimeRun(rewritten, env, { runId: args.runId }),
    repoStore,
  };
}

function parkSink(): {
  onPark: (park: WorkflowPark) => void;
  next: () => Promise<WorkflowPark>;
} {
  const queued: WorkflowPark[] = [];
  let wake: ((park: WorkflowPark) => void) | null = null;
  return {
    onPark: (park) => {
      const resolve = wake;
      wake = null;
      if (resolve !== null) {
        resolve(park);
        return;
      }
      queued.push(park);
    },
    next: () => {
      const park = queued.shift();
      if (park !== undefined) return Promise.resolve(park);
      return new Promise((resolve) => {
        wake = resolve;
      });
    },
  };
}

function gatedChild(id: string): WorkflowDefinition {
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: { hold: awaitSignal({ name: "go" }) },
  });
}

describe("childWorkflow signal relay", () => {
  test("relays an author signal onto the parent and completes the child", async () => {
    const parent = defineWorkflow({
      id: "author-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({ definition: gatedChild("author-child") }),
      },
    });
    const run = runLocal(parent, {
      runId: "run-author",
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
    });
    await run.signal("go", { done: true }, "sig-1");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(relayed(result.events, "spawn", "signal-relay", "go")).toBeDefined();
    const output = spawnOutput(result.outputs.spawn);
    expect(output.terminalStatus).toBe("completed");
    expect(output.childRunId.length).toBeGreaterThan(0);
  });

  test("climbs an author signal through a nested childWorkflow", async () => {
    const middle = defineWorkflow({
      id: "middle",
      trigger: { type: "manual" },
      steps: { inner: childWorkflow({ definition: gatedChild("leaf") }) },
    });
    const parent = defineWorkflow({
      id: "outer",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: middle }) },
    });
    const run = runLocal(parent, {
      runId: "run-grandchild",
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
    });
    await run.signal("go", { done: true }, "sig-1");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(relayed(result.events, "spawn", "signal-relay", "go")).toBeDefined();
    expect(spawnOutput(result.outputs.spawn).terminalStatus).toBe("completed");
  });

  test("relays a gate that sits inside a loop inside the child", async () => {
    const child = defineWorkflow({
      id: "loop-child",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: defineWorkflow({
            id: "loop-body",
            trigger: { type: "manual" },
            steps: { hold: awaitSignal({ name: "go" }) },
          }),
          while: "cont",
          carry: "next",
          input: { literal: 0 },
          maxIterations: 2,
          onExhausted: "fin",
        }),
        fin: step({ agent, after: ["rework"] }),
      },
    });
    const parent = defineWorkflow({
      id: "loop-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const run = runLocal(parent, {
      runId: "run-loop-gate",
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
      loopFns: loopFnsReturning(false),
    });
    await run.signal("go", { done: true }, "sig-1");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(relayed(result.events, "spawn", "signal-relay", "go")).toBeDefined();
    expect(spawnOutput(result.outputs.spawn).terminalStatus).toBe("completed");
  });

  test("relays an approval park with the body's snapshot", async () => {
    const child = defineWorkflow({
      id: "approval-child",
      trigger: { type: "manual" },
      steps: { hold: step({ agent }) },
    });
    const parent = defineWorkflow({
      id: "approval-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const sink = parkSink();
    let resumeKind: string | undefined;
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume === undefined) {
        return {
          suspend: {
            correlationId: "corr-body",
            kind: "approval",
            approvalSnapshot: snapshot,
          },
        };
      }
      resumeKind = req.resume.kind;
      return { output: null };
    };
    const { run } = startRelayed({
      definition: parent,
      runId: "run-approval",
      invokeStep,
      onPark: sink.onPark,
    });
    const park = await sink.next();
    expect(park).toEqual({
      runId: "run-approval",
      correlationId: "corr-body",
      parkKind: "approval",
      approvalSnapshot: snapshot,
    });

    await run.signal(
      signalName(park.correlationId),
      { outcome: "approved" },
      "sig-1",
    );
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(resumeKind).toBe("approval");
    expect(spawnOutput(result.outputs.spawn).terminalStatus).toBe("completed");
    expect(
      relayed(result.events, "spawn", "approval", signalName("corr-body")),
    ).toBeDefined();
  });

  test("relays a trigger-budget input park and delivers the next turn", async () => {
    const child = defineWorkflow({
      id: "input-child",
      trigger: { type: "manual" },
      steps: { chat: step({ agent, triggers: 2 }) },
    });
    const parent = defineWorkflow({
      id: "input-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const sink = parkSink();
    const decisions: unknown[] = [];
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume !== undefined) {
        decisions.push(req.resume.decision);
        expect(req.resume.kind).toBe("input");
      }
      return { output: null };
    };
    const { run } = startRelayed({
      definition: parent,
      runId: "run-input",
      invokeStep,
      onPark: sink.onPark,
    });
    const park = await sink.next();
    expect(park.runId).toBe("run-input");
    expect(park.parkKind).toBe("input");
    expect(park.approvalSnapshot).toBeUndefined();

    const nextTurn = { text: "next-turn" };
    await run.signal(signalName(park.correlationId), nextTurn, "sig-in");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(decisions).toEqual([nextTurn]);
    expect(
      relayed(result.events, "spawn", "input", signalName(park.correlationId)),
    ).toBeDefined();
  });

  test("a loop body that re-arms an input park fails the run", async () => {
    const parent = defineWorkflow({
      id: "loop-input",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: defineWorkflow({
            id: "loop-input-body",
            trigger: { type: "manual" },
            steps: { chat: step({ agent, triggers: 2 }) },
          }),
          while: "cont",
          carry: "next",
          input: { literal: 0 },
          maxIterations: 2,
          onExhausted: "fin",
        }),
        fin: step({ agent, after: ["rework"] }),
      },
    });
    const result = await runLocal(parent, {
      runId: "run-loop-input",
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
      loopFns: loopFnsReturning(false),
    }).complete;

    expect(result.terminalStatus).toBe("failed");
    const messages = result.events.flatMap((event) =>
      event.kind === "StepFailed" ? [event.error.message] : [],
    );
    expect(
      messages.some((message) =>
        message.includes("may not re-arm an input park"),
      ),
    ).toBe(true);
    expect(
      result.events.some(
        (event) => event.kind === "SignalAwaited" && event.parkKind === "input",
      ),
    ).toBe(false);
  });

  test("a second input park on the same run fails the child step", async () => {
    const child = defineWorkflow({
      id: "released-chat",
      trigger: { type: "manual" },
      steps: {
        hold: awaitSignal({ name: "release" }),
        chat: step({ agent, triggers: 2, after: ["hold"] }),
      },
    });
    const parent = defineWorkflow({
      id: "two-inputs",
      trigger: { type: "manual" },
      steps: {
        chat: step({ agent, triggers: 2 }),
        spawn: childWorkflow({ definition: child }),
      },
    });
    const { run, repoStore } = startRelayed({
      definition: parent,
      runId: "run-two-input",
      invokeStep: async () => ({ output: null }),
    });
    await waitForEvent(
      repoStore,
      "run-two-input",
      (event) =>
        event.kind === "SignalAwaited" &&
        event.parkKind === "input" &&
        event.stepId === "chat",
    );
    await waitForEvent(
      repoStore,
      "run-two-input",
      (event) =>
        event.kind === "SignalAwaited" &&
        event.parkKind === "signal-relay" &&
        event.signalName === "release" &&
        event.stepId === "spawn",
    );
    await run.signal("release", { ok: true }, "sig-release");
    const failed = await waitForEvent(
      repoStore,
      "run-two-input",
      (event) => event.kind === "StepFailed" && event.stepId === "spawn",
    );
    if (failed.kind !== "StepFailed") {
      throw new Error("expected StepFailed");
    }
    expect(failed.error.message).toContain("already awaiting an input park");
    await run.cancel("self", "test teardown");
    await run.complete;
  });

  test("refuses an untimed author relay when the parent has no upstream resolver", async () => {
    const parent = defineWorkflow({
      id: "false-author",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: gatedChild("child") }) },
    });
    const result = await runLocal(parent, {
      runId: "run-false-author",
      authorize: allowAll,
      hasUpstreamSignalResolver: false,
    }).complete;

    expect(result.terminalStatus).toBe("failed");
    expect(result.events.some((event) => event.kind === "SignalAwaited")).toBe(
      false,
    );
    const failed = result.events.find((event) => event.kind === "StepFailed");
    const message = failed?.kind === "StepFailed" ? failed.error.message : "";
    expect(message).toContain("go");
    expect(message).toContain("no upstream resolver");
  });

  test("refuses an approval relay when the parent has no upstream resolver", async () => {
    const child = defineWorkflow({
      id: "false-approval-child",
      trigger: { type: "manual" },
      steps: { hold: step({ agent }) },
    });
    const parent = defineWorkflow({
      id: "false-approval",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const invokeStep: StepInvoker = async () => ({
      suspend: {
        correlationId: "corr-body",
        kind: "approval",
        approvalSnapshot: snapshot,
      },
    });
    const result = await runLocal(parent, {
      runId: "run-false-approval",
      authorize: allowAll,
      hasUpstreamSignalResolver: false,
      invokeStep,
    }).complete;

    expect(result.terminalStatus).toBe("failed");
    expect(result.events.some((event) => event.kind === "SignalAwaited")).toBe(
      false,
    );
    const failed = result.events.find((event) => event.kind === "StepFailed");
    const message = failed?.kind === "StepFailed" ? failed.error.message : "";
    expect(message).toContain(signalName("corr-body"));
    expect(message).toContain("no upstream resolver");
  });

  test("a timed gate inside a loop inside the child still runs when the parent cannot be answered", async () => {
    const child = defineWorkflow({
      id: "timed-loop-child",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: defineWorkflow({
            id: "timed-loop-body",
            trigger: { type: "manual" },
            steps: {
              hold: awaitSignal({
                name: "go",
                timeout: 40,
                onTimeout: "done",
              }),
              done: step({ agent, after: ["hold"] }),
            },
          }),
          while: "cont",
          carry: "next",
          input: { literal: 0 },
          maxIterations: 2,
          onExhausted: "fin",
        }),
        fin: step({ agent, after: ["rework"] }),
      },
    });
    const parent = defineWorkflow({
      id: "timed-loop-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const result = await runLocal(parent, {
      runId: "run-timed-loop-child",
      authorize: allowAll,
      hasUpstreamSignalResolver: false,
      loopFns: loopFnsReturning(false),
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(relayed(result.events, "spawn", "signal-relay", "go")).toBeDefined();
  });

  test("two concurrent child input relays leave one park and fail the other", async () => {
    const leftChild = defineWorkflow({
      id: "left-chat",
      trigger: { type: "manual" },
      steps: { chat: step({ agent, triggers: 2 }) },
    });
    const rightChild = defineWorkflow({
      id: "right-chat",
      trigger: { type: "manual" },
      steps: { chat: step({ agent, triggers: 2 }) },
    });
    const parent = defineWorkflow({
      id: "two-chats",
      trigger: { type: "manual" },
      steps: {
        left: childWorkflow({ definition: leftChild }),
        right: childWorkflow({ definition: rightChild }),
      },
    });
    let started = 0;
    let release = (): void => {
      throw new Error("both children were not armed");
    };
    const bothStarted = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { run, repoStore } = startRelayed({
      definition: parent,
      runId: "run-two-chats",
      invokeStep: async () => {
        started += 1;
        if (started >= 2) release();
        await bothStarted;
        return { output: null };
      },
    });
    const failed = await waitForEvent(
      repoStore,
      "run-two-chats",
      (event) => event.kind === "StepFailed",
    );
    if (failed.kind !== "StepFailed") {
      throw new Error("expected StepFailed");
    }
    expect(failed.error.message).toContain("already awaiting an input park");
    const events = await repoStore.read("run-two-chats");
    const inputParks = events.filter(
      (event) => event.kind === "SignalAwaited" && event.parkKind === "input",
    );
    expect(inputParks).toHaveLength(1);
    await run.cancel("self", "test teardown");
    await run.complete;
  });

  test("a child that dies during an approval relay fails the spawn step", async () => {
    const child = defineWorkflow({
      id: "timeout-child",
      trigger: { type: "manual" },
      steps: { hold: step({ agent, timeout: 40 }) },
    });
    const parent = defineWorkflow({
      id: "timeout-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const invokeStep: StepInvoker = async () => ({
      suspend: {
        correlationId: "corr-timeout",
        kind: "approval",
        approvalSnapshot: snapshot,
      },
    });
    const result = await runLocal(parent, {
      runId: "run-timeout-approval",
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
      invokeStep,
    }).complete;

    expect(result.terminalStatus).toBe("failed");
    const failed = result.events.find(
      (event) => event.kind === "StepFailed" && event.stepId === "spawn",
    );
    expect(failed).toBeDefined();
  });

  test("relays an approval inside a loop inside the child onto the spawn step", async () => {
    const child = defineWorkflow({
      id: "loop-approval-child",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: defineWorkflow({
            id: "loop-approval-body",
            trigger: { type: "manual" },
            steps: { hold: step({ agent }) },
          }),
          while: "cont",
          carry: "next",
          input: { literal: 0 },
          maxIterations: 2,
          onExhausted: "fin",
        }),
        fin: step({ agent, after: ["rework"] }),
      },
    });
    const parent = defineWorkflow({
      id: "loop-approval-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const sink = parkSink();
    let suspended = false;
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume !== undefined || suspended) return { output: null };
      suspended = true;
      return {
        suspend: {
          correlationId: "corr-loop",
          kind: "approval",
          approvalSnapshot: snapshot,
        },
      };
    };
    const { run } = startRelayed({
      definition: parent,
      runId: "run-loop-approval",
      invokeStep,
      onPark: sink.onPark,
      loopFns: loopFnsReturning(false),
    });
    const park = await sink.next();
    expect(park.runId).toBe("run-loop-approval");
    expect(park.parkKind).toBe("approval");
    expect(park.approvalSnapshot).toEqual(snapshot);

    await run.signal(
      signalName(park.correlationId),
      { outcome: "approved" },
      "sig-1",
    );
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(
      relayed(result.events, "spawn", "approval", signalName("corr-loop")),
    ).toBeDefined();
  });

  test("two author gates live at once both complete", async () => {
    const child = defineWorkflow({
      id: "two-gates",
      trigger: { type: "manual" },
      steps: {
        alpha: awaitSignal({ name: "alpha" }),
        beta: awaitSignal({ name: "beta" }),
      },
    });
    const parent = defineWorkflow({
      id: "two-gates-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const { run, repoStore } = startRelayed({
      definition: parent,
      runId: "run-two-gates",
      invokeStep: async () => ({ output: null }),
      holdUntilChildAwaits: ["alpha", "beta"],
    });
    await waitForEvent(
      repoStore,
      "run-two-gates",
      (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.parkKind === "signal-relay",
    );
    await run.signal("alpha", { ok: true }, "sig-alpha");
    await run.signal("beta", { ok: true }, "sig-beta");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(
      result.events.some(
        (event) =>
          event.kind === "SignalReceived" && event.signalName === "alpha",
      ),
    ).toBe(true);
    expect(
      result.events.some(
        (event) =>
          event.kind === "SignalReceived" && event.signalName === "beta",
      ),
    ).toBe(true);
  });

  test("an approval and a parallel author gate both complete", async () => {
    const child = defineWorkflow({
      id: "approval-and-gate",
      trigger: { type: "manual" },
      steps: {
        hold: step({ agent }),
        gate: awaitSignal({ name: "go" }),
      },
    });
    const parent = defineWorkflow({
      id: "approval-and-gate-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume !== undefined) return { output: null };
      return {
        suspend: {
          correlationId: "corr-body",
          kind: "approval",
          approvalSnapshot: snapshot,
        },
      };
    };
    const { run, repoStore } = startRelayed({
      definition: parent,
      runId: "run-approval-and-gate",
      invokeStep,
      holdUntilChildAwaits: [signalName("corr-body"), "go"],
    });
    await waitForEvent(
      repoStore,
      "run-approval-and-gate",
      (event) => event.kind === "SignalAwaited" && event.stepId === "spawn",
    );
    await run.signal(
      signalName("corr-body"),
      { outcome: "approved" },
      "sig-approval",
    );
    await run.signal("go", { ok: true }, "sig-go");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(
      result.events.some(
        (event) =>
          event.kind === "SignalReceived" &&
          event.signalName === signalName("corr-body"),
      ),
    ).toBe(true);
    expect(
      result.events.some(
        (event) => event.kind === "SignalReceived" && event.signalName === "go",
      ),
    ).toBe(true);
  });

  test("a timed author gate in the child completes when its timer fires", async () => {
    const child = defineWorkflow({
      id: "timed-gate-child",
      trigger: { type: "manual" },
      steps: {
        hold: awaitSignal({
          name: "go",
          timeout: 40,
          onTimeout: "after",
        }),
        after: step({ agent, after: ["hold"] }),
      },
    });
    const parent = defineWorkflow({
      id: "timed-gate-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const result = await runLocal(parent, {
      runId: "run-timed-gate-child",
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
    }).complete;
    expect(result.terminalStatus).toBe("completed");
    expect(
      result.events.some(
        (event) =>
          event.kind === "SignalAwaitAbandoned" &&
          event.stepId === "spawn" &&
          event.signalName === "go",
      ),
    ).toBe(true);
  });

  test("a signal queued for a gate that already timed out is not recorded as relayed", async () => {
    const seen: string[] = [];
    let releaseTimedOut = (): void => {
      throw new Error("timedOut release was not installed");
    };
    const timedOut = new Promise<void>((resolve) => {
      releaseTimedOut = resolve;
    });
    const invokeStep: StepInvoker = async (req) => {
      const stepId = req.authzContext.stepId;
      if (stepId === undefined) return { output: null };
      seen.push(stepId);
      if (stepId === "timedOut") releaseTimedOut();
      if (stepId === "signaled") {
        throw new Error("signal path must not run");
      }
      return { output: null };
    };
    const child = defineWorkflow({
      id: "stale-child",
      trigger: { type: "manual" },
      steps: {
        slow: awaitSignal({ name: "slow" }),
        pause: sleep({ duration: 50 }),
        fast: awaitSignal({
          name: "fast",
          timeout: 40,
          onTimeout: "timedOut",
          after: ["pause"],
        }),
        timedOut: step({ agent, after: ["fast"] }),
        signaled: step({ agent, after: ["fast"] }),
      },
    });
    const parent = defineWorkflow({
      id: "stale-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const { run, repoStore } = startRelayed({
      definition: parent,
      runId: "run-stale-sibling",
      invokeStep,
    });
    await waitForEvent(
      repoStore,
      "run-stale-sibling",
      (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.signalName === "slow",
    );
    await run.signal("fast", { ok: true }, "sig-fast");
    await timedOut;
    await run.signal("slow", { ok: true }, "sig-slow");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(seen).toContain("timedOut");
    expect(seen).not.toContain("signaled");
    expect(
      result.events.some(
        (event) =>
          event.kind === "SignalReceived" && event.signalName === "slow",
      ),
    ).toBe(true);
    expect(
      result.events.some(
        (event) =>
          event.kind === "SignalReceived" && event.signalName === "fast",
      ),
    ).toBe(false);
    expect(
      result.events.some(
        (event) =>
          event.kind === "SignalAwaitAbandoned" && event.signalName === "fast",
      ),
    ).toBe(true);
  });

  test("a sibling gate that timed out with no signal is abandoned", async () => {
    const seen: string[] = [];
    let releaseTimedOut = (): void => {
      throw new Error("timedOut release was not installed");
    };
    const timedOut = new Promise<void>((resolve) => {
      releaseTimedOut = resolve;
    });
    const invokeStep: StepInvoker = async (req) => {
      const stepId = req.authzContext.stepId;
      if (stepId === undefined) return { output: null };
      seen.push(stepId);
      if (stepId === "timedOut") releaseTimedOut();
      return { output: null };
    };
    const child = defineWorkflow({
      id: "abandon-child",
      trigger: { type: "manual" },
      steps: {
        slow: awaitSignal({ name: "slow" }),
        pause: sleep({ duration: 50 }),
        fast: awaitSignal({
          name: "fast",
          timeout: 40,
          onTimeout: "timedOut",
          after: ["pause"],
        }),
        timedOut: step({ agent, after: ["fast"] }),
      },
    });
    const parent = defineWorkflow({
      id: "abandon-parent",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: child }) },
    });
    const { run, repoStore } = startRelayed({
      definition: parent,
      runId: "run-stale-abandon",
      invokeStep,
    });
    await waitForEvent(
      repoStore,
      "run-stale-abandon",
      (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.signalName === "slow",
    );
    await timedOut;
    await run.signal("slow", { ok: true }, "sig-slow");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(seen).toContain("timedOut");
    expect(
      result.events.some(
        (event) =>
          event.kind === "SignalAwaitAbandoned" && event.signalName === "fast",
      ),
    ).toBe(true);
    expect(
      result.events.some(
        (event) =>
          event.kind === "SignalReceived" && event.signalName === "fast",
      ),
    ).toBe(false);
  });

  test("a release that wins after the signal is logged does not deliver it", async () => {
    const result = await raceLoggedSignalAgainstRelease({ kind: "released" });
    expect(result.delivered).toBe(0);
    expect(result.terminalStatus).toBe("completed");
    expect(result.events.some((event) => event.kind === "StepFailed")).toBe(
      false,
    );
  });

  test("a terminal child after the signal is logged does not fail the spawn", async () => {
    const result = await raceLoggedSignalAgainstRelease({
      kind: "terminal",
      terminalStatus: "completed",
    });
    expect(result.delivered).toBe(0);
    expect(result.terminalStatus).toBe("completed");
    expect(
      result.events.some(
        (event) =>
          event.kind === "StepFailed" &&
          event.error.message.includes("deliver after stop"),
      ),
    ).toBe(false);
  });
});

// The host child store can miss a commit that lands while a read is in
// flight, and its subscribe does not replay `from`. The workflow
// adapter's subscribe() also returns a generator that does not attach
// the tail until the first pull. The watch has to pull before it reads
// again, or a release during that read is in neither place and the
// parent waits forever.
describe("park-release watch", () => {
  test("a release that lands during the read still resolves the watch", async () => {
    const childRunId = "child-watch-gap";
    const inner = createInMemoryRepoStore();
    let armStale = false;
    let watchSubscribed = false;
    let staleUsed = false;
    let armed = false;
    let subscribes = 0;
    let yields = 0;
    let releaseStaleRead = (): void => {
      throw new Error("stale read was not armed");
    };
    let markReadStarted = (): void => {
      throw new Error("read started was not armed");
    };
    const readStarted = new Promise<void>((resolve) => {
      markReadStarted = resolve;
    });
    const repoStore: RepoStore = {
      read: async (runId) => {
        // Stall the read that follows subscribe(), which is the
        // watch's second snapshot. The first snapshot has to complete
        // so the watch reaches subscribe at all.
        if (watchSubscribed && !staleUsed) {
          staleUsed = true;
          // The in-memory store returns its live array. A copy is the
          // pinned snapshot the host read takes before it fetches blobs.
          const snapshot = (await inner.read(runId)).slice();
          markReadStarted();
          await new Promise<void>((resolve) => {
            releaseStaleRead = resolve;
          });
          return snapshot;
        }
        return inner.read(runId);
      },
      append: (runId, event) => inner.append(runId, event),
      appendBatch: (runId, events) => inner.appendBatch(runId, events),
      subscribe: (runId, opts) => {
        if (!armStale) return inner.subscribe(runId, opts);
        subscribes += 1;
        watchSubscribed = true;
        // The host generator attaches on the first pull, not when
        // subscribe() returns. `from: "head"` does not replay.
        let live: ReturnType<RepoStore["subscribe"]> | undefined;
        return {
          [Symbol.asyncIterator]() {
            return this;
          },
          next: () => {
            if (live === undefined) {
              armed = true;
              live = inner.subscribe(runId, {
                signal: opts.signal,
                from: "head",
              });
            }
            return live.next().then((result) => {
              if (result.done !== true) yields += 1;
              return result;
            });
          },
          return: () =>
            live?.return?.() ??
            Promise.resolve({ done: true, value: undefined }),
        };
      },
    };
    const definition = defineWorkflow({
      id: "watch-child",
      trigger: { type: "manual" },
      steps: { hold: awaitSignal({ name: "go" }) },
    });
    const clock = (): Date => new Date();
    const env: WorkflowRuntimeEnv = {
      repoStore,
      scheduler: createInMemoryScheduler({ repoStore, clock }),
      signalChannel: createInMemorySignalChannel(),
      blobs: createInMemoryBlobSubstrate(),
      directors: createDefaultDirectorRegistry(),
      authorize: allowAll,
      invokeStep: async () => ({ output: null }),
      spawnChild: () => Promise.reject(new Error("this child does not spawn")),
      clock,
      newId: (prefix) => `${prefix}-watch`,
      drain: createNoopDrainController(definition),
      hasUpstreamSignalResolver: true,
    };
    const handle = createSuspendableChildHandle(env, {
      definition,
      childRunId,
      input: null,
      depth: 1,
      maxChildSpawnDepth: 32,
      signal: new AbortController().signal,
      surfaceInputPark: true,
    });
    await waitForEvent(
      inner,
      childRunId,
      (event) => event.kind === "SignalAwaited" && event.signalName === "go",
    );
    armStale = true;
    const watch = handle.waitForParkRelease?.(
      "go",
      new AbortController().signal,
    );
    if (watch === undefined) {
      throw new Error("handle does not watch a park release");
    }
    await readStarted;
    // The tail has to be attached while this read is still open. A
    // pull that starts after the read returns misses the commit.
    expect(armed).toBe(true);
    try {
      await commit({ repoStore }, childRunId, {
        kind: "SignalReceived",
        seq: 0,
        at: new Date().toISOString(),
        signalName: "go",
        signalId: "sig-go",
        payload: { ok: true },
      });
    } finally {
      releaseStaleRead();
    }
    const release = await watch;
    expect(release.kind).toBe("released");
    expect(subscribes).toBe(1);
    expect(yields).toBe(1);
  });
});

function pendingSignal(signal: AbortSignal | undefined): Promise<never> {
  return new Promise((_resolve, reject) => {
    const abort = (): void => {
      reject(new Error("aborted"));
    };
    if (signal === undefined) return;
    if (signal.aborted) {
      abort();
      return;
    }
    signal.addEventListener("abort", abort, { once: true });
  });
}

async function raceLoggedSignalAgainstRelease(
  release:
    | { kind: "released" }
    | {
        kind: "terminal";
        terminalStatus: "completed";
      },
): Promise<{
  delivered: number;
  terminalStatus: string;
  events: readonly WorkflowEvent[];
}> {
  let delivered = 0;
  let markWatch = (): void => {
    throw new Error("watch was not armed");
  };
  const watchEntered = new Promise<void>((resolve) => {
    markWatch = resolve;
  });
  let finishWatch = (
    _release:
      | { kind: "released" }
      | {
          kind: "terminal";
          terminalStatus: "completed" | "failed" | "cancelled";
        },
  ): void => {
    throw new Error("watch finish was not armed");
  };
  const watchResult = new Promise<
    | { kind: "released" }
    | {
        kind: "terminal";
        terminalStatus: "completed" | "failed" | "cancelled";
      }
  >((resolve) => {
    finishWatch = resolve;
  });
  let sentPark = false;
  const child = defineWorkflow({
    id: "logged-signal-child",
    trigger: { type: "manual" },
    steps: { hold: awaitSignal({ name: "fast" }) },
  });
  const parent = defineWorkflow({
    id: "logged-signal-parent",
    trigger: { type: "manual" },
    steps: { spawn: childWorkflow({ definition: child }) },
  });
  const runId = `run-logged-${release.kind}`;
  const { run, repoStore } = startRelayed({
    definition: parent,
    runId,
    invokeStep: async () => ({ output: null }),
    signalChannel: {
      deliver: () => Promise.reject(new Error("deliver is not used")),
      awaitNext: (_name, signal) => pendingSignal(signal),
    },
    spawnChild: () =>
      Promise.resolve({
        next: () => {
          if (!sentPark) {
            sentPark = true;
            return Promise.resolve({
              kind: "signal-park" as const,
              name: "fast",
            });
          }
          return Promise.resolve({
            kind: "terminal" as const,
            terminalStatus: "completed" as const,
          });
        },
        resume: () => Promise.resolve(),
        deliverSignal: () => {
          delivered += 1;
          return Promise.reject(
            new Error("signal channel: deliver after stop"),
          );
        },
        readParkRelease: () => Promise.resolve({ kind: "open" as const }),
        waitForParkRelease: () => {
          markWatch();
          return watchResult;
        },
      }),
  });
  await waitForEvent(
    repoStore,
    runId,
    (event) =>
      event.kind === "SignalAwaited" &&
      event.stepId === "spawn" &&
      event.signalName === "fast",
  );
  await watchEntered;
  await commit({ repoStore }, runId, {
    kind: "SignalReceived",
    seq: 0,
    at: new Date().toISOString(),
    signalName: "fast",
    signalId: "sig-fast",
    payload: { ok: true },
  });
  finishWatch(release);
  const result = await run.complete;
  return {
    delivered,
    terminalStatus: result.terminalStatus,
    events: result.events,
  };
}

describe("a parked childWorkflow does not survive a restart", () => {
  const child = gatedChild("resume-child");
  const definition = defineWorkflow({
    id: "resume-parent",
    trigger: { type: "manual" },
    steps: { spawn: childWorkflow({ definition: child }) },
  });

  const parkKinds = ["approval", "input", "signal-relay"] as const;

  for (const parkKind of parkKinds) {
    test(`resume of a ${parkKind} park fails the run and does not spawn again`, async () => {
      let spawns = 0;
      const clock = (): Date => new Date();
      const repoStore = createInMemoryRepoStore();
      const env: WorkflowRuntimeEnv = {
        repoStore,
        scheduler: createInMemoryScheduler({ repoStore, clock }),
        signalChannel: createInMemorySignalChannel(),
        blobs: createInMemoryBlobSubstrate(),
        directors: createDefaultDirectorRegistry(),
        authorize: allowAll,
        invokeStep: async () => ({ output: null }),
        spawnChild: async () => {
          spawns += 1;
          throw new Error("spawnChild must not run");
        },
        clock,
        newId: (prefix) => `${prefix}-resume`,
        drain: createNoopDrainController(definition),
        hasUpstreamSignalResolver: true,
      };
      const runId = `run-resume-${parkKind}`;
      const signal = parkKind === "signal-relay" ? "go" : signalName("corr-1");
      const seed: WorkflowEvent[] = [
        {
          kind: "RunStarted",
          seq: 1,
          at,
          runId,
          definitionHash: "x",
          trigger: { type: "manual", payload: undefined },
        },
        {
          kind: "StepStarted",
          seq: 2,
          at,
          stepId: "spawn",
          attempt: 1,
          input: { ref: "inline:null" },
        },
        {
          kind: "ChildSpawned",
          seq: 3,
          at,
          stepId: "spawn",
          childRunId: "run-child-1",
          childDefinitionRef: "child-ref",
        },
        {
          kind: "SignalAwaited",
          seq: 4,
          at,
          stepId: "spawn",
          signalName: signal,
          parkKind,
        },
      ];
      const result = await runtimeRun(definition, env, {
        runId,
        resumeFromEvents: seed,
      }).complete;

      expect(result.terminalStatus).toBe("failed");
      const last = result.events.at(-1);
      if (last?.kind !== "RunFailed") {
        throw new Error("expected RunFailed");
      }
      expect(last.error.message).toContain(
        "not supported by the in-process runtime",
      );
      expect(spawns).toBe(0);
      expect(
        result.events.filter((event) => event.kind === "ChildSpawned"),
      ).toHaveLength(1);
    });
  }
});
