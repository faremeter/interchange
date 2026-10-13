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
// approval relay fails the spawn step. A parked child survives a process
// restart: resume reattaches the same child run and reestablishes the
// relay. The child stays unaddressable; the parent is the resume root.
// A timed gate keeps its deadline across that resume. That includes a
// gate inside a loop whose parent has not recorded the relay, and a
// deadline that has already passed.

import { describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { correlationIdFromSignalName, signalName } from "@intx/types";
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
  loopBodyRunId,
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
  repoStore?: RepoStore;
  resumeFromEvents?: readonly WorkflowEvent[];
  hasUpstreamSignalResolver?: boolean;
  clock?: () => Date;
  onSignalPark?: (park: {
    runId: string;
    name: string;
    timeout?: number;
  }) => void;
}): { run: RuntimeWorkflowRun; repoStore: RepoStore } {
  const repoStore = args.repoStore ?? createInMemoryRepoStore();
  const clock = args.clock ?? ((): Date => new Date());
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
      : createInMemorySpawnChild(
          childBodies,
          {
            authorize: allowAll,
            invokeStep: args.invokeStep,
            ...(args.loopFns !== undefined ? { loopFns: args.loopFns } : {}),
            ...(args.clock !== undefined ? { clock: args.clock } : {}),
          },
          repoStore,
        ));
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
    hasUpstreamSignalResolver: args.hasUpstreamSignalResolver ?? true,
    ...(args.onPark !== undefined ? { onPark: args.onPark } : {}),
    ...(args.onSignalPark !== undefined
      ? { onSignalPark: args.onSignalPark }
      : {}),
    ...(args.loopFns !== undefined ? { loopFns: args.loopFns } : {}),
  };
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  return {
    run: runtimeRun(rewritten, env, {
      runId: args.runId,
      ...(args.resumeFromEvents !== undefined
        ? { resumeFromEvents: args.resumeFromEvents }
        : {}),
    }),
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

  test("relays an author signal committed before the channel wait", async () => {
    const parent = defineWorkflow({
      id: "preread-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({ definition: gatedChild("preread-child") }),
      },
    });
    const runId = "run-preread";
    const inner = createInMemoryRepoStore();
    let parentTip: WorkflowEvent | undefined;
    const repoStore: RepoStore = {
      read: (id) => inner.read(id),
      append: async (id, event) => {
        if (id === runId) parentTip = event;
        await inner.append(id, event);
      },
      appendBatch: async (id, events) => {
        if (id === runId) {
          const last = events.at(-1);
          if (last !== undefined) parentTip = last;
        }
        await inner.appendBatch(id, events);
      },
      subscribe: (id, opts) => inner.subscribe(id, opts),
    };
    let injected = false;
    let appendFailure: unknown;
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      repoStore,
      signalChannel: {
        deliver: () => Promise.reject(new Error("deliver is not used")),
        awaitNext: (name, signal) => {
          if (!injected && name === "go") {
            injected = true;
            const last = parentTip;
            if (last === undefined) {
              return Promise.reject(new Error("parent log empty"));
            }
            const event: WorkflowEvent = {
              kind: "SignalReceived",
              seq: last.seq + 1,
              at: new Date().toISOString(),
              signalName: name,
              signalId: "sig-early",
              payload: { done: true },
            };
            parentTip = event;
            // The in-memory append updates the log before it yields, so
            // the relay's read after this call sees the signal. The
            // returned wait does not replay it.
            void inner.append(runId, event).catch((cause: unknown) => {
              appendFailure = cause;
            });
          }
          return pendingSignal(signal);
        },
      },
    });
    const result = await run.complete;
    const childId = spawnedChildId(result.events, "spawn");
    const childLog = await copyLog(repoStore, childId);

    expect(injected).toBe(true);
    expect(appendFailure).toBeUndefined();
    expect(result.terminalStatus).toBe("completed");
    expect(
      result.events.filter(
        (event) =>
          event.kind === "SignalReceived" && event.signalId === "sig-early",
      ),
    ).toHaveLength(1);
    expect(
      childLog.filter(
        (event) =>
          event.kind === "SignalReceived" && event.signalId === "sig-early",
      ),
    ).toHaveLength(1);
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
  let watchStarted = false;
  let sawLogRead = false;
  let markLogRead = (): void => {
    throw new Error("log read was not armed");
  };
  const logReadAfterWatch = new Promise<void>((resolve) => {
    markLogRead = resolve;
  });
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
  const inner = createInMemoryRepoStore();
  const repoStore: RepoStore = {
    read: async (id) => {
      const events = await inner.read(id);
      if (watchStarted && id === runId && !sawLogRead) {
        sawLogRead = true;
        markLogRead();
      }
      return events;
    },
    append: (id, event) => inner.append(id, event),
    appendBatch: (id, events) => inner.appendBatch(id, events),
    subscribe: (id, opts) => inner.subscribe(id, opts),
  };
  const { run } = startRelayed({
    definition: parent,
    runId,
    invokeStep: async () => ({ output: null }),
    repoStore,
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
          watchStarted = true;
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
  // The relay reads the log once the waits are started. Commit after
  // that read, so this signal is not the one the log read relays. The
  // release then wins, and the logged signal stays undelivered.
  await logReadAfterWatch;
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

function spawnedChildId(
  events: readonly WorkflowEvent[],
  stepId: string,
): string {
  for (const event of events) {
    if (event.kind === "ChildSpawned" && event.stepId === stepId) {
      return event.childRunId;
    }
  }
  throw new Error(`no ChildSpawned for ${stepId}`);
}

async function copyLog(
  repoStore: RepoStore,
  runId: string,
): Promise<WorkflowEvent[]> {
  return [...(await repoStore.read(runId))];
}

async function seedLog(
  repoStore: RepoStore,
  runId: string,
  events: readonly WorkflowEvent[],
): Promise<void> {
  for (const event of events) {
    await repoStore.append(runId, event);
  }
}

function countEvents(
  events: readonly WorkflowEvent[],
  kind: WorkflowEvent["kind"],
): number {
  return events.filter((event) => event.kind === kind).length;
}

describe("a parked childWorkflow survives a process restart", () => {
  test("reestablishes an author gate and completes on the later signal", async () => {
    const parent = defineWorkflow({
      id: "resume-author-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({ definition: gatedChild("resume-author") }),
      },
    });
    const runId = "run-resume-author";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.signalName === "go",
    );
    const parentLog = await copyLog(first.repoStore, runId);
    const childId = spawnedChildId(parentLog, "spawn");
    const childLog = await copyLog(first.repoStore, childId);
    expect(
      childLog.some(
        (event) => event.kind === "SignalAwaited" && event.signalName === "go",
      ),
    ).toBe(true);

    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      repoStore,
      resumeFromEvents: parentLog,
    });
    await run.signal("go", { done: true }, "sig-resume-author");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(countEvents(result.events, "ChildSpawned")).toBe(1);
    expect(spawnOutput(result.outputs.spawn).childRunId).toBe(childId);
    expect(
      result.events.filter(
        (event) => event.kind === "SignalReceived" && event.signalName === "go",
      ),
    ).toHaveLength(1);
  });

  test("reestablishes an approval and completes on the later decision", async () => {
    const parent = defineWorkflow({
      id: "resume-approval-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "resume-approval-child",
            trigger: { type: "manual" },
            steps: { hold: step({ agent }) },
          }),
        }),
      },
    });
    const runId = "run-resume-approval";
    const suspending: StepInvoker = async (req) => {
      if (req.resume !== undefined) return { output: null };
      return {
        suspend: {
          correlationId: "corr-body",
          kind: "approval",
          approvalSnapshot: snapshot,
        },
      };
    };
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: suspending,
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.parkKind === "approval",
    );
    const parentLog = await copyLog(first.repoStore, runId);
    const childId = spawnedChildId(parentLog, "spawn");
    const childLog = await copyLog(first.repoStore, childId);

    let freshInvokes = 0;
    let resumed: unknown;
    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async (req) => {
        if (req.resume === undefined) {
          freshInvokes += 1;
          return { output: null };
        }
        resumed = req.resume.decision;
        return { output: null };
      },
      repoStore,
      resumeFromEvents: parentLog,
    });
    await run.signal(
      signalName("corr-body"),
      { outcome: "approved" },
      "sig-resume-approval",
    );
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(freshInvokes).toBe(0);
    expect(resumed).toEqual({ outcome: "approved" });
    expect(countEvents(result.events, "ChildSpawned")).toBe(1);
    expect(spawnOutput(result.outputs.spawn).childRunId).toBe(childId);
  });

  test("reestablishes an input park and delivers the next turn", async () => {
    const parent = defineWorkflow({
      id: "resume-input-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "resume-input-child",
            trigger: { type: "manual" },
            steps: { chat: step({ agent, triggers: 2 }) },
          }),
        }),
      },
    });
    const runId = "run-resume-input";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.parkKind === "input",
    );
    const parentLog = await copyLog(first.repoStore, runId);
    const childId = spawnedChildId(parentLog, "spawn");
    const childLog = await copyLog(first.repoStore, childId);
    const awaited = parentLog.find(
      (event) => event.kind === "SignalAwaited" && event.parkKind === "input",
    );
    if (awaited === undefined || awaited.kind !== "SignalAwaited") {
      throw new Error("parent input await missing");
    }
    const correlationId = correlationIdFromSignalName(awaited.signalName);
    if (correlationId === undefined) {
      throw new Error("input await is not a reserved name");
    }

    const decisions: unknown[] = [];
    let freshInvokes = 0;
    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async (req) => {
        if (req.resume === undefined) {
          freshInvokes += 1;
          return { output: null };
        }
        decisions.push(req.resume.decision);
        return { output: null };
      },
      repoStore,
      resumeFromEvents: parentLog,
    });
    const nextTurn = { text: "next-turn" };
    await run.signal(signalName(correlationId), nextTurn, "sig-resume-input");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(freshInvokes).toBe(0);
    expect(decisions).toEqual([nextTurn]);
    expect(countEvents(result.events, "ChildSpawned")).toBe(1);
  });

  test("relays a second park the parent had not reached", async () => {
    const parent = defineWorkflow({
      id: "resume-two-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "resume-two-child",
            trigger: { type: "manual" },
            steps: {
              alpha: awaitSignal({ name: "alpha" }),
              beta: awaitSignal({ name: "beta" }),
            },
          }),
        }),
      },
    });
    const runId = "run-resume-two";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) => event.kind === "SignalAwaited" && event.stepId === "spawn",
    );
    const parentPartial = await copyLog(first.repoStore, runId);
    const childId = spawnedChildId(parentPartial, "spawn");
    await waitForEvent(
      first.repoStore,
      childId,
      (event) => event.kind === "SignalAwaited" && event.signalName === "alpha",
    );
    await waitForEvent(
      first.repoStore,
      childId,
      (event) => event.kind === "SignalAwaited" && event.signalName === "beta",
    );
    const parentLog = await copyLog(first.repoStore, runId);
    const childLog = await copyLog(first.repoStore, childId);
    expect(
      parentLog.filter((event) => event.kind === "SignalAwaited"),
    ).toHaveLength(1);

    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      repoStore,
      resumeFromEvents: parentLog,
    });
    await run.signal("alpha", { ok: true }, "sig-alpha");
    await run.signal("beta", { ok: true }, "sig-beta");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(countEvents(result.events, "ChildSpawned")).toBe(1);
    expect(
      result.events.filter(
        (event) =>
          event.kind === "SignalReceived" &&
          (event.signalName === "alpha" || event.signalName === "beta"),
      ),
    ).toHaveLength(2);
  });

  test("delivers a decision the parent already recorded", async () => {
    const parent = defineWorkflow({
      id: "resume-delivered-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: gatedChild("resume-delivered"),
        }),
      },
    });
    const runId = "run-resume-delivered";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) => event.kind === "SignalAwaited" && event.signalName === "go",
    );
    const parentLog = await copyLog(first.repoStore, runId);
    const childId = spawnedChildId(parentLog, "spawn");
    const childLog = await copyLog(first.repoStore, childId);
    const last = parentLog.at(-1);
    if (last === undefined) throw new Error("empty parent log");
    parentLog.push({
      kind: "SignalReceived",
      seq: last.seq + 1,
      at: new Date().toISOString(),
      signalName: "go",
      signalId: "sig-already",
      payload: { done: true },
    });

    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      repoStore,
      resumeFromEvents: parentLog,
    });
    const result = await run.complete;
    const resumedChild = await copyLog(repoStore, childId);

    expect(result.terminalStatus).toBe("completed");
    expect(countEvents(result.events, "ChildSpawned")).toBe(1);
    expect(
      result.events.filter(
        (event) =>
          event.kind === "SignalReceived" && event.signalId === "sig-already",
      ),
    ).toHaveLength(1);
    expect(
      resumedChild.filter(
        (event) =>
          event.kind === "SignalReceived" && event.signalId === "sig-already",
      ),
    ).toHaveLength(1);
  });

  test("does not deliver a decision the child already consumed", async () => {
    const parent = defineWorkflow({
      id: "resume-consumed-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({ definition: gatedChild("resume-consumed") }),
      },
    });
    const runId = "run-resume-consumed";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
    });
    await first.run.signal("go", { done: true }, "sig-once");
    const finished = await first.run.complete;
    expect(finished.terminalStatus).toBe("completed");
    const childId = spawnedChildId(finished.events, "spawn");
    const childLog = await copyLog(first.repoStore, childId);
    const childSignals = childLog.filter(
      (event) =>
        event.kind === "SignalReceived" && event.signalId === "sig-once",
    ).length;
    expect(childSignals).toBe(1);
    const completedAt = finished.events.findIndex(
      (event) => event.kind === "ChildCompleted",
    );
    if (completedAt < 0) throw new Error("ChildCompleted missing");
    const parentLog = finished.events.slice(0, completedAt);
    expect(
      parentLog.some(
        (event) =>
          event.kind === "SignalReceived" && event.signalId === "sig-once",
      ),
    ).toBe(true);

    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      repoStore,
      resumeFromEvents: parentLog,
    });
    const result = await run.complete;
    const resumedChild = await copyLog(repoStore, childId);

    expect(result.terminalStatus).toBe("completed");
    expect(countEvents(result.events, "ChildSpawned")).toBe(1);
    expect(
      resumedChild.filter(
        (event) =>
          event.kind === "SignalReceived" && event.signalId === "sig-once",
      ),
    ).toHaveLength(1);
  });

  test("an empty child log starts that same id and keeps the spawn input", async () => {
    const parent = defineWorkflow({
      id: "resume-empty-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "resume-empty-child",
            trigger: { type: "manual" },
            steps: {
              seen: step({
                agent,
                input: { from: "trigger.payload" },
              }),
              hold: awaitSignal({ name: "go", after: ["seen"] }),
            },
          }),
          input: { literal: { token: "keep" } },
        }),
      },
    });
    const runId = "run-resume-empty";
    // ChildSpawned is flushed before the spawner runs. Holding the spawner
    // open keeps that crash window: the parent log names a child run whose
    // own log was never written. Letting the child start would share this
    // process's commit chain with the resume.
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      spawnChild: () => new Promise<never>(() => undefined),
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) => event.kind === "ChildSpawned" && event.stepId === "spawn",
    );
    const full = await copyLog(first.repoStore, runId);
    const spawnedAt = full.findIndex((event) => event.kind === "ChildSpawned");
    if (spawnedAt < 0) throw new Error("ChildSpawned missing");
    const parentLog = full.slice(0, spawnedAt + 1);
    expect(parentLog.some((event) => event.kind === "SignalAwaited")).toBe(
      false,
    );
    const childId = spawnedChildId(parentLog, "spawn");

    const seen: unknown[] = [];
    const repoStore = createInMemoryRepoStore();
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async (req) => {
        seen.push(req.input);
        return { output: null };
      },
      repoStore,
      resumeFromEvents: parentLog,
    });
    await run.signal("go", { done: true }, "sig-keep");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(seen).toEqual([{ token: "keep" }]);
    expect(countEvents(result.events, "ChildSpawned")).toBe(1);
    expect(spawnOutput(result.outputs.spawn).childRunId).toBe(childId);
  });

  test("an unproxied approval fails before spawn and writes no await", async () => {
    const parent = defineWorkflow({
      id: "resume-unproxied-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "resume-unproxied-child",
            trigger: { type: "manual" },
            steps: { hold: step({ agent }) },
          }),
        }),
      },
    });
    const runId = "run-resume-unproxied";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async (req) => {
        if (req.resume !== undefined) return { output: null };
        return {
          suspend: {
            correlationId: "corr-body",
            kind: "approval",
            approvalSnapshot: snapshot,
          },
        };
      },
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) =>
        event.kind === "SignalAwaited" && event.parkKind === "approval",
    );
    const full = await copyLog(first.repoStore, runId);
    const childId = spawnedChildId(full, "spawn");
    const childLog = await copyLog(first.repoStore, childId);
    const awaitAt = full.findIndex(
      (event) =>
        event.kind === "SignalAwaited" && event.parkKind === "approval",
    );
    if (awaitAt < 0) throw new Error("approval await missing");
    const parentLog = full.slice(0, awaitAt);

    let spawns = 0;
    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      spawnChild: () => {
        spawns += 1;
        return Promise.reject(new Error("spawnChild must not run"));
      },
      repoStore,
      resumeFromEvents: parentLog,
    });
    const result = await run.complete;
    const failed = result.events.find((event) => event.kind === "StepFailed");

    expect(result.terminalStatus).toBe("failed");
    expect(spawns).toBe(0);
    expect(failed?.kind === "StepFailed" ? failed.error.message : "").toContain(
      "approval",
    );
    expect(failed?.kind === "StepFailed" ? failed.error.message : "").toContain(
      "snapshot",
    );
    expect(result.events.some((event) => event.kind === "SignalAwaited")).toBe(
      false,
    );
  });

  test("a nested childWorkflow still climbs after resume", async () => {
    const middle = defineWorkflow({
      id: "resume-middle",
      trigger: { type: "manual" },
      steps: {
        inner: childWorkflow({ definition: gatedChild("resume-leaf") }),
      },
    });
    const parent = defineWorkflow({
      id: "resume-outer",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: middle }) },
    });
    const runId = "run-resume-nested";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) => event.kind === "SignalAwaited" && event.signalName === "go",
    );
    const parentLog = await copyLog(first.repoStore, runId);
    const middleId = spawnedChildId(parentLog, "spawn");
    const middleLog = await copyLog(first.repoStore, middleId);
    const leafId = spawnedChildId(middleLog, "inner");
    const leafLog = await copyLog(first.repoStore, leafId);

    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, middleId, middleLog);
    await seedLog(repoStore, leafId, leafLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      repoStore,
      resumeFromEvents: parentLog,
    });
    await run.signal("go", { done: true }, "sig-nested");
    const result = await run.complete;
    const resumedMiddle = await copyLog(repoStore, middleId);

    expect(result.terminalStatus).toBe("completed");
    expect(countEvents(result.events, "ChildSpawned")).toBe(1);
    expect(countEvents(resumedMiddle, "ChildSpawned")).toBe(1);
  });

  test("a childWorkflow under a loop still climbs after resume", async () => {
    const parent = defineWorkflow({
      id: "resume-loop-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "resume-loop-child",
            trigger: { type: "manual" },
            steps: {
              rework: loop({
                body: defineWorkflow({
                  id: "resume-loop-body",
                  trigger: { type: "manual" },
                  steps: {
                    inner: childWorkflow({
                      definition: gatedChild("resume-loop-leaf"),
                    }),
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
          }),
        }),
      },
    });
    const runId = "run-resume-loop";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      loopFns: loopFnsReturning(false),
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) => event.kind === "SignalAwaited" && event.signalName === "go",
    );
    const parentLog = await copyLog(first.repoStore, runId);
    const childId = spawnedChildId(parentLog, "spawn");
    const childLog = await copyLog(first.repoStore, childId);
    const bodyId = loopBodyRunId(childId, "rework", 0);
    const bodyLog = await copyLog(first.repoStore, bodyId);
    const leafId = spawnedChildId(bodyLog, "inner");
    const leafLog = await copyLog(first.repoStore, leafId);

    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    await seedLog(repoStore, bodyId, bodyLog);
    await seedLog(repoStore, leafId, leafLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      loopFns: loopFnsReturning(false),
      repoStore,
      resumeFromEvents: parentLog,
    });
    await run.signal("go", { done: true }, "sig-loop");
    const result = await run.complete;
    const resumedBody = await copyLog(repoStore, bodyId);

    expect(result.terminalStatus).toBe("completed");
    expect(countEvents(result.events, "ChildSpawned")).toBe(1);
    expect(countEvents(resumedBody, "ChildSpawned")).toBe(1);
  });

  test("a fresh author relay keeps the remaining timeout", async () => {
    const parent = defineWorkflow({
      id: "resume-timed-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "resume-timed-child",
            trigger: { type: "manual" },
            steps: {
              hold: awaitSignal({
                name: "go",
                timeout: 60_000,
                onTimeout: "after",
              }),
              after: step({ agent, after: ["hold"] }),
            },
          }),
        }),
      },
    });
    const runId = "run-resume-timed";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) => event.kind === "SignalAwaited" && event.signalName === "go",
    );
    const full = await copyLog(first.repoStore, runId);
    const childId = spawnedChildId(full, "spawn");
    const childLog = await copyLog(first.repoStore, childId);
    const awaitAt = full.findIndex(
      (event) => event.kind === "SignalAwaited" && event.signalName === "go",
    );
    if (awaitAt < 0) throw new Error("timed await missing");
    const parentLog = full.slice(0, awaitAt);
    const timeouts: number[] = [];
    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      repoStore,
      resumeFromEvents: parentLog,
      onSignalPark: (park) => {
        if (park.timeout !== undefined) timeouts.push(park.timeout);
      },
    });
    await run.signal("go", { done: true }, "sig-timed");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(timeouts.length).toBeGreaterThan(0);
    const timeout = timeouts[0];
    if (timeout === undefined) throw new Error("timeout missing");
    expect(timeout).toBeGreaterThan(0);
    expect(timeout).toBeLessThanOrEqual(60_000);
  });

  test("a nested timed gate keeps its deadline when the parent has not recorded the relay", async () => {
    const now = Date.parse("2026-01-01T00:00:00.000Z");
    const clock = (): Date => new Date(now);
    const parent = defineWorkflow({
      id: "resume-nested-timed-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "resume-nested-timed-child",
            trigger: { type: "manual" },
            steps: {
              rework: loop({
                body: defineWorkflow({
                  id: "resume-nested-timed-body",
                  trigger: { type: "manual" },
                  steps: {
                    hold: awaitSignal({
                      name: "go",
                      timeout: 60_000,
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
          }),
        }),
      },
    });
    const runId = "run-resume-nested-timed";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      loopFns: loopFnsReturning(false),
      hasUpstreamSignalResolver: false,
      clock,
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.signalName === "go",
    );
    const full = await copyLog(first.repoStore, runId);
    const childId = spawnedChildId(full, "spawn");
    const childLog = await copyLog(first.repoStore, childId);
    const bodyId = loopBodyRunId(childId, "rework", 0);
    const bodyLog = await copyLog(first.repoStore, bodyId);
    expect(
      childLog.some(
        (event) =>
          event.kind === "SignalAwaited" &&
          event.signalName === "go" &&
          event.timeoutAt !== undefined,
      ),
    ).toBe(true);
    const awaitAt = full.findIndex(
      (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.signalName === "go",
    );
    if (awaitAt < 0) throw new Error("nested await missing");
    const parentLog = full.slice(0, awaitAt);
    const timeouts: number[] = [];
    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    await seedLog(repoStore, bodyId, bodyLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      loopFns: loopFnsReturning(false),
      hasUpstreamSignalResolver: false,
      clock,
      repoStore,
      resumeFromEvents: parentLog,
      onSignalPark: (park) => {
        if (park.timeout !== undefined) timeouts.push(park.timeout);
      },
    });
    await run.signal("go", { done: true }, "sig-nested-timed");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(result.events.some((event) => event.kind === "StepFailed")).toBe(
      false,
    );
    expect(timeouts.some((timeout) => timeout > 0)).toBe(true);
  });

  test("a timed gate whose deadline has passed still finishes when the parent cannot be answered", async () => {
    let now = Date.parse("2026-01-01T00:00:00.000Z");
    const clock = (): Date => new Date(now);
    const parent = defineWorkflow({
      id: "resume-expired-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "resume-expired-child",
            trigger: { type: "manual" },
            steps: {
              hold: awaitSignal({
                name: "go",
                timeout: 60_000,
                onTimeout: "after",
              }),
              after: step({ agent, after: ["hold"] }),
            },
          }),
        }),
      },
    });
    const runId = "run-resume-expired";
    const first = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      hasUpstreamSignalResolver: false,
      clock,
    });
    void first.run.complete;
    await waitForEvent(
      first.repoStore,
      runId,
      (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.signalName === "go",
    );
    const full = await copyLog(first.repoStore, runId);
    const childId = spawnedChildId(full, "spawn");
    const childLog = await copyLog(first.repoStore, childId);
    const awaited = childLog.find(
      (event) => event.kind === "SignalAwaited" && event.signalName === "go",
    );
    if (awaited?.kind !== "SignalAwaited" || awaited.timeoutAt === undefined) {
      throw new Error("child timeoutAt missing");
    }
    const awaitAt = full.findIndex(
      (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.signalName === "go",
    );
    if (awaitAt < 0) throw new Error("expired await missing");
    const parentLog = full.slice(0, awaitAt);
    now += 120_000;
    const repoStore = createInMemoryRepoStore();
    await seedLog(repoStore, childId, childLog);
    const { run } = startRelayed({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      hasUpstreamSignalResolver: false,
      clock,
      repoStore,
      resumeFromEvents: parentLog,
    });
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    expect(result.events.some((event) => event.kind === "StepFailed")).toBe(
      false,
    );
  });
});
