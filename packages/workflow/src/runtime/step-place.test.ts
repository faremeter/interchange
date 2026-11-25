// Each agent step's invocation carries the step's place in its deployment's
// run tree: the base step ids from the top-level step down through each
// container (loop, onTrigger, childWorkflow) to the step, and a map item's
// index. The place is the same each time the same step runs, also after a
// crash-resume, and the park hooks receive the place of the step that parked.
// A caller that walks the logs from the top-level run derives the same place
// from the `ChildSpawned` records.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { signalName, type StepPlace } from "@intx/types";
import type { ApprovalSnapshot, ConversationTurn } from "@intx/types/runtime";

import {
  action,
  childWorkflow,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  createSpawnLoopIteration,
  defineWorkflow,
  enumerateInlineLoopBodies,
  findParkedApprovalSnapshot,
  loop,
  loopBodyRunId,
  map,
  resumeFromLog,
  rewriteInlineChildWorkflowBodies,
  runtimeRun,
  sectionBodyRunId,
  step,
  type BlobSubstrate,
  type LoadParkedApproval,
  type PersistRecoveredPark,
  type Primitive,
  type ReadParkedApprovalOps,
  type RepoStore,
  type SignalChannel,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { waitForEvent, waitForNthEvent } from "@intx/workflow/testing";

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "m" }] },
});

const replyTurn: ConversationTurn = {
  role: "assistant",
  content: [{ type: "text", text: "done" }],
  timestamp: 0,
};

const approvalSnapshot: ApprovalSnapshot = {
  name: "gate",
  description: "gate",
  inputSchema: { type: "object" },
  arguments: {},
};

type Invocation = {
  runId: string | undefined;
  stepId: string | undefined;
  place: StepPlace;
  resumed: boolean;
};

// Records the place each invocation carries. With `suspendFirst`, a fresh
// invocation parks on an approval and only a resume replies.
function recordingInvoker(
  invocations: Invocation[],
  opts: { suspendFirst?: boolean } = {},
): StepInvoker {
  return async (req) => {
    invocations.push({
      runId: req.authzContext.runId,
      stepId: req.authzContext.stepId,
      place: req.place,
      resumed: req.resume !== undefined,
    });
    if (opts.suspendFirst === true && req.resume === undefined) {
      return {
        suspend: {
          correlationId: "corr-1",
          kind: "approval",
          approvalSnapshot,
        },
      };
    }
    return { output: { reply: "done", turn: replyTurn } };
  };
}

// The in-process loop spawner drives a child through the shared
// suspendable-child handle over this env. It also stands in for the host's
// onTrigger body and childWorkflow spawners, which use the same handle.
function buildEnv(args: {
  def: WorkflowDefinition;
  repoStore: RepoStore;
  blobs: BlobSubstrate;
  signalChannel: SignalChannel;
  invokeStep: StepInvoker;
  bodies?: ReadonlyMap<string, WorkflowDefinition>;
  readParkedApprovalOps?: ReadParkedApprovalOps;
  loadParkedApproval?: LoadParkedApproval;
  persistRecoveredPark?: PersistRecoveredPark;
}): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const bodies = args.bodies ?? new Map<string, WorkflowDefinition>();
  const env: WorkflowRuntimeEnv = {
    repoStore: args.repoStore,
    scheduler: createInMemoryScheduler({ repoStore: args.repoStore, clock }),
    signalChannel: args.signalChannel,
    blobs: args.blobs,
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: args.invokeStep,
    invokeAction: async () => ({ output: null }),
    spawnChild: () => Promise.reject(new Error("replaced below")),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(args.def),
    hasUpstreamSignalResolver: true,
    loopFns: (ref) => {
      if (ref === "cont") return () => true;
      if (ref === "same") return (_output, input) => input;
      throw new Error(`unknown loop fn ${ref}`);
    },
    ...(args.readParkedApprovalOps !== undefined
      ? { readParkedApprovalOps: args.readParkedApprovalOps }
      : {}),
    ...(args.loadParkedApproval !== undefined
      ? { loadParkedApproval: args.loadParkedApproval }
      : {}),
    ...(args.persistRecoveredPark !== undefined
      ? { persistRecoveredPark: args.persistRecoveredPark }
      : {}),
  };
  const loopBodies = new Map(
    [args.def, ...bodies.values()].flatMap((definition) =>
      enumerateInlineLoopBodies(definition).map(
        (body) => [body.ref, body.definition] as const,
      ),
    ),
  );
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  env.spawnSuspendableChild = createSpawnLoopIteration(env, bodies);
  env.spawnChild = createSpawnLoopIteration(env, bodies);
  return env;
}

// A deployed onTrigger section `section` whose body is `body-ref`.
function sectionWorkflow(): WorkflowDefinition {
  const section: Primitive = {
    kind: "onTrigger",
    id: "",
    on: { type: "mail", to: "run_sec@t.example" },
    body: { ref: "body-ref" },
    drainBehavior: "wait",
  };
  return defineWorkflow({ id: "step-place-section", steps: { section } });
}

const oneStepBody = defineWorkflow({
  id: "step-place-body",
  trigger: { type: "manual" },
  steps: { s: step({ agent }) },
});

// A loop `rework` over `body` that runs two iterations, then `fin`.
function loopWorkflow(body: WorkflowDefinition): WorkflowDefinition {
  return defineWorkflow({
    id: "step-place-loop",
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body,
        while: "cont",
        carry: "same",
        input: { literal: 0 },
        maxIterations: 2,
        onExhausted: "fin",
      }),
      fin: action({ handler: "fin", after: ["rework"] }),
    },
  });
}

async function waitForPark(
  repoStore: RepoStore,
  runId: string,
  stepId: string,
  parkKind: "input" | "approval",
  count: number,
): Promise<void> {
  await waitForNthEvent(
    repoStore,
    runId,
    (e) =>
      e.kind === "SignalAwaited" &&
      e.stepId === stepId &&
      e.parkKind === parkKind,
    count,
  );
}

async function seedLog(
  repoStore: RepoStore,
  runId: string,
  events: readonly WorkflowEvent[],
): Promise<void> {
  for (const event of events) await repoStore.append(runId, event);
}

const isPark = (e: WorkflowEvent): boolean => e.kind === "SignalAwaited";

describe("a step invocation carries the step's place", () => {
  test("a top-level step and each map item", async () => {
    const def = defineWorkflow({
      id: "step-place-top",
      trigger: { type: "manual" },
      steps: {
        s: step({ agent }),
        m: map({ over: { literal: ["a", "b", "c"] }, step: step({ agent }) }),
      },
    });
    const invocations: Invocation[] = [];
    const repoStore = createInMemoryRepoStore();
    const result = await runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs: createInMemoryBlobSubstrate(),
        signalChannel: createInMemorySignalChannel(),
        invokeStep: recordingInvoker(invocations),
      }),
      { runId: "run-top" },
    ).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(invocations.find((i) => i.stepId === "s")?.place).toEqual({
      stepPath: ["s"],
    });
    expect(
      invocations
        .filter((i) => i.stepId !== "s")
        .map((i) => [i.stepId, i.place]),
    ).toEqual([
      ["m[0]", { stepPath: ["m"], mapIndex: 0 }],
      ["m[1]", { stepPath: ["m"], mapIndex: 1 }],
      ["m[2]", { stepPath: ["m"], mapIndex: 2 }],
    ]);
  });

  test("a loop body step, and a map item in a loop body, keep their place across iterations", async () => {
    const body = defineWorkflow({
      id: "step-place-loop-body",
      trigger: { type: "manual" },
      steps: {
        s: step({ agent }),
        m: map({ over: { literal: ["a", "b"] }, step: step({ agent }) }),
      },
    });
    const def = loopWorkflow(body);
    const invocations: Invocation[] = [];
    const runId = "run-loop";
    const result = await runtimeRun(
      def,
      buildEnv({
        def,
        repoStore: createInMemoryRepoStore(),
        blobs: createInMemoryBlobSubstrate(),
        signalChannel: createInMemorySignalChannel(),
        invokeStep: recordingInvoker(invocations),
      }),
      { runId },
    ).complete;

    expect(result.terminalStatus).toBe("completed");
    const placesIn = (iteration: number) =>
      invocations
        .filter((i) => i.runId === loopBodyRunId(runId, "rework", iteration))
        .map((i) => [i.stepId, i.place]);
    for (const iteration of [0, 1]) {
      expect(placesIn(iteration)).toEqual([
        ["s", { stepPath: ["rework", "s"] }],
        ["m[0]", { stepPath: ["rework", "m"], mapIndex: 0 }],
        ["m[1]", { stepPath: ["rework", "m"], mapIndex: 1 }],
      ]);
    }
  });

  test("an onTrigger body step has the same place on each event", async () => {
    const def = sectionWorkflow();
    const runId = "run-section";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const invocations: Invocation[] = [];
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs: createInMemoryBlobSubstrate(),
        signalChannel: channel,
        invokeStep: recordingInvoker(invocations),
        bodies: new Map([["body-ref", oneStepBody]]),
      }),
      { runId, triggerPayload: { text: "event-0" } },
    );
    await waitForPark(repoStore, runId, "section", "input", 1);
    const input = (await repoStore.read(runId)).find(
      (e) => e.kind === "SignalAwaited" && e.parkKind === "input",
    );
    if (input?.kind !== "SignalAwaited") throw new Error("no input park");
    await channel.deliver(input.signalName, { text: "event-1" }, "ev-1");
    await waitForPark(repoStore, runId, "section", "input", 2);
    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);

    expect(invocations.map((i) => [i.runId, i.place])).toEqual([
      [sectionBodyRunId(runId, "section", 0), { stepPath: ["section", "s"] }],
      [sectionBodyRunId(runId, "section", 1), { stepPath: ["section", "s"] }],
    ]);
  });

  test("a childWorkflow child step", async () => {
    const child = defineWorkflow({
      id: "step-place-child",
      trigger: { type: "manual" },
      steps: { ask: step({ agent }) },
    });
    const { workflow: def, bodies } = rewriteInlineChildWorkflowBodies(
      defineWorkflow({
        id: "step-place-parent",
        trigger: { type: "manual" },
        steps: { spawn: childWorkflow({ definition: child }) },
      }),
    );
    const invocations: Invocation[] = [];
    const result = await runtimeRun(
      def,
      buildEnv({
        def,
        repoStore: createInMemoryRepoStore(),
        blobs: createInMemoryBlobSubstrate(),
        signalChannel: createInMemorySignalChannel(),
        invokeStep: recordingInvoker(invocations),
        bodies: new Map(bodies.map((body) => [body.ref, body.definition])),
      }),
      { runId: "run-child" },
    ).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(invocations.map((i) => [i.stepId, i.place])).toEqual([
      ["ask", { stepPath: ["spawn", "ask"] }],
    ]);
  });
});

describe("a body step keeps its place across a crash-resume", () => {
  test.each([
    ["a loop body", "rework"],
    ["an onTrigger body", "section"],
  ])("%s step re-invoked with a grant", async (_name, containerId) => {
    const runId = `run-resume-${containerId}`;
    const def =
      containerId === "rework" ? loopWorkflow(oneStepBody) : sectionWorkflow();
    const bodyRunId =
      containerId === "rework"
        ? loopBodyRunId(runId, "rework", 0)
        : sectionBodyRunId(runId, "section", 0);
    const bodies = new Map([["body-ref", oneStepBody]]);
    const blobs = createInMemoryBlobSubstrate();
    const invocations: Invocation[] = [];
    const invokeStep = recordingInvoker(invocations, { suspendFirst: true });

    const repoStore1 = createInMemoryRepoStore();
    const crash = new AbortController();
    const first = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore: repoStore1,
        blobs,
        signalChannel: createInMemorySignalChannel(),
        invokeStep,
        bodies,
      }),
      { runId, localAbort: crash.signal, triggerPayload: { text: "event-0" } },
    );
    await waitForPark(repoStore1, runId, containerId, "approval", 1);
    // The crash leaves both runs parked. Copy the logs before the abort tears
    // the runs down.
    const parentLog = [...(await repoStore1.read(runId))];
    const bodyLog = [...(await repoStore1.read(bodyRunId))];
    crash.abort();
    await first.complete.catch(() => undefined);

    const repoStore2 = createInMemoryRepoStore();
    await seedLog(repoStore2, bodyRunId, bodyLog);
    const channel2 = createInMemorySignalChannel();
    const second = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore: repoStore2,
        blobs,
        signalChannel: channel2,
        invokeStep,
        bodies,
      }),
      { runId, resumeFromEvents: parentLog },
    );
    await channel2.deliver(
      signalName("corr-1"),
      { outcome: "approved" },
      "g-1",
    );
    await waitForEvent(
      repoStore2,
      bodyRunId,
      (e) => e.kind === "StepCompleted" && e.stepId === "s",
    );
    await second.cancel("supervisor-operator", "test done");
    await second.complete.catch(() => undefined);

    const place = { stepPath: [containerId, "s"] };
    expect(invocations).toEqual([
      { runId: bodyRunId, stepId: "s", place, resumed: false },
      { runId: bodyRunId, stepId: "s", place, resumed: true },
    ]);
  });
});

describe("the park hooks receive the place of the step that parked", () => {
  test.each([
    ["a loop body", "rework"],
    ["an onTrigger body", "section"],
  ])(
    "%s step whose stored park never reached its log",
    async (_name, containerId) => {
      const runId = `run-lost-${containerId}`;
      const def =
        containerId === "rework"
          ? loopWorkflow(oneStepBody)
          : sectionWorkflow();
      const bodyRunId =
        containerId === "rework"
          ? loopBodyRunId(runId, "rework", 0)
          : sectionBodyRunId(runId, "section", 0);
      const bodies = new Map([["body-ref", oneStepBody]]);
      const blobs = createInMemoryBlobSubstrate();
      const invocations: Invocation[] = [];
      const invokeStep = recordingInvoker(invocations, { suspendFirst: true });

      const repoStore1 = createInMemoryRepoStore();
      const crash = new AbortController();
      const first = runtimeRun(
        def,
        buildEnv({
          def,
          repoStore: repoStore1,
          blobs,
          signalChannel: createInMemorySignalChannel(),
          invokeStep,
          bodies,
        }),
        {
          runId,
          localAbort: crash.signal,
          triggerPayload: { text: "event-0" },
        },
      );
      await waitForPark(repoStore1, runId, containerId, "approval", 1);
      // The crash came after the body step stored its park and before the park
      // reached the body's log or the container's.
      const parentLog = [...(await repoStore1.read(runId))];
      const bodyLog = [...(await repoStore1.read(bodyRunId))];
      crash.abort();
      await first.complete.catch(() => undefined);

      const repoStore2 = createInMemoryRepoStore();
      await seedLog(
        repoStore2,
        bodyRunId,
        bodyLog.slice(0, bodyLog.findIndex(isPark)),
      );
      const channel2 = createInMemorySignalChannel();
      const hookCalls: { hook: string; stepId: string; place: StepPlace }[] =
        [];
      const second = runtimeRun(
        def,
        buildEnv({
          def,
          repoStore: repoStore2,
          blobs,
          signalChannel: channel2,
          invokeStep,
          bodies,
          readParkedApprovalOps: async (args) => {
            hookCalls.push({ hook: "read", ...args });
            return args.runId === bodyRunId && args.stepId === "s"
              ? [{ correlationId: "corr-1" }]
              : [];
          },
          loadParkedApproval: async (args) => {
            hookCalls.push({ hook: "load", ...args });
            return approvalSnapshot;
          },
          persistRecoveredPark: async (args) => {
            hookCalls.push({ hook: "persist", ...args });
          },
        }),
        {
          runId,
          resumeFromEvents: parentLog.slice(0, parentLog.findIndex(isPark)),
        },
      );
      await waitForPark(repoStore2, runId, containerId, "approval", 1);
      await channel2.deliver(
        signalName("corr-1"),
        { outcome: "approved" },
        "g-1",
      );
      await waitForEvent(
        repoStore2,
        bodyRunId,
        (e) => e.kind === "StepCompleted" && e.stepId === "s",
      );
      await second.cancel("supervisor-operator", "test done");
      await second.complete.catch(() => undefined);

      const place = { stepPath: [containerId, "s"] };
      for (const hook of ["read", "load", "persist"]) {
        expect(hookCalls.filter((call) => call.hook === hook)).not.toEqual([]);
      }
      expect(
        hookCalls.map((call) => ({ stepId: call.stepId, place: call.place })),
      ).toEqual(hookCalls.map(() => ({ stepId: "s", place })));
    },
  );

  test("the place the runtime gives a step equals the place the ChildSpawned walk derives", async () => {
    const leaf = defineWorkflow({
      id: "step-place-leaf",
      trigger: { type: "manual" },
      steps: { ask: step({ agent }) },
    });
    const { workflow: body, bodies: leaves } = rewriteInlineChildWorkflowBodies(
      defineWorkflow({
        id: "step-place-walk-body",
        trigger: { type: "manual" },
        steps: { spawn: childWorkflow({ definition: leaf }) },
      }),
    );
    const def = sectionWorkflow();
    const runId = "run-walk";
    const repoStore = createInMemoryRepoStore();
    const invocations: Invocation[] = [];
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs: createInMemoryBlobSubstrate(),
        signalChannel: createInMemorySignalChannel(),
        invokeStep: recordingInvoker(invocations, { suspendFirst: true }),
        bodies: new Map([
          ["body-ref", body],
          ...leaves.map((l) => [l.ref, l.definition] as const),
        ]),
      }),
      { runId, triggerPayload: { text: "event-0" } },
    );
    await waitForPark(repoStore, runId, "section", "approval", 1);

    const loaded: StepPlace[] = [];
    const state = resumeFromLog(runId, await repoStore.read(runId));
    const snapshot = await findParkedApprovalSnapshot({
      readLog: (id) => repoStore.read(id),
      load: async (args) => {
        loaded.push(args.place);
        return args.stepId === "ask" ? approvalSnapshot : undefined;
      },
      runId,
      stepPathPrefix: [],
      state,
      stepId: "section",
      attempt: 1,
      correlationId: "corr-1",
    });
    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);

    expect(snapshot).toEqual(approvalSnapshot);
    const [invoked] = invocations;
    expect(invoked?.place).toEqual({ stepPath: ["section", "spawn", "ask"] });
    expect(loaded.at(-1)).toEqual(invoked?.place);
    expect(loaded).toEqual([
      { stepPath: ["section"] },
      { stepPath: ["section", "spawn"] },
      { stepPath: ["section", "spawn", "ask"] },
    ]);
  });
});
