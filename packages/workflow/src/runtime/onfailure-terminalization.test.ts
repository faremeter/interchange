// A unit carrying onFailure whose WORK succeeds but whose terminalization --
// pruning the handler branch, or committing StepCompleted -- fails (a transient
// durable-store blip) must NOT be inverted into a routed failure or retried.
// It lands a bare failure so the run fails loudly via the verdict. Before the
// fix, the terminalization throw fell into the same catch as an invocation
// failure and routed the succeeded unit to its onFailure handler.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  action,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  step,
  StepOutcomeCommitError,
  type Primitive,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRun,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { bufferedEvents, commit } from "./commit-chain";
import { createTerminalChildHandle } from "./suspendable-child-handle";

import { holdFirstUntilSecond } from "./skip-race.test-helpers";

function agent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
}

// A deployed `{ ref }` childWorkflow unit resolved by the env's spawnChild.
function cwUnit(): Primitive {
  return {
    kind: "childWorkflow",
    id: "",
    definition: { ref: "child-ref" },
    drainBehavior: "cancel",
    onFailure: "rescue",
  };
}

// Wrap the in-memory blobs so recordOutput throws ONCE when its key matches --
// a deterministic transient store blip in the success-terminalization window.
// recordOutput is the single choke point: the unit's own StepCompleted records
// under its bare stepId, and each skip StepStarted records under `<id>.input`.
function faultingBlobs(
  inner: WorkflowRuntimeEnv["blobs"],
  faultOnKey: string,
): WorkflowRuntimeEnv["blobs"] {
  let armed = true;
  return {
    ...inner,
    async recordOutput(stepId, attempt, value) {
      if (armed && stepId === faultOnKey) {
        armed = false;
        throw new Error("store blip");
      }
      return inner.recordOutput(stepId, attempt, value);
    },
  };
}

function buildEnv(
  def: WorkflowDefinition,
  opts: {
    invokeAction?: WorkflowRuntimeEnv["invokeAction"];
    blobs?: WorkflowRuntimeEnv["blobs"];
  } = {},
): {
  env: WorkflowRuntimeEnv;
  repoStore: ReturnType<typeof createInMemoryRepoStore>;
} {
  const clock = (): Date => new Date();
  const repoStore = createInMemoryRepoStore();
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs: opts.blobs ?? createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: async () => ({ output: null }),
    spawnChild: async () => createTerminalChildHandle("completed"),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(def),
    hasUpstreamSignalResolver: true,
    ...(opts.invokeAction !== undefined
      ? { invokeAction: opts.invokeAction }
      : {}),
  };
  return { env, repoStore };
}

const isUnitFailed = (e: WorkflowEvent): boolean =>
  e.kind === "StepFailed" && e.stepId === "unit";

// The durable log a host lost right after the unit's StepFailed was committed
// leaves. The first read once that commit returns is its runner's own reload,
// before the run's loop next writes anything.
function durableAfterUnitFailed(
  env: WorkflowRuntimeEnv,
): () => readonly WorkflowEvent[] {
  const inner = env.repoStore;
  let durable: readonly WorkflowEvent[] | undefined;
  env.repoStore = {
    read: async (id) => {
      const log = await inner.read(id);
      const committed =
        log.some(isUnitFailed) || bufferedEvents(id).some(isUnitFailed);
      if (durable === undefined && committed) durable = [...log];
      return log;
    },
    append: (id, event) => inner.append(id, event),
    appendBatch: (id, events) => inner.appendBatch(id, events),
    subscribe: (id, opts) => inner.subscribe(id, opts),
  };
  return () => {
    if (durable === undefined) throw new Error("the unit never failed");
    return durable;
  };
}

function unitRoutedTo(log: readonly WorkflowEvent[]): unknown {
  const failed = log.find(
    (e): e is Extract<WorkflowEvent, { kind: "StepFailed" }> =>
      e.kind === "StepFailed" && e.stepId === "unit",
  );
  return failed?.routedTo;
}

// The handler reads the trigger rather than the unit's output, so it is
// invoked whenever the scheduler offers it, not only when the unit completed.
function rescueStep(): Primitive {
  return step({
    agent: agent("rescue"),
    after: ["unit"],
    input: { from: "trigger.payload" },
  });
}

function recordInvocations(env: WorkflowRuntimeEnv): (string | undefined)[] {
  const invoked: (string | undefined)[] = [];
  env.invokeStep = async (req) => {
    invoked.push(req.authzContext.stepId);
    return { output: null };
  };
  return invoked;
}

function stepDef(id: string): WorkflowDefinition {
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: {
      unit: step({ agent: agent("unit"), onFailure: "rescue" }),
      rescue: rescueStep(),
      normal: step({ agent: agent("normal"), after: ["unit"] }),
    },
  });
}

describe("onFailure success-terminalization failures do not route", () => {
  test("a step whose StepCompleted commit fails lands a bare failure, not routed", async () => {
    const def = stepDef("term-step-complete");
    // Fault on the unit's own output key -- the StepCompleted after a
    // successful invocation.
    const { env, repoStore } = buildEnv(def, {
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "unit"),
    });
    const invoked = recordInvocations(env);
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(unitRoutedTo(await repoStore.read("r"))).toBeUndefined();
    expect(invoked).not.toContain("rescue");
  });

  test("a step whose success prune fails lands a bare failure, not routed", async () => {
    const def = stepDef("term-step-prune");
    // Fault inside the success prune: the handler-branch skip StepStarted
    // records under `rescue.input`.
    const { env, repoStore } = buildEnv(def, {
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "rescue.input"),
    });
    const invoked = recordInvocations(env);
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(unitRoutedTo(await repoStore.read("r"))).toBeUndefined();
    expect(invoked).not.toContain("rescue");
  });

  test("an action whose StepCompleted commit fails lands a bare failure, not routed", async () => {
    const def = defineWorkflow({
      id: "term-action",
      trigger: { type: "manual" },
      steps: {
        unit: action({ handler: "do", onFailure: "rescue" }),
        rescue: rescueStep(),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const { env, repoStore } = buildEnv(def, {
      invokeAction: async () => ({ output: null }),
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "unit"),
    });
    const invoked = recordInvocations(env);
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(unitRoutedTo(await repoStore.read("r"))).toBeUndefined();
    expect(invoked).not.toContain("rescue");
  });

  test("a childWorkflow whose StepCompleted commit fails lands a bare failure, not routed", async () => {
    const def = defineWorkflow({
      id: "term-child",
      trigger: { type: "manual" },
      steps: {
        unit: cwUnit(),
        rescue: rescueStep(),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    // The child completes; the unit's StepCompleted (key "unit") then faults.
    const { env, repoStore } = buildEnv(def, {
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "unit"),
    });
    const invoked = recordInvocations(env);
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(unitRoutedTo(await repoStore.read("r"))).toBeUndefined();
    expect(invoked).not.toContain("rescue");
  });

  test("a multi-attempt step whose terminalization fails is not re-invoked", async () => {
    // The sentinel branch sits above the retry branch: a step that already did
    // its work must not be re-invoked when its terminal fails to land.
    const def = defineWorkflow({
      id: "term-noreinvoke",
      trigger: { type: "manual" },
      steps: {
        unit: step({
          agent: agent("unit"),
          onFailure: "rescue",
          retry: { maxAttempts: 2, initialBackoffMs: 1 },
        }),
        rescue: rescueStep(),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const { env, repoStore } = buildEnv(def, {
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "unit"),
    });
    const invoked = recordInvocations(env);
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked.filter((id) => id === "unit")).toHaveLength(1);
    expect(invoked).not.toContain("rescue");
    expect(unitRoutedTo(await repoStore.read("r"))).toBeUndefined();
  });

  test("a multi-attempt step whose invoker could not commit its outcome is neither retried nor routed", async () => {
    // The host commits a cold step's turn inside the invocation, after the
    // agent ran; losing that commit must not re-run the agent's work.
    const def = defineWorkflow({
      id: "term-uncommitted-outcome",
      trigger: { type: "manual" },
      steps: {
        unit: step({
          agent: agent("unit"),
          onFailure: "rescue",
          retry: { maxAttempts: 2, initialBackoffMs: 1 },
        }),
        rescue: rescueStep(),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const invoked: (string | undefined)[] = [];
    const { env, repoStore } = buildEnv(def);
    env.invokeStep = async (req) => {
      invoked.push(req.authzContext.stepId);
      if (req.authzContext.stepId === "unit") {
        throw new StepOutcomeCommitError(new Error("push blip"), "commit");
      }
      return { output: null };
    };
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked.filter((id) => id === "unit")).toHaveLength(1);
    expect(invoked).not.toContain("rescue");
    const log = await repoStore.read("r");
    expect(unitRoutedTo(log)).toBeUndefined();
    const failed = log.find(
      (e): e is Extract<WorkflowEvent, { kind: "StepFailed" }> =>
        e.kind === "StepFailed" && e.stepId === "unit",
    );
    expect(failed?.retriesExhausted).toBe(true);
  });

  test("a resume does not route a step whose invoker could not commit its outcome", async () => {
    const def = stepDef("term-uncommitted-outcome-resume");
    const live = buildEnv(def);
    live.env.invokeStep = async (req) => {
      if (req.authzContext.stepId === "unit") {
        throw new StepOutcomeCommitError(new Error("push blip"), "commit");
      }
      return { output: null };
    };
    const durable = durableAfterUnitFailed(live.env);
    await runtimeRun(def, live.env, { runId: "r", triggerPayload: null })
      .complete;

    const resume = buildEnv(def);
    const invoked = recordInvocations(resume.env);
    const res = await runtimeRun(def, resume.env, {
      runId: "r",
      triggerPayload: null,
      resumeFromEvents: durable(),
    }).complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).toEqual([]);
  });

  test("a resume does not route an action whose StepCompleted commit failed", async () => {
    const def = defineWorkflow({
      id: "term-action-resume",
      trigger: { type: "manual" },
      steps: {
        unit: action({ handler: "do", onFailure: "rescue" }),
        rescue: rescueStep(),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const live = buildEnv(def, {
      invokeAction: async () => ({ output: null }),
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "unit"),
    });
    const durable = durableAfterUnitFailed(live.env);
    await runtimeRun(def, live.env, { runId: "r", triggerPayload: null })
      .complete;

    const actions: string[] = [];
    const resume = buildEnv(def, {
      invokeAction: async (req) => {
        actions.push(req.handler);
        return { output: null };
      },
    });
    const invoked = recordInvocations(resume.env);
    const res = await runtimeRun(def, resume.env, {
      runId: "r",
      triggerPayload: null,
      resumeFromEvents: durable(),
    }).complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).toEqual([]);
    expect(actions).toEqual([]);
  });

  test("a multi-attempt step whose invocation fails once still retries and completes", async () => {
    // Boundary pin: an ordinary invocation failure on attempt 1 still retries
    // -- the sentinel only diverts a post-success terminalization throw.
    const def = defineWorkflow({
      id: "term-boundary",
      trigger: { type: "manual" },
      steps: {
        unit: step({
          agent: agent("unit"),
          onFailure: "rescue",
          retry: { maxAttempts: 2, initialBackoffMs: 1 },
        }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    let unitInvocations = 0;
    const { env } = buildEnv(def);
    env.invokeStep = async (req) => {
      if (req.authzContext.stepId === "unit") {
        unitInvocations += 1;
        if (unitInvocations === 1) throw new Error("attempt 1 boom");
      }
      return { output: null };
    };
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("completed");
    expect(unitInvocations).toBe(2);
  });

  test("cancellation while preparing a retry neither retries nor invokes the failure handler", async () => {
    const def = defineWorkflow({
      id: "cancel-retry-preparation",
      trigger: { type: "manual" },
      steps: {
        unit: step({
          agent: agent("unit"),
          onFailure: "rescue",
          retry: { maxAttempts: 2, initialBackoffMs: 0 },
        }),
        rescue: rescueStep(),
      },
    });
    const { env } = buildEnv(def);
    const invoked: (string | undefined)[] = [];
    env.invokeStep = async (request) => {
      invoked.push(request.authzContext.stepId);
      throw new Error("temporary failure");
    };
    const runId = "cancel-retry-preparation";
    const cancellation = Promise.withResolvers<undefined>();
    env.newId = (prefix) => {
      // A cancel committed while the runner prepares its retry queues on the
      // run's commit chain ahead of the retry batch, after the runner's
      // earlier cancellation check. It must still win that race.
      if (prefix === "timer") {
        void commit(env, runId, {
          kind: "CancelRequested",
          seq: 0,
          at: new Date().toISOString(),
          reason: "cancel before retry",
          origin: "self",
        })
          .then(() => run.applyCommittedCancellation())
          .then(() => cancellation.resolve(undefined), cancellation.reject);
      }
      return `${prefix}-${crypto.randomUUID()}`;
    };
    const run = runtimeRun(def, env, { runId });
    const [result] = await Promise.all([run.complete, cancellation.promise]);

    expect(result.terminalStatus).toBe("cancelled");
    expect(invoked).toEqual(["unit"]);
    expect(
      result.events.some(
        (event) =>
          event.kind === "TimerSet" ||
          event.kind === "AttemptScheduled" ||
          event.kind === "TimerFired",
      ),
    ).toBe(false);
  });

  test("an unrouted failure whose handler branch cannot be skipped fails the run without the handler", async () => {
    // Neither the unit's output nor the skip of its handler branch can be
    // recorded. The handler reads the trigger, so it would be invoked if the
    // scheduler offered it.
    const def = defineWorkflow({
      id: "term-unskippable-handler",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({
          agent: agent("rescue"),
          after: ["unit"],
          input: { from: "trigger.payload" },
        }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const inner = createInMemoryBlobSubstrate();
    const { env } = buildEnv(def, {
      blobs: {
        ...inner,
        async recordOutput(stepId, attempt, value) {
          if (stepId === "unit" || stepId === "rescue.input") {
            throw new Error("store down");
          }
          return inner.recordOutput(stepId, attempt, value);
        },
      },
    });
    const invoked: string[] = [];
    env.invokeStep = async (req) => {
      invoked.push(req.agent.id);
      return { output: null };
    };

    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).toEqual(["unit"]);
    const failed = res.events.find(
      (e): e is Extract<WorkflowEvent, { kind: "StepFailed" }> =>
        e.kind === "StepFailed" && e.stepId === "unit",
    );
    expect(failed?.retriesExhausted).toBe(true);
    expect(failed?.routedTo).toBeUndefined();
    const terminal = res.events.at(-1);
    expect(terminal?.kind).toBe("RunFailed");
    if (terminal?.kind !== "RunFailed") throw new Error("unreachable");
    expect(terminal.error.message).toContain("store down");
  });

  test("a cancel that lands while an unrouted failure's branch is skipped cancels the run", async () => {
    const def = defineWorkflow({
      id: "term-cancel-mid-skip",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({
          agent: agent("rescue"),
          after: ["unit"],
          input: { from: "trigger.payload" },
        }),
      },
    });
    const inner = createInMemoryBlobSubstrate();
    const handle: { run?: WorkflowRun } = {};
    const { env } = buildEnv(def, {
      blobs: {
        ...inner,
        async recordOutput(stepId, attempt, value) {
          // The unit's output cannot be recorded, so it fails without
          // routing, and the cancel lands while its branch is skipped.
          if (stepId === "unit") throw new Error("store blip");
          if (stepId === "rescue.input") {
            await handle.run?.cancel("self", "cancel mid-skip");
          }
          return inner.recordOutput(stepId, attempt, value);
        },
      },
    });
    const invoked: string[] = [];
    env.invokeStep = async (req) => {
      invoked.push(req.agent.id);
      return { output: null };
    };
    handle.run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    const res = await handle.run.complete;

    expect(res.terminalStatus).toBe("cancelled");
    expect(invoked).toEqual(["unit"]);
  });

  test("an unrouted failure whose write fails stops a running sibling before the run rejects", async () => {
    const def = defineWorkflow({
      id: "term-unrouted-write-sibling",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: rescueStep(),
        sibling: step({ agent: agent("sibling") }),
      },
    });
    const { env, repoStore } = buildEnv(def, {
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "unit"),
    });
    // The unit's output cannot be recorded, so it lands an unrouted failure,
    // and writing that failure fails.
    const boom = new Error("failure write failed");
    const carriesUnitFailure = (events: readonly WorkflowEvent[]): boolean =>
      events.some((e) => e.kind === "StepFailed" && e.stepId === "unit");
    env.repoStore = {
      ...repoStore,
      async append(runId, event) {
        if (carriesUnitFailure([event])) throw boom;
        await repoStore.append(runId, event);
      },
      async appendBatch(runId, events) {
        if (carriesUnitFailure(events)) throw boom;
        await repoStore.appendBatch(runId, events);
      },
    };
    const siblingRunning = Promise.withResolvers<undefined>();
    let siblingAborted = false;
    env.invokeStep = async (req) => {
      if (req.agent.id === "unit") {
        await siblingRunning.promise;
        return { output: null };
      }
      siblingRunning.resolve(undefined);
      return new Promise((_, reject) => {
        req.signal.addEventListener(
          "abort",
          () => {
            siblingAborted = true;
            reject(new Error("aborted"));
          },
          { once: true },
        );
      });
    };

    const complete = runtimeRun(def, env, {
      runId: "r",
      triggerPayload: null,
    }).complete;

    await expect(complete).rejects.toBe(boom);
    expect(siblingAborted).toBe(true);
    const log = await repoStore.read("r");
    expect(
      log.some(
        (e) =>
          (e.kind === "StepCompleted" || e.kind === "StepFailed") &&
          e.stepId === "sibling",
      ),
    ).toBe(false);
    expect(log.some((e) => e.kind === "RunFailed")).toBe(false);
  });

  test("a skip whose input record fails stops a running sibling before the run fails", async () => {
    const def = defineWorkflow({
      id: "term-skip-record-sibling",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: rescueStep(),
        sibling: step({ agent: agent("sibling") }),
      },
    });
    // The unit's output cannot be recorded, so its failure does not route,
    // and recording the skipped handler's input fails too.
    const { env, repoStore } = buildEnv(def, {
      blobs: faultingBlobs(
        faultingBlobs(createInMemoryBlobSubstrate(), "rescue.input"),
        "unit",
      ),
    });
    const siblingRunning = Promise.withResolvers<undefined>();
    let siblingAborted = false;
    let siblingAbortedAtRunFailed: boolean | undefined;
    env.repoStore = {
      ...repoStore,
      async append(runId, event) {
        if (event.kind === "RunFailed") {
          siblingAbortedAtRunFailed = siblingAborted;
        }
        await repoStore.append(runId, event);
      },
      async appendBatch(runId, events) {
        if (events.some((e) => e.kind === "RunFailed")) {
          siblingAbortedAtRunFailed = siblingAborted;
        }
        await repoStore.appendBatch(runId, events);
      },
    };
    env.invokeStep = async (req) => {
      if (req.agent.id === "unit") {
        await siblingRunning.promise;
        return { output: null };
      }
      siblingRunning.resolve(undefined);
      return new Promise((_, reject) => {
        req.signal.addEventListener(
          "abort",
          () => {
            siblingAborted = true;
            reject(new Error("aborted"));
          },
          { once: true },
        );
      });
    };

    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(siblingAbortedAtRunFailed).toBe(true);
  });

  test("an unrouted failure and a success skip a join below both handlers once", async () => {
    const def = defineWorkflow({
      id: "term-shared-join",
      trigger: { type: "manual" },
      steps: {
        u1: step({ agent: agent("u1"), onFailure: "h1" }),
        u2: step({ agent: agent("u2"), onFailure: "h2" }),
        h1: step({ agent: agent("h1"), after: ["u1"] }),
        h2: step({ agent: agent("h2"), after: ["u2"] }),
        join: step({ agent: agent("join"), after: ["h1", "h2"] }),
      },
    });
    // u1's output cannot be recorded, so it fails without routing while u2
    // succeeds, and both skips reach the join before either writes it.
    const { env, repoStore } = buildEnv(def, {
      blobs: holdFirstUntilSecond(
        faultingBlobs(createInMemoryBlobSubstrate(), "u1"),
        "join.input",
      ),
    });
    const invoked: string[] = [];
    env.invokeStep = async (req) => {
      invoked.push(req.agent.id);
      return { output: null };
    };
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect([...invoked].sort()).toEqual(["u1", "u2"]);
    const log = await repoStore.read("r");
    expect(
      log.some((e) => e.kind === "StepCompleted" && e.stepId === "u2"),
    ).toBe(true);
    expect(
      log.filter((e) => e.kind === "StepStarted" && e.stepId === "join"),
    ).toHaveLength(1);
  });
});
