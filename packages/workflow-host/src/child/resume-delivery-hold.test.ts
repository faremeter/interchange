// A signal for a park a run resumed with lands only once the run waits on that
// park again. The sweep lands the signal after each read a resume makes, which
// is where a signal landing unheld fails the step or loses the input.

import { describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { signalName } from "@intx/types";
import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  createTerminalChildHandle,
  defineWorkflow,
  resumeFromLog,
  runtimeRun,
  step,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { beginDelivery, sectionBodyRunId } from "@intx/workflow/runtime";

import {
  createResumeDeliveryHold,
  parkedSignalNames,
} from "./resume-delivery-hold";

const at = new Date().toISOString();

const tick = (): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, 0));

async function settled(
  waiting: Promise<boolean> | undefined,
): Promise<unknown> {
  await tick();
  return Promise.race([waiting, Promise.resolve("pending")]);
}

describe("resume delivery hold", () => {
  test("a name no resumed park holds can land at once", () => {
    const hold = createResumeDeliveryHold();
    hold.hold("run", ["held"]);
    expect(hold.until("run", "other")).toBeUndefined();
    expect(hold.until("other-run", "held")).toBeUndefined();
    hold.close();
  });

  test("a signal waits until the run parks on it again, and later ones land at once", async () => {
    const endWork = beginDelivery();
    const hold = createResumeDeliveryHold();
    hold.hold("run", ["a", "b"]);
    const a = hold.until("run", "a");
    const b = hold.until("run", "b");
    expect(await settled(a)).toBe("pending");

    hold.reparked("run", "a");
    expect(await settled(a)).toBe(true);
    expect(await settled(b)).toBe("pending");
    expect(hold.until("run", "a")).toBeUndefined();

    hold.reparked("run", "b");
    expect(await settled(b)).toBe(true);
    hold.close();
    endWork();
  });

  test("the run ending refuses a held signal", async () => {
    const endWork = beginDelivery();
    const hold = createResumeDeliveryHold();
    hold.hold("run", ["a"]);
    const a = hold.until("run", "a");
    hold.runEnded("run");
    expect(await settled(a)).toBe(false);
    hold.close();
    endWork();
  });

  test("the process going quiet lets a held signal go on", async () => {
    const endWork = beginDelivery();
    const hold = createResumeDeliveryHold();
    hold.hold("run", ["a"]);
    const a = hold.until("run", "a");
    expect(await settled(a)).toBe("pending");
    endWork();
    expect(await settled(a)).toBe(true);
    hold.close();
  });

  test("closing refuses a held signal", async () => {
    const endWork = beginDelivery();
    const hold = createResumeDeliveryHold();
    hold.hold("run", ["a"]);
    const a = hold.until("run", "a");
    hold.close();
    expect(await settled(a)).toBe(false);
    endWork();
  });
});

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "m" }] },
});

const started = (runId: string): WorkflowEvent => ({
  kind: "RunStarted",
  seq: 1,
  at,
  runId,
  definitionHash: "x",
  trigger: { type: "manual", payload: { text: "event-0" } },
});
const stepStarted = (stepId: string): WorkflowEvent => ({
  kind: "StepStarted",
  seq: 2,
  at,
  stepId,
  attempt: 1,
  input: { ref: "inline:null" },
});

type Scenario = {
  label: string;
  definition: WorkflowDefinition;
  name: string;
  seed: (runId: string) => WorkflowEvent[];
  sectionBody?: boolean;
};

const scenarios: Scenario[] = [
  {
    label: "an agent step's approval",
    definition: defineWorkflow({
      id: "hold-approval",
      trigger: { type: "manual" },
      steps: { a: step({ agent }) },
    }),
    name: signalName("corr-approval"),
    seed: (runId) => [
      started(runId),
      stepStarted("a"),
      {
        kind: "SignalAwaited",
        seq: 3,
        at,
        stepId: "a",
        signalName: signalName("corr-approval"),
        parkKind: "approval",
      },
    ],
  },
  {
    label: "an agent step's input",
    definition: defineWorkflow({
      id: "hold-input",
      trigger: { type: "manual" },
      steps: { a: step({ agent, triggers: 2 }) },
    }),
    name: signalName("corr-input"),
    seed: (runId) => [
      started(runId),
      stepStarted("a"),
      {
        kind: "SignalAwaited",
        seq: 3,
        at,
        stepId: "a",
        signalName: signalName("corr-input"),
        parkKind: "input",
      },
    ],
  },
  {
    label: "an onTrigger section's next event",
    definition: defineWorkflow({
      id: "hold-section",
      steps: {
        section: {
          kind: "onTrigger",
          id: "",
          on: { type: "mail", to: "run_sec@t.example" },
          body: { ref: "body-ref" },
          drainBehavior: "wait",
        },
      },
    }),
    name: signalName("corr-rearm"),
    sectionBody: true,
    seed: (runId) => [
      started(runId),
      stepStarted("section"),
      {
        kind: "ChildSpawned",
        seq: 3,
        at,
        stepId: "section",
        childRunId: sectionBodyRunId(runId, "section", 0),
        childDefinitionRef: "body-ref",
      },
      {
        kind: "ChildCompleted",
        seq: 4,
        at,
        childRunId: sectionBodyRunId(runId, "section", 0),
        terminalStatus: "completed",
      },
      {
        kind: "SignalAwaited",
        seq: 5,
        at,
        stepId: "section",
        signalName: signalName("corr-rearm"),
        parkKind: "input",
      },
    ],
  },
];

// Asks to deliver the signal after read `requestAfterRead` of the run's log, the
// way the child does: through the hold, then a commit, then the channel.
async function trial(
  scenario: Scenario,
  requestAfterRead: number,
): Promise<{ pastTheWindow: boolean }> {
  const runId = `run-${scenario.definition.id}-${String(requestAfterRead)}`;
  const clock = (): Date => new Date();
  const repoStore = createInMemoryRepoStore();
  const channel = createInMemorySignalChannel();
  const seed = scenario.seed(runId);
  const hold = createResumeDeliveryHold();
  hold.hold(runId, parkedSignalNames(resumeFromLog(runId, seed)));

  const decisions: unknown[] = [];
  const spawned = Promise.withResolvers<undefined>();
  let spawnCount = 0;
  let requested: Promise<void> | undefined;
  const requestDelivery = (): Promise<void> =>
    (requested ??= (async () => {
      const canLand = await (hold.until(runId, scenario.name) ?? true);
      expect(canLand).toBe(true);
      const tip = (await repoStore.read(runId)).at(-1);
      if (tip === undefined) throw new Error("run log empty");
      await repoStore.append(runId, {
        kind: "SignalReceived",
        seq: tip.seq + 1,
        at,
        signalName: scenario.name,
        signalId: "sig-hold",
        payload: { text: "decision" },
      });
      await channel.deliver(scenario.name, { text: "decision" }, "sig-hold");
    })());

  let reads = 0;
  let pastTheWindow = false;
  const env: WorkflowRuntimeEnv = {
    repoStore: {
      ...repoStore,
      read: async (id) => {
        const log = [...(await repoStore.read(id))];
        if (id === runId && log.length > 0) {
          reads += 1;
          if (reads === requestAfterRead) void requestDelivery();
        }
        return log;
      },
    },
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: channel,
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: async (req) => {
      if (req.resume !== undefined) decisions.push(req.resume.decision);
      return { output: null };
    },
    spawnChild: async () => createTerminalChildHandle("completed"),
    ...(scenario.sectionBody === true
      ? {
          spawnSuspendableChild: async () => {
            spawnCount += 1;
            spawned.resolve(undefined);
            return {
              next: () => new Promise(() => undefined),
              resume: async () => undefined,
              deliverSignal: async () => undefined,
            };
          },
        }
      : {}),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(scenario.definition),
    hasUpstreamSignalResolver: true,
    onSignalAwaiter: (park) => {
      hold.reparked(park.runId, park.name);
      if (requested === undefined) {
        pastTheWindow = true;
        void requestDelivery();
      }
    },
  };

  const run = runtimeRun(scenario.definition, env, {
    runId,
    resumeFromEvents: seed,
  });
  if (scenario.sectionBody === true) {
    await spawned.promise;
    expect(spawnCount).toBe(1);
  } else {
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(decisions).toEqual([{ text: "decision" }]);
  }
  await requested;
  const awaits = (await repoStore.read(runId)).filter(
    (event) =>
      event.kind === "SignalAwaited" && event.signalName === scenario.name,
  );
  expect(awaits).toHaveLength(1);
  if (scenario.sectionBody === true) {
    await run.cancel("supervisor-operator", "test done").catch(() => undefined);
  }
  hold.close();
  return { pastTheWindow };
}

describe("a signal asked for after each read of a resume", () => {
  for (const scenario of scenarios) {
    test(scenario.label, async () => {
      for (let read = 1; read <= 40; read += 1) {
        const { pastTheWindow } = await trial(scenario, read);
        if (pastTheWindow) return;
      }
      throw new Error("the run never parked on the signal again");
    });
  }
});
