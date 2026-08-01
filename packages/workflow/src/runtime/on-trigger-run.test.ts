// runOnTrigger: a long-lived onTrigger section services each occurrence of
// its trigger as an EVENT -- spawning the body via the suspendable-child
// seam, awaiting the body's terminal (proxying any body approval park up on
// the SAME correlation via this run's own park machinery), then re-arming on
// a snapshot-less input park to await the next occurrence. The container never
// self-completes; it settles only when a body run ends non-completed
// (terminal-is-final) or the run is cancelled.
//
// This exercises the runtime driver in isolation with a test-wired
// spawnSuspendableChild -- no sidecar. The DEPLOYED section form (a `{ ref }`
// body) is built directly, since the deploy step is a separate layer.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";
import { signalName } from "@intx/types";
import type { ApprovalSnapshot } from "@intx/types/runtime";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  type Primitive,
  type RepoStore,
  type SignalChannel,
  type SpawnSuspendableChild,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const noopInvokeStep: StepInvoker = () => {
  throw new Error("onTrigger test: invokeStep must not be called");
};

function buildEnv(args: {
  def: WorkflowDefinition;
  repoStore: RepoStore;
  signalChannel: SignalChannel;
  spawnSuspendableChild: SpawnSuspendableChild;
}): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  return {
    repoStore: args.repoStore,
    scheduler: createInMemoryScheduler({ repoStore: args.repoStore, clock }),
    signalChannel: args.signalChannel,
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: noopInvokeStep,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    spawnSuspendableChild: args.spawnSuspendableChild,
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(args.def),
  };
}

// The DEPLOYED form of a section carries a body ref (deploy materializes the
// inline body to a workflow asset); build it directly since runOnTrigger runs
// the body by ref.
function sectionWorkflow(): WorkflowDefinition {
  const section: Primitive = {
    kind: "onTrigger",
    id: "",
    on: { type: "mail", to: "ins_sec@t.example" },
    body: { ref: "body-ref" },
    drainBehavior: "wait",
  };
  return defineWorkflow({ id: "on-trigger-run", steps: { section } });
}

async function waitForPark(
  repoStore: RepoStore,
  runId: string,
  parkKind: "input" | "approval",
  count: number,
): Promise<string> {
  for (let i = 0; i < 200; i += 1) {
    const events = await repoStore.read(runId);
    const awaits = events.filter(
      (e) => e.kind === "SignalAwaited" && e.parkKind === parkKind,
    );
    const latest = awaits[awaits.length - 1];
    if (awaits.length >= count && latest?.kind === "SignalAwaited") {
      return latest.signalName;
    }
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`timed out waiting for ${parkKind} park #${String(count)}`);
}

describe("runOnTrigger", () => {
  test("services each event as a body child run, re-arming on an input park", async () => {
    const runId = "sec-run";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const spawnRefs: string[] = [];
    const spawnInputs: unknown[] = [];
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      definitionRef,
      input,
    }) => {
      spawnRefs.push(definitionRef);
      spawnInputs.push(input);
      let done = false;
      return {
        next: async () => {
          if (done) throw new Error("next after terminal");
          done = true;
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async () => {
          throw new Error("resume on a non-suspending body");
        },
      };
    };
    const def = sectionWorkflow();
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: channel,
        spawnSuspendableChild,
      }),
      { runId, triggerPayload: { text: "event-0" } },
    );

    const ch1 = await waitForPark(repoStore, runId, "input", 1);
    await channel.deliver(ch1, { text: "event-1" }, "sig-1");
    await waitForPark(repoStore, runId, "input", 2);

    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "RunStarted").length).toBe(1);
    expect(log.filter((e) => e.kind === "StepStarted").length).toBe(1);
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(2);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(2);
    expect(
      log.some((e) => e.kind === "RunCompleted" || e.kind === "RunFailed"),
    ).toBe(false);
    expect(spawnRefs).toEqual(["body-ref", "body-ref"]);
    expect(spawnInputs).toEqual([{ text: "event-0" }, { text: "event-1" }]);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual(["section__0", "section__1"]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("proxies a body approval park up on the same correlation and resumes the child", async () => {
    const runId = "sec-approve";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const snapshot: ApprovalSnapshot = {
      name: "charge_card",
      description: "Charge the customer",
      inputSchema: {},
      arguments: { amount: 100 },
    };
    let resumedCorr: string | undefined;
    let resumedDecision: unknown;
    const bodyCorr = "body-corr-1";
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      let stage = 0;
      return {
        next: async () => {
          stage += 1;
          if (stage === 1) {
            return {
              kind: "park",
              park: { correlationId: bodyCorr, approvalSnapshot: snapshot },
            };
          }
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async (correlationId, decision) => {
          resumedCorr = correlationId;
          resumedDecision = decision;
        },
      };
    };
    const def = sectionWorkflow();
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: channel,
        spawnSuspendableChild,
      }),
      { runId, triggerPayload: { text: "event-0" } },
    );

    // The body parked on approval -> the section proxied it up as an approval
    // park on the SAME correlation, carrying the body's snapshot. Grant it.
    const approvalChannel = await waitForPark(repoStore, runId, "approval", 1);
    expect(approvalChannel).toBe(signalName(bodyCorr));
    await channel.deliver(approvalChannel, { approved: true }, "grant-1");

    // The section relayed the grant into the child; the body completed and the
    // section re-armed for the next event on an input park.
    await waitForPark(repoStore, runId, "input", 1);
    expect(resumedCorr).toBe(bodyCorr);
    expect(resumedDecision).toEqual({ approved: true });

    const log = await repoStore.read(runId);
    expect(
      log.some((e) => e.kind === "SignalAwaited" && e.parkKind === "approval"),
    ).toBe(true);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("a failed body run ends the section (terminal-is-final)", async () => {
    const runId = "sec-fail";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const spawnSuspendableChild: SpawnSuspendableChild = async () => ({
      next: async () => ({ kind: "terminal", terminalStatus: "failed" }),
      resume: async () => undefined,
    });
    const def = sectionWorkflow();
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: channel,
        spawnSuspendableChild,
      }),
      { runId, triggerPayload: { text: "event-0" } },
    );

    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(1);
    expect(
      log.some((e) => e.kind === "SignalAwaited" && e.parkKind === "input"),
    ).toBe(false);
  });
});
