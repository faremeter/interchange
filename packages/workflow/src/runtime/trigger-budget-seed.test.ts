// The finite trigger-budget seed: a respawned run recovers how many triggers
// its step has already serviced by counting the step's input-park
// `SignalAwaited`s in the durable log. These tests pin the seed at crash
// points where an off-by-one in either direction is observable (the
// durable-long-lived-run test crashes after turn 2 of 3, where an inflated
// seed is observationally identical to a correct one):
//
//   1. Crash after turn 1 of 3: an overcounting seed completes the run one
//      trigger early; an undercounting seed re-arms one extra time and the
//      run never completes. The final log must hold EXACTLY two input
//      SignalAwaiteds (one per re-arm; the crash-resume re-park re-adopts the
//      durable one rather than minting another), and every crash-recovered
//      resume must come back `kind: "input"`, not the legacy-approval read.
//   2. An approval park must NOT count toward the seed: turn 1 suspends on an
//      approval gate before replying, then the run crashes parked on the
//      input re-arm. Were the approval SignalAwaited counted, the respawned
//      run would complete a trigger early.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { signalName } from "@intx/types";
import type { ApprovalSnapshot, ConversationTurn } from "@intx/types/runtime";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  step,
  type BlobSubstrate,
  type RepoStore,
  type SignalChannel,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

function textOf(turn: ConversationTurn): string {
  return turn.content.map((b) => (b.type === "text" ? b.text : "")).join("");
}

function userTurn(text: string): ConversationTurn {
  return { role: "user", content: [{ type: "text", text }], timestamp: 0 };
}

function assistantTurn(text: string): ConversationTurn {
  return { role: "assistant", content: [{ type: "text", text }], timestamp: 0 };
}

const agent = defineAgent({
  id: "chat",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "m" }] },
});

const approvalSnapshot: ApprovalSnapshot = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

// A conversation store shared across the crash boundary (the durable stand-in;
// see durable-long-lived-run.test for the memory-durability proof itself).
interface Store {
  load(): Promise<ConversationTurn[]>;
  save(turns: ConversationTurn[]): Promise<void>;
}

function createStore(): Store {
  let turns: ConversationTurn[] = [];
  return {
    async load() {
      return turns.map((t) => ({ ...t, content: [...t.content] }));
    },
    async save(next) {
      turns = next.map((t) => ({ ...t, content: [...t.content] }));
    },
  };
}

function buildEnv(args: {
  def: WorkflowDefinition;
  repoStore: RepoStore;
  blobs: BlobSubstrate;
  signalChannel: SignalChannel;
  invokeStep: StepInvoker;
}): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  return {
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
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(args.def),
  };
}

function chatWorkflow(triggers: number | "unbounded"): WorkflowDefinition {
  return defineWorkflow({
    id: "budget-seed",
    trigger: { type: "mail", to: "ins_seed@t.example" },
    steps: { s: step({ agent, triggers }) },
  });
}

async function waitForInputPark(
  repoStore: RepoStore,
  runId: string,
  count: number,
): Promise<string> {
  for (let i = 0; i < 200; i += 1) {
    const events = await repoStore.read(runId);
    const inputAwaits = events.filter(
      (e) => e.kind === "SignalAwaited" && e.parkKind === "input",
    );
    const latest = inputAwaits[inputAwaits.length - 1];
    if (inputAwaits.length >= count && latest?.kind === "SignalAwaited") {
      return latest.signalName;
    }
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`timed out waiting for input park #${String(count)}`);
}

async function waitForApprovalPark(
  repoStore: RepoStore,
  runId: string,
  name: string,
): Promise<void> {
  for (let i = 0; i < 200; i += 1) {
    const events = await repoStore.read(runId);
    if (
      events.some((e) => e.kind === "SignalAwaited" && e.signalName === name)
    ) {
      return;
    }
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`timed out waiting for approval park ${name}`);
}

function createChatInvoker(store: Store): StepInvoker {
  return async (req) => {
    const inbound =
      req.resume !== undefined
        ? // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test payload shape
          (req.resume.decision as { text: string })
        : // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test payload shape
          (req.input as { text: string });
    const convo = await store.load();
    convo.push(userTurn(inbound.text));
    const heard = convo.filter((t) => t.role === "user").map(textOf);
    const replyText = `reply#${String(heard.length)}; heard=[${heard.join("|")}]`;
    convo.push(assistantTurn(replyText));
    await store.save(convo);
    return { output: { finalReply: replyText, turns: convo.length } };
  };
}

describe("retry/budget combination at the runtime read point", () => {
  test("a hydrated retry + multi-trigger step fails loud at runStep entry", async () => {
    // A definition hydrated from workflow.json never passes through `step()`
    // or `map()`, so the runtime's read-point guard at runStep entry is the
    // enforcement that actually protects a persisted definition. Build the
    // forbidden combination directly, as hydration would, and drive it
    // through the runtime: the step must fail with the rejection -- BEFORE
    // any agent invocation -- rather than servicing a wrong conversation.
    const runId = "run-retry-budget-rejected";
    const def = chatWorkflow(3);
    const s = def.steps["s"];
    if (s?.kind !== "step") throw new Error("expected the chat step");
    const hydrated: WorkflowDefinition = {
      ...def,
      steps: {
        s: { ...s, retry: { maxAttempts: 2, initialBackoffMs: 1 } },
      },
    };

    let invoked = 0;
    const result = await runtimeRun(
      hydrated,
      buildEnv({
        def: hydrated,
        repoStore: createInMemoryRepoStore(),
        blobs: createInMemoryBlobSubstrate(),
        signalChannel: createInMemorySignalChannel(),
        invokeStep: async () => {
          invoked += 1;
          return { output: null };
        },
      }),
      { runId, triggerPayload: { text: "one" } },
    ).complete;

    expect(result.terminalStatus).toBe("failed");
    expect(invoked).toBe(0);
    const failures = result.events.filter((e) => e.kind === "StepFailed");
    expect(failures.length).toBeGreaterThan(0);
    for (const f of failures) {
      if (f.kind !== "StepFailed") throw new Error("unreachable");
      expect(f.error.message).toContain("cannot combine with a trigger budget");
    }
  });
});

describe("finite trigger-budget seed across a respawn", () => {
  test("budget 3, crash after turn 1: exactly two more triggers complete the run", async () => {
    const runId = "run-crash-after-1";
    const repoStore = createInMemoryRepoStore();
    const blobs = createInMemoryBlobSubstrate();
    const store = createStore();
    const def = chatWorkflow(3);

    const channelA = createInMemorySignalChannel();
    const runA = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs,
        signalChannel: channelA,
        invokeStep: createChatInvoker(store),
      }),
      { runId, triggerPayload: { text: "one" } },
    );

    // Turn 1 serviced; the step re-armed on input park #1. Crash here.
    const ch1 = await waitForInputPark(repoStore, runId, 1);
    void runA;
    const seed = await repoStore.read(runId);

    const channelB = createInMemorySignalChannel();
    const resumeKinds: string[] = [];
    const innerB = createChatInvoker(store);
    const invokeStepB: StepInvoker = async (req) => {
      if (req.resume !== undefined) resumeKinds.push(req.resume.kind);
      return innerB(req);
    };
    const runB = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs,
        signalChannel: channelB,
        invokeStep: invokeStepB,
      }),
      { runId, resumeFromEvents: seed },
    );

    // Deliver trigger 2 on the recovered channel; budget remaining (2 < 3)
    // must re-arm a SECOND time -- an overcounting seed would complete here.
    await waitForInputPark(repoStore, runId, 1);
    await channelB.deliver(ch1, { text: "two" }, "sig-2");
    const ch2 = await waitForInputPark(repoStore, runId, 2);
    await channelB.deliver(ch2, { text: "three" }, "sig-3");

    const result = await runB.complete;
    expect(result.terminalStatus).toBe("completed");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test output shape
    const finalOutput = result.outputs["s"] as {
      finalReply: string;
      turns: number;
    };
    expect(finalOutput.finalReply).toBe("reply#3; heard=[one|two|three]");
    expect(finalOutput.turns).toBe(6);

    // Every resume the respawned invoker saw was an input resume -- the
    // crash-recovered park kind came back as "input", not the
    // legacy-approval read.
    expect(resumeKinds).toEqual(["input", "input"]);

    // Exactly two input parks over the whole run: one per re-arm, none added
    // by the crash-resume re-park (it re-adopts the durable SignalAwaited).
    const finalLog = await repoStore.read(runId);
    expect(
      finalLog.filter(
        (e) => e.kind === "SignalAwaited" && e.parkKind === "input",
      ).length,
    ).toBe(2);
  });

  test("an approval park does not count toward the finite-budget seed", async () => {
    const runId = "run-approval-not-counted";
    const repoStore = createInMemoryRepoStore();
    const blobs = createInMemoryBlobSubstrate();
    const store = createStore();
    const def = chatWorkflow(3);

    // Turn 1 suspends on an approval gate before replying; the approved
    // resume produces turn 1's output. Later turns are plain input resumes.
    function createApprovalFirstInvoker(): StepInvoker {
      const inner = createChatInvoker(store);
      return async (req) => {
        if (req.resume === undefined) {
          return {
            suspend: {
              correlationId: "corr-appr",
              kind: "approval",
              approvalSnapshot,
            },
          };
        }
        if (req.resume.kind === "approval") {
          // The approved decision carries turn 1's text. Drop `resume`
          // structurally (exactOptionalPropertyTypes forbids assigning
          // undefined) so the inner invoker reads it as a first send.
          const { resume: _resume, ...rest } = req;
          return inner({ ...rest, input: req.resume.decision });
        }
        return inner(req);
      };
    }

    const channelA = createInMemorySignalChannel();
    const runA = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs,
        signalChannel: channelA,
        invokeStep: createApprovalFirstInvoker(),
      }),
      { runId, triggerPayload: { text: "one" } },
    );

    // Approve the gate; turn 1 completes and the step re-arms (park #1).
    await waitForApprovalPark(repoStore, runId, signalName("corr-appr"));
    await channelA.deliver(signalName("corr-appr"), { text: "one" }, "sig-a");
    const ch1 = await waitForInputPark(repoStore, runId, 1);

    // Crash parked on the input re-arm. The log now holds one approval
    // SignalAwaited and one input SignalAwaited; the seed must be 1.
    void runA;
    const seed = await repoStore.read(runId);

    const channelB = createInMemorySignalChannel();
    const runB = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs,
        signalChannel: channelB,
        invokeStep: createApprovalFirstInvoker(),
      }),
      { runId, resumeFromEvents: seed },
    );

    // Deliver trigger 2. If the approval park were counted, serviced would
    // reach 3 here and the run would complete a trigger early.
    await waitForInputPark(repoStore, runId, 1);
    await channelB.deliver(ch1, { text: "two" }, "sig-2");
    const ch2 = await waitForInputPark(repoStore, runId, 2);
    await channelB.deliver(ch2, { text: "three" }, "sig-3");

    const result = await runB.complete;
    expect(result.terminalStatus).toBe("completed");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test output shape
    const finalOutput = result.outputs["s"] as {
      finalReply: string;
      turns: number;
    };
    expect(finalOutput.finalReply).toBe("reply#3; heard=[one|two|three]");
    expect(finalOutput.turns).toBe(6);
  });
});
