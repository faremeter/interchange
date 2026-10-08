// The finite trigger-budget seed: a respawned run recovers how many triggers
// its step has serviced by counting the step's input-park `SignalAwaited`s in
// the durable log. The tests crash where an off-by-one is observable:
//
//  1. Crash after turn 1 of 3: an overcounting seed completes a trigger
//     early; an undercounting seed re-arms one extra time and never
//     completes. The final log must hold exactly two input SignalAwaiteds
//     (the crash-resume re-park re-adopts the durable one), and every
//     crash-recovered resume must be `kind: "input"`.
//  2. An approval park must not count: turn 1 suspends on an approval gate,
//     then the run crashes parked on the input re-arm. Were the approval
//     SignalAwaited counted, the respawned run would complete early.

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
import { waitForEvent, waitForNthEvent } from "@intx/workflow/testing";

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
    hasUpstreamSignalResolver: true,
  };
}

function chatWorkflow(triggers: number | "unbounded"): WorkflowDefinition {
  return defineWorkflow({
    id: "budget-seed",
    trigger: { type: "mail", to: "run_seed@t.example" },
    steps: { s: step({ agent, triggers }) },
  });
}

// The re-arm mints a random `corr-<id>` channel, so the deliverer discovers it
// from the log. Strict equality on `parkKind` -- the reducer reads an absent
// parkKind as "approval", this does not.
async function waitForInputPark(
  repoStore: RepoStore,
  runId: string,
  count: number,
): Promise<string> {
  const event = await waitForNthEvent(
    repoStore,
    runId,
    (e) => e.kind === "SignalAwaited" && e.parkKind === "input",
    count,
  );
  if (event.kind !== "SignalAwaited") {
    throw new Error(`expected SignalAwaited, got ${event.kind}`);
  }
  return event.signalName;
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
    // Hydrated definitions skip `step()`/`map()`, so the runStep read-point
    // guard protects persisted definitions. Build the forbidden combination
    // as hydration would: the step must fail before any agent invocation.
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

    // Every crash-recovered resume came back "input", not the legacy-approval read.
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

    // Turn 1 suspends on an approval gate; later turns are plain input resumes.
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
    await waitForEvent(
      repoStore,
      runId,
      (e) =>
        e.kind === "SignalAwaited" && e.signalName === signalName("corr-appr"),
    );
    await channelA.deliver(signalName("corr-appr"), { text: "one" }, "sig-a");
    const ch1 = await waitForInputPark(repoStore, runId, 1);

    // Crash parked on the input re-arm. The log holds one approval and one
    // input SignalAwaited; the seed must be 1.
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
