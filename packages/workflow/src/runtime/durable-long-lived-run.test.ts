// A step's `triggers` budget drives how long it lives, and the runtime re-arms
// it between triggers -- N triggers become N turns in ONE run.
//
// This is the general run-topology: a step declares `triggers` (default 1). A
// batch step (1) completes on its first output. A finite budget services N
// triggers then completes. `"unbounded"` never self-completes -- it absorbs
// every trigger as another turn (the interactive agent). Between triggers the
// runtime parks the step on a snapshot-less input control-plane channel it
// mints per turn; the next delivered trigger is that turn's input. The
// delivering owner discovers the current channel from the run's reduced
// `awaitingSignal.name` -- this test discovers it the same way, from the
// durable log.
//
// The subtle part is memory across a respawn. The run's durable log records
// the PARKS and the delivered payloads, but the agent's intermediate replies
// live only in the warm reactor and die with the child. So a run designed to
// span many turns needs a DURABLE CONVERSATION STORE, written per turn and
// rehydrated on resume -- otherwise the run resumes, accepts the next trigger,
// and answers with amnesia. (The store here is an in-test stand-in; the
// run/step-keyed substrate that owns it in production already exists in the
// sidecar. This test pins the runtime behavior the store plugs into: the
// budget-driven re-arm, and crash-safe resume that enforces a finite budget
// across a respawn via the durable log.)

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import type { ConversationTurn } from "@intx/types/runtime";

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

// ---------------------------------------------------------------------------
// The durable conversation store the run rehydrates from. In production this
// is the sidecar's per-agent `agent-state/<key>` substrate; here an in-memory
// map the two "processes" (phase A and phase B) share by instance, standing in
// for a store that survives a child respawn. `load` returns a copy so a caller
// mutating the returned array cannot corrupt the stored history.
// ---------------------------------------------------------------------------
interface ConversationStore {
  load(runId: string): Promise<ConversationTurn[]>;
  save(runId: string, turns: ConversationTurn[]): Promise<void>;
}

function createDurableConversationStore(): ConversationStore {
  const byRun = new Map<string, ConversationTurn[]>();
  const clone = (turns: ConversationTurn[]): ConversationTurn[] =>
    turns.map((t) => ({ ...t, content: [...t.content] }));
  return {
    async load(runId) {
      const turns = byRun.get(runId);
      return turns === undefined ? [] : clone(turns);
    },
    async save(runId, turns) {
      byRun.set(runId, clone(turns));
    },
  };
}

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

// ---------------------------------------------------------------------------
// The reactor: a STATELESS invoker over the durable store. It holds no
// conversation in its closure -- every turn it rehydrates from the store,
// appends the inbound message, and persists. It returns `{ output }` every
// turn and NEVER parks itself: the runtime owns the re-arm (driven by the
// step's `triggers` budget). This is exactly the production shape -- the real
// adapter returns a reply as `{ output }`; only the runtime decides to keep the
// step alive. A fresh closure with an empty warm cache still answers with full
// memory because the memory lives in the store, not the process.
//
// Turn 1's message is the launch trigger (`req.input`); later turns' message is
// the delivered trigger (`req.resume.decision`, a `kind: "input"` resume).
// ---------------------------------------------------------------------------
function createReactor(
  store: ConversationStore,
  runId: string,
): { invokeStep: StepInvoker; warmInvocations: () => number } {
  let warmInvocations = 0;
  const invokeStep: StepInvoker = async (req) => {
    warmInvocations += 1;
    const inbound =
      req.resume !== undefined
        ? // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the delivered decision is the test's own mail payload shape
          (req.resume.decision as { text: string })
        : // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the launch input is the test's own mail payload shape
          (req.input as { text: string });
    const convo = await store.load(runId);
    convo.push(userTurn(inbound.text));
    const heard = convo.filter((t) => t.role === "user").map(textOf);
    const replyIndex = convo.filter((t) => t.role === "assistant").length + 1;
    const replyText = `reply#${String(replyIndex)}; heard=[${heard.join("|")}]`;
    convo.push(assistantTurn(replyText));
    await store.save(runId, convo);
    return { output: { finalReply: replyText, turns: convo.length } };
  };
  return { invokeStep, warmInvocations: () => warmInvocations };
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
    id: "durable-chat",
    trigger: { type: "mail", to: "run_chat@t.example" },
    steps: { s: step({ agent, triggers }) },
  });
}

// The reserved channels the runtime mints per re-arm are opaque to the caller
// (a `corr-<random>` id). The delivering owner discovers the CURRENT one from
// the reduced state; here we read it from the durable log: wait until the
// step has re-armed for the Nth time (N input `SignalAwaited`s) and return
// the latest input channel's name, the channel the next trigger is delivered
// on.
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

describe("step trigger budget drives re-arm", () => {
  test("a finite-budget step services N triggers as N turns in one run, across a respawn", async () => {
    const runId = "run-budget-3";
    // Durable substrates -- these SURVIVE the simulated respawn.
    const repoStore = createInMemoryRepoStore();
    const blobs = createInMemoryBlobSubstrate();
    const store = createDurableConversationStore();
    const def = chatWorkflow(3);

    // ---- Phase A: first "child process". Ephemeral channel + reactor. ------
    const channelA = createInMemorySignalChannel();
    const reactorA = createReactor(store, runId);
    const runA = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs,
        signalChannel: channelA,
        invokeStep: reactorA.invokeStep,
      }),
      { runId, triggerPayload: { text: "hello" } },
    );

    // Trigger 1 is the launch payload; the runtime runs turn 1 and, budget
    // remaining (1 < 3), re-arms on a fresh input channel.
    const ch1 = await waitForInputPark(repoStore, runId, 1);
    await channelA.deliver(ch1, { text: "how are you" }, "sig-2");
    const ch2 = await waitForInputPark(repoStore, runId, 2);

    // Still ONE run, not terminal, two turns serviced.
    const midLog = await repoStore.read(runId);
    expect(midLog.some((e) => e.kind === "RunCompleted")).toBe(false);
    expect(midLog.filter((e) => e.kind === "StepStarted").length).toBe(1);
    expect((await store.load(runId)).length).toBe(4); // 2 user + 2 assistant

    // ---- CRASH: abandon phase A parked awaiting trigger 3 (ch2). Keep the
    // durable substrates; discard the warm channel + reactor. ---------------
    void runA;
    const seed = await repoStore.read(runId);

    // ---- Phase B: respawn. FRESH channel + FRESH reactor (empty warm cache),
    // SAME durable substrates. The finite budget's serviced-count is recovered
    // from the log, so the run still completes at exactly 3 triggers. ---------
    const channelB = createInMemorySignalChannel();
    const reactorB = createReactor(store, runId);
    const runB = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs,
        signalChannel: channelB,
        invokeStep: reactorB.invokeStep,
      }),
      { runId, resumeFromEvents: seed },
    );

    // The resumed run re-parks on the recovered channel (ch2). Deliver the
    // third trigger; budget spent (3 == 3) -> the step completes, run ends.
    await waitForInputPark(repoStore, runId, 2);
    await channelB.deliver(ch2, { text: "bye" }, "sig-3");

    const result = await runB.complete;
    expect(result.terminalStatus).toBe("completed");

    // THE PROOF: turn 3 ran in a fresh process whose reactor held nothing, yet
    // its reply reflects ALL THREE triggers -- the memory came from the durable
    // store, not warm process state.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the step output is the reactor's own final-reply shape
    const finalOutput = result.outputs["s"] as {
      finalReply: string;
      turns: number;
    };
    expect(finalOutput.finalReply).toBe(
      "reply#3; heard=[hello|how are you|bye]",
    );
    expect(finalOutput.turns).toBe(6); // 3 user + 3 assistant

    // The fresh reactor serviced ONLY the third trigger -- turns 1 and 2 were
    // not replayed (their continuity is the store's, not a re-run's).
    expect(reactorB.warmInvocations()).toBe(1);

    // One run, one step bracket: a single StepStarted and a single
    // StepCompleted span all three turns.
    expect(result.events.filter((e) => e.kind === "StepStarted").length).toBe(
      1,
    );
    expect(result.events.filter((e) => e.kind === "StepCompleted").length).toBe(
      1,
    );

    // The intermediate replies lived ONLY in the store: the run event log never
    // carried "reply#1"/"reply#2" (it records parks + delivered triggers + the
    // single final StepCompleted output). This is why the store is load-bearing.
    const logText = JSON.stringify(result.events);
    expect(logText).not.toContain("reply#1");
    expect(logText).not.toContain("reply#2");

    // The full conversation is durable.
    expect(
      (await store.load(runId)).map((t) => `${t.role}:${textOf(t)}`),
    ).toEqual([
      "user:hello",
      "assistant:reply#1; heard=[hello]",
      "user:how are you",
      "assistant:reply#2; heard=[hello|how are you]",
      "user:bye",
      "assistant:reply#3; heard=[hello|how are you|bye]",
    ]);
  });

  test("an unbounded step never self-completes -- it re-arms after every trigger", async () => {
    const runId = "run-unbounded";
    const repoStore = createInMemoryRepoStore();
    const def = chatWorkflow("unbounded");
    const channel = createInMemorySignalChannel();
    const reactor = createReactor(createDurableConversationStore(), runId);
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs: createInMemoryBlobSubstrate(),
        signalChannel: channel,
        invokeStep: reactor.invokeStep,
      }),
      { runId, triggerPayload: { text: "one" } },
    );

    // Drive two triggers; after each the run re-arms rather than completing.
    const ch1 = await waitForInputPark(repoStore, runId, 1);
    await channel.deliver(ch1, { text: "two" }, "sig-2");
    await waitForInputPark(repoStore, runId, 2);

    const events = await repoStore.read(runId);
    expect(events.some((e) => e.kind === "RunCompleted")).toBe(false);
    expect(events.some((e) => e.kind === "StepCompleted")).toBe(false);
    // It is parked awaiting the next trigger, not done.
    const inputAwaits = events.filter(
      (e) => e.kind === "SignalAwaited" && e.parkKind === "input",
    );
    expect(inputAwaits.length).toBe(2);

    // The run would park forever; cancel to end the test cleanly.
    await run.cancel("self", "test teardown");
    await run.complete;
  });
});
