// One long-lived workflow run consumes N mails as N turns and survives a child
// respawn with its conversation memory intact -- the durability contract the
// trigger->run unification rests on. A run whose agent-shaped step parks on
// "await next input" (a snapshot-less `kind:"input"` control-plane park) and
// re-arms after each turn stays alive indefinitely; the supervisor delivers
// each inbound mail to that live run rather than firing a new run per mail.
//
// The subtle part is memory. The runtime's durable log records the PARKS and
// the delivered payloads (SignalAwaited/SignalReceived), but the agent's
// intermediate replies live only in the warm reactor and die with the child.
// So a run designed never to terminate needs a DURABLE CONVERSATION STORE,
// written per turn and rehydrated on resume -- otherwise the run resumes,
// accepts the next mail, and answers with amnesia. (The store here is an
// in-test stand-in; the run/step-keyed substrate that owns it in production
// lands in a later commit -- this test pins the runtime capability the store
// plugs into: input parks, re-arm, and crash-safe resume.)
//
// What this exercises against the REAL runtime (runtimeRun, the suspend/resume
// bridge in runStep, parkOnSignal, the input park kind, and the crash re-drive
// path), with in-memory substrates:
//
//   1. A single `step` workflow whose reactor parks on "await next mail"
//      between turns and RE-ARMS -- the runtime natively hosts a long-lived run
//      that consumes many external inputs as many turns (the `while(true)`
//      suspend/resume loop in runStep).
//   2. Each mail delivered via the signal channel resumes the SAME run (one
//      stable runId) for another turn -- NOT a new run per mail.
//   3. A durable conversation store the reactor writes each turn.
//   4. A simulated respawn: a FRESH signal channel and a FRESH reactor closure
//      (warm memory gone), but the SAME durable repoStore/blobs/conversation
//      store. The resumed run re-parks on the recovered channel, and the next
//      mail's turn rehydrates the full prior conversation FROM THE STORE.
//
// The proof of durability: after the respawn, the turn's reply reflects EVERY
// prior mail, even though the fresh reactor held nothing in memory and the run
// event log never carried the intermediate replies.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { signalName } from "@intx/types";
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
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

// ---------------------------------------------------------------------------
// The durable conversation store the spike is validating. In production this
// is a hub/sidecar-owned substrate keyed to the run; here an in-memory map
// that the two "processes" (phase A and phase B) share by instance, standing
// in for a store that survives a child respawn. `load` returns a copy so a
// caller mutating the returned array cannot corrupt the stored history.
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

const chatWorkflow = defineWorkflow({
  id: "durable-chat",
  trigger: { type: "mail", to: "ins_chat@t.example" },
  steps: { s: step({ agent }) },
});

// The reserved correlation a turn parks on before the Nth mail. Deterministic
// so the test can address deliveries; in production it is a minted id the hub
// registers out-of-band.
function awaitCorrelation(mailIndex: number): string {
  return `await-mail-${String(mailIndex)}`;
}

// ---------------------------------------------------------------------------
// The reactor: a STATELESS invoker over the durable store. It holds no
// conversation in its closure -- every turn it rehydrates from the store,
// appends the inbound mail and its reply, and persists. This is exactly the
// property a respawn needs: a fresh closure with an empty warm cache still
// answers with full memory because the memory lives in the store, not the
// process. A fresh step start parks awaiting mail #1; each delivered mail
// drives one turn and re-arms for the next, until "bye" ends the run.
// ---------------------------------------------------------------------------
function createReactor(
  store: ConversationStore,
  runId: string,
): { invokeStep: StepInvoker; warmInvocations: number } {
  const box = { warmInvocations: 0 };
  const invokeStep: StepInvoker = async (req) => {
    box.warmInvocations += 1;
    if (req.resume === undefined) {
      // Fresh step start (or a crash re-drive that re-parks before any
      // invoke): no mail yet -- park awaiting the first one.
      return { suspend: { correlationId: awaitCorrelation(1), kind: "input" } };
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the delivered decision is the test's own mail payload shape
    const mail = req.resume.decision as { text: string };
    const convo = await store.load(runId);
    convo.push(userTurn(mail.text));
    const heard = convo.filter((t) => t.role === "user").map(textOf);
    const replyIndex = convo.filter((t) => t.role === "assistant").length + 1;
    const replyText = `reply#${String(replyIndex)}; heard=[${heard.join("|")}]`;
    convo.push(assistantTurn(replyText));
    await store.save(runId, convo);
    if (mail.text === "bye") {
      return { output: { finalReply: replyText, turns: convo.length } };
    }
    // Re-arm: park awaiting the next mail.
    return {
      suspend: {
        correlationId: awaitCorrelation(heard.length + 1),
        kind: "input",
      },
    };
  };
  return {
    invokeStep,
    get warmInvocations() {
      return box.warmInvocations;
    },
  } as { invokeStep: StepInvoker; warmInvocations: number };
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

// Poll the durable log until it carries a SignalAwaited for `name` (a park is
// flushed durable before the step suspends, so this is the "the run is now
// waiting for this mail" barrier). Deliveries are FIFO-queued per name even if
// they land pre-await, but waiting keeps the sequence legible and the log
// assertions deterministic.
async function waitForAwait(
  repoStore: RepoStore,
  runId: string,
  name: string,
): Promise<void> {
  for (let i = 0; i < 100; i += 1) {
    const events = await repoStore.read(runId);
    if (
      events.some((e) => e.kind === "SignalAwaited" && e.signalName === name)
    ) {
      return;
    }
    await new Promise((r) => setTimeout(r, 10));
  }
  const events = await repoStore.read(runId);
  const summary = events
    .map((e) =>
      e.kind === "StepFailed"
        ? `StepFailed(${e.error.message})`
        : e.kind === "SignalAwaited"
          ? `SignalAwaited(${e.signalName})`
          : e.kind,
    )
    .join(",");
  throw new Error(
    `timed out waiting for SignalAwaited ${name}; log=${summary}`,
  );
}

function eventLogText(events: readonly WorkflowEvent[]): string {
  return JSON.stringify(events);
}

describe("durable long-lived run consumes mails as turns", () => {
  test("one run takes N mails as turns and remembers them across a respawn", async () => {
    const runId = "run-durable-chat";
    // Durable substrates -- these SURVIVE the simulated respawn (shared by
    // instance across phase A and phase B).
    const repoStore = createInMemoryRepoStore();
    const blobs = createInMemoryBlobSubstrate();
    const store = createDurableConversationStore();

    // ---- Phase A: the first "child process". Ephemeral channel + reactor. --
    const channelA = createInMemorySignalChannel();
    const reactorA = createReactor(store, runId);
    const envA = buildEnv({
      def: chatWorkflow,
      repoStore,
      blobs,
      signalChannel: channelA,
      invokeStep: reactorA.invokeStep,
    });

    const runA = runtimeRun(chatWorkflow, envA, { runId });

    // The step starts and parks awaiting mail #1.
    await waitForAwait(repoStore, runId, signalName(awaitCorrelation(1)));

    // Mail #1 -> turn 1. The run re-arms awaiting mail #2 (same runId).
    await channelA.deliver(
      signalName(awaitCorrelation(1)),
      { text: "hello" },
      "sig-1",
    );
    await waitForAwait(repoStore, runId, signalName(awaitCorrelation(2)));

    // Mail #2 -> turn 2. Re-arm awaiting mail #3.
    await channelA.deliver(
      signalName(awaitCorrelation(2)),
      { text: "how are you" },
      "sig-2",
    );
    await waitForAwait(repoStore, runId, signalName(awaitCorrelation(3)));

    // The run is still ONE run, not terminal, holding two turns.
    const midLog = await repoStore.read(runId);
    expect(midLog.some((e) => e.kind === "RunCompleted")).toBe(false);
    expect(midLog.filter((e) => e.kind === "StepStarted").length).toBe(1);
    const midConvo = await store.load(runId);
    expect(midConvo.length).toBe(4); // 2 user + 2 assistant

    // ---- CRASH: abandon phase A parked on mail #3. We never deliver mail #3
    // on channelA and never await runA.complete; the parked awaiter is an
    // inert pending promise (no timer). The durable log + blobs + conversation
    // store persist; the warm channel and reactor are discarded. ------------
    void runA;
    const seed = await repoStore.read(runId);

    // ---- Phase B: the "respawned child". FRESH channel + FRESH reactor
    // (empty warm cache), SAME durable substrates. ------------------------
    const channelB = createInMemorySignalChannel();
    const reactorB = createReactor(store, runId);
    const envB = buildEnv({
      def: chatWorkflow,
      repoStore,
      blobs,
      signalChannel: channelB,
      invokeStep: reactorB.invokeStep,
    });

    const runB = runtimeRun(chatWorkflow, envB, {
      runId,
      resumeFromEvents: seed,
    });

    // The resumed run re-parks on the recovered channel (mail #3). Deliver the
    // final mail live on the NEW channel; "bye" ends the conversation.
    await waitForAwait(repoStore, runId, signalName(awaitCorrelation(3)));
    await channelB.deliver(
      signalName(awaitCorrelation(3)),
      { text: "bye" },
      "sig-3",
    );

    const result = await runB.complete;
    expect(result.terminalStatus).toBe("completed");

    // THE PROOF: turn 3 ran in a fresh process whose reactor held nothing, yet
    // its reply reflects ALL THREE mails -- the memory came from the durable
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

    // The fresh reactor only ran the resume turn -- it did NOT replay turns 1
    // and 2 (they were never re-sent to the agent), so continuity is the
    // store's, not a re-run's.
    expect(reactorB.warmInvocations).toBe(1);

    // The full conversation is durable.
    const finalConvo = await store.load(runId);
    expect(finalConvo.map((t) => `${t.role}:${textOf(t)}`)).toEqual([
      "user:hello",
      "assistant:reply#1; heard=[hello]",
      "user:how are you",
      "assistant:reply#2; heard=[hello|how are you]",
      "user:bye",
      "assistant:reply#3; heard=[hello|how are you|bye]",
    ]);

    // And the intermediate replies lived ONLY in the store: the run event log
    // never carried "reply#1"/"reply#2" (the log records parks + delivered
    // mail payloads + the single final StepCompleted output, not per-turn
    // replies). This is why the durable store is load-bearing.
    const logText = eventLogText(result.events);
    expect(logText).not.toContain("reply#1");
    expect(logText).not.toContain("reply#2");
  });
});
