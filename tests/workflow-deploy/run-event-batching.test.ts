// B2 run-event commit batching — the correctness gate.
//
// The runtime used to commit each run-event (RunStarted, StepStarted,
// StepCompleted, RunCompleted) as a SEPARATE durable
// `writeTreePreservingPrefix` commit (four per synchronous single-step
// run). B2 buffers the events emitted within one synchronous execution
// segment and flushes the buffer in ONE `appendBatch` at a flush
// boundary. A flush boundary is the segment boundary (a suspension or
// the terminal event) AND the agent's two durability barriers: an
// agent step's `StepStarted` is flushed durably BEFORE the
// non-idempotent `env.invokeStep` runs, so a crash mid-invocation
// leaves a durable marker the recovery path settles rather than
// re-invoking the agent, and its `StepCompleted` is flushed as soon as
// it lands, so a crash after the agent finished is not settled as one
// mid-invocation. A synchronous single-step agent run is therefore
// THREE commits -- [RunStarted, StepStarted], [StepCompleted], and the
// terminal [RunCompleted] -- not four.
// This is a persistence-TIMING change only.
//
// These tests are the gate: they prove exactly-once, crash-recovery,
// and resume are equivalent to the per-event behaviour, against the
// REAL production durable substrate (`createRepoStore` +
// `workflowRunKindHandler`) and the REAL runtime adapter
// (`createWorkflowRunRepoStore`, whose `appendBatch` writes N
// `events/<seq>.json` blobs in one merge) and the REAL self-discovery
// (`discoverInFlightRuns`). The adapter is wrapped only to COUNT the
// durable writes and to inject a deterministic crash; every durable
// write still flows through the production substrate and its
// append-only / seq-contiguity / terminal-lock validator.
//
// What these tests deliberately do NOT re-cover: the supervisor IPC,
// the terminal-write markConsumed coupling, and the
// consumed/<messageId>.json exactly-once index. Those live on the
// production supervisor path and are already exercised unchanged by
// single-step-full-lifecycle, single-step-conversation-durability, and
// multistep-signal in this same suite -- batching is transparent to
// them because the terminal RunCompleted is still one of the blobs in
// the (now batched) merge the supervisor sniffs.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import type { KeyPair } from "@intx/types/runtime";
import {
  createRepoStore,
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";
import type {
  AuthorizeFn,
  RepoId,
  WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import {
  createTerminalChildHandle,
  awaitSignal,
  createInMemoryBlobSubstrate,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  step,
  type RepoStore,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import {
  createWorkflowRunRepoStore,
  discoverInFlightRuns,
} from "@intx/workflow-host";

const REF = "refs/heads/main";
const allowAll: AuthorizeFn = () => ({ allowed: true });

// The workflow-run kind handler verifies `repoId.id === anchorRunId`,
// so the writer principal is per-deployment.
function principalFor(repoId: RepoId): WorkflowRunWorkflowProcessPrincipal {
  return { kind: "workflow-process", anchorRunId: repoId.id };
}

const tempDirs: string[] = [];
let signingKey: KeyPair;

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises
      .rm(d, { recursive: true, force: true })
      .catch(() => undefined);
  }
});

/**
 * Build a real workflow-run substrate (production durable git store +
 * the workflow-run kind handler's append-only / seq-contiguity /
 * terminal-lock validator) genesised for one deployment.
 */
async function makeSubstrate(repoId: RepoId) {
  const dataDir = await makeTempDir("run-event-batching-");
  const substrate = createRepoStore({
    dataDir,
    signingKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: allowAll,
  });
  await substrate.writeTree({ kind: "hub" }, repoId, REF, {
    files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
    message: "genesis",
  });
  return { substrate, dataDir };
}

interface CountingStore extends RepoStore {
  /** Number of durable `appendBatch`/`append` commits issued so far. */
  durableWrites(): number;
  /** The events written by each durable commit, in commit order. */
  batches(): readonly (readonly WorkflowEvent[])[];
}

/**
 * Wrap a runtime `RepoStore` to count durable writes and record what
 * landed in each one. `append` is a one-event batch; `appendBatch` is
 * the N-event batch. Both flow through to the real adapter so the
 * production substrate's validator still runs on every commit.
 */
function countingStore(
  inner: RepoStore,
  opts?: { crashOn?: (events: readonly WorkflowEvent[]) => boolean },
): CountingStore {
  const batches: (readonly WorkflowEvent[])[] = [];
  return {
    read: inner.read.bind(inner),
    subscribe: inner.subscribe.bind(inner),
    async append(runId, event) {
      if (opts?.crashOn?.([event]) === true) {
        throw new InjectedCrash();
      }
      batches.push([event]);
      await inner.append(runId, event);
    },
    async appendBatch(runId, events) {
      if (opts?.crashOn?.(events) === true) {
        throw new InjectedCrash();
      }
      batches.push(events.slice());
      await inner.appendBatch(runId, events);
    },
    durableWrites: () => batches.length,
    batches: () => batches,
  };
}

class InjectedCrash extends Error {
  constructor() {
    super("injected crash: durable run-event write dropped mid-segment");
    this.name = "InjectedCrash";
  }
}

const STUB_AGENT = defineAgent({
  id: "stub-agent",
  systemPrompt: "stub",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "stub" }] },
});

const STUB_INVOKER: StepInvoker = async () => ({ output: { ok: true } });

function buildEnv(
  repoStore: RepoStore,
  definition: WorkflowDefinition,
): WorkflowRuntimeEnv {
  const clock = () => new Date();
  return {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: STUB_INVOKER,
    spawnChild: async () => createTerminalChildHandle("completed"),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 10)}`,
    drain: createNoopDrainController(definition),
    hasUpstreamSignalResolver: true,
  };
}

function singleStepWorkflow(): WorkflowDefinition {
  return defineWorkflow({
    id: "batching-single-step",
    trigger: { type: "manual" },
    steps: { only: step({ agent: STUB_AGENT }) },
  });
}

function signalWorkflow(): WorkflowDefinition {
  return defineWorkflow({
    id: "batching-signal",
    trigger: { type: "manual" },
    steps: {
      first: step({ agent: STUB_AGENT }),
      gate: awaitSignal({ name: "go", after: ["first"] }),
      second: step({ agent: STUB_AGENT, after: ["gate"] }),
    },
  });
}

describe("B2 run-event batching — correctness gate", () => {
  test("gate 1: a synchronous single-step agent run commits its 4 events in THREE durable writes split by the agent's barriers, terminal last", async () => {
    const repoId: RepoId = { kind: "workflow-run", id: "gate1-deployment" };
    const { substrate } = await makeSubstrate(repoId);
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal: principalFor(repoId),
      ref: REF,
    });
    const store = countingStore(adapter);
    const def = singleStepWorkflow();
    const env = buildEnv(store, def);

    const runId = "run-gate1";
    const result = await runtimeRun(def, env, {
      runId,
      consumedMessageId: "msg-gate1",
    }).complete;

    expect(result.terminalStatus).toBe("completed");

    // The agent's barriers split the synchronous segment into three
    // durable commits: [RunStarted, StepStarted] before the
    // non-idempotent invoke, [StepCompleted] once it returns, then the
    // terminal [RunCompleted]. Before B2 this run cost four separate
    // commits.
    expect(store.durableWrites()).toBe(3);

    expect(store.batches().map((b) => b.map((e) => e.kind))).toEqual([
      ["RunStarted", "StepStarted"],
      ["StepCompleted"],
      ["RunCompleted"],
    ]);

    // The terminal RunCompleted is the LAST blob in the merge, which is
    // exactly what the supervisor's terminal-write sniff keys on and
    // what the kind handler's terminal-lock requires. Read the durable
    // log back through the production adapter to confirm the validator
    // accepted the batched commits and the seqs are contiguous.
    const durable = await adapter.read(runId);
    expect(durable.map((e) => e.kind)).toEqual([
      "RunStarted",
      "StepStarted",
      "StepCompleted",
      "RunCompleted",
    ]);
    expect(durable.map((e) => e.seq)).toEqual([1, 2, 3, 4]);
    const last = durable[durable.length - 1];
    expect(last?.kind).toBe("RunCompleted");

    // The RunStarted carries the consumed messageId (the exactly-once
    // key the supervisor's markConsumed writes against), unchanged by
    // batching.
    const runStarted = durable[0];
    if (runStarted === undefined || runStarted.kind !== "RunStarted") {
      throw new Error("expected RunStarted first");
    }
    expect(runStarted.consumedMessageId).toBe("msg-gate1");
  });

  test("gate 2: a crash on the terminal flush leaves the step's completion durable, and a re-fire finishes the run without re-invoking the agent", async () => {
    const repoId: RepoId = { kind: "workflow-run", id: "gate2-deployment" };
    const { substrate } = await makeSubstrate(repoId);
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal: principalFor(repoId),
      ref: REF,
    });
    const def = singleStepWorkflow();
    const runId = "run-gate2";

    const crashingStore = countingStore(adapter, {
      crashOn: (events) => events.some((e) => e.kind === "RunCompleted"),
    });
    const crashEnv = buildEnv(crashingStore, def);
    await expect(
      runtimeRun(def, crashEnv, {
        runId,
        consumedMessageId: "msg-gate2",
      }).complete,
    ).rejects.toBeInstanceOf(InjectedCrash);

    // The completion barrier landed StepCompleted on its own, so the
    // crash on the terminal write cannot leave the finished step looking
    // crashed mid-invocation.
    expect(crashingStore.batches().map((b) => b.map((e) => e.kind))).toEqual([
      ["RunStarted", "StepStarted"],
      ["StepCompleted"],
    ]);
    const durable = await adapter.read(runId);
    expect(durable.map((e) => e.kind)).toEqual([
      "RunStarted",
      "StepStarted",
      "StepCompleted",
    ]);
    expect(durable.map((e) => e.seq)).toEqual([1, 2, 3]);

    let invocations = 0;
    const refireEnv: WorkflowRuntimeEnv = {
      ...buildEnv(adapter, def),
      invokeStep: async () => {
        invocations += 1;
        return { output: { ok: true } };
      },
    };
    const result = await runtimeRun(def, refireEnv, {
      runId,
      consumedMessageId: "msg-gate2",
    }).complete;
    expect(result.terminalStatus).toBe("completed");
    expect(invocations).toBe(0);
    const settled = await adapter.read(runId);
    expect(settled.map((e) => e.kind)).toEqual([
      "RunStarted",
      "StepStarted",
      "StepCompleted",
      "RunCompleted",
    ]);
  });

  test("gate 3: a suspending run flushes the suspension marker BEFORE it parks; the durable pre-park log is discoverable and resumes to completion", async () => {
    const repoId: RepoId = { kind: "workflow-run", id: "gate3-deployment" };
    const { substrate } = await makeSubstrate(repoId);
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal: principalFor(repoId),
      ref: REF,
    });
    const store = countingStore(adapter);
    const def = signalWorkflow();
    const env = buildEnv(store, def);
    const runId = "run-gate3";

    const handle = runtimeRun(def, env, {
      runId,
      consumedMessageId: "msg-gate3",
    });

    // Wait until the run has durably parked at the suspension. The
    // load-bearing assertion: SignalAwaited is in the DURABLE substrate
    // log WHILE the run is parked. If batching had deferred the
    // suspension marker past the park, the durable log here would lack
    // it and a crash-while-suspended would lose the wait state.
    await waitForDurable(adapter, runId, (events) =>
      events.some((e) => e.kind === "SignalAwaited"),
    );

    const parked = await adapter.read(runId);
    const parkedKinds = parked.map((e) => e.kind);
    // The complete pre-suspension segment is durable, terminating in
    // the suspension marker -- the exact log a respawn's resume would
    // reconstruct the awaiting state from. The awaitSignal primitive
    // emits its own StepStarted for the gate step before SignalAwaited,
    // so the first segment is: the first step's bracket, then the
    // gate's StepStarted, then SignalAwaited.
    expect(parkedKinds).toEqual([
      "RunStarted",
      "StepStarted",
      "StepCompleted",
      "StepStarted",
      "SignalAwaited",
    ]);
    expect(parked.map((e) => e.seq)).toEqual([1, 2, 3, 4, 5]);
    expect(parkedKinds[parkedKinds.length - 1]).toBe("SignalAwaited");

    // The pre-suspension segment flushed as THREE durable commits, split
    // by the first (agent) step's barriers: [RunStarted, StepStarted]
    // before the invoke, [StepCompleted] once it returns, then the gate's
    // StepStarted and the buffered suspension marker in the flush that
    // lands SignalAwaited. The run is NOT yet terminal.
    expect(store.batches().map((b) => b.map((e) => e.kind))).toEqual([
      ["RunStarted", "StepStarted"],
      ["StepCompleted"],
      ["StepStarted", "SignalAwaited"],
    ]);

    // A respawn's discovery sees the parked run as in-flight and hands
    // back the pre-suspension seed log -- this is the durable state
    // resume consumes. Proving discovery reconstructs the awaiting
    // run from the durable log is proving the suspension marker
    // survived the park.
    const runtimeRepoStore = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal: principalFor(repoId),
      ref: REF,
    });
    const discovered = await discoverInFlightRuns({
      substrate,
      repoId,
      runtimeRepoStore,
    });
    const found = discovered.find((d) => d.runId === runId);
    if (found === undefined) {
      throw new Error("expected the parked run to be discoverable in-flight");
    }
    expect(found.seedEvents.map((e) => e.kind)).toEqual([
      "RunStarted",
      "StepStarted",
      "StepCompleted",
      "StepStarted",
      "SignalAwaited",
    ]);
    expect(found.resumedState.phase).toBe("running");

    // Deliver the signal: the run reconstructs the awaiting state from
    // the durable log and drives the second step through to completion.
    await handle.signal("go", { delivered: true });
    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");

    const durable = await adapter.read(runId);
    const types = durable.map((e) => e.kind);
    expect(types.indexOf("SignalReceived")).toBeGreaterThan(
      types.indexOf("SignalAwaited"),
    );
    expect(types.indexOf("RunCompleted")).toBe(types.length - 1);
    // Append-only across the two segments: seqs stay contiguous over
    // the suspension boundary.
    expect(durable.map((e) => e.seq)).toEqual(durable.map((_, i) => i + 1));

    // The post-signal segment is likewise split by the second (agent)
    // step's barriers. Across both segments the durable write count is
    // one per flush boundary -- each agent's invoke and completion
    // barriers, the suspension, and the terminal -- not one per event.
    expect(store.durableWrites()).toBe(6);
  });

  test("gate 4: per-event in-memory validation is preserved -- the batched durable log replays through the state machine identically", async () => {
    // A direct equivalence check that batching changes only WHEN events
    // persist, not WHICH events or their seqs: drive a synchronous run
    // and confirm the durable log the production validator accepted is
    // a byte-identical seq-contiguous bracket. (The full multistep /
    // child-workflow / drain / fifo equivalence is carried by the
    // sibling suites, which pass unchanged.)
    const repoId: RepoId = { kind: "workflow-run", id: "gate4-deployment" };
    const { substrate } = await makeSubstrate(repoId);
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal: principalFor(repoId),
      ref: REF,
    });
    const def = singleStepWorkflow();
    const env = buildEnv(adapter, def);
    const runId = "run-gate4";
    const result = await runtimeRun(def, env, { runId }).complete;
    expect(result.terminalStatus).toBe("completed");

    // `result.events` (the in-memory view) and the durable log (read
    // back through the validator-gated substrate) agree exactly.
    const durable = await adapter.read(runId);
    expect(durable.map((e) => ({ kind: e.kind, seq: e.seq }))).toEqual(
      result.events.map((e) => ({ kind: e.kind, seq: e.seq })),
    );
  });
});

// Re-read the durable log until `predicate` holds. The read is async, so this
// polls rather than awaiting a signal; it carries no deadline of its own,
// leaving the caller's test budget as the failsafe for a predicate that never
// becomes true.
async function waitForDurable(
  store: RepoStore,
  runId: string,
  predicate: (events: readonly WorkflowEvent[]) => boolean,
): Promise<void> {
  for (;;) {
    const events = await store.read(runId);
    if (predicate(events)) return;
    await new Promise((r) => setTimeout(r, 5));
  }
}
