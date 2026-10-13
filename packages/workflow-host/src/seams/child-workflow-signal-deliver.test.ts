// A decision already on the parent log has to reach a child whose own
// log still ends at the park. The child channel is the production host
// channel: `readState` is empty and `awaitNext` tails from head, so a
// delivery that lands before that subscription is invisible.

import { afterAll, beforeAll, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { generateKeyPair } from "@intx/crypto";
import { createRepoStore } from "@intx/hub-sessions";
import type {
  AuthorizeFn,
  KindHandler,
  Principal,
  RepoId,
  ValidatePushResult,
} from "@intx/hub-sessions";
import type { ApprovalSnapshot, KeyPair } from "@intx/types/runtime";
import {
  awaitSignal,
  childWorkflow,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  createSuspendableChildHandle,
  defineWorkflow,
  emptyState,
  rewriteInlineChildWorkflowBodies,
  runtimeRun,
  step,
  type RepoStore,
  type RuntimeWorkflowRun,
  type StepInvoker,
  type WorkflowAuthorizeFn,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { dropChain } from "@intx/workflow/runtime";
import { waitForEvent } from "@intx/workflow/testing";

import { createWorkflowRunRepoStore } from "../adapters/repo-store";
import {
  createWorkflowHostSignalChannel,
  type SignalChannelHandle,
} from "./signal-channel";

const REF = "refs/heads/main";

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
});

const snapshot: ApprovalSnapshot = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const dir = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const dir of tempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

const authorizeSubstrate: AuthorizeFn = () => ({ allowed: true });
const principal: Principal = { kind: "test" };

function permissiveHandler(directoryPrefix: string): KindHandler {
  return {
    kind: "agent-state",
    directoryPrefix,
    validatePush(): ValidatePushResult {
      return { ok: true };
    },
    onRefUpdated() {
      /* no-op */
    },
  };
}

type SignalAwaitedEvent = Extract<WorkflowEvent, { kind: "SignalAwaited" }>;

function isSignalAwaited(event: WorkflowEvent): event is SignalAwaitedEvent {
  return event.kind === "SignalAwaited";
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

function withRecordedDecision(
  parentLog: readonly WorkflowEvent[],
  signalName: string,
  signalId: string,
  payload: unknown,
): WorkflowEvent[] {
  const last = parentLog.at(-1);
  if (last === undefined) throw new Error("empty parent log");
  return [
    ...parentLog,
    {
      kind: "SignalReceived",
      seq: last.seq + 1,
      at: "2026-01-01T00:00:00.000Z",
      signalName,
      signalId,
      payload,
    },
  ];
}

type SignalReceivedEvent = Extract<WorkflowEvent, { kind: "SignalReceived" }>;

function receivedCount(
  events: readonly WorkflowEvent[],
  signalId: string,
): number {
  return events.filter(
    (event) => event.kind === "SignalReceived" && event.signalId === signalId,
  ).length;
}

function receivedOnName(
  events: readonly WorkflowEvent[],
  signalName: string,
): SignalReceivedEvent[] {
  return events.filter(
    (event): event is SignalReceivedEvent =>
      event.kind === "SignalReceived" && event.signalName === signalName,
  );
}

function startCapture(args: {
  definition: WorkflowDefinition;
  runId: string;
  invokeStep: StepInvoker;
}): { run: RuntimeWorkflowRun; repoStore: RepoStore } {
  const repoStore = createInMemoryRepoStore();
  const clock = (): Date => new Date();
  let nextId = 0;
  const newId = (prefix: string): string => {
    nextId += 1;
    return `${prefix}-${args.runId}-${String(nextId)}`;
  };
  const { workflow: rewritten, bodies } = rewriteInlineChildWorkflowBodies(
    args.definition,
  );
  const childBodies = new Map(
    bodies.map((body) => [body.ref, body.definition]),
  );
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel({ newId: () => newId("sig") }),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: allowAll,
    invokeStep: args.invokeStep,
    spawnChild: async (input) => {
      const resolved = childBodies.get(input.definitionRef);
      if (resolved === undefined) {
        throw new Error(
          `childWorkflow ${input.definitionRef} has no lifted definition`,
        );
      }
      const { workflow: childWorkflowDef } =
        rewriteInlineChildWorkflowBodies(resolved);
      const childStore = repoStore;
      const childEnv: WorkflowRuntimeEnv = {
        repoStore: childStore,
        scheduler: createInMemoryScheduler({ repoStore: childStore, clock }),
        signalChannel: createInMemorySignalChannel({
          newId: () => newId("sig"),
        }),
        blobs: createInMemoryBlobSubstrate(),
        directors: createDefaultDirectorRegistry(),
        authorize: allowAll,
        invokeStep: args.invokeStep,
        spawnChild: () =>
          Promise.reject(new Error("this test child does not spawn")),
        clock,
        newId,
        drain: createNoopDrainController(childWorkflowDef),
        hasUpstreamSignalResolver: true,
      };
      return createSuspendableChildHandle(childEnv, {
        definition: childWorkflowDef,
        childRunId: input.childRunId,
        input: input.input,
        depth: input.depth,
        maxChildSpawnDepth: input.maxChildSpawnDepth,
        ...(input.resumeFromEvents !== undefined
          ? { resumeFromEvents: input.resumeFromEvents }
          : {}),
        signal: input.signal,
        surfaceInputPark: true,
      });
    },
    clock,
    newId,
    drain: createNoopDrainController(rewritten),
    hasUpstreamSignalResolver: true,
  };
  return {
    run: runtimeRun(rewritten, env, { runId: args.runId }),
    repoStore,
  };
}

async function captureParked(args: {
  definition: WorkflowDefinition;
  runId: string;
  invokeStep: StepInvoker;
  parentAwait: (event: WorkflowEvent) => boolean;
}): Promise<{
  parentLog: WorkflowEvent[];
  childLog: WorkflowEvent[];
  childId: string;
  signalName: string;
}> {
  const first = startCapture(args);
  const settled = first.run.complete;
  try {
    await waitForEvent(first.repoStore, args.runId, args.parentAwait);
    const parentLog = [...(await first.repoStore.read(args.runId))];
    const childId = spawnedChildId(parentLog, "spawn");
    const childLog = [...(await first.repoStore.read(childId))];
    const awaited = parentLog.find(
      (event) => args.parentAwait(event) && isSignalAwaited(event),
    );
    if (awaited === undefined || !isSignalAwaited(awaited)) {
      throw new Error("parent await missing");
    }
    return {
      parentLog,
      childLog,
      childId,
      signalName: awaited.signalName,
    };
  } finally {
    await first.run.cancel("supervisor-operator", "capture done");
    await settled;
    dropChain(args.runId);
    const parentLog = await first.repoStore.read(args.runId);
    const childId = parentLog.find(
      (event) => event.kind === "ChildSpawned" && event.stepId === "spawn",
    );
    if (childId !== undefined && childId.kind === "ChildSpawned") {
      dropChain(childId.childRunId);
    }
  }
}

async function resumeOnHost(args: {
  prefix: string;
  runId: string;
  definition: WorkflowDefinition;
  invokeStep: StepInvoker;
  parentLog: readonly WorkflowEvent[];
  childId: string;
  childLog: readonly WorkflowEvent[];
}): Promise<{ terminalStatus: string; childLog: readonly WorkflowEvent[] }> {
  const dataDir = await makeTempDir(args.prefix);
  const substrate = createRepoStore({
    dataDir,
    signingKey,
    handlers: { "agent-state": permissiveHandler(args.prefix) },
    authorize: authorizeSubstrate,
  });
  const repoId: RepoId = { kind: "agent-state", id: args.prefix };
  const runtimeStore = createWorkflowRunRepoStore({
    substrate,
    repoId,
    principal,
    ref: REF,
  });
  for (const event of args.childLog) {
    await runtimeStore.append(args.childId, event);
  }
  const channels: SignalChannelHandle[] = [];
  const hostChannel = (channelRunId: string): SignalChannelHandle => {
    const channel = createWorkflowHostSignalChannel({
      repoStore: substrate,
      runtimeStore,
      principal,
      repoId,
      ref: REF,
      runId: channelRunId,
      readState: () => emptyState(channelRunId),
      newId: () => "generated-signal",
      clock: () => new Date("2026-01-01T00:00:00.000Z"),
    });
    channels.push(channel);
    return channel;
  };
  const clock = (): Date => new Date("2026-01-01T00:00:00.000Z");
  const { workflow: rewritten, bodies } = rewriteInlineChildWorkflowBodies(
    args.definition,
  );
  const childBodies = new Map(
    bodies.map((body) => [body.ref, body.definition]),
  );
  let nextId = 0;
  const newId = (prefix: string): string => {
    nextId += 1;
    return `${prefix}-${args.runId}-${String(nextId)}`;
  };
  const env: WorkflowRuntimeEnv = {
    repoStore: runtimeStore,
    scheduler: createInMemoryScheduler({ repoStore: runtimeStore, clock }),
    signalChannel: createInMemorySignalChannel({ newId: () => newId("sig") }),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: allowAll,
    invokeStep: args.invokeStep,
    spawnChild: async (input) => {
      const resolved = childBodies.get(input.definitionRef);
      if (resolved === undefined) {
        throw new Error(
          `childWorkflow ${input.definitionRef} has no lifted definition`,
        );
      }
      const { workflow: childWorkflowDef } =
        rewriteInlineChildWorkflowBodies(resolved);
      const channel = hostChannel(input.childRunId);
      const childEnv: WorkflowRuntimeEnv = {
        repoStore: runtimeStore,
        scheduler: createInMemoryScheduler({
          repoStore: runtimeStore,
          clock,
        }),
        signalChannel: channel,
        blobs: createInMemoryBlobSubstrate(),
        directors: createDefaultDirectorRegistry(),
        authorize: allowAll,
        invokeStep: args.invokeStep,
        spawnChild: () =>
          Promise.reject(new Error("this test child does not spawn")),
        clock,
        newId,
        drain: createNoopDrainController(childWorkflowDef),
        hasUpstreamSignalResolver: true,
      };
      return createSuspendableChildHandle(childEnv, {
        definition: childWorkflowDef,
        childRunId: input.childRunId,
        input: input.input,
        depth: input.depth,
        maxChildSpawnDepth: input.maxChildSpawnDepth,
        ...(input.resumeFromEvents !== undefined
          ? { resumeFromEvents: input.resumeFromEvents }
          : {}),
        signal: input.signal,
        cleanup: () => channel.stop(),
        surfaceInputPark: true,
      });
    },
    clock,
    newId,
    drain: createNoopDrainController(rewritten),
    hasUpstreamSignalResolver: true,
  };
  try {
    const run = runtimeRun(rewritten, env, {
      runId: args.runId,
      resumeFromEvents: args.parentLog,
    });
    const result = await run.complete;
    return {
      terminalStatus: result.terminalStatus,
      childLog: await runtimeStore.read(args.childId),
    };
  } finally {
    dropChain(args.runId);
    dropChain(args.childId);
    await Promise.all(channels.map((channel) => channel.stop()));
  }
}

test(
  "delivers an author decision the parent already recorded",
  async () => {
    const parent = defineWorkflow({
      id: "host-author-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "host-author-child",
            trigger: { type: "manual" },
            steps: { hold: awaitSignal({ name: "go" }) },
          }),
        }),
      },
    });
    const runId = "run-host-author";
    const signalId = "sig-already-author";
    const payload = { done: true };
    const captured = await captureParked({
      definition: parent,
      runId,
      invokeStep: async () => ({ output: null }),
      parentAwait: (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.signalName === "go",
    });
    expect(receivedCount(captured.childLog, signalId)).toBe(0);
    expect(
      captured.childLog.some(
        (event) => event.kind === "SignalAwaited" && event.signalName === "go",
      ),
    ).toBe(true);

    const resumed = await resumeOnHost({
      prefix: "child-sig-author",
      runId,
      definition: parent,
      invokeStep: async () => ({ output: null }),
      parentLog: withRecordedDecision(
        captured.parentLog,
        captured.signalName,
        signalId,
        payload,
      ),
      childId: captured.childId,
      childLog: captured.childLog,
    });

    expect(resumed.terminalStatus).toBe("completed");
    expect(receivedCount(resumed.childLog, signalId)).toBe(1);
  },
  { timeout: 20000 },
);

test(
  "delivers an approval decision the parent already recorded",
  async () => {
    const parent = defineWorkflow({
      id: "host-approval-parent",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "host-approval-child",
            trigger: { type: "manual" },
            steps: { hold: step({ agent }) },
          }),
        }),
      },
    });
    const runId = "run-host-approval";
    const signalId = "sig-already-approval";
    const payload = { outcome: "approved" };
    let resumedDecision: unknown;
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume !== undefined) {
        resumedDecision = req.resume.decision;
        return { output: null };
      }
      return {
        suspend: {
          correlationId: "corr-body",
          kind: "approval",
          approvalSnapshot: snapshot,
        },
      };
    };
    const captured = await captureParked({
      definition: parent,
      runId,
      invokeStep,
      parentAwait: (event) =>
        event.kind === "SignalAwaited" &&
        event.stepId === "spawn" &&
        event.parkKind === "approval",
    });
    expect(receivedCount(captured.childLog, signalId)).toBe(0);

    const resumed = await resumeOnHost({
      prefix: "child-sig-approval",
      runId,
      definition: parent,
      invokeStep,
      parentLog: withRecordedDecision(
        captured.parentLog,
        captured.signalName,
        signalId,
        payload,
      ),
      childId: captured.childId,
      childLog: captured.childLog,
    });

    const delivered = receivedOnName(resumed.childLog, captured.signalName);
    expect(resumed.terminalStatus).toBe("completed");
    expect(delivered).toHaveLength(1);
    expect(delivered[0]?.payload).toEqual(payload);
    expect(resumedDecision).toEqual(payload);
  },
  { timeout: 20000 },
);
