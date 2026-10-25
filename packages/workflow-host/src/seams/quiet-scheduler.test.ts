// A child in standby is woken for its earliest queued timer, so a sleeping
// run may look quiet only once the host scheduler has queued its timer, and a
// fired timer must count as delivery until the woken run counts as working.
// These drive the real host scheduler and its runtime adapter: in a child,
// `scheduleIn` queues nothing, and the timer is queued by the scheduler's own
// TimerSet subscription over the proxy store.

import { afterAll, beforeEach, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { createDefaultDirectorRegistry } from "@intx/agent";
import { generateKeyPair } from "@intx/crypto";
import { createRepoStore } from "@intx/hub-sessions";
import type {
  AuthorizeFn,
  KindHandler,
  Principal,
  RepoId,
  ValidatePushResult,
} from "@intx/hub-sessions";
import {
  awaitSignal,
  createInMemoryBlobSubstrate,
  createNoopDrainController,
  defineWorkflow,
  emptyState,
  runtimeRun,
  sleep,
  type WorkflowDefinition,
} from "@intx/workflow";
import { bufferedEvents, isQuiet, whenQuiet } from "@intx/workflow/runtime";

import { createWorkflowRunRepoStore } from "../adapters/repo-store";
import { createProxyWorkflowRunRepoStore } from "../child/proxy-repo-store";
import { createWorkflowHostScheduler } from "./scheduler";
import { adaptHostScheduler } from "./scheduler-adapter";
import { createWorkflowHostSignalChannel } from "./signal-channel";

const REF = "refs/heads/main";
const principal: Principal = { kind: "test" };
const allowAll: AuthorizeFn = () => ({ allowed: true });
const tempDirs: string[] = [];

beforeEach(() => {
  if (!isQuiet())
    throw new Error("work or a delivery from an earlier test is still running");
});

afterAll(async () => {
  for (const dir of tempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

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

async function childWithScheduler(
  prefix: string,
  definition: WorkflowDefinition,
) {
  const dataDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dataDir);
  const bare = createRepoStore({
    dataDir,
    signingKey: await generateKeyPair(),
    handlers: { "agent-state": permissiveHandler(prefix) },
    authorize: allowAll,
  });
  const repoId: RepoId = { kind: "agent-state", id: prefix };
  const proxy = createProxyWorkflowRunRepoStore({
    bareStore: bare,
    bridge: {
      async submit(req) {
        const result = await bare.writeTreePreservingPrefix(
          principal,
          repoId,
          req.ref,
          {
            preservePrefix: req.preservePrefix,
            message: req.message,
            merge: req.merge,
          },
        );
        return { commitSha: result.commitSha };
      },
      handleMergeRequest: () => undefined,
      handleWriteResponse: () => undefined,
      cancelAll: () => undefined,
      pendingCount: 0,
    },
    workflowRunRepoId: repoId,
  });
  const adapter = createWorkflowRunRepoStore({
    substrate: proxy,
    repoId,
    principal,
    ref: REF,
  });
  // What each run has made durable, readable at once.
  const durable = new Map<string, string[]>();
  const record = (runId: string, kinds: readonly string[]): void => {
    durable.set(runId, [...(durable.get(runId) ?? []), ...kinds]);
  };
  const runtimeStore: typeof adapter = {
    ...adapter,
    read: (runId) => adapter.read(runId),
    subscribe: (runId, opts) => adapter.subscribe(runId, opts),
    async append(runId, event) {
      await adapter.append(runId, event);
      record(runId, [event.kind]);
    },
    async appendBatch(runId, events) {
      await adapter.appendBatch(runId, events);
      record(
        runId,
        events.map((event) => event.kind),
      );
    },
  };
  // Armed timers fire on demand instead of after their delay.
  const armed: (() => void)[] = [];
  const scheduler = createWorkflowHostScheduler({
    repoStore: proxy,
    runtimeStore,
    principal,
    listActiveDeployments: () => [repoId],
    ref: REF,
    clock: () => new Date(),
    scheduleTimeout: (handler) => {
      armed.push(handler);
      return () => undefined;
    },
  });
  await scheduler.start();

  const start = (runId: string) => {
    const teardown = new AbortController();
    const channel = createWorkflowHostSignalChannel({
      repoStore: proxy,
      runtimeStore,
      principal,
      repoId,
      ref: REF,
      runId,
      readState: () => emptyState(runId),
      newId: () => "generated-signal",
      clock: () => new Date(),
    });
    const run = runtimeRun(
      definition,
      {
        repoStore: runtimeStore,
        scheduler: adaptHostScheduler(scheduler),
        signalChannel: channel,
        blobs: createInMemoryBlobSubstrate(),
        directors: createDefaultDirectorRegistry(),
        authorize: async () => ({
          effect: "allow",
          matchingGrants: [],
          resolvedBy: null,
        }),
        invokeStep: () => {
          throw new Error("no agent step runs here");
        },
        invokeAction: async () => ({ output: "done" }),
        spawnChild: () => {
          throw new Error("no nested run here");
        },
        clock: () => new Date(),
        newId: (idPrefix) => `${idPrefix}-id`,
        drain: createNoopDrainController(definition),
        hasUpstreamSignalResolver: true,
      },
      { runId, triggerPayload: null, localAbort: teardown.signal },
    );
    return {
      run,
      async stop(): Promise<void> {
        teardown.abort();
        await run.complete.catch(() => undefined);
        await channel.stop();
        await scheduler.stop();
      },
    };
  };
  const events = (runId: string): string[] => [
    ...(durable.get(runId) ?? []),
    ...bufferedEvents(runId).map((event) => event.kind),
  ];
  return { scheduler, armed, start, events };
}

test("a sleeping run looks quiet only once the host scheduler has queued its timer", async () => {
  const definition = defineWorkflow({
    id: "real-scheduler-sleep",
    trigger: { type: "manual" },
    steps: { nap: sleep({ duration: 3_600_000 }) },
  });
  const child = await childWithScheduler("real-scheduler-sleep-", definition);
  const { stop } = child.start("run-sleep");
  const queued = await whenQuiet().then(() =>
    child.scheduler.queuedTimers().map((timer) => timer.runId),
  );
  await stop();
  expect(queued).toEqual(["run-sleep"]);
});

test("a timed gate looks quiet only once the host scheduler has queued its timeout", async () => {
  const definition = defineWorkflow({
    id: "real-scheduler-gate",
    trigger: { type: "manual" },
    steps: { gate: awaitSignal({ name: "go", timeout: 3_600_000 }) },
  });
  const child = await childWithScheduler("real-scheduler-gate-", definition);
  const { stop } = child.start("run-gate");
  const queued = await whenQuiet().then(() =>
    child.scheduler.queuedTimers().map((timer) => timer.runId),
  );
  await stop();
  expect(queued).toEqual(["run-gate"]);
});

test("a timer the host scheduler fires counts as delivery until the woken run works", async () => {
  const definition = defineWorkflow({
    id: "real-scheduler-fire",
    trigger: { type: "manual" },
    steps: { nap: sleep({ duration: 3_600_000 }) },
  });
  const child = await childWithScheduler("real-scheduler-fire-", definition);
  const { run, stop } = child.start("run-fire");
  await whenQuiet();
  const fire = child.armed.shift();
  if (fire === undefined) throw new Error("the scheduler armed no timer");
  fire();
  const seen = await whenQuiet().then(() => child.events("run-fire"));
  await run.complete;
  await stop();
  expect(seen).toContain("RunCompleted");
});
