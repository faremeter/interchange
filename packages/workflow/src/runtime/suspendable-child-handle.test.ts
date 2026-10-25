import { describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";

import { action, awaitSignal, defineWorkflow } from "../definition/index";
import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
} from "../runlocal/index";
import { beginWork, isQuiet, isWorking, waitIdle, whenQuiet } from "./activity";
import { createNoopDrainController } from "./drain";
import type { SuspendableChildHandle, WorkflowRuntimeEnv } from "./env";
import {
  createSuspendableChildHandle,
  createTerminalChildHandle,
} from "./suspendable-child-handle";

const noWake = (): void => undefined;

const body = defineWorkflow({
  id: "settle-body",
  trigger: { type: "manual" },
  steps: { touch: action({ handler: "touch" }) },
});

function bodyEnv(): WorkflowRuntimeEnv {
  const repoStore = createInMemoryRepoStore();
  const clock = (): Date => new Date(0);
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
    invokeStep: async () => ({ output: null }),
    invokeAction: async () => ({ output: "done" }),
    spawnChild: async () => createTerminalChildHandle("completed"),
    clock,
    newId: (prefix) => `${prefix}-0`,
    drain: createNoopDrainController(body),
    hasUpstreamSignalResolver: true,
  };
}

describe("createTerminalChildHandle", () => {
  test("next returns the same terminal on every call", async () => {
    const handle = createTerminalChildHandle("failed");
    const first = await handle.next(noWake);
    const second = await handle.next(noWake);
    expect(first).toEqual({ kind: "terminal", terminalStatus: "failed" });
    expect(second).toBe(first);
  });

  test("resume and deliverSignal reject because the child has settled", async () => {
    const handle = createTerminalChildHandle("completed");
    await expect(
      handle.resume("corr", { outcome: "approved" }),
    ).rejects.toThrow(/already settled completed/);
    await expect(handle.deliverSignal("go", null, "sig-1")).rejects.toThrow(
      /already settled completed/,
    );
  });
});

describe("createSuspendableChildHandle", () => {
  const waits: [
    string,
    (handle: SuspendableChildHandle, onWake: () => void) => Promise<unknown>,
  ][] = [
    ["next", (handle, onWake) => handle.next(onWake)],
    [
      "re-arm wait",
      (handle, onWake) =>
        handle.whenSignalAwaiter?.("go", onWake) ?? Promise.resolve(),
    ],
  ];
  for (const [name, wait] of waits) {
    // The body has stopped counting as work, and its handle has not seen
    // the result yet.
    test(`a ${name} that starts as the body ends counts as working`, async () => {
      if (!isQuiet())
        throw new Error("an earlier test left work or a delivery running");
      const handle = createSuspendableChildHandle(bodyEnv(), {
        definition: body,
        childRunId: "settle-child",
        input: null,
        depth: 0,
        maxChildSpawnDepth: 32,
        signal: new AbortController().signal,
        surfaceInputPark: false,
      });
      const working = await whenQuiet().then(() => {
        const endParent = beginWork();
        void waitIdle((resume) => wait(handle, resume))
          .catch(() => undefined)
          .finally(endParent);
        return isWorking();
      });
      expect(working).toBe(true);
    });
  }

  const gated = defineWorkflow({
    id: "gated-body",
    trigger: { type: "manual" },
    steps: { gate: awaitSignal({ name: "go" }) },
  });

  // A body parked on `go`, which ends once it gets that signal.
  async function parkedBody(env = bodyEnv()) {
    if (!isQuiet())
      throw new Error("an earlier test left work or a delivery running");
    const stop = new AbortController();
    const handle = createSuspendableChildHandle(env, {
      definition: gated,
      childRunId: "gated-child",
      input: null,
      depth: 0,
      maxChildSpawnDepth: 32,
      signal: stop.signal,
      surfaceInputPark: false,
    });
    await handle.next(noWake);
    await whenQuiet();
    return { handle, stop };
  }

  const waitsUnderWay: [
    string,
    (handle: SuspendableChildHandle, onWake: () => void) => Promise<unknown>,
  ][] = [
    [
      "re-arm wait",
      (handle, onWake) =>
        handle.whenSignalAwaiter?.("later", onWake) ?? Promise.resolve(),
    ],
  ];
  for (const [name, wait] of waitsUnderWay) {
    test(`a ${name} under way counts as working before the body that ended stops counting`, async () => {
      const { handle, stop } = await parkedBody();
      let parentDone = false;
      const endParent = beginWork();
      void waitIdle((resume) => wait(handle, resume))
        .catch(() => undefined)
        .finally(() => {
          parentDone = true;
          endParent();
        });

      await handle.deliverSignal("go", null, "sig-1");
      await whenQuiet();
      expect(parentDone).toBe(true);
      stop.abort();
    });
  }

  test("a park watch keeps the process from looking quiet while it reads the body's log", async () => {
    const env = bodyEnv();
    const inner = env.repoStore;
    const reached = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    let holding = false;
    env.repoStore = {
      ...inner,
      read: async (runId) => {
        if (holding) {
          holding = false;
          reached.resolve(undefined);
          await release.promise;
        }
        return inner.read(runId);
      },
    };
    const { handle, stop } = await parkedBody(env);
    const endParent = beginWork();
    holding = true;
    const watching = waitIdle<unknown>(
      (resume) =>
        handle.waitForParkRelease?.("go", stop.signal, resume) ??
        Promise.resolve(),
    )
      .catch(() => undefined)
      .finally(endParent);
    await reached.promise;
    expect(isQuiet()).toBe(false);

    stop.abort();
    release.resolve(undefined);
    await watching;
    await whenQuiet();
  });
});
