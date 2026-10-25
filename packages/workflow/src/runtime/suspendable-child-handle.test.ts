import { describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";

import { action, defineWorkflow } from "../definition/index";
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
});
