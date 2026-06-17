// Pins C-A: the multi-step undeploy hook must shut down the per-
// deployment supervisor so the workflow-process child, its IPC pipes,
// and its event-channel fd are released. Without the shutdown the
// supervisor's reference outlives every other piece of routing state
// the undeploy hook tears down, leaking the child for the life of the
// sidecar.
//
// The harness drives the multi-step deploy through the same spawn
// handshake the existing wiring tests use, captures the
// `SubprocessHandle.kill` call, and asserts that `undeploy(frame)`
// invokes it and awaits the handle's `exited` settlement.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto-node";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { RepoId, RepoStore } from "@intx/hub-sessions";
import {
  createControlChannelSender,
  type FrameReader,
  type NdjsonReader,
  type NdjsonWriter,
  type SubprocessHandle,
  type SubprocessSpawner,
} from "@intx/workflow-host";
import type { AgentDeployFrame } from "@intx/types/sidecar";

import { createSidecarDeployRouter } from "./workflow-host-wiring";
import {
  createMultistepDrainRouter,
  createMultistepMailRouter,
  createMultistepSignalRouter,
} from "./workflow-run-pack-client";

function createMemoryNdjsonStream() {
  const buffer: string[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }
  const reader: NdjsonReader = {
    read(): AsyncIterableIterator<string> {
      return (async function* () {
        while (true) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) {
              throw new Error("buffer shift returned undefined");
            }
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };
  const writer: NdjsonWriter = {
    write(line: string) {
      buffer.push(line.replace(/\n$/, ""));
      wake();
      return Promise.resolve();
    },
  };
  return {
    writer,
    reader,
    inject(line: string) {
      buffer.push(line.replace(/\n$/, ""));
      wake();
    },
    close() {
      done = true;
      wake();
    },
  };
}

function createMemoryFrameStream() {
  const buffer: Uint8Array[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }
  const reader: FrameReader = {
    read(): AsyncIterableIterator<Uint8Array> {
      return (async function* () {
        while (true) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) {
              throw new Error("frame buffer shift returned undefined");
            }
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };
  return {
    reader,
    close() {
      done = true;
      wake();
    },
  };
}

function createSpawnTestRepoStore(tempBase: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(tempBase, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(_p, _id, _ref, args) {
      await args.merge(new Map());
      return { commitSha: "stub-sha" };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub
  return new Proxy(stub as RepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(
          `stub RepoStore: ${String(prop)} not implemented for this test`,
        );
      };
    },
  });
}

describe("createSidecarDeployRouter multi-step undeploy shuts the supervisor down", () => {
  test("undeploy invokes the spawned child's kill and awaits exited", async () => {
    // Per-spawn tracking.
    type Spawn = {
      handle: SubprocessHandle;
      childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
      supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
      eventChildToSupervisor: ReturnType<typeof createMemoryFrameStream>;
      env: Record<string, string>;
      killed: boolean;
      exitedResolved: boolean;
      resolveExited: (code: number) => void;
    };
    const spawns: Spawn[] = [];

    const spawner: SubprocessSpawner = ({ env }) => {
      const supervisorToChild = createMemoryNdjsonStream();
      const childToSupervisor = createMemoryNdjsonStream();
      const eventChildToSupervisor = createMemoryFrameStream();
      let resolveExit: ((code: number) => void) | undefined;
      const exited = new Promise<number>((resolve) => {
        resolveExit = resolve;
      });
      const entry: Spawn = {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- assigned below
        handle: undefined as unknown as SubprocessHandle,
        supervisorToChild,
        childToSupervisor,
        eventChildToSupervisor,
        env,
        killed: false,
        exitedResolved: false,
        resolveExited: (code) => {
          entry.exitedResolved = true;
          resolveExit?.(code);
        },
      };
      const handle: SubprocessHandle = {
        pid: 5100 + spawns.length,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          entry.killed = true;
          childToSupervisor.close();
          eventChildToSupervisor.close();
          entry.resolveExited(0);
        },
        exited,
      };
      entry.handle = handle;
      spawns.push(entry);
      return handle;
    };

    const transport = createInMemoryTransport();
    const keyPair = await generateKeyPair();
    const tempBase = await fs.mkdtemp(
      path.join(os.tmpdir(), "sidecar-undeploy-supervisor-"),
    );
    const dataDir = await fs.mkdtemp(
      path.join(os.tmpdir(), "sidecar-undeploy-supervisor-data-"),
    );
    const repoStore = createSpawnTestRepoStore(tempBase);

    const mailRouter = createMultistepMailRouter();
    const signalRouter = createMultistepSignalRouter();
    const drainRouter = createMultistepDrainRouter();

    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- multi-step branch never invokes sessions
      sessions: {
        provisionAgent: async () => {
          throw new Error("multi-step branch must not invoke provisionAgent");
        },
        persistHubPublicKey: async () => {
          throw new Error(
            "multi-step branch must not invoke persistHubPublicKey",
          );
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- multi-step branch never invokes keyStore
      keyStore: {
        recordHubKey: () => {
          throw new Error("multi-step branch must not invoke recordHubKey");
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      onAgentEvent: () => () => {
        /* unused */
      },
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      registerDeployment: () => {
        /* no-op */
      },
      unregisterDeployment: () => {
        /* no-op */
      },
      multistepSubprocessSpawner: spawner,
      multistepSubstrateEnv: {
        SIDECAR_DATA_DIR: dataDir,
      },
      multistepMailRouter: mailRouter,
      multistepSignalRouter: signalRouter,
      multistepDrainRouter: drainRouter,
    });

    const frame: AgentDeployFrame = {
      type: "agent.deploy",
      agentAddress: "undeploy-supervisor@example.com",
      agentId: "undeploy-supervisor-agent",
      hubPublicKey: "hub-pk",
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the multi-step branch does not read config
      config: {} as AgentDeployFrame["config"],
      workflow: {
        definition: {
          id: "wf-undeploy-supervisor",
          triggers: [{ type: "manual" }],
          stepOrder: ["step-1"],
          steps: { "step-1": { kind: "step" } },
        },
        sources: {
          "step-1": {
            id: "step-1",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-step-1",
            model: "claude-3-5",
          },
        },
      },
    };

    const deployPromise = router.deploy(frame);

    while (spawns.length === 0) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const spawn = spawns[0];
    if (spawn === undefined) throw new Error("unreachable");

    const channelId = spawn.env.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID missing from spawn env");
    }
    const childIpcKeyPair = await generateKeyPair();
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          spawn.childToSupervisor.inject(line);
          return Promise.resolve();
        },
      },
    });
    await childSender.send({
      type: "ready",
      data: {
        childPid: spawn.handle.pid,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });

    await deployPromise;

    expect(spawn.killed).toBe(false);
    expect(spawn.exitedResolved).toBe(false);

    const undeploy = router.undeploy;
    if (undeploy === undefined) {
      throw new Error("router.undeploy is undefined");
    }

    await undeploy({
      type: "agent.undeploy",
      agentAddress: frame.agentAddress,
      reason: "test undeploy",
    });

    expect(spawn.killed).toBe(true);
    expect(spawn.exitedResolved).toBe(true);
  });
});
