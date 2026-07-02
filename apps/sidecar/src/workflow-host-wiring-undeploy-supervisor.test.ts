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

import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
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

import {
  createSidecarDeployRouter,
  deriveTrivialDeploymentId,
} from "./workflow-host-wiring";
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
      return { commitSha: "stub-sha", newlyTerminalRuns: [] };
    },
    // The deploy router's grants bridge writes `state/grants.json` to
    // each step's agent-state repo before `spawn()`. Mirror the
    // `getRepoDir` layout so the write lands where the subsequent
    // `assembleCredentialsSnapshot` working-tree read looks for it.
    async writeTree(_p, repoId, _ref, content) {
      const dir = path.join(tempBase, repoId.kind, repoId.id);
      for (const [relPath, contents] of Object.entries(content.files)) {
        const full = path.join(dir, relPath);
        await fs.mkdir(path.dirname(full), { recursive: true });
        await fs.writeFile(full, contents);
      }
      return { commitSha: "stub-sha", newlyTerminalRuns: [] };
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
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the single-step branch invokes only initRepo (head deploy-tree repo); provisionAgent/persistHubPublicKey stay unused (the supervised child mints its own key and persists no hub-agent config)
      sessions: {
        provisionAgent: async () => {
          throw new Error("single-step branch must not invoke provisionAgent");
        },
        persistHubPublicKey: async () => {
          throw new Error(
            "single-step branch must not invoke persistHubPublicKey",
          );
        },
        initRepo: async () => undefined,
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the single-step branch registers the agent's signing key (loadOrGenerateKey) and records the hub key (recordHubKey) at the head before spawn
      keyStore: {
        recordHubKey: () => undefined,
        loadOrGenerateKey: async () => ({
          keyPair: await generateKeyPair(),
          isNew: false,
        }),
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      onAgentEvent: () => () => {
        /* unused */
      },
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      createAgentCrypto: createEd25519Crypto,
      assertSourceBuildable: () => undefined,
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
      // Single-step projection: the deploy router derives the sole
      // step's agent-state repo from `parseAgentId(agentAddress)`, which
      // requires the canonical `ins_<id>@<domain>` instance shape.
      agentAddress: "ins_undeploy-supervisor@example.com",
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
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });

    await deployPromise;

    expect(spawn.killed).toBe(false);
    expect(spawn.exitedResolved).toBe(false);

    const undeploy = router.undeploy;
    if (undeploy === undefined) {
      throw new Error("router.undeploy is undefined");
    }

    // Pre-seed the on-disk per-step scratch the child roots under
    // `<dataDir>/workflow-step-state/<deploymentId>/` and the durable
    // conversation under `<dataDir>/agent-conversation-state/<deploymentId>/`.
    // The warm subtree is the stable per-agent workspace (one dir, not
    // one-per-message); a stale cold `runs/<runId>/` subtree models a
    // multi-step leftover the per-run cleanup did not drop. An unrelated
    // deployment's step-state subtree must survive the undeploy sweep.
    const deploymentId = deriveTrivialDeploymentId(frame.agentAddress);
    const stepStateRoot = path.join(dataDir, "workflow-step-state");
    const warmWorkspaceFile = path.join(
      stepStateRoot,
      deploymentId,
      "warm",
      encodeURIComponent("step-1"),
      "workspace",
      "notes.txt",
    );
    const coldLeftoverFile = path.join(
      stepStateRoot,
      deploymentId,
      "runs",
      "run-stale",
      "steps",
      "step-1",
      "attempt-1",
      "workspace",
      "scratch.txt",
    );
    const otherDeploymentFile = path.join(
      stepStateRoot,
      "other-deployment",
      "warm",
      "step-1",
      "workspace",
      "keep.txt",
    );
    const durableConversationFile = path.join(
      dataDir,
      "agent-conversation-state",
      deploymentId,
      encodeURIComponent("step-1"),
      "checkpoint.json",
    );
    for (const file of [
      warmWorkspaceFile,
      coldLeftoverFile,
      otherDeploymentFile,
      durableConversationFile,
    ]) {
      await fs.mkdir(path.dirname(file), { recursive: true });
      await fs.writeFile(file, "x");
    }

    await undeploy({
      type: "agent.undeploy",
      agentAddress: frame.agentAddress,
      reason: "test undeploy",
    });

    expect(spawn.killed).toBe(true);
    expect(spawn.exitedResolved).toBe(true);

    // The deployment's whole step-state subtree is reclaimed -- warm
    // stable workspace AND any cold leftover -- now that its supervisor
    // and child are torn down.
    await expect(
      fs.stat(path.join(stepStateRoot, deploymentId)),
    ).rejects.toThrow();
    // A different deployment's scratch is untouched: the sweep is scoped
    // to this deployment's `<deploymentId>` subtree only.
    expect(await fs.readFile(otherDeploymentFile, "utf8")).toBe("x");
    // The durable conversation lives under a DIFFERENT root and must
    // survive so a re-deploy restores the prior conversation.
    expect(await fs.readFile(durableConversationFile, "utf8")).toBe("x");
  });
});
