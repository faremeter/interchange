// Pins the security-critical registration lifecycle of the launched-agent
// signing key on the host transport: the single-step deploy registers the
// agent's CryptoProvider on the host transport at spawn (sub-step 4.3's
// outbound-signing registration), and undeploy UNREGISTERS it so no signing
// key leaks for a torn-down agent. The undeploy-supervisor wiring test drives
// the same harness but never asserts the transport registration state.
//
// Probe: `getTransportFor(address)` throws "is not registered" for an address
// with no CryptoProvider; it succeeds once registered. We assert (a) it
// throws BEFORE deploy, (b) succeeds AFTER deploy, (c) throws again AFTER
// undeploy.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
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
            if (next === undefined) throw new Error("buffer shift undefined");
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
            if (next === undefined) throw new Error("frame shift undefined");
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
        throw new Error(`stub RepoStore: ${String(prop)} not implemented`);
      };
    },
  });
}

const AGENT_ADDRESS = "ins_keylifecycle@example.com";

function isRegistered(
  transport: ReturnType<typeof createInMemoryTransport>,
): boolean {
  try {
    transport.getTransportFor(AGENT_ADDRESS);
    return true;
  } catch {
    return false;
  }
}

describe("agent signing-key registration lifecycle on the host transport", () => {
  test("single-step deploy registers the agent crypto; undeploy unregisters it", async () => {
    type SpawnEntry = {
      env: Record<string, string>;
      childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
      handle: SubprocessHandle;
      killed: boolean;
      resolveExited: (code: number) => void;
    };
    const spawns: SpawnEntry[] = [];
    const spawner: SubprocessSpawner = ({ env }) => {
      const supervisorToChild = createMemoryNdjsonStream();
      const childToSupervisor = createMemoryNdjsonStream();
      const eventChildToSupervisor = createMemoryFrameStream();
      let resolveExited: (code: number) => void = () => undefined;
      const exited = new Promise<number>((resolve) => {
        resolveExited = resolve;
      });
      const entry: SpawnEntry = {
        env,
        childToSupervisor,
        killed: false,
        resolveExited,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- assigned below
        handle: undefined as unknown as SubprocessHandle,
      };
      const handle: SubprocessHandle = {
        pid: 6100 + spawns.length,
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
      path.join(os.tmpdir(), "sidecar-keylifecycle-"),
    );
    const dataDir = await fs.mkdtemp(
      path.join(os.tmpdir(), "sidecar-keylifecycle-data-"),
    );
    const repoStore = createSpawnTestRepoStore(tempBase);

    const router = createSidecarDeployRouter({
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- multi-step branch never invokes sessions
      sessions: {
        provisionAgent: async () => {
          throw new Error("must not invoke provisionAgent");
        },
        persistHubPublicKey: async () => {
          throw new Error("must not invoke persistHubPublicKey");
        },
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["sessions"],
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- only loadOrGenerateKey is exercised (the single-step branch registers the agent's signing key on the transport before spawn)
      keyStore: {
        recordHubKey: () => {
          throw new Error("must not invoke recordHubKey");
        },
        loadOrGenerateKey: async () => ({
          keyPair: await generateKeyPair(),
          isNew: false,
        }),
      } as unknown as Parameters<
        typeof createSidecarDeployRouter
      >[0]["keyStore"],
      onAgentEvent: () => () => undefined,
      transport,
      repoStore,
      signingKeySeed: keyPair.privateKey,
      createAgentCrypto: createEd25519Crypto,
      registerDeployment: () => undefined,
      unregisterDeployment: () => undefined,
      multistepSubprocessSpawner: spawner,
      multistepSubstrateEnv: { SIDECAR_DATA_DIR: dataDir },
      multistepMailRouter: createMultistepMailRouter(),
      multistepSignalRouter: createMultistepSignalRouter(),
      multistepDrainRouter: createMultistepDrainRouter(),
    });

    const frame: AgentDeployFrame = {
      type: "agent.deploy",
      agentAddress: AGENT_ADDRESS,
      agentId: "keylifecycle-agent",
      hubPublicKey: "hub-pk",
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- multi-step branch does not read config
      config: {} as AgentDeployFrame["config"],
      workflow: {
        definition: {
          id: "wf-keylifecycle",
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

    // (a) Not registered before deploy.
    expect(isRegistered(transport)).toBe(false);

    const deployPromise = router.deploy(frame);
    while (spawns.length === 0) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const spawn = spawns[0];
    if (spawn === undefined) throw new Error("unreachable");
    const channelId = spawn.env.IPC_CHANNEL_ID;
    if (channelId === undefined) throw new Error("IPC_CHANNEL_ID missing");
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

    // (b) Registered after deploy -- the supervisor can now sign agent mail.
    expect(isRegistered(transport)).toBe(true);

    const undeploy = router.undeploy;
    if (undeploy === undefined) throw new Error("router.undeploy undefined");
    await undeploy({
      type: "agent.undeploy",
      agentAddress: AGENT_ADDRESS,
      reason: "key-lifecycle undeploy",
    });

    // (c) Unregistered after undeploy -- no leaked key for a torn-down agent.
    expect(isRegistered(transport)).toBe(false);

    await fs.rm(tempBase, { recursive: true, force: true });
    await fs.rm(dataDir, { recursive: true, force: true });
  });
});
