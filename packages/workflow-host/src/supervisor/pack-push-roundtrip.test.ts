// End-to-end IPC round-trip for the workflow-run pack-push bridge.
//
// Wires the real supervisor + real child bridge across a memory-stream
// pair so a `sendRequest` on the child bridge surfaces a
// `pack.push.request` upstream, the supervisor's handler routes it
// through the `pushWorkflowRunPack` binding, and the supervisor's
// `pack.push.response` makes the child bridge's awaiter resolve (or
// reject).
//
// The supervisor is constructed against a stub spawner that returns
// the memory streams the test uses to feed `ready` and `pack.push.request`
// frames upstream; the child bridge is built directly against the
// upstream sender the test drives. The IPC primitives (signature,
// channel id, seq) are the production implementations.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto-node";
import type { RepoId, RepoStore } from "@intx/hub-sessions";

import {
  createWorkflowSupervisor,
  type MailBusBindings,
  type SubprocessHandle,
  type SubprocessSpawner,
  type WorkflowSupervisorBindings,
} from "./index";
import { defaultStepRepoId, STEP_GRANTS_PATH } from "./credentials";
import {
  createChildPackPushBridge,
  type ChildPackPushBridge,
  type PackPushResponseSink,
} from "../child/run-child";
import {
  createControlChannelSender,
  receiveControlChannel,
  type ControlPayload,
  type FrameReader,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";

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

function createMockMailBus(): MailBusBindings & {
  registered(): readonly string[];
} {
  const registered: string[] = [];
  return {
    registerAddress(address: string) {
      registered.push(address);
    },
    unregisterAddress(address: string) {
      const idx = registered.lastIndexOf(address);
      if (idx >= 0) registered.splice(idx, 1);
    },
    subscribeMailForAddress() {
      return () => {
        /* unused */
      };
    },
    registered(): readonly string[] {
      return registered.slice();
    },
  };
}

async function makeTempDir(prefix: string): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

function createStubRepoStore(baseDir: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix() {
      return { commitSha: "deadbeefcafef00d" };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- partial stub; the proxy below throws on any method the supervisor reaches into that this test does not implement
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

async function seedStepGrants(
  baseDir: string,
  repoId: RepoId,
  grants: unknown[],
): Promise<void> {
  const dir = path.join(baseDir, repoId.kind, repoId.id);
  await fs.mkdir(path.join(dir, "state"), { recursive: true });
  await fs.writeFile(
    path.join(dir, STEP_GRANTS_PATH),
    JSON.stringify({ grants }),
  );
}

interface RoundtripHandle {
  supervisorIpcKeyPair: { privateKey: Uint8Array; publicKey: Uint8Array };
  childIpcKeyPair: { privateKey: Uint8Array; publicKey: Uint8Array };
  childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
  supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
  channelId: string;
  childUpstreamSender: ReturnType<typeof createControlChannelSender>;
  bridge: ChildPackPushBridge;
  pushCalls: {
    agentAddress: string;
    repoId: RepoId;
    pack: Uint8Array;
    ref: string;
    commitSha: string;
  }[];
  shutdown: () => Promise<void>;
}

async function setupRoundtrip(opts: {
  pushWorkflowRunPack: WorkflowSupervisorBindings["pushWorkflowRunPack"];
}): Promise<RoundtripHandle> {
  const baseDir = await makeTempDir("pack-push-roundtrip-");
  await seedStepGrants(
    baseDir,
    defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
    [{ resource: "thing", action: "read" }],
  );
  const supervisorIpcKeyPair = await generateKeyPair();
  const childIpcKeyPair = await generateKeyPair();
  const supervisorToChild = createMemoryNdjsonStream();
  const childToSupervisor = createMemoryNdjsonStream();
  const eventStream = createMemoryFrameStream();
  let resolveExit: ((code: number) => void) | undefined;
  const exited = new Promise<number>((resolve) => {
    resolveExit = resolve;
  });
  let observedEnv: Record<string, string> | undefined;
  const spawner: SubprocessSpawner = ({ env }) => {
    observedEnv = env;
    const handle: SubprocessHandle = {
      pid: 7001,
      controlWriter: supervisorToChild.writer,
      controlReader: childToSupervisor.reader,
      eventReader: eventStream.reader,
      kill: () => {
        childToSupervisor.close();
        supervisorToChild.close();
        eventStream.close();
        resolveExit?.(0);
      },
      exited,
    };
    return handle;
  };
  const mailBus = createMockMailBus();
  const pushCalls: RoundtripHandle["pushCalls"] = [];
  const bindings: WorkflowSupervisorBindings = {
    repoStore: createStubRepoStore(baseDir),
    signAsPrincipal: () => ({
      sig: new Uint8Array(64),
      principalKind: "supervisor",
    }),
    mailBus,
    subprocessSpawner: spawner,
    binaryPath: "/fake/bin/workflow-child",
    substrateEnv: { DATA_DIR: baseDir },
    workflowRunRepoId: { kind: "workflow-run", id: "deployment-x" },
    workflowRunRef: "refs/heads/main",
    deploymentId: "deployment-x",
    deploymentMailAddress: "deployment-x@example.com",
    readPrincipal: { kind: "supervisor" },
    deriveStepAddress: ({ deploymentId, stepId }) =>
      `${deploymentId}-${stepId}@example.com`,
    trivialLaunch: () => {
      throw new Error("trivialLaunch not provided in this round-trip test");
    },
    ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
    pushWorkflowRunPack: async (call) => {
      pushCalls.push(call);
      if (opts.pushWorkflowRunPack !== undefined) {
        await opts.pushWorkflowRunPack(call);
      }
    },
  };
  const supervisor = createWorkflowSupervisor(bindings);
  const spawnPromise = supervisor.spawn({
    stepOrder: ["step-1"],
    definitionHash: "def-hash-roundtrip",
    onInferenceEvent: () => {
      /* unused */
    },
  });
  while (observedEnv === undefined) {
    await new Promise((r) => setTimeout(r, 1));
  }
  const channelId = observedEnv.IPC_CHANNEL_ID;
  if (channelId === undefined) {
    throw new Error("IPC_CHANNEL_ID missing from spawn env");
  }
  const childUpstreamSender = createControlChannelSender({
    privateKeySeed: childIpcKeyPair.privateKey,
    channelId,
    writer: {
      write(line: string) {
        childToSupervisor.inject(line);
      },
    },
  });
  while (!mailBus.registered().includes("deployment-x@example.com")) {
    await new Promise((r) => setTimeout(r, 1));
  }
  // Send ready first so the supervisor's bootstrap pins to the child's
  // public key and the upstream control pump starts.
  await childUpstreamSender.send({
    type: "ready",
    data: {
      childPid: 7001,
      childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
    },
  });
  await spawnPromise;

  // The pack-push bridge sends via the same upstream sender that
  // produced `ready`; the IPC seq counter therefore stays monotonic.
  const bridge = createChildPackPushBridge({
    upstreamSender: childUpstreamSender,
  });

  // Pump downstream control frames into the bridge so a
  // `pack.push.response` from the supervisor resolves the awaiter.
  const sink: PackPushResponseSink = bridge;
  const downstreamIter = receiveControlChannel({
    publicKey: supervisorIpcKeyPair.publicKey,
    channelId,
    reader: supervisorToChild.reader,
    onCrash: () => {
      /* round-trip stream is torn down by shutdown */
    },
  });
  const pumpPromise = (async () => {
    for await (const payload of downstreamIter) {
      if (payload.type === "pack.push.response") {
        sink.handleResponse(payload.data);
      }
    }
  })();

  return {
    supervisorIpcKeyPair,
    childIpcKeyPair,
    childToSupervisor,
    supervisorToChild,
    channelId,
    childUpstreamSender,
    bridge,
    pushCalls,
    shutdown: async () => {
      await supervisor.shutdown();
      await pumpPromise.catch(() => {
        /* the iterator ends when the supervisor kills the stream */
      });
    },
  };
}

describe("workflow-run pack push IPC round-trip", () => {
  test("child sendRequest resolves on a successful host pushWorkflowRunPack", async () => {
    const handle = await setupRoundtrip({
      pushWorkflowRunPack: async () => {
        /* success */
      },
    });
    const pack = new Uint8Array([7, 8, 9, 10, 11]);
    await handle.bridge.sendRequest({
      agentAddress: "deployment-x@example.com",
      repoId: { kind: "workflow-run", id: "deployment-x" },
      pack,
      ref: "refs/heads/main",
      commitSha: "feedface",
    });
    expect(handle.pushCalls).toHaveLength(1);
    const call = handle.pushCalls[0];
    if (call === undefined) throw new Error("no host push call captured");
    expect(call.agentAddress).toBe("deployment-x@example.com");
    expect(Array.from(call.pack)).toEqual(Array.from(pack));
    expect(call.commitSha).toBe("feedface");
    expect(handle.bridge.pendingCount).toBe(0);
    await handle.shutdown();
  });

  test("child sendRequest rejects when the host pushWorkflowRunPack rejects", async () => {
    const handle = await setupRoundtrip({
      pushWorkflowRunPack: () =>
        Promise.reject(new Error("hub side: pack rejected")),
    });
    await expect(
      handle.bridge.sendRequest({
        agentAddress: "deployment-x@example.com",
        repoId: { kind: "workflow-run", id: "deployment-x" },
        pack: new Uint8Array([1, 2, 3]),
        ref: "refs/heads/main",
        commitSha: "feedface",
      }),
    ).rejects.toThrow(/hub side: pack rejected/);
    expect(handle.bridge.pendingCount).toBe(0);
    await handle.shutdown();
  });
});

void ((): ControlPayload | undefined => undefined);
