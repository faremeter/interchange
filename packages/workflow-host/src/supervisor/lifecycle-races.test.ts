// Lifecycle-edge race regression tests for the supervisor.
//
// Gap A pins the `waitForReady` -> `pumpUpstreamControl` iterator
// handoff: an upstream control frame that arrives in the wire-level
// buffer immediately after `ready` (before the upstream pump
// subscribes) must still flow into `pumpUpstreamControl` and not be
// dropped on the floor when the generator's first consumer
// (`waitForReady`) exits.
//
// Gap B pins the spawn-time crash vs `shutdownInternal` race: when a
// `shutdown()` lands while `spawn()` is still awaiting the child's
// `ready` frame, the spawn-time error (control channel ended before
// `ready`) must surface to the awaiting `spawn()` caller and not be
// silently swallowed by the shutdown teardown.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto-node";
import type { RepoId, RepoStore } from "@intx/hub-sessions";

import {
  createWorkflowSupervisor,
  type InboxPrimitives,
  type MailBusBindings,
  type SignedPayload,
  type SubprocessHandle,
  type SubprocessSpawner,
  type WorkflowSupervisorBindings,
} from "./index";
import { defaultStepRepoId, STEP_GRANTS_PATH } from "./credentials";
import {
  createControlChannelSender,
  type FrameReader,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";

async function makeTempDir(prefix: string): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

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
    flushed(): readonly string[] {
      return buffer.slice();
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
  deliver(address: string, message: Uint8Array): void;
} {
  const registered: string[] = [];
  const subscribers = new Map<string, Set<(rawMessage: Uint8Array) => void>>();
  return {
    registerAddress(address: string) {
      registered.push(address);
    },
    unregisterAddress(address: string) {
      const idx = registered.lastIndexOf(address);
      if (idx >= 0) registered.splice(idx, 1);
      subscribers.delete(address);
    },
    subscribeMailForAddress(
      address: string,
      handler: (rawMessage: Uint8Array) => void,
    ) {
      let set = subscribers.get(address);
      if (set === undefined) {
        set = new Set();
        subscribers.set(address, set);
      }
      set.add(handler);
      return () => {
        const current = subscribers.get(address);
        current?.delete(handler);
      };
    },
    sendOutbound() {
      throw new Error("sendOutbound not exercised in this test");
    },
    registered(): readonly string[] {
      return registered.slice();
    },
    deliver(address: string, message: Uint8Array) {
      const set = subscribers.get(address);
      if (set === undefined) return;
      for (const handler of set) handler(message);
    },
  };
}

function createStubRepoStore(baseDir: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(_principal, _repoId, _ref, _args) {
      return { commitSha: "deadbeefcafef00d", newlyTerminalRuns: [] };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; missing methods surface a precise failure via the proxy
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

function createMemoryInboxPrimitives(): InboxPrimitives {
  type Entry = {
    messageId: string;
    receivedAt: number;
    mailAuditRef: { store: string; path: string };
  };
  const state = new Map<
    string,
    {
      inbox: Map<string, Entry>;
      processing: Map<string, Entry>;
      consumed: Map<string, Entry>;
    }
  >();
  function getOrCreate(address: string) {
    let entry = state.get(address);
    if (entry === undefined) {
      entry = { inbox: new Map(), processing: new Map(), consumed: new Map() };
      state.set(address, entry);
    }
    return entry;
  }
  function key(receivedAt: number, messageId: string): string {
    return `${String(receivedAt)}-${messageId}`;
  }
  return {
    async enqueueInbox(_store, _principal, _repoId, args) {
      const s = getOrCreate(args.address);
      const k = key(args.receivedAt, args.messageId);
      const envelope: Entry = {
        messageId: args.messageId,
        receivedAt: args.receivedAt,
        mailAuditRef: args.mailAuditRef,
      };
      s.inbox.set(k, envelope);
      return {
        commitSha: "memory",
        inboxKey: k,
        envelope: {
          messageId: args.messageId,
          receivedAt: args.receivedAt,
          address: args.address,
          mailAuditRef: args.mailAuditRef,
        },
      };
    },
    async dequeueToProcessing(_store, _principal, _repoId, address) {
      const s = getOrCreate(address);
      const entries = [...s.inbox.entries()].sort(([, a], [, b]) => {
        if (a.receivedAt !== b.receivedAt) return a.receivedAt - b.receivedAt;
        if (a.messageId < b.messageId) return -1;
        if (a.messageId > b.messageId) return 1;
        return 0;
      });
      if (entries.length === 0) return null;
      const head = entries[0];
      if (head === undefined) throw new Error("unreachable");
      const [k, envelope] = head;
      s.inbox.delete(k);
      s.processing.set(k, envelope);
      return {
        commitSha: "memory",
        key: k,
        envelope: {
          messageId: envelope.messageId,
          receivedAt: envelope.receivedAt,
          address,
          mailAuditRef: envelope.mailAuditRef,
        },
      };
    },
    async markConsumed(_store, _principal, _repoId, args) {
      const s = getOrCreate(args.address);
      let foundKey: string | null = null;
      let envelope: Entry | null = null;
      for (const [k, value] of s.processing) {
        if (value.messageId === args.messageId) {
          foundKey = k;
          envelope = value;
          break;
        }
      }
      if (foundKey === null || envelope === null) {
        throw new Error("processing entry not found");
      }
      s.processing.delete(foundKey);
      s.consumed.set(args.messageId, envelope);
      return {
        commitSha: "memory",
        envelope: {
          messageId: envelope.messageId,
          receivedAt: envelope.receivedAt,
          address: args.address,
          runId: args.runId,
          consumedAt: args.consumedAt,
          mailAuditRef: envelope.mailAuditRef,
        },
        watermark: 0,
        prunedMessageIds: [],
      };
    },
    async replayProcessingToInbox(_store, _principal, _repoId, address) {
      const s = getOrCreate(address);
      const replayedKeys: string[] = [];
      for (const [k, value] of s.processing) {
        s.inbox.set(k, value);
        replayedKeys.push(k);
      }
      s.processing.clear();
      return { commitSha: "memory", replayedKeys };
    },
  };
}

async function buildBindings(opts: {
  baseDir: string;
  spawner: SubprocessSpawner;
  mailBus: MailBusBindings;
  ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
}): Promise<WorkflowSupervisorBindings> {
  return {
    repoStore: createStubRepoStore(opts.baseDir),
    signAsPrincipal: async (): Promise<SignedPayload> => ({
      sig: new Uint8Array(64),
      principalKind: "supervisor",
    }),
    mailBus: opts.mailBus,
    subprocessSpawner: opts.spawner,
    binaryPath: "/fake/bin/workflow-child",
    substrateEnv: { DATA_DIR: opts.baseDir },
    workflowRunRepoId: { kind: "workflow-run", id: "deployment-x" },
    workflowRunRef: "refs/heads/main",
    deploymentId: "deployment-x",
    deploymentMailAddress: "deployment-x@example.com",
    readPrincipal: { kind: "supervisor" },
    deriveStepAddress: ({ deploymentId, stepId }) =>
      `${deploymentId}-${stepId}@example.com`,
    trivialLaunch: () => {
      throw new Error("trivialLaunch must not run in lifecycle-races tests");
    },
    ipcKeyPairFactory: () => Promise.resolve(opts.ipcKeypair),
    inboxPrimitives: createMemoryInboxPrimitives(),
  };
}

describe("waitForReady -> pumpUpstreamControl iterator handoff (Gap A)", () => {
  test("a recycle.request that lands immediately after ready is delivered to the upstream pump rather than dropped", async () => {
    // The synthetic child injects `ready` and `recycle.request` back-
    // to-back into the supervisor's upstream control reader BEFORE the
    // supervisor's `spawn()` consumes them. The handoff under test:
    // `waitForReady` consumes `ready` off the receiver generator and
    // returns; `pumpUpstreamControl` (started later in `spawn()`)
    // continues iterating the SAME generator. If the handoff finalized
    // the generator on `ready`, the buffered `recycle.request` frame
    // would be silently dropped and the supervisor would never react.
    // The assertion: the supervisor honours the request by invoking
    // its recycle path, which spawns a fresh workflow-process child
    // under a new channelId.
    const baseDir = await makeTempDir("lifecycle-races-gap-a-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const ipcKeypair = await generateKeyPair();
    const childIpcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();

    type FakeChild = {
      pid: number;
      channelId: string | undefined;
      controlIn: ReturnType<typeof createMemoryNdjsonStream>;
      controlOut: ReturnType<typeof createMemoryNdjsonStream>;
      events: ReturnType<typeof createMemoryFrameStream>;
      killed: boolean;
      resolveExit: ((code: number) => void) | undefined;
      exited: Promise<number>;
    };
    const children: FakeChild[] = [];
    const spawner: SubprocessSpawner = ({ env }) => {
      const controlIn = createMemoryNdjsonStream();
      const controlOut = createMemoryNdjsonStream();
      const events = createMemoryFrameStream();
      let resolveExit: ((code: number) => void) | undefined;
      const exited = new Promise<number>((resolve) => {
        resolveExit = resolve;
      });
      const child: FakeChild = {
        pid: 5000 + children.length,
        channelId: env.IPC_CHANNEL_ID,
        controlIn,
        controlOut,
        events,
        killed: false,
        resolveExit,
        exited,
      };
      children.push(child);
      const handle: SubprocessHandle = {
        pid: child.pid,
        controlWriter: controlOut.writer,
        controlReader: controlIn.reader,
        eventReader: events.reader,
        kill: () => {
          child.killed = true;
          controlIn.close();
          events.close();
          child.resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    const bindings = await buildBindings({
      baseDir,
      spawner,
      mailBus,
      ipcKeypair,
    });
    const supervisor = createWorkflowSupervisor(bindings);

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,
      onInferenceEvent: () => undefined,
    });

    // Wait until the supervisor has invoked the spawner. At this
    // moment the supervisor's `wireChild` has constructed the receive
    // generator but no consumer has subscribed -- both frames injected
    // below sit in the reader's internal buffer until the generator
    // pulls them.
    while (children.length === 0 || children[0]?.channelId === undefined) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const first = children[0];
    if (first === undefined || first.channelId === undefined) {
      throw new Error("first child not minted");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeypair.privateKey,
      channelId: first.channelId,
      writer: {
        write(line: string) {
          first.controlIn.inject(line);
        },
      },
    });
    // Inject `ready` followed IMMEDIATELY by `recycle.request`. Both
    // frames are buffered in the reader before any iterator consumer
    // pulls. `waitForReady` will consume `ready` and exit; the
    // upstream pump must observe the buffered `recycle.request` next.
    await childSender.send({
      type: "ready",
      data: {
        childPid: first.pid,
        childPublicKey: Buffer.from(childIpcKeypair.publicKey).toString("hex"),
      },
    });
    await childSender.send({
      type: "recycle.request",
      data: { reason: "early-arriving" },
    });

    await spawnPromise;

    // The supervisor's recycle path under origin=self spawns a fresh
    // workflow-process child. Wait for the second spawn; if Gap A
    // were unhealed (e.g. `for await ... return` over the receiver
    // generator), the buffered `recycle.request` would be silently
    // dropped and `children.length` would never reach 2.
    const deadline = Date.now() + 1000;
    while (children.length < 2 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(children.length).toBeGreaterThanOrEqual(2);
    const second = children[1];
    if (second === undefined || second.channelId === undefined) {
      throw new Error("second child not minted");
    }
    expect(second.channelId).not.toBe(first.channelId);

    // Drive the replacement child's `ready` so the recycle path
    // settles cleanly before shutdown.
    const secondSender = createControlChannelSender({
      privateKeySeed: childIpcKeypair.privateKey,
      channelId: second.channelId,
      writer: {
        write(line: string) {
          second.controlIn.inject(line);
        },
      },
    });
    await secondSender.send({
      type: "ready",
      data: {
        childPid: second.pid,
        childPublicKey: Buffer.from(childIpcKeypair.publicKey).toString("hex"),
      },
    });

    await supervisor.shutdown();
  });
});

describe("shutdownInternal vs spawn-time crash (Gap B)", () => {
  test("a spawn that loses its control channel mid-handshake surfaces the failure to the awaiting spawn() caller even when shutdown races in", async () => {
    // The spawner returns a handle whose control reader never yields
    // `ready`. The test starts `spawn()`, lets the supervisor reach
    // the `waitForReady` await, then calls `shutdown()`. The shutdown
    // path kills the handle, which closes the control reader. The
    // supervisor's `waitForReady` observes the iterator ending and
    // throws "control channel ended before child emitted ready". That
    // error MUST surface as a `spawn()` rejection -- not be swallowed
    // by the shutdown teardown's catches.
    const baseDir = await makeTempDir("lifecycle-races-gap-b-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();

    let spawnerInvoked = false;
    let controlIn: ReturnType<typeof createMemoryNdjsonStream> | undefined;
    let events: ReturnType<typeof createMemoryFrameStream> | undefined;
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let killed = false;
    const spawner: SubprocessSpawner = () => {
      spawnerInvoked = true;
      controlIn = createMemoryNdjsonStream();
      const controlOut = createMemoryNdjsonStream();
      events = createMemoryFrameStream();
      const handle: SubprocessHandle = {
        pid: 9991,
        controlWriter: controlOut.writer,
        controlReader: controlIn.reader,
        eventReader: events.reader,
        kill: () => {
          killed = true;
          controlIn?.close();
          events?.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    const bindings = await buildBindings({
      baseDir,
      spawner,
      mailBus,
      ipcKeypair,
    });
    const supervisor = createWorkflowSupervisor(bindings);

    // Spawn but never deliver `ready`. The supervisor will sit on
    // `waitForReady`.
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,
      onInferenceEvent: () => undefined,
    });
    // Wait for the spawn to be in flight and registered with the
    // mail bus so we know the supervisor is past `wireChild` and
    // mid-handshake.
    while (
      !spawnerInvoked ||
      !mailBus.registered().includes("deployment-x@example.com")
    ) {
      await new Promise((r) => setTimeout(r, 1));
    }

    // Concurrently trigger shutdown. The shutdown's `prior.handle.kill()`
    // closes the control reader; the supervisor's `waitForReady`
    // throws "control channel ended before child emitted ready" and
    // unwinds out of `spawn()`. The test asserts the rejection is
    // observable -- a swallowed error would either hang `spawnPromise`
    // forever or let it resolve with a malformed `SpawnResult`.
    const shutdownPromise = supervisor.shutdown();

    let spawnError: unknown;
    try {
      await spawnPromise;
      spawnError = "spawn resolved unexpectedly";
    } catch (cause) {
      spawnError = cause;
    }
    await shutdownPromise;

    expect(killed).toBe(true);
    expect(spawnError).toBeInstanceOf(Error);
    const message =
      spawnError instanceof Error ? spawnError.message : String(spawnError);
    expect(message).toMatch(/control channel ended before child emitted ready/);
  });
});
