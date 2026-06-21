// Recycle path tests against fake bindings.
//
// The recycle path's six-step sequence (drain -> kill -> respawn ->
// self-discover -> resume -> drain-buffer) is exercised here through
// the supervisor's caller-facing `recycle()` API plus direct calls
// into `createRecyclePolicy` for the policy-origin coverage.
//
// Strictly orthogonal to redeploy: every test below operates against
// the same fake deploy tree across the recycle. None of these tests
// re-seeds the workflow definition or mutates step credentials in a
// way that would conflate the recycle path with a redeploy.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

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
import { createRecyclePolicy, type RecyclePolicyBounds } from "./recycle";
import { defaultStepRepoId, STEP_GRANTS_PATH } from "./credentials";
import {
  createControlChannelSender,
  ControlPayload,
  SignedEnvelope,
  type FrameReader,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";

/**
 * Parse the `runId` carried on each `trigger.fire` frame written to
 * the in-memory child control stream. Validates every signed envelope
 * through the canonical `ControlPayload` narrow so the helper does not
 * need to `as`-cast at the boundary.
 */
function parseTriggerFireRunIds(lines: readonly string[]): string[] {
  const ids: string[] = [];
  for (const line of lines) {
    if (!line.includes("trigger.fire")) continue;
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) continue;
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) continue;
    if (payload.type !== "trigger.fire") continue;
    ids.push(payload.data.runId);
  }
  return ids;
}

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
  registrationHistory(): readonly string[];
} {
  const registered: string[] = [];
  const history: string[] = [];
  const subscribers = new Map<string, Set<(rawMessage: Uint8Array) => void>>();
  return {
    registerAddress(address: string) {
      registered.push(address);
      history.push(`register:${address}`);
    },
    unregisterAddress(address: string) {
      const idx = registered.lastIndexOf(address);
      if (idx >= 0) registered.splice(idx, 1);
      subscribers.delete(address);
      history.push(`unregister:${address}`);
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
    registrationHistory(): readonly string[] {
      return history.slice();
    },
  };
}

function createStubRepoStore(baseDir: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(_principal, _repoId, _ref, _args) {
      return { commitSha: "deadbeefcafef00d" };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; missing methods surface as a precise failure via the proxy
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

type FakeChild = {
  pid: number;
  channelId: string | undefined;
  supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
  childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
  eventChildToSupervisor: ReturnType<typeof createMemoryFrameStream>;
  killSignals: string[];
  resolveExit: ((code: number) => void) | undefined;
  exited: Promise<number>;
};

type SpawnTracker = {
  spawner: SubprocessSpawner;
  children: FakeChild[];
  totalSpawns: number;
};

function createSpawnTracker(opts: { sigtermExits?: boolean }): SpawnTracker {
  const children: FakeChild[] = [];
  const spawner: SubprocessSpawner = ({ env }) => {
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    const killSignals: string[] = [];
    const child: FakeChild = {
      pid: 4000 + children.length,
      channelId: env.IPC_CHANNEL_ID,
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      killSignals,
      resolveExit,
      exited,
    };
    children.push(child);
    const handle: SubprocessHandle = {
      pid: child.pid,
      controlWriter: supervisorToChild.writer,
      controlReader: childToSupervisor.reader,
      eventReader: eventChildToSupervisor.reader,
      kill: (signal) => {
        const sig = typeof signal === "string" ? signal : String(signal ?? "");
        killSignals.push(sig);
        if (opts.sigtermExits !== false || sig === "SIGKILL") {
          eventChildToSupervisor.close();
          childToSupervisor.close();
          child.resolveExit?.(0);
        }
      },
      exited,
    };
    return handle;
  };
  return {
    spawner,
    children,
    get totalSpawns() {
      return children.length;
    },
  };
}

async function driveReady(
  child: FakeChild,
  ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array },
): Promise<ReturnType<typeof createControlChannelSender>> {
  if (child.channelId === undefined) {
    throw new Error("test child has no channelId; spawn did not invoke env");
  }
  const childSender = createControlChannelSender({
    privateKeySeed: ipcKeypair.privateKey,
    channelId: child.channelId,
    writer: {
      write(line: string) {
        child.childToSupervisor.inject(line);
      },
    },
  });
  await childSender.send({
    type: "ready",
    data: {
      childPid: child.pid,
      childPublicKey: Buffer.from(ipcKeypair.publicKey).toString("hex"),
    },
  });
  return childSender;
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
  recyclePolicy?: RecyclePolicyBounds;
  inboxPrimitives?: InboxPrimitives;
}): Promise<WorkflowSupervisorBindings> {
  const repoStore = createStubRepoStore(opts.baseDir);
  return {
    repoStore,
    signAsPrincipal: (): SignedPayload => ({
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
      throw new Error("trivialLaunch must not run in recycle tests");
    },
    ipcKeyPairFactory: () => Promise.resolve(opts.ipcKeypair),
    inboxPrimitives: opts.inboxPrimitives ?? createMemoryInboxPrimitives(),
    ...(opts.recyclePolicy !== undefined
      ? { recyclePolicy: opts.recyclePolicy }
      : {}),
  };
}

async function spawnSupervisor(opts: {
  baseDir: string;
  tracker: SpawnTracker;
  mailBus: MailBusBindings;
  ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
  recyclePolicy?: RecyclePolicyBounds;
}) {
  await seedStepGrants(
    opts.baseDir,
    defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
    [{ resource: "thing", action: "read" }],
  );
  const bindings = await buildBindings({
    baseDir: opts.baseDir,
    spawner: opts.tracker.spawner,
    mailBus: opts.mailBus,
    ipcKeypair: opts.ipcKeypair,
    ...(opts.recyclePolicy !== undefined
      ? { recyclePolicy: opts.recyclePolicy }
      : {}),
  });
  const supervisor = createWorkflowSupervisor(bindings);
  const spawnPromise = supervisor.spawn({
    stepOrder: ["step-1"],
    definitionHash: "def-hash-abc",
    warmKeep: false,
    onInferenceEvent: () => undefined,
  });
  // Wait until the spawner has been invoked and the channelId is
  // known, then drive the synthetic child's `ready` frame.
  while (opts.tracker.children.length === 0) {
    await new Promise((r) => setTimeout(r, 1));
  }
  const first = opts.tracker.children[0];
  if (first === undefined) {
    throw new Error("tracker.children[0] missing");
  }
  const firstSender = await driveReady(first, opts.ipcKeypair);
  const result = await spawnPromise;
  return { supervisor, spawnResult: result, firstSender };
}

describe("supervisor recycle: operator-initiated", () => {
  test("drain mail sends, child is killed, fresh child spawns with a new channelId", async () => {
    const baseDir = await makeTempDir("recycle-op-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const { supervisor, spawnResult } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
    });
    const firstChild = tracker.children[0];
    if (firstChild === undefined) {
      throw new Error("first child missing after spawn");
    }
    const firstChannelId = spawnResult.channelId;

    // Kick the recycle. Drive the second child's `ready` once it
    // spawns.
    const recyclePromise = supervisor.recycle({ reason: "operator-asked" });
    while (tracker.children.length < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const secondChild = tracker.children[1];
    if (secondChild === undefined) {
      throw new Error("second child missing after recycle spawn");
    }
    await driveReady(secondChild, ipcKeypair);
    const attempt = await recyclePromise;

    expect(attempt.origin).toBe("operator");
    expect(attempt.reason).toBe("operator-asked");
    expect(attempt.previousChannelId).toBe(firstChannelId);
    expect(attempt.newChannelId).not.toBe(firstChannelId);
    expect(attempt.newChannelId).toMatch(/^[0-9a-f]{32}$/);
    // The first child received SIGTERM during the kill step.
    expect(firstChild.killSignals).toContain("SIGTERM");
    // The supervisor never unregistered the mail address across the
    // recycle (it holds the registration). The registration history
    // should contain exactly one `register` and no `unregister`.
    const history = mailBus.registrationHistory();
    expect(history.filter((h) => h.startsWith("register:")).length).toBe(1);
    expect(history.filter((h) => h.startsWith("unregister:")).length).toBe(0);
    // The new child's channelId is reflected in the second spawn's
    // env so a downstream verifier could pin the rotation.
    expect(secondChild.channelId).toBe(attempt.newChannelId);

    await supervisor.shutdown();
  });
});

describe("supervisor recycle: failure after the cohort handoff", () => {
  test("triggerRecycle throwing transitions the supervisor to a clean stopped state and re-throws", async () => {
    const baseDir = await makeTempDir("recycle-fail-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    // Track whether the second spawn has been attempted; force the
    // second spawn to throw so `triggerRecycle` fails mid-flight after
    // the transition to `recycling`.
    const tracker = createSpawnTracker({});
    const realSpawner = tracker.spawner;
    let spawnCount = 0;
    const failingSpawner: SubprocessSpawner = (opts) => {
      spawnCount += 1;
      if (spawnCount === 1) return realSpawner(opts);
      throw new Error("spawner failure on recycle");
    };
    const bindings = await buildBindings({
      baseDir,
      spawner: failingSpawner,
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
    while (tracker.children.length === 0) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const first = tracker.children[0];
    if (first === undefined) {
      throw new Error("tracker.children[0] missing");
    }
    await driveReady(first, ipcKeypair);
    await spawnPromise;

    let caught: unknown;
    try {
      await supervisor.recycle({ reason: "test-failure" });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(caught instanceof Error && caught.message).toMatch(
      /spawner failure on recycle/,
    );
    // The supervisor's failure recovery routes through `shutdownInternal`,
    // which kills the prior cohort and reaches `stopped`. A follow-up
    // shutdown must therefore be a no-op (the function early-returns
    // when phase is already `stopped`).
    await supervisor.shutdown();
    expect(first.killSignals).toContain("SIGTERM");
  });
});

describe("supervisor recycle: deliverSignal phase guard", () => {
  test("deliverSignal during the recycling window rejects rather than writing to the dying cohort's controlSender", async () => {
    // M2: during `recycling`, the supervisor's `state.controlSender`
    // still references the dying child's sender; a signal forwarded
    // there either buffers behind the impending SIGTERM (best case)
    // or writes into a closed pipe and is silently lost (worst case).
    // The phase guard on `deliverSignal` rejects any caller that
    // tries to land a signal during this window so the race surfaces
    // to the operator rather than dropping the signal on the floor.
    const baseDir = await makeTempDir("recycle-deliver-signal-guard-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
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
    while (tracker.children.length === 0) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await driveReady(first, ipcKeypair);
    await spawnPromise;

    // Kick the recycle. Wait for the second spawn to happen (so the
    // supervisor's state has transitioned to `recycling`) but do NOT
    // drive its ready frame -- triggerRecycle hangs on `waitForReady`,
    // leaving the supervisor in `recycling` for the deliverSignal
    // call to observe.
    const recyclePromise = supervisor.recycle({ reason: "guard-test" });
    while (tracker.children.length < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");

    let caught: unknown;
    try {
      await supervisor.deliverSignal({
        runId: "run-during-recycle",
        signalName: "test",
        signalId: "sig_test",
        payload: {},
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(caught instanceof Error && caught.message).toMatch(
      /deliverSignal called in phase recycling/,
    );

    // Drive the second child's ready so the recycle settles and
    // shutdown is clean. The signal rejection above is what the test
    // pins; the recycle flow itself is covered by the broader suite.
    await driveReady(second, ipcKeypair);
    await recyclePromise;
    await supervisor.shutdown();
  });
});

describe("supervisor recycle: mail buffered during the kill/respawn gap", () => {
  test("inbound mail during the gap drains into the new child after ready", async () => {
    const baseDir = await makeTempDir("recycle-mail-buffer-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
    });

    const recyclePromise = supervisor.recycle({ reason: "buffer-test" });
    // Wait for the new child to be spawned; mail delivered now sits
    // in the supervisor's buffer because the second child has not
    // emitted `ready` yet.
    while (tracker.children.length < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("gap-1"),
    );
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("gap-2"),
    );

    const secondChild = tracker.children[1];
    if (secondChild === undefined) {
      throw new Error("second child missing for buffer test");
    }
    // Count the supervisor's outbound frames to the new child before
    // `ready`. The supervisor must NOT forward the buffered mail
    // until `ready` lands; pre-ready, the only frame the supervisor
    // would write would be a drain. Recycle's drain step ran against
    // the FIRST child's controlSender, not this one.
    expect(secondChild.supervisorToChild.flushed()).toEqual([]);

    const secondChildSender = await driveReady(secondChild, ipcKeypair);
    const attempt = await recyclePromise;
    expect(attempt.origin).toBe("operator");

    // After ready, the new child's dispatch loop drains the inbox
    // claim-check queue in FIFO order. The loop processes runs
    // serially -- one `trigger.fire` per iteration, waiting for the
    // run's terminal event before advancing. The supervisor's
    // per-cohort broadcaster settles each dispatch on a `terminal.event`
    // upstream frame from the child; the test drives that frame
    // through the child's IPC sender so the second message lands on
    // the same loop iteration after the first one closes out.
    const flushedTriggerRunIds = (): string[] =>
      parseTriggerFireRunIds(secondChild.supervisorToChild.flushed());
    const firstDeadline = Date.now() + 1_000;
    while (flushedTriggerRunIds().length < 1 && Date.now() < firstDeadline) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const firstRunIds = flushedTriggerRunIds();
    expect(firstRunIds.length).toBeGreaterThanOrEqual(1);
    const firstRunId = firstRunIds[0];
    if (firstRunId === undefined) throw new Error("missing first runId");
    await secondChildSender.send({
      type: "terminal.event",
      data: {
        runId: firstRunId,
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });
    const secondDeadline = Date.now() + 1_000;
    while (flushedTriggerRunIds().length < 2 && Date.now() < secondDeadline) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const allRunIds = flushedTriggerRunIds();
    expect(allRunIds.length).toBeGreaterThanOrEqual(2);
    const secondRunId = allRunIds.find((id) => id !== firstRunId);
    if (secondRunId !== undefined) {
      await secondChildSender.send({
        type: "terminal.event",
        data: {
          runId: secondRunId,
          seq: 0,
          kind: "RunCompleted",
          at: "test",
        },
      });
    }

    await supervisor.shutdown();
  });
});

describe("supervisor recycle: policy-initiated (max-uptime trip)", () => {
  test("the policy's max-uptime threshold trips recycle with a policy-origin reason", async () => {
    const baseDir = await makeTempDir("recycle-policy-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    let nowMs = 1_700_000_000_000;

    let trigger: ((reason: string) => Promise<void>) | null = null;
    const policy = createRecyclePolicy({
      bounds: { maxUptimeMs: 10_000 },
      intervalMs: 1_000,
      now: () => nowMs,
      spawnedAt: nowMs,
      setTimer: () => undefined,
      clearTimer: () => undefined,
      trigger: async (reason) => {
        if (trigger !== null) await trigger(reason);
      },
    });

    const observedReasons: string[] = [];
    trigger = async (reason) => {
      observedReasons.push(reason);
    };

    // Before the threshold trips, ticks do nothing.
    await policy.tick();
    expect(observedReasons).toEqual([]);

    // Advance past the threshold; the next tick must trip.
    nowMs += 11_000;
    await policy.tick();
    expect(observedReasons.length).toBe(1);
    const firstReason = observedReasons[0];
    if (firstReason === undefined) {
      throw new Error("policy trigger did not fire");
    }
    expect(firstReason).toContain("max-uptime");

    policy.stop();
    // Manual policy test does not spawn a supervisor; nothing to
    // shut down. The mail bus and tracker stay unused here.
    expect(mailBus.registered()).toEqual([]);
    expect(tracker.totalSpawns).toBe(0);
    expect(ipcKeypair.publicKey.length).toBe(32);
  });
});

describe("supervisor recycle: child self-initiated via recycle.request", () => {
  test("a child-side recycle.request frame triggers the supervisor's recycle path with self origin", async () => {
    const baseDir = await makeTempDir("recycle-self-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const { supervisor, firstSender } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
    });
    const firstChild = tracker.children[0];
    if (firstChild === undefined) {
      throw new Error("first child missing for self-recycle test");
    }

    // The first child sends a `recycle.request` upstream. The
    // supervisor's upstream pump receives it and funnels into the
    // recycle path with origin=self. We reuse the same sender from
    // the initial ready so the seq counter stays monotonic against
    // the supervisor's receiver.
    await firstSender.send({
      type: "recycle.request",
      data: { reason: "child-detected-bug" },
    });

    // Wait for the supervisor to spawn the replacement child.
    while (tracker.children.length < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const secondChild = tracker.children[1];
    if (secondChild === undefined) {
      throw new Error("second child missing for self-recycle test");
    }
    await driveReady(secondChild, ipcKeypair);

    // The new child runs under a fresh channelId.
    expect(secondChild.channelId).not.toBe(firstChild.channelId);
    expect(secondChild.channelId).toMatch(/^[0-9a-f]{32}$/);
    // The mail-bus registration is held across the recycle.
    expect(mailBus.registered()).toContain("deployment-x@example.com");

    await supervisor.shutdown();
  });
});

describe("supervisor recycle: channelId rotation on respawn", () => {
  test("consecutive recycles each produce distinct channelIds", async () => {
    const baseDir = await makeTempDir("recycle-rotation-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const { supervisor, spawnResult } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
    });
    const channelIds = [spawnResult.channelId];

    for (let i = 0; i < 3; i += 1) {
      const recyclePromise = supervisor.recycle({
        reason: `rotation-${String(i)}`,
      });
      while (tracker.children.length < i + 2) {
        await new Promise((r) => setTimeout(r, 1));
      }
      const nextChild = tracker.children[i + 1];
      if (nextChild === undefined) {
        throw new Error(`expected child at index ${String(i + 1)}`);
      }
      await driveReady(nextChild, ipcKeypair);
      const attempt = await recyclePromise;
      channelIds.push(attempt.newChannelId);
    }

    // Every channelId is distinct: 4 spawns total, 4 unique ids.
    expect(new Set(channelIds).size).toBe(channelIds.length);
    for (const id of channelIds) {
      expect(id).toMatch(/^[0-9a-f]{32}$/);
    }

    await supervisor.shutdown();
  });
});

describe("supervisor recycle: terminal-event broadcaster cohort", () => {
  test("the previous cohort's terminal broadcaster is disposed on installNewChild", async () => {
    const baseDir = await makeTempDir("recycle-cohort-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});

    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const baseBindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus,
      ipcKeypair,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
    };
    const supervisor = createWorkflowSupervisor(bindings);
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,
      onInferenceEvent: () => undefined,
    });
    while (tracker.children.length === 0) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await driveReady(first, ipcKeypair);
    await spawnPromise;

    // Deliver one mail so an in-flight run exists, then drain so the
    // supervisor arms an accumulator. Each accumulator subscribes to
    // the cohort's terminal broadcaster for its tracked runId.
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("cohort-pre-recycle"),
    );
    await new Promise((r) => setTimeout(r, 5));
    await supervisor.drain({ deadlineMs: 5_000 });

    // Recycle. The installNewChild path aborts the prior cohort,
    // disposes the prior broadcaster (which settles every minted
    // iterator with `done: true`), and stops every armed accumulator
    // -- which fires `return()` on each watcher iterator. The
    // observable invariant is structural: after the recycle returns,
    // the supervisor accepts a fresh inbound mail and the new
    // cohort's broadcaster handles its dispatch end-to-end.
    const recyclePromise = supervisor.recycle({ reason: "cohort-finalise" });
    while (tracker.children.length < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recyclePromise;
    // Give the cohort abort microtask a chance to land.
    await new Promise((r) => setTimeout(r, 5));

    // The new cohort still operates: deliver a fresh mail and
    // confirm the supervisor forwards a trigger.fire through the
    // new child's controlSender. A wedged prior cohort would have
    // left the dispatch loop blocked on the disposed broadcaster
    // and the trigger.fire would never reach the new child's stream.
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("cohort-post-recycle"),
    );
    const deadline = Date.now() + 1_000;
    while (Date.now() < deadline) {
      const flushed = second.supervisorToChild.flushed();
      if (flushed.some((f) => f.includes("trigger.fire"))) break;
      await new Promise((r) => setTimeout(r, 1));
    }
    const triggerFires = second.supervisorToChild
      .flushed()
      .filter((f) => f.includes("trigger.fire"));
    expect(triggerFires.length).toBeGreaterThanOrEqual(1);

    await supervisor.shutdown();
  });
});

describe("supervisor recycle: drain-side processing replay", () => {
  test("triggerRecycle invokes replayProcessingToInbox between drain and kill", async () => {
    // This is a focused unit test against `triggerRecycle`'s ordering
    // contract: the `replayProcessingToInbox` callback must fire after
    // `drain` returns and before `killChildHandle` lands. The test
    // observes the order via call recording.
    const baseDir = await makeTempDir("recycle-replay-order-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const calls: string[] = [];
    const recordingInbox: InboxPrimitives = {
      ...createMemoryInboxPrimitives(),
      async replayProcessingToInbox(_store, _principal, _repoId, _address) {
        calls.push("replayProcessingToInbox");
        return { commitSha: "memory", replayedKeys: [] };
      },
    };
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus,
      ipcKeypair,
      inboxPrimitives: recordingInbox,
    });
    const originalKill = tracker.children;
    // Wrap the spawner so we can hook kill to record the call order.
    const wrappedSpawner: SubprocessSpawner = (args) => {
      const handle = tracker.spawner(args);
      const originalKillFn = handle.kill;
      return {
        ...handle,
        kill: (signal?: number | string) => {
          calls.push("kill");
          originalKillFn(signal);
        },
      };
    };
    const bindingsWithSpawner: WorkflowSupervisorBindings = {
      ...bindings,
      subprocessSpawner: wrappedSpawner,
    };
    const supervisor = createWorkflowSupervisor(bindingsWithSpawner);
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,
      onInferenceEvent: () => undefined,
    });
    while (originalKill.length === 0) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const first = originalKill[0];
    if (first === undefined) throw new Error("first child missing");
    await driveReady(first, ipcKeypair);
    await spawnPromise;
    // Clear out the spawn-time replay call so we can isolate the
    // recycle-time one.
    calls.length = 0;

    const recyclePromise = supervisor.recycle({ reason: "ordering-test" });
    while (originalKill.length < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const second = originalKill[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recyclePromise;
    // The recycle path invokes `replayProcessingToInbox` BEFORE the
    // kill. The order recorded in `calls` reflects that.
    const replayIdx = calls.indexOf("replayProcessingToInbox");
    const killIdx = calls.indexOf("kill");
    expect(replayIdx).toBeGreaterThanOrEqual(0);
    expect(killIdx).toBeGreaterThanOrEqual(0);
    expect(replayIdx).toBeLessThan(killIdx);
    await supervisor.shutdown();
  });
});

describe("supervisor recycle: shutdown during the kill/respawn gap", () => {
  test("a shutdown landing while triggerRecycle awaits the new child's ready kills the new wiring and leaves no orphan", async () => {
    // The race: `installNewChild` writes `state = { phase: "running",
    // ... }` unconditionally. If `shutdown()` lands during the gap
    // between `subprocessSpawner` (which returns a live handle) and
    // `installNewChild` (which would register that handle on
    // `state`), the supervisor's state has been flipped to
    // `stopping`/`stopped` -- but the late `installNewChild` write
    // clobbers it back to `running` and leaves the freshly spawned
    // child running, orphaned. The fix is a phase guard at
    // `installNewChild` that kills the new wiring when the
    // supervisor is no longer in `recycling`.
    const baseDir = await makeTempDir("recycle-shutdown-gap-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus,
      ipcKeypair,
    });
    const supervisor = createWorkflowSupervisor(bindings);
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-shutdown-race",
      warmKeep: false,
      onInferenceEvent: () => undefined,
    });
    while (tracker.children.length === 0) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const first = tracker.children[0];
    if (first === undefined) {
      throw new Error("tracker.children[0] missing");
    }
    await driveReady(first, ipcKeypair);
    await spawnPromise;

    // Start the recycle. `triggerRecycle` runs drain -> replay ->
    // abortPriorCohort -> kill (the first child gets SIGTERM) ->
    // subprocessSpawner (the second child handle materialises) ->
    // `await waitForReady`. The second child's ready frame is NOT
    // driven yet, so the recycle parks.
    const recyclePromise = supervisor.recycle({ reason: "race-test" });
    while (tracker.children.length < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const second = tracker.children[1];
    if (second === undefined) {
      throw new Error("tracker.children[1] missing");
    }

    // Concurrent shutdown. `shutdownInternal` synchronously flips
    // `state.phase` to "stopping" at its head (supervisor.ts L1583
    // at the time of writing). Yield a microtask so the synchronous
    // prelude is guaranteed to have run before we drive the second
    // child's ready -- without this yield, `installNewChild` could
    // race `state.phase` and fire while phase is still "recycling",
    // making the test pass by coincidence rather than by the guard
    // catching the actual race window the fix targets.
    const shutdownPromise = supervisor.shutdown();
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });

    // Release the second child's ready frame so `triggerRecycle`
    // proceeds past `waitForReady` and calls `installNewChild`.
    // Without the phase guard, `installNewChild` clobbers
    // `state.phase` back to `running` and the second child stays
    // alive. With the guard, `installNewChild` observes the
    // shutdown-flipped phase and kills the new wiring.
    await driveReady(second, ipcKeypair);

    await Promise.allSettled([recyclePromise, shutdownPromise]);

    // The orphan child must have been killed. Without the guard,
    // `second.killSignals` is empty (the new child stays alive
    // under a stopped supervisor).
    expect(second.killSignals).toContain("SIGTERM");

    // A subsequent shutdown is a no-op -- phase is already stopped.
    await supervisor.shutdown();
  });
});

describe("supervisor recycle: external drain phase guard", () => {
  test("external drain() during the recycling window is a silent no-op and does not write to the dying controlSender", async () => {
    // The window: between `triggerRecycle`'s kill step and
    // `installNewChild`, the supervisor still reports `recycling` but
    // `state.controlSender` references a sender whose pipe is being
    // torn down. The recycle path's OWN drain step (the kill-prep
    // call) runs BEFORE kill so its sender is alive; an EXTERNAL
    // drain that lands later in the window can silently lose its
    // frame. The public surface admits only `running` / `starting`
    // for external callers; the recycle path bypasses through the
    // internal `drainImpl` helper.
    const baseDir = await makeTempDir("recycle-drain-guard-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus,
      ipcKeypair,
    });
    const supervisor = createWorkflowSupervisor(bindings);
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-drain-guard",
      warmKeep: false,
      onInferenceEvent: () => undefined,
    });
    while (tracker.children.length === 0) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await driveReady(first, ipcKeypair);
    await spawnPromise;

    // Kick the recycle. The recycle path's drain step writes one
    // drain frame onto the first child's controlSender (this is the
    // kill-prep drain). triggerRecycle then runs replay -> abort ->
    // kill -> spawn -> waitForReady; the second child has not been
    // driven to `ready` yet, so the supervisor parks in `recycling`.
    const recyclePromise = supervisor.recycle({ reason: "drain-guard-test" });
    while (tracker.children.length < 2) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");

    // Snapshot how many drain frames the recycle path's drain step
    // wrote. The exact count depends on the recycle's drain
    // contract, but any external drain during the parked window
    // must NOT add to it.
    function countDrainFrames(
      stream: ReturnType<typeof createMemoryNdjsonStream>,
    ): number {
      let count = 0;
      for (const line of stream.flushed()) {
        const raw: unknown = JSON.parse(line);
        const signed = SignedEnvelope(raw);
        if (signed instanceof type.errors) continue;
        const payload = ControlPayload(signed.envelope.payload);
        if (payload instanceof type.errors) continue;
        if (payload.type === "drain") count += 1;
      }
      return count;
    }
    const drainFramesBefore = countDrainFrames(first.supervisorToChild);

    // External drain landing in the parked window. Must silently
    // no-op rather than write into the dying controlSender; the
    // public surface rejects `recycling` for non-recycle callers.
    await supervisor.drain({ deadlineMs: 60_000 });

    expect(countDrainFrames(first.supervisorToChild)).toBe(drainFramesBefore);
    // The second child's stream must not have received a drain
    // either -- the new cohort's wiring is not yet installed.
    expect(countDrainFrames(second.supervisorToChild)).toBe(0);

    // Drive the second child's ready so the recycle settles and
    // shutdown is clean.
    await driveReady(second, ipcKeypair);
    await recyclePromise;
    await supervisor.shutdown();
  });
});
