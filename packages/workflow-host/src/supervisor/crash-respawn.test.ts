// Crash-respawn tests against fake bindings.
//
// The supervisor arms an exit-watcher on `handle.exited` when a child
// becomes the running cohort. An unexpected exit (the fake child's
// `exited` resolves without a shutdown or recycle in flight) drives the
// respawn path: replay any stranded mail, spawn a fresh child, resume
// dispatch. A crash-loop guard bounds this -- `crashLoopMaxCount`
// unexpected exits within `crashLoopWindowMs` latch the deployment to a
// terminal state instead of respawning, and a stable run resets the
// counter.
//
// These tests exercise the guard's classification (planned kills from
// shutdown and recycle must NOT respawn), the happy-path respawn + replay,
// the latch, and the stable-run reset. The harness mirrors the one in
// `recycle.test.ts`: a fake spawner whose children expose a controllable
// `exited` promise, an in-memory inbox, and a control-channel `driveReady`.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
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
  const subscribers = new Map<
    string,
    Set<(rawMessage: Uint8Array) => Promise<void>>
  >();
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
      handler: (rawMessage: Uint8Array) => Promise<void>,
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
      for (const handler of set) void handler(message).catch(() => undefined);
    },
  };
}

function createStubRepoStore(
  baseDir: string,
  writtenPrefixes?: string[],
): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(_principal, _repoId, _ref, args) {
      writtenPrefixes?.push(args.preservePrefix);
      return { commitSha: "deadbeefcafef00d", newlyTerminalRuns: [] };
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
  childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
  supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
  eventChildToSupervisor: ReturnType<typeof createMemoryFrameStream>;
  killSignals: string[];
  crash: () => void;
  exited: Promise<number>;
};

type SpawnTracker = {
  spawner: SubprocessSpawner;
  children: FakeChild[];
  get totalSpawns(): number;
};

function createSpawnTracker(): SpawnTracker {
  const children: FakeChild[] = [];
  const spawner: SubprocessSpawner = ({ env }) => {
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: (code: number) => void = () => undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    const killSignals: string[] = [];
    // Simulate a process death: close both channels and resolve `exited`.
    // A deliberate kill and a crash take the same terminal steps; the
    // difference the supervisor cares about is only WHEN they happen
    // relative to its own lifecycle, which the exit-watcher classifies.
    const die = () => {
      eventChildToSupervisor.close();
      childToSupervisor.close();
      resolveExit(0);
    };
    const child: FakeChild = {
      pid: 5000 + children.length,
      channelId: env.IPC_CHANNEL_ID,
      supervisorToChild,
      childToSupervisor,
      eventChildToSupervisor,
      killSignals,
      crash: die,
      exited,
    };
    children.push(child);
    const handle: SubprocessHandle = {
      pid: child.pid,
      controlWriter: supervisorToChild.writer,
      controlReader: childToSupervisor.reader,
      eventReader: eventChildToSupervisor.reader,
      kill: (signal) => {
        killSignals.push(
          typeof signal === "string" ? signal : String(signal ?? ""),
        );
        die();
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
      childPublicKey: hexEncode(ipcKeypair.publicKey),
    },
  });
  return childSender;
}

function createMemoryInboxPrimitives(): InboxPrimitives & {
  replayCalls(): number;
} {
  let replayCalls = 0;
  type Entry = {
    messageId: string;
    receivedAt: number;
    mailAuditRef: { store: string; path: string };
    rawMessage?: string;
  };
  const state = new Map<
    string,
    { inbox: Map<string, Entry>; processing: Map<string, Entry> }
  >();
  function getOrCreate(address: string) {
    let entry = state.get(address);
    if (entry === undefined) {
      entry = { inbox: new Map(), processing: new Map() };
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
      s.inbox.set(k, {
        messageId: args.messageId,
        receivedAt: args.receivedAt,
        mailAuditRef: args.mailAuditRef,
        ...(args.rawMessage !== undefined
          ? { rawMessage: args.rawMessage }
          : {}),
      });
      return {
        outcome: "enqueued",
        commitSha: "memory",
        inboxKey: k,
        envelope: {
          messageId: args.messageId,
          receivedAt: args.receivedAt,
          address: args.address,
          mailAuditRef: args.mailAuditRef,
          ...(args.rawMessage !== undefined
            ? { rawMessage: args.rawMessage }
            : {}),
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
          ...(envelope.rawMessage !== undefined
            ? { rawMessage: envelope.rawMessage }
            : {}),
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
      replayCalls += 1;
      const s = getOrCreate(address);
      const replayedKeys: string[] = [];
      for (const [k, value] of s.processing) {
        s.inbox.set(k, value);
        replayedKeys.push(k);
      }
      s.processing.clear();
      return { commitSha: "memory", replayedKeys };
    },
    replayCalls() {
      return replayCalls;
    },
  };
}

async function buildBindings(opts: {
  baseDir: string;
  spawner: SubprocessSpawner;
  mailBus: MailBusBindings;
  ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
  inboxPrimitives: InboxPrimitives;
  crashLoopMaxCount?: number;
  crashLoopStableResetMs?: number;
  setTimer?: (cb: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  writtenPrefixes?: string[];
}): Promise<WorkflowSupervisorBindings> {
  const repoStore = createStubRepoStore(opts.baseDir, opts.writtenPrefixes);
  return {
    repoStore,
    signAsPrincipal: async (): Promise<SignedPayload> => ({
      sig: new Uint8Array(64),
      principalKind: "supervisor",
    }),
    mailBus: opts.mailBus,
    subprocessSpawner: opts.spawner,
    binaryPath: "/fake/bin/workflow-child",
    substrateEnv: {
      DATA_DIR: opts.baseDir,
      CLOSURE_PACKAGE_DIR: "/fake/closure/package",
    },
    dynamicSpawnEnv: () => ({}),
    workflowRunRepoId: { kind: "workflow-run", id: "run_deployment-x" },
    workflowRunRef: "refs/heads/main",
    anchorRunId: "run_deployment-x",
    stepCount: 1,
    deploymentMailAddress: "run_deployment-x@example.com",
    readPrincipal: { kind: "supervisor" },
    deriveStepAddress: ({ runId, stepId }) => `${runId}-${stepId}@example.com`,
    ipcKeyPairFactory: () => Promise.resolve(opts.ipcKeypair),
    inboxPrimitives: opts.inboxPrimitives,
    ...(opts.crashLoopMaxCount !== undefined
      ? { crashLoopMaxCount: opts.crashLoopMaxCount }
      : {}),
    ...(opts.crashLoopStableResetMs !== undefined
      ? { crashLoopStableResetMs: opts.crashLoopStableResetMs }
      : {}),
    ...(opts.setTimer !== undefined ? { setTimer: opts.setTimer } : {}),
    ...(opts.clearTimer !== undefined ? { clearTimer: opts.clearTimer } : {}),
  };
}

async function spawnSupervisor(opts: {
  baseDir: string;
  tracker: SpawnTracker;
  mailBus: MailBusBindings;
  ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
  inboxPrimitives: InboxPrimitives;
  crashLoopMaxCount?: number;
  crashLoopStableResetMs?: number;
  setTimer?: (cb: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  writtenPrefixes?: string[];
}) {
  await seedStepGrants(
    opts.baseDir,
    defaultStepRepoId({ runId: "run_deployment-x", stepId: "step-1" }),
    [{ resource: "thing", action: "read" }],
  );
  const bindings = await buildBindings({
    baseDir: opts.baseDir,
    spawner: opts.tracker.spawner,
    mailBus: opts.mailBus,
    ipcKeypair: opts.ipcKeypair,
    inboxPrimitives: opts.inboxPrimitives,
    ...(opts.crashLoopMaxCount !== undefined
      ? { crashLoopMaxCount: opts.crashLoopMaxCount }
      : {}),
    ...(opts.crashLoopStableResetMs !== undefined
      ? { crashLoopStableResetMs: opts.crashLoopStableResetMs }
      : {}),
    ...(opts.setTimer !== undefined ? { setTimer: opts.setTimer } : {}),
    ...(opts.clearTimer !== undefined ? { clearTimer: opts.clearTimer } : {}),
    ...(opts.writtenPrefixes !== undefined
      ? { writtenPrefixes: opts.writtenPrefixes }
      : {}),
  });
  const supervisor = createWorkflowSupervisor(bindings);
  const spawnPromise = supervisor.spawn({
    stepOrder: ["step-1"],
    definitionHash: "def-hash-abc",
    warmKeep: false,
    onInferenceEvent: () => undefined,
  });
  while (opts.tracker.children.length === 0) {
    await new Promise((r) => setTimeout(r, 1));
  }
  const first = opts.tracker.children[0];
  if (first === undefined) throw new Error("tracker.children[0] missing");
  await driveReady(first, opts.ipcKeypair);
  await spawnPromise;
  return { supervisor };
}

// A deterministic timer registry used to drive the crash-loop stable-run
// reset without wall-clock waits. `setTimer`/`clearTimer` back the
// supervisor's injectable timer seam; `fireByDelay` invokes every armed,
// uncleared timer scheduled at exactly `ms`. The spawn ready-handshake
// deadline is armed on the same seam but cleared on `ready`, so firing by
// the distinctive stable-reset delay never disturbs it.
function createFakeTimers() {
  type FakeTimer = {
    id: number;
    cb: () => void;
    delayMs: number;
    cleared: boolean;
  };
  const timers: FakeTimer[] = [];
  let nextId = 1;
  return {
    setTimer: (cb: () => void, ms: number): unknown => {
      const timer: FakeTimer = { id: nextId, cb, delayMs: ms, cleared: false };
      nextId += 1;
      timers.push(timer);
      return timer.id;
    },
    clearTimer: (handle: unknown): void => {
      const timer = timers.find((t) => t.id === handle);
      if (timer !== undefined) timer.cleared = true;
    },
    fireByDelay: (ms: number): number => {
      let fired = 0;
      for (const timer of timers) {
        if (!timer.cleared && timer.delayMs === ms) {
          timer.cleared = true;
          timer.cb();
          fired += 1;
        }
      }
      return fired;
    },
  };
}

/** Wait until the tracker has spawned at least `n` children, or throw. */
async function waitForChildren(
  tracker: SpawnTracker,
  n: number,
  timeoutMs = 2_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (tracker.children.length < n) {
    if (Date.now() > deadline) {
      throw new Error(
        `timed out waiting for ${String(n)} children (have ${String(tracker.children.length)})`,
      );
    }
    await new Promise((r) => setTimeout(r, 1));
  }
}

describe("supervisor crash-respawn: unexpected exit", () => {
  test("an unexpected child exit respawns the child and replays stranded mail", async () => {
    const baseDir = await makeTempDir("crash-respawn-happy-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
    });

    const firstChild = tracker.children[0];
    if (firstChild === undefined) throw new Error("first child missing");

    // The child dies unexpectedly (no shutdown, no recycle in flight).
    firstChild.crash();

    // The supervisor respawns without any external call; drive the new
    // child's ready so the respawn completes.
    await waitForChildren(tracker, 2);
    const secondChild = tracker.children[1];
    if (secondChild === undefined) throw new Error("second child missing");
    await driveReady(secondChild, ipcKeypair);

    // Let the respawn settle.
    const deadline = Date.now() + 2_000;
    while (inbox.replayCalls() < 1 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 1));
    }

    // The respawn ran the mail-recovery replay before resuming dispatch.
    expect(inbox.replayCalls()).toBeGreaterThanOrEqual(1);
    // Exactly one respawn happened.
    expect(tracker.totalSpawns).toBe(2);
    // The fresh child runs under a new channelId.
    expect(secondChild.channelId).not.toBe(firstChild.channelId);
    // The mail-bus registration is held across the respawn (never
    // unregistered then re-registered).
    expect(mailBus.registered()).toContain("run_deployment-x@example.com");

    await supervisor.shutdown();
  });

  test("a planned shutdown does not respawn the child", async () => {
    const baseDir = await makeTempDir("crash-respawn-shutdown-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
    });

    // Shutdown kills the child; its `exited` resolves, but the exit
    // observed in a non-running phase must NOT drive a respawn.
    await supervisor.shutdown();
    // Give any errant respawn a chance to spawn a second child.
    await new Promise((r) => setTimeout(r, 20));
    expect(tracker.totalSpawns).toBe(1);
  });

  test("a recycle does not trigger a spurious crash-respawn", async () => {
    const baseDir = await makeTempDir("crash-respawn-recycle-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
    });

    // Recycle deliberately kills the old child (resolving its `exited`)
    // and stands up a new one. The old child's exit must be classified as
    // planned -- generation-stale plus observed during `recycling` -- so it
    // does not spawn a THIRD child on top of the recycle's respawn.
    const recyclePromise = supervisor.recycle({ reason: "planned" });
    await waitForChildren(tracker, 2);
    const secondChild = tracker.children[1];
    if (secondChild === undefined) throw new Error("second child missing");
    await driveReady(secondChild, ipcKeypair);
    await recyclePromise;

    // Give any spurious crash-respawn a chance to fire.
    await new Promise((r) => setTimeout(r, 20));
    expect(tracker.totalSpawns).toBe(2);

    await supervisor.shutdown();
  });
});

describe("supervisor crash-respawn: crash-loop guard", () => {
  test("N unexpected exits within the window latch the deployment", async () => {
    const baseDir = await makeTempDir("crash-loop-latch-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const writtenPrefixes: string[] = [];
    // Latch on the 2nd unexpected exit. The default stable-reset window
    // (60s) never fires within this test, so the counter does not reset.
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      crashLoopMaxCount: 2,
      writtenPrefixes,
    });

    // Crash 1: under the threshold -> respawn (child 2).
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    first.crash();
    await waitForChildren(tracker, 2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);

    // Crash 2: reaches the threshold -> latch, no further respawn. Wait for
    // the latch's post-teardown RunFailed commit to land.
    second.crash();
    const deadline = Date.now() + 2_000;
    while (
      !writtenPrefixes.includes("runs/run_deployment-x/events/") &&
      Date.now() < deadline
    ) {
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(tracker.totalSpawns).toBe(2);

    // The latch committed a RunFailed tombstone to the deployment's stable
    // run so its external status flips to `failed`.
    expect(writtenPrefixes).toContain("runs/run_deployment-x/events/");

    // The deployment latched to the terminal `crash-looping` state
    // specifically (not a clean `stopped`): recycle is rejected naming it.
    let caught: unknown;
    try {
      await supervisor.recycle({ reason: "after-latch" });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(caught instanceof Error && caught.message).toMatch(
      /in phase crash-looping/,
    );
  });

  test("a stable run resets the crash counter so a later crash does not latch", async () => {
    const baseDir = await makeTempDir("crash-loop-reset-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const timers = createFakeTimers();
    // Latch on the 2nd exit, with a distinctive stable-reset delay driven
    // deterministically through the injectable timer seam. Firing that
    // timer (not a wall-clock wait) is what clears the crash counter.
    const stableResetMs = 500_000;
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      crashLoopMaxCount: 2,
      crashLoopStableResetMs: stableResetMs,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    // Crash 1 -> respawn (child 2). The respawn arms the stable-run reset
    // timer against child 2's generation.
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    first.crash();
    await waitForChildren(tracker, 2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);

    // Fire the stable-run reset timer: child 2 has run "stably", so the
    // crash counter clears. Exactly one such timer is armed.
    const deadline = Date.now() + 2_000;
    while (timers.fireByDelay(stableResetMs) === 0) {
      if (Date.now() > deadline) {
        throw new Error("stable-run reset timer was never armed");
      }
      await new Promise((r) => setTimeout(r, 1));
    }

    // Crash 2: with the counter reset by the stable run, this is again
    // under the threshold, so it respawns (child 3) rather than latching.
    second.crash();
    await waitForChildren(tracker, 3);
    const third = tracker.children[2];
    if (third === undefined) throw new Error("third child missing");
    await driveReady(third, ipcKeypair);
    expect(tracker.totalSpawns).toBe(3);

    await supervisor.shutdown();
  });
});
