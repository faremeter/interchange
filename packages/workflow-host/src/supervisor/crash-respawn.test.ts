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

import {
  describe,
  test,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
} from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import type { RepoId, RepoStore } from "@intx/hub-sessions";
import {
  createMemoryFrameStream,
  createMemoryNdjsonStream,
  createMockMailBus,
  createChangeNotifier,
  createLogCapture,
} from "@intx/workflow-host/testing";

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
import { createControlChannelSender } from "../ipc/index";

async function makeTempDir(prefix: string): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

type WriteRecorder = {
  /** Record one write's preserve-prefix and report the change. */
  record(preservePrefix: string): void;
  /** The preserve-prefixes recorded so far, in write order. */
  prefixes(): readonly string[];
  /** Resolve once `predicate` holds over the recorded prefixes. */
  until(predicate: () => boolean): Promise<void>;
};

// Pairs the recorded writes with the notifier that reports them, so a test can
// wait for the commit it expects instead of re-reading the array on a timer.
//
// The notifier is created per recorder rather than once per module: a test
// creates its own recorder and threads it to the one store it cares about, so
// no other store in the file can wake its waiter. A module-level notifier is
// the shape `supervisor-reaper.ts` rejects for the same reason -- the unit pass
// gives a worker one module registry for every file it runs, so module state
// here is shared across tests and across files.
function createWriteRecorder(): WriteRecorder {
  const prefixes: string[] = [];
  const changes = createChangeNotifier();
  return {
    record(preservePrefix: string) {
      prefixes.push(preservePrefix);
      changes.notify();
    },
    prefixes: () => prefixes.slice(),
    until: changes.until,
  };
}

function createStubRepoStore(
  baseDir: string,
  writeRecorder?: WriteRecorder,
): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(_principal, _repoId, _ref, args) {
      writeRecorder?.record(args.preservePrefix);
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
  /** Resolve once the spawner has produced at least `count` children. */
  awaitChildren(count: number): Promise<void>;
  get totalSpawns(): number;
};

function createSpawnTracker(): SpawnTracker {
  const children: FakeChild[] = [];
  const spawnChanges = createChangeNotifier();
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
    spawnChanges.notify();
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
    awaitChildren: (count: number) =>
      spawnChanges.until(() => children.length >= count),
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
  /** Resolve once `replayProcessingToInbox` has been called `count` times. */
  awaitReplayCalls(count: number): Promise<void>;
} {
  let replayCalls = 0;
  // Reports each replay so a test can wait for it rather than re-reading the
  // counter on a timer.
  const replayChanges = createChangeNotifier();
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
      replayChanges.notify();
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
    awaitReplayCalls(count: number) {
      return replayChanges.until(() => replayCalls >= count);
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
  respawnBackoffInitialMs?: number;
  respawnBackoffMaxMs?: number;
  setTimer?: (cb: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  writeRecorder?: WriteRecorder;
  onSelfTerminate?: (info: {
    phase: "stopped" | "crash-looping";
    reason: string;
  }) => void;
}): Promise<WorkflowSupervisorBindings> {
  const repoStore = createStubRepoStore(opts.baseDir, opts.writeRecorder);
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
    ...(opts.respawnBackoffInitialMs !== undefined
      ? { respawnBackoffInitialMs: opts.respawnBackoffInitialMs }
      : {}),
    ...(opts.respawnBackoffMaxMs !== undefined
      ? { respawnBackoffMaxMs: opts.respawnBackoffMaxMs }
      : {}),
    ...(opts.setTimer !== undefined ? { setTimer: opts.setTimer } : {}),
    ...(opts.clearTimer !== undefined ? { clearTimer: opts.clearTimer } : {}),
    ...(opts.onSelfTerminate !== undefined
      ? { onSelfTerminate: opts.onSelfTerminate }
      : {}),
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
  respawnBackoffInitialMs?: number;
  respawnBackoffMaxMs?: number;
  setTimer?: (cb: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  writeRecorder?: WriteRecorder;
  onSelfTerminate?: (info: {
    phase: "stopped" | "crash-looping";
    reason: string;
  }) => void;
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
    ...(opts.respawnBackoffInitialMs !== undefined
      ? { respawnBackoffInitialMs: opts.respawnBackoffInitialMs }
      : {}),
    ...(opts.respawnBackoffMaxMs !== undefined
      ? { respawnBackoffMaxMs: opts.respawnBackoffMaxMs }
      : {}),
    ...(opts.setTimer !== undefined ? { setTimer: opts.setTimer } : {}),
    ...(opts.clearTimer !== undefined ? { clearTimer: opts.clearTimer } : {}),
    ...(opts.writeRecorder !== undefined
      ? { writeRecorder: opts.writeRecorder }
      : {}),
    ...(opts.onSelfTerminate !== undefined
      ? { onSelfTerminate: opts.onSelfTerminate }
      : {}),
  });
  const supervisor = createWorkflowSupervisor(bindings);
  const spawnPromise = supervisor.spawn({
    stepOrder: ["step-1"],
    definitionHash: "def-hash-abc",
    warmKeep: false,
    onInferenceEvent: () => undefined,
  });
  await opts.tracker.awaitChildren(1);
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
  // Reports each arm and clear. The supervisor arms its backoff from a
  // coroutine the test does not await, so "is the timer armed yet" had been
  // answered by re-reading this array on a real timer -- a wall-clock wait
  // to observe a fake clock.
  const changes = createChangeNotifier();
  return {
    setTimer: (cb: () => void, ms: number): unknown => {
      const timer: FakeTimer = { id: nextId, cb, delayMs: ms, cleared: false };
      nextId += 1;
      timers.push(timer);
      changes.notify();
      return timer.id;
    },
    clearTimer: (handle: unknown): void => {
      const timer = timers.find((t) => t.id === handle);
      if (timer !== undefined) timer.cleared = true;
      changes.notify();
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
    // Delays of every currently-armed (uncleared) timer, in arm order.
    pendingDelays: (): number[] =>
      timers.filter((t) => !t.cleared).map((t) => t.delayMs),
    /** Resolve once a timer for `ms` is armed. */
    awaitArmed: (ms: number): Promise<void> =>
      changes.until(() => timers.some((t) => !t.cleared && t.delayMs === ms)),
    /** Resolve once at least `count` timers for `ms` are armed. */
    awaitArmedCount: (ms: number, count: number): Promise<void> =>
      changes.until(
        () =>
          timers.filter((t) => !t.cleared && t.delayMs === ms).length >= count,
      ),
  };
}

/** Wait until the tracker has spawned at least `n` children. */
async function waitForChildren(
  tracker: SpawnTracker,
  n: number,
): Promise<void> {
  await tracker.awaitChildren(n);
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
      // Near-zero backoff: this test uses real timers and asserts the
      // respawn happens, not its timing.
      respawnBackoffInitialMs: 1,
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

    // The replay is what "settled" meant: the respawned child's spawn path
    // replays processing entries back to the inbox, so the call is the event.
    await inbox.awaitReplayCalls(1);

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
    const selfTerminations: {
      phase: "stopped" | "crash-looping";
      reason: string;
    }[] = [];
    // The fake clock is what makes the negative checkable. A respawn cannot
    // begin without arming its backoff, and with the real timer that arming
    // is invisible -- leaving the assertion below to notice a spawn that,
    // being seconds out, was never going to have happened yet either way.
    const timers = createFakeTimers();
    const backoffMs = 40;
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      respawnBackoffInitialMs: backoffMs,
      respawnBackoffMaxMs: backoffMs,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
      onSelfTerminate: (info) => selfTerminations.push(info),
    });

    // Shutdown kills the child; its `exited` resolves, but the exit
    // observed in a non-running phase must NOT drive a respawn.
    await supervisor.shutdown();
    // No pause: the awaited shutdown is what licenses the negative. Its
    // teardown observes the child's exit and reaches a terminal phase before
    // resolving, so an exit-driven respawn would already have armed its
    // backoff. The armed timer is the observable half -- the spawn itself
    // would not follow until the backoff fired, which under this clock only
    // happens when the test says so.
    expect(timers.pendingDelays()).not.toContain(backoffMs);
    expect(tracker.totalSpawns).toBe(1);

    // A host-requested `shutdown()` is not a self-termination: the host
    // already knows the deployment is down, so the sink must stay silent.
    // Firing here would make the sidecar reclaim an address the host is
    // deliberately tearing down.
    expect(selfTerminations).toHaveLength(0);
  });

  test("a recycle does not trigger a spurious crash-respawn", async () => {
    const baseDir = await makeTempDir("crash-respawn-recycle-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    // As in the planned-shutdown test: the arming of a backoff is the first
    // observable step of a respawn, and only a controllable clock exposes it.
    const timers = createFakeTimers();
    const backoffMs = 40;
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      respawnBackoffInitialMs: backoffMs,
      respawnBackoffMaxMs: backoffMs,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
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

    // The awaited recycle is the boundary: it installs the replacement child
    // and returns, so a spurious crash-respawn would already have armed its
    // backoff. That armed timer is what this looks for; the interval it
    // replaces only widened a window in which nothing observable happened.
    expect(timers.pendingDelays()).not.toContain(backoffMs);
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
    const writeRecorder = createWriteRecorder();
    const selfTerminations: {
      phase: "stopped" | "crash-looping";
      reason: string;
    }[] = [];
    const latched = Promise.withResolvers<undefined>();
    // Latch on the 2nd unexpected exit. The default stable-reset window
    // (60s) never fires within this test, so the counter does not reset.
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      crashLoopMaxCount: 2,
      // Near-zero backoff: real timers, asserting the latch not its timing.
      respawnBackoffInitialMs: 1,
      writeRecorder,
      onSelfTerminate: (info) => {
        selfTerminations.push(info);
        latched.resolve(undefined);
      },
    });

    // Crash 1: under the threshold -> respawn (child 2).
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    first.crash();
    await waitForChildren(tracker, 2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);

    // Crash 2: reaches the threshold -> latch, no further respawn. The latch
    // reports its self-termination last, after its RunFailed commit landed.
    second.crash();
    await latched.promise;
    expect(tracker.totalSpawns).toBe(2);

    // The latch committed a RunFailed tombstone to the deployment's stable
    // run so its external status flips to `failed`.
    expect(writeRecorder.prefixes()).toContain("runs/run_deployment-x/events/");

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

    // The latch is a self-termination, so the host-facing sink fired exactly
    // once with the terminal `crash-looping` phase. This is the signal the
    // sidecar subscribes to so it reclaims the deployment address.
    expect(selfTerminations).toHaveLength(1);
    expect(selfTerminations[0]?.phase).toBe("crash-looping");
    expect(selfTerminations[0]?.reason).toMatch(/crash-loop/);
  });

  test("the latch commits its RunFailed tombstone before it reports the self-termination", async () => {
    const baseDir = await makeTempDir("crash-loop-tombstone-order-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const writeRecorder = createWriteRecorder();
    // The sidecar drops the deployment from its registry in this sink, and a
    // forced stop that then finds no supervisor reports the refs as final.
    const tombstoneAtSelfTermination = Promise.withResolvers<boolean>();
    await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      crashLoopMaxCount: 1,
      writeRecorder,
      onSelfTerminate: () => {
        tombstoneAtSelfTermination.resolve(
          writeRecorder.prefixes().includes("runs/run_deployment-x/events/"),
        );
      },
    });

    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    first.crash();

    expect(await tombstoneAtSelfTermination.promise).toBe(true);
  });

  test("a stable run resets the crash counter so a later crash does not latch", async () => {
    const baseDir = await makeTempDir("crash-loop-reset-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const timers = createFakeTimers();
    // Latch on the 2nd exit, with distinctive backoff and stable-reset
    // delays driven deterministically through the injectable timer seam.
    // The backoff is pinned (initial == max) so each respawn's wait fires
    // at the same delay.
    const backoffMs = 100_000;
    const stableResetMs = 500_000;
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      crashLoopMaxCount: 2,
      crashLoopStableResetMs: stableResetMs,
      respawnBackoffInitialMs: backoffMs,
      respawnBackoffMaxMs: backoffMs,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    // Fire the armed backoff wait (poll until it is armed), then await the
    // respawned child.
    const fireBackoffAndAwaitChild = async (
      nextCount: number,
    ): Promise<void> => {
      await timers.awaitArmed(backoffMs);
      timers.fireByDelay(backoffMs);
      await waitForChildren(tracker, nextCount);
    };

    // Crash 1 -> backoff -> respawn (child 2). The respawn arms the
    // stable-run reset timer against child 2's generation.
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    first.crash();
    await fireBackoffAndAwaitChild(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);

    // Fire the stable-run reset timer: child 2 has run "stably", so the
    // crash counter clears. Exactly one such timer is armed.
    await timers.awaitArmed(stableResetMs);
    timers.fireByDelay(stableResetMs);

    // Crash 2: with the counter reset by the stable run, this is again
    // under the threshold, so it respawns (child 3) rather than latching.
    second.crash();
    await fireBackoffAndAwaitChild(3);
    const third = tracker.children[2];
    if (third === undefined) throw new Error("third child missing");
    await driveReady(third, ipcKeypair);
    expect(tracker.totalSpawns).toBe(3);

    await supervisor.shutdown();
  });
});

/**
 * Crash `child`, fire the armed backoff timer at exactly `backoffMs` (awaiting
 * the fake-timer double's arm report), await the respawned child at
 * `nextCount`, and drive its ready. The EXACT expected delay is the assertion:
 * a backoff armed at any other value never satisfies `awaitArmed`, so the
 * caller hangs into the lane timeout instead of passing.
 */
async function crashFireBackoffAndReady(opts: {
  timers: ReturnType<typeof createFakeTimers>;
  tracker: SpawnTracker;
  ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
  child: FakeChild;
  backoffMs: number;
  nextCount: number;
  stableResetMs: number;
}): Promise<void> {
  opts.child.crash();
  await opts.timers.awaitArmed(opts.backoffMs);
  if (opts.timers.fireByDelay(opts.backoffMs) === 0) {
    throw new Error(
      `respawn backoff of ${String(opts.backoffMs)}ms was cleared between being armed and being fired`,
    );
  }
  await waitForChildren(opts.tracker, opts.nextCount);
  const next = opts.tracker.children[opts.nextCount - 1];
  if (next === undefined) throw new Error("respawned child missing");
  await driveReady(next, opts.ipcKeypair);
  // Wait until the respawn fully settles: `handleUnexpectedChildExit` arms
  // the stable-run reset timer as its final step, after the async ready
  // handshake completes. Returning before that would let a subsequent
  // crash's entry-clear race the not-yet-armed timer.
  await opts.timers.awaitArmed(opts.stableResetMs);
}

// Capture LogTape records file-wide, so a test can await a record the
// supervisor emits for a decision that leaves no other trace.
// `configureSync` is process-global, so the prior configuration is saved and
// restored around this file.
const logs = createLogCapture();

beforeAll(() => {
  logs.install();
});

afterAll(() => {
  logs.restore();
});

beforeEach(() => {
  logs.reset();
});

describe("supervisor crash-respawn: exponential backoff", () => {
  test("the respawn backoff doubles per crash and caps at the maximum", async () => {
    const baseDir = await makeTempDir("crash-backoff-double-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const timers = createFakeTimers();
    // A high crash threshold so the guard never latches; the backoff
    // sequence 1000 -> 2000 -> 4000 -> 4000 (capped) is what we pin. A
    // distinctive stable-reset delay lets the helper detect when each
    // respawn has fully settled.
    const stableResetMs = 777_000;
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      crashLoopMaxCount: 100,
      crashLoopStableResetMs: stableResetMs,
      respawnBackoffInitialMs: 1_000,
      respawnBackoffMaxMs: 4_000,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");

    // Each call fires the EXACT expected backoff, proving the doubling and
    // the cap: 1s, then 2s, then 4s, then 4s again (capped, not 8s).
    await crashFireBackoffAndReady({
      timers,
      tracker,
      ipcKeypair,
      child: first,
      backoffMs: 1_000,
      nextCount: 2,
      stableResetMs,
    });
    await crashFireBackoffAndReady({
      timers,
      tracker,
      ipcKeypair,
      child: tracker.children[1] ?? first,
      backoffMs: 2_000,
      nextCount: 3,
      stableResetMs,
    });
    await crashFireBackoffAndReady({
      timers,
      tracker,
      ipcKeypair,
      child: tracker.children[2] ?? first,
      backoffMs: 4_000,
      nextCount: 4,
      stableResetMs,
    });
    await crashFireBackoffAndReady({
      timers,
      tracker,
      ipcKeypair,
      child: tracker.children[3] ?? first,
      backoffMs: 4_000,
      nextCount: 5,
      stableResetMs,
    });
    expect(tracker.totalSpawns).toBe(5);

    await supervisor.shutdown();
  });

  test("a recycle during the backoff wait does not cause a spurious respawn", async () => {
    const baseDir = await makeTempDir("crash-backoff-recycle-race-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const timers = createFakeTimers();
    // A large backoff that stays parked so a recycle can race it.
    const backoffMs = 100_000;
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      crashLoopMaxCount: 100,
      respawnBackoffInitialMs: backoffMs,
      respawnBackoffMaxMs: backoffMs,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    // Crash 1: the crash-respawn coroutine parks on the (unfired) backoff.
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    first.crash();
    // Wait until the backoff is armed (the coroutine reached the wait).
    await timers.awaitArmed(backoffMs);

    // An operator recycle runs WHILE the backoff is parked. It installs a
    // fresh cohort (child 2), advancing the generation.
    const recyclePromise = supervisor.recycle({ reason: "race-the-backoff" });
    await waitForChildren(tracker, 2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recyclePromise;

    // Now fire the parked backoff. The crash coroutine wakes, sees the
    // generation has advanced past the cohort it was armed for, and bails
    // -- it must NOT respawn the healthy cohort the recycle just installed.
    timers.fireByDelay(backoffMs);
    // `fireByDelay` is synchronous: it runs the timer callback and returns,
    // which only resumes the coroutine. Nothing it goes on to decide has
    // happened yet, so wait for the decision itself. The bail is announced
    // in the same synchronous step that takes it -- the guard either logs
    // this and returns, or falls through into `runRespawn` with no await in
    // between -- which makes the record the decision rather than a report
    // that trails it.
    await logs.waitForRecord(
      "respawn backoff elapsed but the crashed cohort is no longer the running one",
    );
    // The recycle's cohort is the second and final spawn; a respawn of the
    // dead cohort would be a third child.
    expect(tracker.totalSpawns).toBe(2);

    await supervisor.shutdown();
  });

  test("a crash disarms the prior cohort's stable-run reset timer so it cannot clear the counter mid-backoff", async () => {
    const baseDir = await makeTempDir("crash-backoff-stable-disarm-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const timers = createFakeTimers();
    const backoffMs = 100_000;
    const stableResetMs = 500_000;
    // `onSelfTerminate` is the crash-loop latch's own report. The supervisor
    // fires it after the terminal transition is committed, so it is the
    // signal that says the phase is now `crash-looping`.
    const selfTerminations: {
      phase: "stopped" | "crash-looping";
      reason: string;
    }[] = [];
    const latched = createChangeNotifier();
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      crashLoopMaxCount: 3,
      crashLoopStableResetMs: stableResetMs,
      respawnBackoffInitialMs: backoffMs,
      respawnBackoffMaxMs: backoffMs,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
      onSelfTerminate: (info) => {
        selfTerminations.push(info);
        latched.notify();
      },
    });

    // Crash 1 -> backoff -> respawn (child 2). The respawn arms child 2's
    // stable-run reset timer.
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await crashFireBackoffAndReady({
      timers,
      tracker,
      ipcKeypair,
      child: first,
      backoffMs,
      nextCount: 2,
      stableResetMs,
    });

    // Crash 2 BEFORE child 2's stable timer fires. The handler disarms that
    // timer at entry, so firing the stable delay now matches nothing -- the
    // crash that just happened must not be rewarded as a stable run.
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    second.crash();
    await timers.awaitArmed(backoffMs);
    expect(timers.fireByDelay(stableResetMs)).toBe(0);

    // Fire crash 2's backoff -> respawn (child 3). Crash 3 then reaches the
    // threshold and latches -- proving the counter was never wrongly
    // cleared (with the disarm bug, crashes 1-2 would have been reset).
    timers.fireByDelay(backoffMs);
    await waitForChildren(tracker, 3);
    const third = tracker.children[2];
    if (third === undefined) throw new Error("third child missing");
    await driveReady(third, ipcKeypair);

    third.crash();
    // Await the latch report, then probe `recycle` ONCE. The retry loop this
    // replaces was standing in for the missing signal: it re-probed until the
    // phase had flipped. The sink fires after `shutdownInternal` commits
    // `phase = "crash-looping"`, so by here the single probe cannot race it.
    await latched.until(() => selfTerminations.length >= 1);
    expect(selfTerminations[0]?.phase).toBe("crash-looping");
    let caught: unknown;
    try {
      await supervisor.recycle({ reason: "probe-latch" });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(caught instanceof Error && caught.message).toMatch(
      /in phase crash-looping/,
    );
    // No 4th child: crash 3 latched rather than respawning.
    expect(tracker.totalSpawns).toBe(3);
  });

  test("shutdown cancels every parked backoff wait, even overlapping ones", async () => {
    const baseDir = await makeTempDir("crash-backoff-multi-cancel-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInboxPrimitives();
    const timers = createFakeTimers();
    // A large pinned backoff so waits stay parked; a distinctive
    // stable-reset delay so it never collides with the backoff.
    const backoffMs = 100_000;
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      inboxPrimitives: inbox,
      crashLoopMaxCount: 100,
      crashLoopStableResetMs: 700_000,
      respawnBackoffInitialMs: backoffMs,
      respawnBackoffMaxMs: backoffMs,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    const pendingBackoffCount = (): number =>
      timers.pendingDelays().filter((d) => d === backoffMs).length;
    const awaitPendingBackoffs = async (n: number): Promise<void> => {
      await timers.awaitArmedCount(backoffMs, n);
    };

    // Crash 1: coroutine A parks on its backoff (never fired).
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    first.crash();
    await awaitPendingBackoffs(1);

    // A recycle installs a fresh, LIVE child (child 2) while A is parked.
    const recyclePromise = supervisor.recycle({ reason: "install-live" });
    await waitForChildren(tracker, 2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recyclePromise;

    // Crash the recycled child: coroutine B parks on ITS backoff. Now two
    // backoff waits are armed at once -- the case a single-slot tracker
    // would drop, leaking the earlier timer past shutdown.
    second.crash();
    await awaitPendingBackoffs(2);

    // Shutdown must cancel BOTH parked waits: no backoff timer survives.
    await supervisor.shutdown();
    expect(pendingBackoffCount()).toBe(0);
  });
});
