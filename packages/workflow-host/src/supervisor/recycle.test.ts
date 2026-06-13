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

import { generateKeyPair } from "@intx/crypto-node";
import type { RepoId, RepoStore } from "@intx/hub-sessions";

import {
  createWorkflowSupervisor,
  type MailBusBindings,
  type SignedPayload,
  type SubprocessHandle,
  type SubprocessSpawner,
  type TerminalEventSource,
  type TerminalRunEvent,
  type WorkflowSupervisorBindings,
} from "./index";
import { createRecyclePolicy, type RecyclePolicyBounds } from "./recycle";
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

async function buildBindings(opts: {
  baseDir: string;
  spawner: SubprocessSpawner;
  mailBus: MailBusBindings;
  ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
  recyclePolicy?: RecyclePolicyBounds;
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

    await driveReady(secondChild, ipcKeypair);
    const attempt = await recyclePromise;
    expect(attempt.origin).toBe("operator");

    // After ready, the supervisor drains the buffer into the new
    // child as `trigger.fire` frames. There should be at least two
    // outbound NDJSON lines to the new child (one per buffered
    // message).
    const flushed = secondChild.supervisorToChild.flushed();
    expect(flushed.length).toBeGreaterThanOrEqual(2);

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

describe("supervisor recycle: terminal-event watcher cohort", () => {
  test("the previous cohort's watcher iterators are finalised on installNewChild", async () => {
    const baseDir = await makeTempDir("recycle-cohort-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});

    type ProducedIterator = {
      returnCalled: boolean;
    };
    const produced: ProducedIterator[] = [];
    const terminalEventSource: TerminalEventSource = () => ({
      [Symbol.asyncIterator](): AsyncIterator<TerminalRunEvent> {
        const iterTracker: ProducedIterator = { returnCalled: false };
        produced.push(iterTracker);
        return {
          next() {
            return new Promise<IteratorResult<TerminalRunEvent>>(
              () => undefined,
            );
          },
          return() {
            iterTracker.returnCalled = true;
            return Promise.resolve({ value: undefined, done: true } as const);
          },
        };
      },
    });

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
      terminalEventSource,
    };
    const supervisor = createWorkflowSupervisor(bindings);
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
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
    // supervisor arms an accumulator -- which mints a terminal-event
    // watcher under the active cohort controller.
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("cohort-pre-recycle"),
    );
    await new Promise((r) => setTimeout(r, 5));
    await supervisor.drain({ deadlineMs: 5_000 });
    expect(produced.length).toBeGreaterThanOrEqual(1);
    const preRecycleCount = produced.length;

    // Recycle. The installNewChild path aborts the prior cohort and
    // stops every armed accumulator, which fires `return()` on each
    // watcher iterator.
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

    // Every iterator minted before the recycle had its `return()`
    // invoked.
    for (let i = 0; i < preRecycleCount; i += 1) {
      const entry = produced[i];
      if (entry === undefined) throw new Error("missing produced entry");
      expect(entry.returnCalled).toBe(true);
    }

    await supervisor.shutdown();
  });
});
