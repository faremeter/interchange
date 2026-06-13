import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto-node";
import type { RepoId, RepoStore } from "@intx/hub-sessions";

import {
  createWorkflowSupervisor,
  type DrainTimeoutAccumulator,
  type DrainTimeoutAccumulatorFactory,
  type DrainTimeoutOpts,
  type MailBusBindings,
  type SubprocessSpawner,
  type SubprocessHandle,
  type SignedPayload,
  type SupervisorRunEvent,
  type TerminalEventSource,
  type TerminalRunEvent,
  type WorkflowSupervisorBindings,
} from "./index";
import {
  assembleCredentialsSnapshot,
  defaultStepRepoId,
  hashGrants,
  STEP_GRANTS_PATH,
} from "./credentials";
import { commitCancelRequested } from "./cancel-signing";
import {
  createControlChannelSender,
  createEventChannelSender,
  receiveControlChannel,
  generateHmacKey,
  generateChannelId,
  hexDecode,
  type FrameReader,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";

const CancelRequestedBlob = type({
  type: "string",
  seq: "number",
  origin: "string",
  reason: "string",
  signature: {
    principalKind: "string",
    sig: "string",
  },
  "+": "ignore",
});

function readCancelRequestedBlob(
  raw: string,
): typeof CancelRequestedBlob.infer {
  const parsed: unknown = JSON.parse(raw);
  const validated = CancelRequestedBlob(parsed);
  if (validated instanceof type.errors) {
    throw new Error(`unexpected blob shape: ${validated.summary}`);
  }
  return validated;
}

const RunEventBlob = type({
  type: "string",
  seq: "number",
  runId: "string",
  at: "string",
  signature: {
    principalKind: "string",
    sig: "string",
  },
  "+": "ignore",
});

function readRunEventBlob(raw: string): typeof RunEventBlob.infer {
  const parsed: unknown = JSON.parse(raw);
  const validated = RunEventBlob(parsed);
  if (validated instanceof type.errors) {
    throw new Error(`unexpected run-event blob shape: ${validated.summary}`);
  }
  return validated;
}

async function makeTempDir(prefix: string): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  return dir;
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
    inject(bytes: Uint8Array) {
      buffer.push(bytes);
      wake();
    },
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

/**
 * Create a stub `RepoStore` that satisfies only the subset of the
 * interface the supervisor reaches into in this commit. The
 * supervisor calls `getRepoDir` (credentials assembly) and
 * `writeTreePreservingPrefix` (cancel signing). All other methods
 * throw so a test that accidentally triggers an untested code path
 * surfaces a precise failure.
 */
function createStubRepoStore(opts: {
  baseDir: string;
  onWrite?: (args: {
    principal: { kind: string };
    repoId: RepoId;
    ref: string;
    files: Record<string, string | Uint8Array>;
  }) => void;
  /**
   * When true, the stub carries committed files across
   * `writeTreePreservingPrefix` invocations keyed by (repoId.id, ref,
   * preservePrefix), so a sequence of appends sees the prior commits
   * in its merge callback's `existing` map. Off by default to keep
   * tests that assert per-call shape from racing across calls.
   */
  statefulWrites?: boolean;
}): RepoStore {
  const committed = new Map<string, Map<string, Uint8Array>>();
  function keyFor(repoId: RepoId, ref: string, preservePrefix: string): string {
    return `${repoId.kind}/${repoId.id}\x00${ref}\x00${preservePrefix}`;
  }
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(opts.baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(principal, repoId, ref, args) {
      const key = keyFor(repoId, ref, args.preservePrefix);
      const existing =
        opts.statefulWrites === true
          ? (committed.get(key) ?? new Map<string, Uint8Array>())
          : new Map<string, Uint8Array>();
      const files = await args.merge(existing);
      opts.onWrite?.({ principal, repoId, ref, files });
      if (opts.statefulWrites === true) {
        const next = new Map<string, Uint8Array>();
        for (const [path, bytes] of Object.entries(files)) {
          if (!path.startsWith(args.preservePrefix)) continue;
          next.set(
            path,
            typeof bytes === "string" ? new TextEncoder().encode(bytes) : bytes,
          );
        }
        committed.set(key, next);
      }
      return { commitSha: "deadbeefcafef00d" };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; only the subset the supervisor invokes is implemented and a missing method throws via the proxy below
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

async function buildBindings(opts: {
  baseDir: string;
  spawner: SubprocessSpawner;
  signSpy: (kind: string, payload: Uint8Array) => SignedPayload;
  mailBus: MailBusBindings;
  trivialLaunch?: WorkflowSupervisorBindings["trivialLaunch"];
  onWrite?: (args: {
    principal: { kind: string };
    repoId: RepoId;
    ref: string;
    files: Record<string, string | Uint8Array>;
  }) => void;
  statefulWrites?: boolean;
}): Promise<WorkflowSupervisorBindings> {
  const repoStore = createStubRepoStore({
    baseDir: opts.baseDir,
    ...(opts.onWrite !== undefined ? { onWrite: opts.onWrite } : {}),
    ...(opts.statefulWrites === true ? { statefulWrites: true } : {}),
  });
  return {
    repoStore,
    signAsPrincipal: (kind, payload) => opts.signSpy(kind, payload),
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
    trivialLaunch:
      opts.trivialLaunch ??
      (() => {
        throw new Error("trivialLaunch not provided to this test binding");
      }),
  };
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

describe("createWorkflowSupervisor", () => {
  test("factory accepts the documented WorkflowSupervisorBindings shape", async () => {
    const baseDir = await makeTempDir("supervisor-bindings-");
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner not invoked in this test");
      },
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus: createMockMailBus(),
    });
    const supervisor = createWorkflowSupervisor(bindings);
    expect(typeof supervisor.deploy).toBe("function");
    expect(typeof supervisor.spawn).toBe("function");
    expect(typeof supervisor.requestCancel).toBe("function");
    expect(typeof supervisor.shutdown).toBe("function");
    expect(typeof supervisor.drain).toBe("function");
    expect(typeof supervisor.recycle).toBe("function");
    expect(supervisor.getCredentialsSnapshot()).toBeNull();
  });

  test("spawn completes the IPC handshake, registers mail, and pushes credentials", async () => {
    const baseDir = await makeTempDir("supervisor-spawn-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    // Deterministic IPC keypairs so the test's "child" side can sign
    // a `ready` frame the supervisor accepts. Two keypairs ride per
    // spawn: the supervisor's (downstream signing) and the child's
    // (upstream signing). The supervisor never sees the child's
    // private key; the child publishes its public half in the
    // `ready` frame's payload.
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let killed = false;

    let observedEnv: Record<string, string> | undefined;
    let observedBinary: string | undefined;
    const spawner: SubprocessSpawner = ({ binaryPath, env }) => {
      observedBinary = binaryPath;
      observedEnv = env;
      const handle: SubprocessHandle = {
        pid: 4321,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          killed = true;
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
    };
    const supervisor = createWorkflowSupervisor(bindings);

    const eventsObserved: { type: string }[] = [];
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      onInferenceEvent: (event) => {
        eventsObserved.push({ type: event.type });
      },
    });
    // Drive the synthetic child side: wait until the spawner has
    // been invoked (so we have the channelId), then sign a `ready`
    // frame with the controlled IPC private key and inject it into
    // the child-to-supervisor stream.
    while (observedEnv === undefined) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
      privateKeySeed: childIpcKeyPair.privateKey,
      channelId,
      writer: {
        write(line: string) {
          childToSupervisor.inject(line);
        },
      },
    });
    // Wait for the supervisor to register the mail address on the
    // bus so the delivers below land inside the supervisor's
    // subscription handler -- a deliver before subscription is a
    // no-op against the mock bus.
    while (!mailBus.registered().includes("deployment-x@example.com")) {
      await new Promise((r) => setTimeout(r, 1));
    }
    // Deliver mail while the supervisor is still in `starting`; the
    // supervisor buffers it and replays it after `ready` lands.
    mailBus.deliver("deployment-x@example.com", new TextEncoder().encode("m1"));
    mailBus.deliver("deployment-x@example.com", new TextEncoder().encode("m2"));
    await childSender.send({
      type: "ready",
      data: {
        childPid: 4321,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });

    const result = await spawnPromise;
    expect(observedBinary).toBe("/fake/bin/workflow-child");
    expect(observedEnv).toMatchObject({
      DATA_DIR: baseDir,
      DEPLOYMENT_ID: "deployment-x",
      DEFINITION_HASH: "def-hash-abc",
      MAILBOX_ADDRESS: "deployment-x@example.com",
    });
    expect(observedEnv.IPC_CHANNEL_ID).toMatch(/^[0-9a-f]{32}$/);
    expect(observedEnv.IPC_HMAC_KEY).toMatch(/^[0-9a-f]{64}$/);
    expect(observedEnv.HOST_PUBKEY).toMatch(/^[0-9a-f]{64}$/);
    expect(observedEnv).not.toHaveProperty("HOST_PRIVATE_KEY");
    expect(result.pid).toBe(4321);
    expect(result.channelId).toBe(channelId);
    expect(result.credentialsSnapshot.steps).toHaveLength(1);
    expect(result.credentialsSnapshot.steps[0]?.address).toBe(
      "deployment-x-step-1@example.com",
    );
    expect(mailBus.registered()).toContain("deployment-x@example.com");
    expect(supervisor.getCredentialsSnapshot()).not.toBeNull();

    // The buffered mail was forwarded as `trigger.fire` frames into
    // the supervisor-to-child stream after `ready` landed.
    const forwarded = supervisorToChild.flushed();
    expect(forwarded.length).toBeGreaterThanOrEqual(2);

    await supervisor.shutdown();
    expect(killed).toBe(true);
    expect(mailBus.registered()).not.toContain("deployment-x@example.com");
  });

  test("drain() forwards the `drain` control frame and arms a drainTimeout accumulator per in-flight run", async () => {
    const baseDir = await makeTempDir("supervisor-drain-arm-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      const handle: SubprocessHandle = {
        pid: 9999,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    // Mock accumulator factory the supervisor's `drain()` should
    // consult. Each invocation records the opts and returns a
    // controllable stub whose `start`/`stop` calls are visible to the
    // test. The factory shape matches `createDrainTimeoutAccumulator`
    // exactly so the supervisor binds it through the public
    // `WorkflowSupervisorBindings.drainTimeoutAccumulatorFactory`
    // slot.
    type StubAccumulator = DrainTimeoutAccumulator & {
      __opts: DrainTimeoutOpts;
      __startCount: number;
      __stopCount: number;
    };
    const stubs: StubAccumulator[] = [];
    const factory: DrainTimeoutAccumulatorFactory = (opts) => {
      const stub: StubAccumulator = {
        __opts: opts,
        __startCount: 0,
        __stopCount: 0,
        start() {
          this.__startCount += 1;
        },
        pause() {
          /* unused by the supervisor's arming path */
        },
        resume() {
          /* unused by the supervisor's arming path */
        },
        stop() {
          this.__stopCount += 1;
        },
        accumulatedMs() {
          return 0;
        },
        get escalated() {
          return false;
        },
        disposed() {
          return Promise.resolve();
        },
      };
      stubs.push(stub);
      return stub;
    };

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      drainTimeoutAccumulatorFactory: factory,
      drainTimeoutMs: 7_500,
    };
    const supervisor = createWorkflowSupervisor(bindings);

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      onInferenceEvent: () => {
        /* unused in this test */
      },
    });
    while (observedEnv === undefined) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
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
    // Two pre-ready messages -> two in-flight runIds (the supervisor
    // mints one runId per `trigger.fire` per discovery Q3.1).
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("drain-msg-A"),
    );
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("drain-msg-B"),
    );
    await childSender.send({
      type: "ready",
      data: {
        childPid: 9999,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });
    await spawnPromise;

    // No accumulators armed yet -- drain has not been called.
    expect(stubs).toHaveLength(0);

    await supervisor.drain({ deadlineMs: 7_500 });

    // The supervisor's `drain` control frame landed on the
    // supervisor-to-child stream. Two frames preceded it
    // (`trigger.fire` per buffered message); the last frame is the
    // drain payload.
    const forwarded = supervisorToChild.flushed();
    expect(forwarded.length).toBeGreaterThanOrEqual(3);
    const drainFrameRaw = forwarded[forwarded.length - 1];
    if (drainFrameRaw === undefined) {
      throw new Error("no frames observed on supervisor-to-child stream");
    }
    const SignedFrame = type({
      envelope: {
        seq: "number",
        channelId: "string",
        payload: {
          type: "string",
          "+": "ignore",
        },
        "+": "ignore",
      },
      "+": "ignore",
    });
    const drainFrame = SignedFrame(JSON.parse(drainFrameRaw));
    if (drainFrame instanceof type.errors) {
      throw new Error(
        `drain frame failed envelope shape check: ${drainFrame.summary}`,
      );
    }
    expect(drainFrame.envelope.payload).toMatchObject({
      type: "drain",
      data: { deadlineMs: 7_500 },
    });

    // One accumulator armed per in-flight run. Each accumulator was
    // started (the supervisor calls `start()` directly after the
    // factory call); none has been stopped yet.
    expect(stubs).toHaveLength(2);
    for (const stub of stubs) {
      expect(stub.__startCount).toBe(1);
      expect(stub.__stopCount).toBe(0);
      expect(stub.__opts.deploymentId).toBe("deployment-x");
      expect(stub.__opts.repoId).toEqual({
        kind: "workflow-run",
        id: "deployment-x",
      });
      expect(stub.__opts.ref).toBe("refs/heads/main");
      expect(stub.__opts.drainTimeoutMs).toBe(7_500);
      expect(typeof stub.__opts.runId).toBe("string");
      expect(stub.__opts.runId.length).toBeGreaterThan(0);
    }
    const runIds = stubs.map((s) => s.__opts.runId);
    expect(new Set(runIds).size).toBe(2);

    // Shutdown stops every armed accumulator before tearing the
    // child down.
    await supervisor.shutdown();
    for (const stub of stubs) {
      expect(stub.__stopCount).toBe(1);
    }
  });

  test("drain() escalates via signAsPrincipal when the accumulator's timeout fires", async () => {
    // Production-shaped wiring: bind the real
    // `createDrainTimeoutAccumulator` and observe the
    // `CancelRequested{origin: "supervisor-drain"}` commit landing on
    // the stub RepoStore's write side after the supervisor's fake
    // clock advances past the configured `drainTimeoutMs`. This is
    // the supervisor-equivalent of the in-process round-trip the
    // 13c integration test exercises.
    const baseDir = await makeTempDir("supervisor-drain-escalate-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      const handle: SubprocessHandle = {
        pid: 8888,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
      return handle;
    };

    type FakeTimer = { cb: () => void; ms: number; cancelled: boolean };
    const timers = new Set<FakeTimer>();
    let fakeNow = 1_700_000_000_000;
    const observedWrites: {
      principal: { kind: string };
      repoId: RepoId;
      ref: string;
      files: Record<string, string | Uint8Array>;
    }[] = [];

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
      onWrite: (args) => observedWrites.push(args),
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      drainTimeoutMs: 1_000,
      now: () => fakeNow,
      setTimer: (cb, ms) => {
        const t: FakeTimer = { cb, ms, cancelled: false };
        timers.add(t);
        return t;
      },
      clearTimer: (handle) => {
        if (handle === null || typeof handle !== "object") return;
        for (const t of timers) {
          if (t === handle) {
            t.cancelled = true;
            timers.delete(t);
            return;
          }
        }
      },
    };
    const supervisor = createWorkflowSupervisor(bindings);

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      onInferenceEvent: () => {
        /* unused in this test */
      },
    });
    while (observedEnv === undefined) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
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
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("escalate-msg"),
    );
    await childSender.send({
      type: "ready",
      data: {
        childPid: 8888,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });
    await spawnPromise;

    await supervisor.drain({ deadlineMs: 1_000 });
    expect(timers.size).toBe(1);
    // Advance the fake clock past the timeout and fire the
    // accumulator's pending timer.
    fakeNow += 1_000;
    const due = [...timers];
    for (const t of due) {
      if (t.cancelled) continue;
      timers.delete(t);
      t.cb();
    }
    // Allow the async escalate commit to settle.
    await new Promise<void>((r) => setTimeout(r, 5));

    // The accumulator's escalation committed a CancelRequested event
    // through the supervisor's substrate handle.
    expect(observedWrites.length).toBe(1);
    const write = observedWrites[0];
    if (write === undefined) {
      throw new Error("no CancelRequested commit captured");
    }
    expect(write.principal.kind).toBe("supervisor");
    expect(write.repoId).toEqual({
      kind: "workflow-run",
      id: "deployment-x",
    });
    const eventEntry = Object.entries(write.files).find(([k]) =>
      k.includes("/events/"),
    );
    if (eventEntry === undefined) {
      throw new Error("no event blob captured in the commit");
    }
    const [, blobBytes] = eventEntry;
    const blobJson =
      typeof blobBytes === "string"
        ? blobBytes
        : new TextDecoder().decode(blobBytes);
    const blob = readCancelRequestedBlob(blobJson);
    expect(blob.type).toBe("CancelRequested");
    expect(blob.origin).toBe("supervisor-drain");
    expect(blob.signature.principalKind).toBe("supervisor");

    await supervisor.shutdown();
  });

  test("requestCancel signs CancelRequested via signAsPrincipal for every origin", async () => {
    const baseDir = await makeTempDir("supervisor-cancel-");
    const signSpyCalls: { kind: string; payload: Uint8Array }[] = [];
    const observedWrites: {
      principal: { kind: string };
      repoId: RepoId;
      ref: string;
      files: Record<string, string | Uint8Array>;
    }[] = [];
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawn not invoked in cancel test");
      },
      signSpy: (kind, payload) => {
        signSpyCalls.push({ kind, payload });
        // Synthetic 64-byte signature with the run id encoded in the
        // first bytes so the test asserts which call produced it.
        const sig = new Uint8Array(64);
        sig[0] = signSpyCalls.length;
        return { sig, principalKind: "supervisor" };
      },
      mailBus: createMockMailBus(),
      onWrite: (args) => observedWrites.push(args),
    });
    const supervisor = createWorkflowSupervisor(bindings);

    const origins = [
      "self",
      "supervisor-drain",
      "supervisor-operator",
      "hub-admin",
    ] as const;
    for (const origin of origins) {
      const result = await supervisor.requestCancel({
        runId: `run-${origin}`,
        origin,
        reason: `reason for ${origin}`,
        at: "2026-01-01T00:00:00.000Z",
      });
      expect(result.commitSha).toBe("deadbeefcafef00d");
    }

    // Every origin flows through the supervisor's signing callback
    // with principal kind `"supervisor"`. The kind-handler-side
    // principal-vs-origin map for hub-admin is enforced when the
    // push is presented; the supervisor's signing path itself does
    // not vary by origin.
    expect(signSpyCalls.length).toBe(origins.length);
    for (const call of signSpyCalls) {
      expect(call.kind).toBe("supervisor");
      expect(call.payload).toBeInstanceOf(Uint8Array);
      const text = new TextDecoder().decode(call.payload);
      expect(text).toContain("CancelRequested");
    }

    expect(observedWrites.length).toBe(origins.length);
    for (const write of observedWrites) {
      expect(write.principal.kind).toBe("supervisor");
      expect(write.repoId).toEqual({
        kind: "workflow-run",
        id: "deployment-x",
      });
    }
    const firstWrite = observedWrites[0];
    if (firstWrite === undefined) {
      throw new Error("no observed writes captured");
    }
    const firstEntry = Object.entries(firstWrite.files)[0];
    if (firstEntry === undefined) {
      throw new Error("first write produced no files");
    }
    const [, firstBytes] = firstEntry;
    const firstJson =
      typeof firstBytes === "string"
        ? firstBytes
        : new TextDecoder().decode(firstBytes);
    const onDisk = readCancelRequestedBlob(firstJson);
    expect(onDisk.type).toBe("CancelRequested");
    expect(onDisk.origin).toBe("self");
    expect(onDisk.signature.principalKind).toBe("supervisor");
    expect(onDisk.signature.sig).toMatch(/^01[0-9a-f]+$/);
  });

  test("deploy routes the trivial branch through the host-injected trivialLaunch callback", async () => {
    const baseDir = await makeTempDir("supervisor-deploy-trivial-");
    const trivialCalls: {
      agentAddress: string;
      agentId: string;
      hubPublicKey: string;
      config: unknown;
    }[] = [];
    const signSpyCalls: { kind: string }[] = [];
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error(
          "subprocessSpawner must not be invoked on the trivial branch",
        );
      },
      signSpy: (kind) => {
        signSpyCalls.push({ kind });
        return { sig: new Uint8Array(64), principalKind: "supervisor" };
      },
      mailBus: createMockMailBus(),
      trivialLaunch: async (b) => {
        trivialCalls.push({
          agentAddress: b.agentAddress,
          agentId: b.agentId,
          hubPublicKey: b.hubPublicKey,
          config: b.config,
        });
      },
    });
    const supervisor = createWorkflowSupervisor(bindings);
    const frame = {
      agentAddress: "agent-1@example.com",
      agentId: "agent-1",
      config: { sentinel: "config-bytes" },
      hubPublicKey: "deadbeef",
    };
    await supervisor.deploy(frame);
    expect(trivialCalls).toHaveLength(1);
    expect(trivialCalls[0]).toEqual({
      agentAddress: "agent-1@example.com",
      agentId: "agent-1",
      hubPublicKey: "deadbeef",
      config: { sentinel: "config-bytes" },
    });
    // No signAsPrincipal calls on the trivial branch -- the
    // workflow-process-cancel path never engages.
    expect(signSpyCalls).toEqual([]);
    // credentialsSnapshot is multi-step-only; the trivial deploy
    // does not assemble one.
    expect(supervisor.getCredentialsSnapshot()).toBeNull();
  });

  test("deploy does not register a mailbox or open IPC on the trivial branch", async () => {
    const baseDir = await makeTempDir("supervisor-deploy-no-mailbus-");
    const mailBus = createMockMailBus();
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner must not be invoked on the trivial branch");
      },
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
      trivialLaunch: () => Promise.resolve(),
    });
    const supervisor = createWorkflowSupervisor(bindings);
    await supervisor.deploy({
      agentAddress: "agent-2@example.com",
      agentId: "agent-2",
      config: {},
      hubPublicKey: "cafef00d",
    });
    // The mail bus is the multi-step branch's seam; the trivial
    // branch must not touch it.
    expect(mailBus.registered()).not.toContain("deployment-x@example.com");
  });

  test("deploy hands recordRunEvent into trivialLaunch and commits the canonical four-event chain", async () => {
    const baseDir = await makeTempDir("supervisor-deploy-run-events-");
    const signSpyCalls: { kind: string; payload: Uint8Array }[] = [];
    const observedWrites: {
      principal: { kind: string };
      repoId: RepoId;
      ref: string;
      files: Record<string, string | Uint8Array>;
    }[] = [];
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner must not be invoked on the trivial branch");
      },
      signSpy: (kind, payload) => {
        signSpyCalls.push({ kind, payload });
        const sig = new Uint8Array(64);
        sig[0] = signSpyCalls.length;
        return { sig, principalKind: "supervisor" };
      },
      mailBus: createMockMailBus(),
      onWrite: (args) => observedWrites.push(args),
      statefulWrites: true,
      trivialLaunch: async (b) => {
        const runId = "run-trivial-1";
        const messageId = "msg-1";
        const stepId = "step-1";
        const chain: readonly SupervisorRunEvent[] = [
          {
            kind: "RunStarted",
            runId,
            at: "2026-01-01T00:00:00.000Z",
            definitionHash: "def-hash-trivial",
            trigger: { type: "mail", payload: { to: b.agentAddress } },
            consumedMessageId: messageId,
          },
          {
            kind: "StepStarted",
            runId,
            at: "2026-01-01T00:00:00.001Z",
            stepId,
            attempt: 1,
            input: { ref: "refs/heads/main" },
          },
          {
            kind: "StepCompleted",
            runId,
            at: "2026-01-01T00:00:00.002Z",
            stepId,
            attempt: 1,
            output: { ref: "refs/heads/main" },
          },
          {
            kind: "RunCompleted",
            runId,
            at: "2026-01-01T00:00:00.003Z",
          },
        ];
        for (const event of chain) {
          await b.recordRunEvent(event);
        }
      },
    });
    const supervisor = createWorkflowSupervisor(bindings);
    await supervisor.deploy({
      agentAddress: "agent-4@example.com",
      agentId: "agent-4",
      config: {},
      hubPublicKey: "abc",
    });

    // Every event in the chain flowed through signAsPrincipal with
    // kind `"supervisor"` and against payload bytes containing the
    // expected discriminator.
    expect(signSpyCalls.map((c) => c.kind)).toEqual([
      "supervisor",
      "supervisor",
      "supervisor",
      "supervisor",
    ]);
    const decodedPayloads = signSpyCalls.map((c) =>
      new TextDecoder().decode(c.payload),
    );
    expect(decodedPayloads[0]).toContain("RunStarted");
    expect(decodedPayloads[0]).toContain("def-hash-trivial");
    expect(decodedPayloads[0]).toContain("msg-1");
    expect(decodedPayloads[1]).toContain("StepStarted");
    expect(decodedPayloads[2]).toContain("StepCompleted");
    expect(decodedPayloads[3]).toContain("RunCompleted");

    // Every commit went through the supervisor principal against the
    // deployment's workflow-run repo.
    expect(observedWrites.length).toBe(4);
    for (const write of observedWrites) {
      expect(write.principal.kind).toBe("supervisor");
      expect(write.repoId).toEqual({
        kind: "workflow-run",
        id: "deployment-x",
      });
      expect(write.ref).toBe("refs/heads/main");
    }

    // The on-disk envelopes are filed under runs/<runId>/events/<seq>.json
    // with monotonically increasing seq from 0.
    const expectedTypes = [
      "RunStarted",
      "StepStarted",
      "StepCompleted",
      "RunCompleted",
    ];
    for (const [index, write] of observedWrites.entries()) {
      const expectedSeq = index;
      const expectedPath = `runs/run-trivial-1/events/${String(expectedSeq)}.json`;
      const bytes = write.files[expectedPath];
      if (bytes === undefined) {
        throw new Error(
          `commit ${String(index)} did not contain the expected event blob at ${expectedPath}`,
        );
      }
      const expectedType = expectedTypes[index];
      if (expectedType === undefined) {
        throw new Error(
          `unreachable: expectedTypes[${String(index)}] is undefined`,
        );
      }
      // Every prior commit's blob is also carried through the prefix-
      // preserving merge so the substrate's append-only invariant
      // holds; assert the count here so a regression that drops a
      // prior blob surfaces at the test seam.
      expect(Object.keys(write.files)).toHaveLength(expectedSeq + 1);
      const blobJson =
        typeof bytes === "string" ? bytes : new TextDecoder().decode(bytes);
      const blob = readRunEventBlob(blobJson);
      expect(blob.type).toBe(expectedType);
      expect(blob.seq).toBe(expectedSeq);
      expect(blob.runId).toBe("run-trivial-1");
      expect(blob.signature.principalKind).toBe("supervisor");
      expect(blob.signature.sig.length).toBe(128);
    }
    // The trivial branch does not assemble a credentials snapshot
    // even though the event chain landed; observability and
    // process topology are independent surfaces.
    expect(supervisor.getCredentialsSnapshot()).toBeNull();
  });

  test("drain() threads the terminalEventSource binding into each accumulator's opts", async () => {
    const baseDir = await makeTempDir("supervisor-drain-terminal-source-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      return {
        pid: 7777,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
    };

    type StubAccumulator = DrainTimeoutAccumulator & {
      __opts: DrainTimeoutOpts;
    };
    const stubs: StubAccumulator[] = [];
    const factory: DrainTimeoutAccumulatorFactory = (opts) => {
      const stub: StubAccumulator = {
        __opts: opts,
        start() {
          /* unused */
        },
        pause() {
          /* unused */
        },
        resume() {
          /* unused */
        },
        stop() {
          /* unused */
        },
        accumulatedMs() {
          return 0;
        },
        get escalated() {
          return false;
        },
        disposed() {
          return Promise.resolve();
        },
      };
      stubs.push(stub);
      return stub;
    };

    const sourceCalls: string[] = [];
    const terminalEventSource: TerminalEventSource = (runId) => {
      sourceCalls.push(runId);
      return {
        [Symbol.asyncIterator](): AsyncIterator<TerminalRunEvent> {
          return {
            next(): Promise<IteratorResult<TerminalRunEvent>> {
              return new Promise<IteratorResult<TerminalRunEvent>>(
                () => undefined,
              );
            },
            return(): Promise<IteratorResult<TerminalRunEvent>> {
              return Promise.resolve({ value: undefined, done: true });
            },
          };
        },
      };
    };

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      drainTimeoutAccumulatorFactory: factory,
      drainTimeoutMs: 5_000,
      terminalEventSource,
    };
    const supervisor = createWorkflowSupervisor(bindings);

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      onInferenceEvent: () => undefined,
    });
    while (observedEnv === undefined) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
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
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("term-msg-A"),
    );
    await childSender.send({
      type: "ready",
      data: {
        childPid: 7777,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });
    await spawnPromise;
    await supervisor.drain({ deadlineMs: 5_000 });

    // The factory was handed a terminalEventSource and the
    // accumulator pulled a per-runId iterator from it.
    expect(stubs).toHaveLength(1);
    const stub = stubs[0];
    if (stub === undefined) throw new Error("expected one stub accumulator");
    expect(stub.__opts.terminalEventSource).toBeDefined();
    const factorySource = stub.__opts.terminalEventSource;
    if (factorySource === undefined) {
      throw new Error(
        "expected accumulator opts to carry a terminalEventSource",
      );
    }
    // Calling the source factory itself walks through to the binding
    // and records the runId on the recorder.
    const iterable = factorySource(stub.__opts.runId);
    const iter = iterable[Symbol.asyncIterator]();
    expect(sourceCalls).toContain(stub.__opts.runId);
    await iter.return?.(undefined);

    await supervisor.shutdown();
  });

  test("drain() omits terminalEventSource on the accumulator opts when no binding is configured", async () => {
    const baseDir = await makeTempDir("supervisor-drain-no-term-source-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    let observedEnv: Record<string, string> | undefined;
    const spawner: SubprocessSpawner = ({ env }) => {
      observedEnv = env;
      return {
        pid: 6666,
        controlWriter: supervisorToChild.writer,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
    };

    type StubAccumulator = DrainTimeoutAccumulator & {
      __opts: DrainTimeoutOpts;
    };
    const stubs: StubAccumulator[] = [];
    const factory: DrainTimeoutAccumulatorFactory = (opts) => {
      const stub: StubAccumulator = {
        __opts: opts,
        start() {
          /* unused */
        },
        pause() {
          /* unused */
        },
        resume() {
          /* unused */
        },
        stop() {
          /* unused */
        },
        accumulatedMs() {
          return 0;
        },
        get escalated() {
          return false;
        },
        disposed() {
          return Promise.resolve();
        },
      };
      stubs.push(stub);
      return stub;
    };

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      drainTimeoutAccumulatorFactory: factory,
      drainTimeoutMs: 5_000,
    };
    const supervisor = createWorkflowSupervisor(bindings);

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      onInferenceEvent: () => undefined,
    });
    while (observedEnv === undefined) {
      await new Promise((r) => setTimeout(r, 1));
    }
    const channelId = observedEnv.IPC_CHANNEL_ID;
    if (channelId === undefined) {
      throw new Error("IPC_CHANNEL_ID not set in spawn-time env");
    }
    const childSender = createControlChannelSender({
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
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("no-term-msg"),
    );
    await childSender.send({
      type: "ready",
      data: {
        childPid: 6666,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });
    await spawnPromise;
    await supervisor.drain({ deadlineMs: 5_000 });

    expect(stubs).toHaveLength(1);
    const stub = stubs[0];
    if (stub === undefined) throw new Error("expected one stub accumulator");
    // The no-op default leaves the accumulator on timer-only
    // settlement: the supervisor passes no terminalEventSource through
    // to the factory.
    expect(stub.__opts.terminalEventSource).toBeUndefined();

    await supervisor.shutdown();
  });

  test("deploy surfaces a trivialLaunch failure to the caller", async () => {
    const baseDir = await makeTempDir("supervisor-deploy-error-");
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner must not be invoked on the trivial branch");
      },
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus: createMockMailBus(),
      trivialLaunch: () =>
        Promise.reject(new Error("provisionAgent failed in test")),
    });
    const supervisor = createWorkflowSupervisor(bindings);
    await expect(
      supervisor.deploy({
        agentAddress: "agent-3@example.com",
        agentId: "agent-3",
        config: {},
        hubPublicKey: "abc",
      }),
    ).rejects.toThrow(/provisionAgent failed in test/);
  });
});

describe("assembleCredentialsSnapshot", () => {
  test("enumerates each step's agent-state repo and pins per-step grants by hash", async () => {
    const baseDir = await makeTempDir("supervisor-creds-");
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "d1", stepId: "alpha" }),
      [{ resource: "alpha-thing", action: "read" }],
    );
    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "d1", stepId: "beta" }),
      [
        { resource: "beta-thing", action: "read" },
        { resource: "beta-thing", action: "write" },
      ],
    );
    const repoStore = createStubRepoStore({ baseDir });
    const snapshot = await assembleCredentialsSnapshot({
      repoStore,
      principal: { kind: "supervisor" },
      stepOrder: ["alpha", "beta"],
      deploymentId: "d1",
      deriveStepAddress: ({ deploymentId, stepId }) =>
        `${deploymentId}-${stepId}@example.com`,
    });
    expect(snapshot.steps).toHaveLength(2);
    expect(snapshot.steps[0]?.stepId).toBe("alpha");
    expect(snapshot.steps[0]?.address).toBe("d1-alpha@example.com");
    expect(snapshot.steps[0]?.grants).toEqual([
      { resource: "alpha-thing", action: "read" },
    ]);
    expect(snapshot.steps[0]?.contentHash).toBe(
      hashGrants([{ resource: "alpha-thing", action: "read" }]),
    );
    expect(snapshot.steps[1]?.stepId).toBe("beta");
    expect(snapshot.steps[1]?.grants).toHaveLength(2);
    expect(snapshot.steps[0]?.contentHash).not.toBe(
      snapshot.steps[1]?.contentHash,
    );
  });

  test("treats a missing per-step grants file as an empty grant array", async () => {
    const baseDir = await makeTempDir("supervisor-creds-empty-");
    const repoStore = createStubRepoStore({ baseDir });
    const snapshot = await assembleCredentialsSnapshot({
      repoStore,
      principal: { kind: "supervisor" },
      stepOrder: ["solo"],
      deploymentId: "d2",
      deriveStepAddress: ({ deploymentId }) => `${deploymentId}@example.com`,
    });
    expect(snapshot.steps).toHaveLength(1);
    expect(snapshot.steps[0]?.grants).toEqual([]);
    expect(snapshot.steps[0]?.contentHash).toBe(hashGrants([]));
  });

  test("a malformed grants file fails loudly rather than silently treating it as empty", async () => {
    const baseDir = await makeTempDir("supervisor-creds-bad-");
    const repoId = defaultStepRepoId({ deploymentId: "d3", stepId: "s" });
    const dir = path.join(baseDir, repoId.kind, repoId.id);
    await fs.mkdir(path.join(dir, "state"), { recursive: true });
    await fs.writeFile(path.join(dir, STEP_GRANTS_PATH), "not json");
    const repoStore = createStubRepoStore({ baseDir });
    await expect(
      assembleCredentialsSnapshot({
        repoStore,
        principal: { kind: "supervisor" },
        stepOrder: ["s"],
        deploymentId: "d3",
        deriveStepAddress: () => "d3-s@example.com",
      }),
    ).rejects.toThrow(/is not valid JSON/);
  });
});

describe("commitCancelRequested (low-level)", () => {
  test("attaches the signed payload to the on-disk CancelRequested blob", async () => {
    const baseDir = await makeTempDir("cancel-signing-");
    let observedFiles: Record<string, string | Uint8Array> | undefined;
    const repoStore = createStubRepoStore({
      baseDir,
      onWrite: ({ files }) => {
        observedFiles = files;
      },
    });
    const signed = await commitCancelRequested({
      substrate: repoStore,
      repoId: { kind: "workflow-run", id: "deploy" },
      ref: "refs/heads/main",
      deploymentId: "deploy",
      runId: "r1",
      origin: "self",
      reason: "tests pass",
      at: "2026-01-01T00:00:00.000Z",
      signAsPrincipal: (kind, payload) => {
        expect(kind).toBe("supervisor");
        const sig = new Uint8Array(64);
        // Embed the payload length so we can verify it was signed.
        sig[0] = payload.length & 0xff;
        return { sig, principalKind: "supervisor" };
      },
    });
    expect(signed.commitSha).toBe("deadbeefcafef00d");
    expect(signed.seq).toBe(0);
    if (observedFiles === undefined) {
      throw new Error("writeTreePreservingPrefix was not invoked");
    }
    const entry = Object.entries(observedFiles).find(([k]) =>
      k.endsWith("/events/0.json"),
    );
    if (entry === undefined) {
      throw new Error("no events/0.json entry observed in commit");
    }
    const [, blobBytes] = entry;
    const blobJson =
      typeof blobBytes === "string"
        ? blobBytes
        : new TextDecoder().decode(blobBytes);
    const blob = readCancelRequestedBlob(blobJson);
    expect(blob.type).toBe("CancelRequested");
    expect(blob.origin).toBe("self");
    expect(blob.reason).toBe("tests pass");
    expect(blob.signature.principalKind).toBe("supervisor");
    expect(blob.signature.sig.length).toBe(128);
  });
});

describe("IPC integration smoke", () => {
  test("a sender/receiver round-trip on the synthetic streams used by the supervisor tests", async () => {
    // Sanity check that the in-memory stream helpers do not regress
    // the IPC contract -- the supervisor tests rely on these same
    // helpers shaped against the same primitives the production IPC
    // module exposes.
    const upstream = createMemoryNdjsonStream();
    const downstream = createMemoryNdjsonStream();
    const keyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const sender = createControlChannelSender({
      privateKeySeed: keyPair.privateKey,
      channelId,
      writer: upstream.writer,
    });
    await sender.send({
      type: "ready",
      data: {
        childPid: 1,
        childPublicKey: Buffer.from(keyPair.publicKey).toString("hex"),
      },
    });
    expect(upstream.flushed()).toHaveLength(1);

    const eventStream = createMemoryFrameStream();
    const hmacKey = generateHmacKey();
    const eventSender = createEventChannelSender({
      hmacKey,
      channelId,
      writer: {
        write(bytes: Uint8Array) {
          eventStream.inject(bytes);
        },
      },
    });
    await eventSender.send({
      type: "message.run.started",
      seq: 1,
      data: {
        messageId: "m",
        messageRunId: "r",
        receivedAt: 1,
      },
    });
    eventStream.close();

    // Verify the receiver pipeline picks up the framed bytes.
    const crashes: string[] = [];
    const recvIter = receiveControlChannel({
      publicKey: keyPair.publicKey,
      channelId,
      reader: {
        read(): AsyncIterableIterator<string> {
          return upstream.reader.read();
        },
      },
      onCrash: (reason) => crashes.push(reason),
    });
    upstream.close();
    let firstPayload: { type: string } | undefined;
    for await (const payload of recvIter) {
      firstPayload = { type: payload.type };
      break;
    }
    expect(firstPayload?.type).toBe("ready");
    expect(crashes).toHaveLength(0);
    void downstream;
    void hexDecode;
  });
});
