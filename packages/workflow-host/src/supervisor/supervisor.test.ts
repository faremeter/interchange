import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto";
import { hexDecode, hexEncode } from "@intx/types";
import type { InferenceSource } from "@intx/types/runtime";
import type { RepoId, RepoStore } from "@intx/hub-sessions";

import {
  createWorkflowSupervisor,
  type DrainTimeoutAccumulator,
  type DrainTimeoutAccumulatorFactory,
  type DrainTimeoutOpts,
  type InboxPrimitives,
  type MailBusBindings,
  type SubprocessSpawner,
  type SubprocessHandle,
  type SignedPayload,
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
  ControlPayload,
  receiveControlChannel,
  SignedEnvelope,
  generateHmacKey,
  generateChannelId,
  type FrameReader,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";

/**
 * Parse the `runId` carried on each `trigger.fire` frame the
 * supervisor wrote to the in-memory child control stream. Validates
 * every signed envelope's payload through the canonical `ControlPayload`
 * narrow so the helper does not need to `as`-cast at the boundary.
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

function parseSourcesUpdatedFrames(
  lines: readonly string[],
): { sources: InferenceSource[]; defaultSource: string }[] {
  const out: { sources: InferenceSource[]; defaultSource: string }[] = [];
  for (const line of lines) {
    if (!line.includes("sources-updated")) continue;
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) continue;
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) continue;
    if (payload.type !== "sources-updated") continue;
    out.push({
      sources: payload.data.sources,
      defaultSource: payload.data.defaultSource,
    });
  }
  return out;
}

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
      return { commitSha: "deadbeefcafef00d", newlyTerminalRuns: [] };
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

/**
 * Per-address claim-check state for the in-memory inbox stub. Mirrors
 * the substrate's three subdirectories (`inbox`, `processing`,
 * `consumed`) so a sequence of `enqueueInbox` / `dequeueToProcessing`
 * / `markConsumed` / `replayProcessingToInbox` calls is observable
 * without standing up a real git repo.
 */
type MemoryInboxEntry = {
  messageId: string;
  receivedAt: number;
  mailAuditRef: { store: string; path: string };
};

export type MemoryInboxState = {
  inbox: Map<string, MemoryInboxEntry>;
  processing: Map<string, MemoryInboxEntry>;
  consumed: Map<string, MemoryInboxEntry>;
};

export type MemoryInboxPrimitives = InboxPrimitives & {
  /** Snapshot the in-memory state for a given address (testing only). */
  snapshot(address: string): MemoryInboxState;
};

function filenameKey(receivedAt: number, messageId: string): string {
  return `${String(receivedAt)}-${messageId}`;
}

function createMemoryInboxPrimitives(): MemoryInboxPrimitives {
  const byAddress = new Map<string, MemoryInboxState>();
  function getOrCreate(address: string): MemoryInboxState {
    let entry = byAddress.get(address);
    if (entry === undefined) {
      entry = {
        inbox: new Map(),
        processing: new Map(),
        consumed: new Map(),
      };
      byAddress.set(address, entry);
    }
    return entry;
  }
  return {
    snapshot(address: string): MemoryInboxState {
      return getOrCreate(address);
    },
    async enqueueInbox(_store, _principal, _repoId, args) {
      const state = getOrCreate(args.address);
      const key = filenameKey(args.receivedAt, args.messageId);
      if (state.consumed.has(args.messageId)) {
        throw new Error(
          `claim_check_already_consumed: ${args.address} ${args.messageId}`,
        );
      }
      for (const existingKey of state.inbox.keys()) {
        if (existingKey.endsWith(`-${args.messageId}`)) {
          throw new Error(
            `claim_check_already_inbox: ${args.address} ${args.messageId}`,
          );
        }
      }
      for (const existingKey of state.processing.keys()) {
        if (existingKey.endsWith(`-${args.messageId}`)) {
          throw new Error(
            `claim_check_already_processing: ${args.address} ${args.messageId}`,
          );
        }
      }
      const envelope: MemoryInboxEntry = {
        messageId: args.messageId,
        receivedAt: args.receivedAt,
        mailAuditRef: args.mailAuditRef,
      };
      state.inbox.set(key, envelope);
      return {
        commitSha: "memory-inbox",
        inboxKey: key,
        envelope: {
          messageId: args.messageId,
          receivedAt: args.receivedAt,
          address: args.address,
          mailAuditRef: args.mailAuditRef,
        },
      };
    },
    async dequeueToProcessing(_store, _principal, _repoId, address) {
      const state = getOrCreate(address);
      const entries = [...state.inbox.entries()].sort(([, a], [, b]) => {
        if (a.receivedAt !== b.receivedAt) return a.receivedAt - b.receivedAt;
        if (a.messageId < b.messageId) return -1;
        if (a.messageId > b.messageId) return 1;
        return 0;
      });
      if (entries.length === 0) return null;
      const head = entries[0];
      if (head === undefined) throw new Error("unreachable");
      const [key, envelope] = head;
      state.inbox.delete(key);
      state.processing.set(key, envelope);
      return {
        commitSha: "memory-inbox",
        key,
        envelope: {
          messageId: envelope.messageId,
          receivedAt: envelope.receivedAt,
          address,
          mailAuditRef: envelope.mailAuditRef,
        },
      };
    },
    async markConsumed(_store, _principal, _repoId, args) {
      const state = getOrCreate(args.address);
      let foundKey: string | null = null;
      let envelope: MemoryInboxEntry | null = null;
      for (const [key, value] of state.processing) {
        if (value.messageId === args.messageId) {
          foundKey = key;
          envelope = value;
          break;
        }
      }
      if (foundKey === null || envelope === null) {
        throw new Error(
          `claim_check_processing_not_found: ${args.address} ${args.messageId}`,
        );
      }
      state.processing.delete(foundKey);
      state.consumed.set(args.messageId, envelope);
      return {
        commitSha: "memory-inbox",
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
      const state = getOrCreate(address);
      const replayedKeys: string[] = [];
      for (const [key, value] of state.processing) {
        if (state.inbox.has(key)) {
          throw new Error(
            `claim_check_replay_collision: inbox already has ${key}`,
          );
        }
        state.inbox.set(key, value);
        replayedKeys.push(key);
      }
      state.processing.clear();
      return { commitSha: "memory-inbox", replayedKeys };
    },
  };
}

async function buildBindings(opts: {
  baseDir: string;
  spawner: SubprocessSpawner;
  signSpy: (kind: string, payload: Uint8Array) => SignedPayload;
  mailBus: MailBusBindings;
  onWrite?: (args: {
    principal: { kind: string };
    repoId: RepoId;
    ref: string;
    files: Record<string, string | Uint8Array>;
  }) => void;
  statefulWrites?: boolean;
  inboxPrimitives?: InboxPrimitives;
}): Promise<WorkflowSupervisorBindings> {
  const repoStore = createStubRepoStore({
    baseDir: opts.baseDir,
    ...(opts.onWrite !== undefined ? { onWrite: opts.onWrite } : {}),
    ...(opts.statefulWrites === true ? { statefulWrites: true } : {}),
  });
  return {
    repoStore,
    signAsPrincipal: async (kind, payload) => opts.signSpy(kind, payload),
    mailBus: opts.mailBus,
    subprocessSpawner: opts.spawner,
    binaryPath: "/fake/bin/workflow-child",
    substrateEnv: { DATA_DIR: opts.baseDir },
    workflowRunRepoId: { kind: "workflow-run", id: "deployment-x" },
    workflowRunRef: "refs/heads/main",
    deploymentId: "deployment-x",
    stepCount: 1,
    deploymentMailAddress: "deployment-x@example.com",
    readPrincipal: { kind: "supervisor" },
    deriveStepAddress: ({ deploymentId, stepId }) =>
      `${deploymentId}-${stepId}@example.com`,
    inboxPrimitives: opts.inboxPrimitives ?? createMemoryInboxPrimitives(),
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
      warmKeep: false,
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
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
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
    // the supervisor-to-child stream after `ready` landed. The FIFO
    // claim-check pipeline serializes dispatch on each run's terminal
    // event -- the dispatch loop sits on `waitForRunTerminal` after
    // the first forward, so the test drives m1's terminal event back
    // to release the loop and observe m2's forward.
    const waitForTriggerFires = async (n: number): Promise<string[]> => {
      const deadline = Date.now() + 500;
      while (Date.now() < deadline) {
        const ids = parseTriggerFireRunIds(supervisorToChild.flushed());
        if (ids.length >= n) return ids;
        await new Promise((r) => setTimeout(r, 1));
      }
      return parseTriggerFireRunIds(supervisorToChild.flushed());
    };
    const firstFired = await waitForTriggerFires(1);
    expect(firstFired.length).toBeGreaterThanOrEqual(1);
    const firstRunId = firstFired[0];
    if (firstRunId === undefined) throw new Error("first runId missing");
    await childSender.send({
      type: "terminal.event",
      data: {
        runId: firstRunId,
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });
    const fired = await waitForTriggerFires(2);
    expect(fired.length).toBeGreaterThanOrEqual(2);
    const secondRunId = fired.find((id) => id !== firstRunId);
    if (secondRunId !== undefined) {
      await childSender.send({
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
    expect(killed).toBe(true);
    expect(mailBus.registered()).not.toContain("deployment-x@example.com");
  });

  // Harness for the ready-timeout tests: an injected FakeTimer registry
  // (deterministic, per greybeard's ruling against real timers) plus a
  // controllable child whose control reader the test can close to model a
  // child that exits before signalling ready. `createdTimers` retains every
  // timer even after it is cleared, so a test can capture the ready deadline
  // and later assert it was cancelled.
  async function makeReadyTimeoutHarness(readyTimeoutMs: number) {
    type FakeTimer = { cb: () => void; ms: number; cancelled: boolean };
    const timers = new Set<FakeTimer>();
    const createdTimers: FakeTimer[] = [];

    const baseDir = await makeTempDir("supervisor-ready-timeout-");
    const supervisorIpcKeyPair = await generateKeyPair();
    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventChildToSupervisor = createMemoryFrameStream();
    let resolveExit: ((code: number) => void) | undefined;
    const exited = new Promise<number>((resolve) => {
      resolveExit = resolve;
    });
    const killSignals: string[] = [];

    const spawner: SubprocessSpawner = ({ env: _env }) => ({
      pid: 5150,
      controlWriter: supervisorToChild.writer,
      controlReader: childToSupervisor.reader,
      eventReader: eventChildToSupervisor.reader,
      kill: (signal) => {
        killSignals.push(
          typeof signal === "string" ? signal : String(signal ?? ""),
        );
        childToSupervisor.close();
        eventChildToSupervisor.close();
        resolveExit?.(0);
      },
      exited,
    });

    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({ sig: new Uint8Array(64), principalKind: "supervisor" }),
      mailBus,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      readyTimeoutMs,
      setTimer: (cb, ms) => {
        const t: FakeTimer = { cb, ms, cancelled: false };
        timers.add(t);
        createdTimers.push(t);
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

    // Resolve once the spawn has armed its ready deadline (which happens
    // after the spawner is invoked, so this also confirms the child spawned).
    async function waitForReadyDeadline(): Promise<FakeTimer> {
      for (;;) {
        const t = createdTimers.find((x) => x.ms === readyTimeoutMs);
        if (t !== undefined) return t;
        await new Promise((r) => setTimeout(r, 1));
      }
    }

    return { supervisor, killSignals, childToSupervisor, waitForReadyDeadline };
  }

  const readyTimeoutSpawnOpts = {
    stepOrder: ["step-1"],
    definitionHash: "def-hash-abc",
    warmKeep: false,
    onInferenceEvent: () => {
      /* unused in the ready-timeout tests */
    },
  };

  test("spawn times out, kills the child, rejects, and clears the ready deadline", async () => {
    const h = await makeReadyTimeoutHarness(7_777);
    // Never send `ready`. Spawn blocks on the handshake until the deadline.
    const spawnPromise = h.supervisor.spawn(readyTimeoutSpawnOpts);
    const readyDeadline = await h.waitForReadyDeadline();
    readyDeadline.cb();

    await expect(spawnPromise).rejects.toThrow(
      /child did not emit ready within 7777ms; killed/,
    );
    expect(h.killSignals).toContain("SIGTERM");
    // The unconditional deadline-timer clear ran on the timeout path.
    expect(readyDeadline.cancelled).toBe(true);
  });

  test("spawn clears the ready deadline when the child exits before ready", async () => {
    const h = await makeReadyTimeoutHarness(8_888);
    const spawnPromise = h.supervisor.spawn(readyTimeoutSpawnOpts);
    const readyDeadline = await h.waitForReadyDeadline();

    // The child exits before signalling ready: closing the control reader
    // ends `waitForReady`, rejecting the ready promise. Because the outcomes
    // are folded to values, the race resolves to the failed outcome rather
    // than rejecting, so the unconditional deadline-timer clear still runs.
    // A race that rejected here would skip the clear and leak an armed
    // deadline that keeps the event loop alive for up to readyTimeoutMs.
    h.childToSupervisor.close();

    await expect(spawnPromise).rejects.toThrow(
      /control channel ended before child emitted ready/,
    );
    expect(readyDeadline.cancelled).toBe(true);
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
      warmKeep: false,
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
    // Two pre-ready messages. The supervisor's FIFO inbox queue
    // serializes dispatch: only one run is in-flight at a time. By
    // the time `drain()` is called below, the second message may
    // still be mid-dispatch behind the first's `markConsumed`. The
    // accumulator count reflects whichever in-flight runIds remain.
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
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;

    // No accumulators armed yet -- drain has not been called.
    expect(stubs).toHaveLength(0);

    // Wait for the dispatch loop to dequeue the first buffered mail
    // and forward its `trigger.fire`. The H-S1 contract gates the
    // dispatch loop's first iteration on the spawn-time replayDone;
    // without polling for the forwarded frame the test would call
    // `drain()` while `inFlightRuns` is still empty and no
    // accumulator would arm.
    const triggerFireDeadline = Date.now() + 500;
    while (Date.now() < triggerFireDeadline) {
      const ids = parseTriggerFireRunIds(supervisorToChild.flushed());
      if (ids.length >= 1) break;
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(
      parseTriggerFireRunIds(supervisorToChild.flushed()).length,
    ).toBeGreaterThanOrEqual(1);

    await supervisor.drain({ deadlineMs: 7_500 });

    // The supervisor's `drain` control frame landed on the
    // supervisor-to-child stream alongside the buffered-mail
    // `trigger.fire` frames. The FIFO claim-check pipeline keeps the
    // dispatch loop running concurrently with `drain()`, so a fresh
    // `trigger.fire` can land before or after the drain frame; find
    // the drain frame by payload type rather than indexing the tail.
    const forwarded = supervisorToChild.flushed();
    expect(forwarded.length).toBeGreaterThanOrEqual(2);
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
    const drainFrame = (() => {
      for (const line of forwarded) {
        const parsed = SignedFrame(JSON.parse(line));
        if (parsed instanceof type.errors) continue;
        if (parsed.envelope.payload.type === "drain") return parsed;
      }
      throw new Error("no drain frame observed on supervisor-to-child stream");
    })();
    expect(drainFrame.envelope.payload).toMatchObject({
      type: "drain",
      data: { deadlineMs: 7_500 },
    });

    // The FIFO inbox queue serializes dispatch: one run is in-flight
    // at a time. The drain() call observes exactly one in-flight run
    // (whichever message was mid-dispatch when the loop awaited
    // `waitForRunTerminal`) and arms one accumulator for it. The
    // second message stays in the inbox until the first completes.
    expect(stubs.length).toBeGreaterThanOrEqual(1);
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
    expect(new Set(runIds).size).toBe(stubs.length);

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
      warmKeep: false,
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
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;

    // Wait for the dispatch loop to forward the buffered mail's
    // `trigger.fire` so the run is in `inFlightRuns` when `drain()`
    // arms its accumulator. With the H-S1 replayDone gate the first
    // dispatch is no longer synchronous with `await spawnPromise`.
    const triggerFireDeadline = Date.now() + 500;
    while (Date.now() < triggerFireDeadline) {
      const ids = parseTriggerFireRunIds(supervisorToChild.flushed());
      if (ids.length >= 1) break;
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(
      parseTriggerFireRunIds(supervisorToChild.flushed()).length,
    ).toBeGreaterThanOrEqual(1);

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

  test("drain() threads the per-cohort terminal broadcaster into each accumulator's opts", async () => {
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
      warmKeep: false,
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
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;
    // Wait for the dispatch loop to forward the buffered mail's
    // `trigger.fire` so the run is in `inFlightRuns` when `drain()`
    // arms its accumulator. The H-S1 replayDone gate moves the first
    // dispatch off the `await spawnPromise` critical path.
    const triggerFireDeadline = Date.now() + 500;
    while (Date.now() < triggerFireDeadline) {
      const ids = parseTriggerFireRunIds(supervisorToChild.flushed());
      if (ids.length >= 1) break;
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(
      parseTriggerFireRunIds(supervisorToChild.flushed()).length,
    ).toBeGreaterThanOrEqual(1);
    await supervisor.drain({ deadlineMs: 5_000 });

    // The supervisor's per-cohort terminal broadcaster always backs
    // the accumulator's terminal-event source; the accumulator factory
    // sees a non-undefined slot and can mint a per-runId iterator
    // through it.
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
    const iterable = factorySource(stub.__opts.runId);
    const iter = iterable[Symbol.asyncIterator]();
    await iter.return?.(undefined);

    await supervisor.shutdown();
  });

  test("drain() arms the broadcaster-backed accumulator source on the active cohort", async () => {
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
      warmKeep: false,
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
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;
    // Wait for the dispatch loop to forward the buffered mail's
    // `trigger.fire` so the run is in `inFlightRuns` when `drain()`
    // arms its accumulator. The H-S1 replayDone gate moves the first
    // dispatch off the `await spawnPromise` critical path.
    const triggerFireDeadline = Date.now() + 500;
    while (Date.now() < triggerFireDeadline) {
      const ids = parseTriggerFireRunIds(supervisorToChild.flushed());
      if (ids.length >= 1) break;
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(
      parseTriggerFireRunIds(supervisorToChild.flushed()).length,
    ).toBeGreaterThanOrEqual(1);
    await supervisor.drain({ deadlineMs: 5_000 });

    expect(stubs).toHaveLength(1);
    const stub = stubs[0];
    if (stub === undefined) throw new Error("expected one stub accumulator");
    // The supervisor owns the per-cohort terminal broadcaster; the
    // accumulator factory always receives a non-undefined terminal
    // source backed by the active cohort's broadcaster. There is no
    // path through the supervisor today that leaves the accumulator
    // on timer-only settlement -- the broadcaster supplants the
    // pre-binding behaviour wholesale.
    expect(stub.__opts.terminalEventSource).toBeDefined();

    await supervisor.shutdown();
  });

  test("drain() is a no-op when the supervisor is idle (no spawn has run)", async () => {
    // Pins the defensive contract for an inbound drain.deliver frame
    // that lands while the supervisor has no in-flight runs to escalate
    // (e.g. the deployment's only run already reached a terminal
    // state). `drain` returns silently in `idle`/`stopping`/`stopped`,
    // does not throw, does not forward a `drain` control frame to a
    // dead child, and does not arm any accumulators. This is the
    // contract higher-level host shutdown sequences depend on -- they
    // call `drain` unconditionally without sniffing the phase.
    const baseDir = await makeTempDir("supervisor-drain-idle-");
    const accumulatorInvocations: DrainTimeoutOpts[] = [];
    const accumulatorFactory: DrainTimeoutAccumulatorFactory = (opts) => {
      accumulatorInvocations.push(opts);
      const stub: DrainTimeoutAccumulator = {
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
      return stub;
    };
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner must not be invoked on the idle drain path");
      },
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus: createMockMailBus(),
    });
    const supervisor = createWorkflowSupervisor({
      ...bindings,
      drainTimeoutAccumulatorFactory: accumulatorFactory,
    });
    await supervisor.drain({ deadlineMs: 5_000 });
    expect(accumulatorInvocations).toHaveLength(0);
  });

  test("deliverSignal() rejects when the supervisor is idle (no spawn has run)", async () => {
    // Pins the defensive contract for an inbound signal.deliver frame
    // landing against a supervisor that is not in `starting`/`running`/
    // `recycling`. The supervisor throws so the router's
    // `tryRoute` rejection propagates up to the hub-link's
    // `handleSignalDeliver`, which logs and drops without crashing the
    // sidecar or contaminating sibling deployments.
    const baseDir = await makeTempDir("supervisor-deliver-signal-idle-");
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner must not be invoked on the idle signal path");
      },
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus: createMockMailBus(),
    });
    const supervisor = createWorkflowSupervisor(bindings);
    await expect(
      supervisor.deliverSignal({
        runId: "run-stale",
        signalName: "approve",
        signalId: "sig-stale",
        payload: null,
      }),
    ).rejects.toThrow(/deliverSignal called in phase idle/);
  });

  test("deliverSources() rejects when the supervisor is idle (no spawn has run)", async () => {
    // Same phase-guard contract as deliverSignal: a sources rotation
    // landing against a supervisor that is not starting/running throws so
    // the sidecar router's rejection surfaces to the hub-link rather than
    // writing into a dead child's pipe.
    const baseDir = await makeTempDir("supervisor-deliver-sources-idle-");
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("spawner must not be invoked on the idle sources path");
      },
      signSpy: () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus: createMockMailBus(),
    });
    const supervisor = createWorkflowSupervisor(bindings);
    await expect(
      supervisor.deliverSources({
        sources: [
          {
            id: "primary",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-x",
            model: "claude-test",
          },
        ],
        defaultSource: "primary",
      }),
    ).rejects.toThrow(/deliverSources called in phase idle/);
  });

  test("deliverSources() sends a sources-updated frame when running", async () => {
    const baseDir = await makeTempDir("supervisor-deliver-sources-running-");
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
        pid: 4321,
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

    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({ sig: new Uint8Array(64), principalKind: "supervisor" }),
      mailBus: createMockMailBus(),
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
    };
    const supervisor = createWorkflowSupervisor(bindings);

    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: true,
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
    await childSender.send({
      type: "ready",
      data: {
        childPid: 4321,
        childPublicKey: Buffer.from(childIpcKeyPair.publicKey).toString("hex"),
      },
    });
    await spawnPromise;

    const sources: InferenceSource[] = [
      {
        id: "primary",
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "sk-primary",
        model: "claude-test",
      },
    ];
    await supervisor.deliverSources({ sources, defaultSource: "primary" });

    const frames = parseSourcesUpdatedFrames(supervisorToChild.flushed());
    expect(frames).toHaveLength(1);
    expect(frames[0]?.sources).toEqual(sources);
    expect(frames[0]?.defaultSource).toBe("primary");

    await supervisor.shutdown();
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
      await hashGrants([{ resource: "alpha-thing", action: "read" }]),
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
    expect(snapshot.steps[0]?.contentHash).toBe(await hashGrants([]));
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
      signAsPrincipal: async (kind, payload) => {
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
        childPublicKey: hexEncode(keyPair.publicKey),
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

describe("supervisor inbox FIFO dispatch loop", () => {
  async function buildFifoTestFixture(opts: {
    label: string;
    inbox: InboxPrimitives;
    deriveMailAuditRef?: (
      messageId: string,
      rawMessage: Uint8Array,
    ) => { store: string; path: string };
  }) {
    const baseDir = await makeTempDir(opts.label);
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
        pid: 11111,
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
    const mailBus = createMockMailBus();
    const baseBindings = await buildBindings({
      baseDir,
      spawner,
      signSpy: () => ({ sig: new Uint8Array(64), principalKind: "supervisor" }),
      mailBus,
      inboxPrimitives: opts.inbox,
    });
    const bindings: WorkflowSupervisorBindings = {
      ...baseBindings,
      ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
      ...(opts.deriveMailAuditRef !== undefined
        ? { deriveMailAuditRef: opts.deriveMailAuditRef }
        : {}),
    };
    const supervisor = createWorkflowSupervisor(bindings);
    const spawnPromise = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash-abc",
      warmKeep: false,
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
    await childSender.send({
      type: "ready",
      data: {
        childPid: 11111,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;
    return {
      supervisor,
      mailBus,
      supervisorToChild,
      childSender,
    };
  }

  test("default deriveMailAuditRef stamps `in-process` store on enqueued envelopes", async () => {
    const inbox = createMemoryInboxPrimitives();
    const { supervisor, mailBus } = await buildFifoTestFixture({
      label: "fifo-default-audit-",
      inbox,
    });
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("audit-default-1"),
    );
    // Wait for the enqueue to land in the in-memory inbox. The
    // dispatch loop may pull the entry into `processing` before the
    // assertion fires (the loop dequeues immediately once the
    // supervisor's spawn handshake completes), so the check covers
    // every claim-check substate.
    const deadline = Date.now() + 500;
    while (Date.now() < deadline) {
      const snap = inbox.snapshot("deployment-x@example.com");
      if (
        snap.inbox.size > 0 ||
        snap.processing.size > 0 ||
        snap.consumed.size > 0
      ) {
        break;
      }
      await new Promise((r) => setTimeout(r, 1));
    }
    const snapshot = inbox.snapshot("deployment-x@example.com");
    const all = [
      ...snapshot.consumed.values(),
      ...snapshot.processing.values(),
      ...snapshot.inbox.values(),
    ];
    expect(all.length).toBeGreaterThanOrEqual(1);
    const first = all[0];
    if (first === undefined) throw new Error("unreachable");
    expect(first.mailAuditRef.store).toBe("in-process");
    expect(first.mailAuditRef.path.length).toBeGreaterThan(0);
    await supervisor.shutdown();
  });

  test("deriveMailAuditRef override is invoked with messageId and stamps the envelope", async () => {
    const inbox = createMemoryInboxPrimitives();
    const observed: { messageId: string; len: number }[] = [];
    const { supervisor, mailBus } = await buildFifoTestFixture({
      label: "fifo-override-audit-",
      inbox,
      deriveMailAuditRef: (messageId, rawMessage) => {
        observed.push({ messageId, len: rawMessage.byteLength });
        return {
          store: "test-audit",
          path: `deployment-x/${messageId}`,
        };
      },
    });
    const payload = new TextEncoder().encode("audit-override-1");
    mailBus.deliver("deployment-x@example.com", payload);
    const deadline = Date.now() + 500;
    while (Date.now() < deadline && observed.length === 0) {
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(observed.length).toBe(1);
    const observedEntry = observed[0];
    if (observedEntry === undefined) throw new Error("unreachable");
    expect(observedEntry.len).toBe(payload.byteLength);
    expect(observedEntry.messageId.length).toBeGreaterThan(0);
    const overrideSnapshot = inbox.snapshot("deployment-x@example.com");
    const allEntries = [
      ...overrideSnapshot.inbox.values(),
      ...overrideSnapshot.processing.values(),
      ...overrideSnapshot.consumed.values(),
    ];
    expect(allEntries.length).toBeGreaterThanOrEqual(1);
    const first = allEntries[0];
    if (first === undefined) throw new Error("unreachable");
    expect(first.mailAuditRef.store).toBe("test-audit");
    expect(first.mailAuditRef.path).toBe(
      `deployment-x/${observedEntry.messageId}`,
    );
    await supervisor.shutdown();
  });

  test("two enqueued messages dispatch serially in FIFO order with terminal gating", async () => {
    const inbox = createMemoryInboxPrimitives();
    // The supervisor's per-cohort terminal broadcaster gates each
    // dispatch on a `terminal.event` upstream control frame the test
    // mints through the child IPC sender. Until the test sends the
    // frame, the dispatch loop sits on `waitForRunTerminal` for the
    // forwarded run.
    const { supervisor, mailBus, supervisorToChild, childSender } =
      await buildFifoTestFixture({
        label: "fifo-serial-",
        inbox,
      });
    // Two messages. With FIFO dispatch and terminal gating, exactly
    // one trigger.fire lands per terminal.event the test sends.
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("serial-msg-A"),
    );
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("serial-msg-B"),
    );
    // Helper that pulls the runId carried on the first/next
    // trigger.fire frame the supervisor wrote to the child stream.
    function triggerRunIds(): string[] {
      return parseTriggerFireRunIds(supervisorToChild.flushed());
    }
    // Wait for the first trigger.fire to land on the child stream.
    const deadlineOne = Date.now() + 500;
    while (Date.now() < deadlineOne) {
      if (triggerRunIds().length >= 1) break;
      await new Promise((r) => setTimeout(r, 1));
    }
    let firedIds = triggerRunIds();
    expect(firedIds.length).toBe(1);
    // Release the first run's terminal event. The dispatch loop
    // proceeds to markConsumed and pulls the second message.
    const firstRunId = firedIds[0];
    if (firstRunId === undefined) throw new Error("first run not minted");
    await childSender.send({
      type: "terminal.event",
      data: {
        runId: firstRunId,
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });
    const deadlineTwo = Date.now() + 500;
    while (Date.now() < deadlineTwo) {
      firedIds = triggerRunIds();
      if (firedIds.length >= 2) break;
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(firedIds.length).toBe(2);
    // The first message must be `markConsumed` before the second is
    // dispatched -- check the in-memory state.
    const consumed = inbox.snapshot("deployment-x@example.com").consumed;
    expect(consumed.size).toBeGreaterThanOrEqual(1);
    // Release the second run so the loop completes its cycle.
    const secondRunId = firedIds.find((id) => id !== firstRunId);
    if (secondRunId !== undefined) {
      await childSender.send({
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

  test("spawn-time replayProcessingToInbox moves orphaned processing entries back to inbox", async () => {
    const inbox = createMemoryInboxPrimitives();
    // Seed a `processing/` entry before the supervisor spawns. The
    // entry should be moved back to `inbox/` during `spawn()`.
    const state = inbox.snapshot("deployment-x@example.com");
    state.processing.set("1000-msg-orphan", {
      messageId: "msg-orphan",
      receivedAt: 1000,
      mailAuditRef: { store: "test", path: "test/orphan" },
    });
    // The test never sends a `terminal.event` upstream control frame
    // for the dispatched run, so the supervisor's per-cohort
    // broadcaster never settles the dispatch loop. The loop sits on
    // `waitForRunTerminal` after forwarding the trigger.fire, which
    // is the observation point the assertions below pin.
    const { supervisor, supervisorToChild } = await buildFifoTestFixture({
      label: "fifo-replay-spawn-",
      inbox,
    });
    // Wait for the dispatch loop to pull the recovered inbox entry
    // and send the trigger.fire. The terminal-event source above
    // never resolves so the loop sits on the dispatch indefinitely.
    const deadline = Date.now() + 500;
    while (Date.now() < deadline) {
      const flushed = supervisorToChild.flushed();
      const triggerFires = flushed.filter((f) => f.includes("trigger.fire"));
      if (triggerFires.length >= 1) break;
      await new Promise((r) => setTimeout(r, 1));
    }
    const triggerFires = supervisorToChild
      .flushed()
      .filter((f) => f.includes("trigger.fire"));
    expect(triggerFires.length).toBeGreaterThanOrEqual(1);
    // The processing entry was moved back to inbox during spawn,
    // then dequeued by the dispatch loop into processing again.
    const snapshot = inbox.snapshot("deployment-x@example.com");
    expect(snapshot.processing.size).toBe(1);
    const processingEntry = [...snapshot.processing.values()][0];
    if (processingEntry === undefined) throw new Error("unreachable");
    expect(processingEntry.messageId).toBe("msg-orphan");
    await supervisor.shutdown();
  });
});
