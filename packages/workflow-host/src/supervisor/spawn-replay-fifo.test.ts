// Regression tests for the spawn-time replay FIFO contract.
//
// H-S1 (FIFO breach): `spawn()` kicks `replayProcessingToInbox` off
// the critical path so orphaned `processing/` entries from a prior
// supervisor incarnation are moved back to `inbox/` ahead of the
// dispatch loop's first dequeue. A fresh `mail.inbound` that enqueues
// during that replay window must not ship to the child ahead of the
// orphan once the replay completes. The dispatch loop's first
// iteration awaits the spawn-time `replayDone` before its first
// `dequeueToProcessing`; this test pins that contract by gating the
// replay, injecting a fresh mail during the gated window, and
// asserting the orphan reaches the child first.
//
// H-S3 (shutdown races ahead of replay): `shutdownInternal` must
// await `state.replayDone` before tearing the bindings down.
// Without that await, a shutdown that lands during the replay window
// would let the substrate write outlive the supervisor and a
// subsequent boot could observe a partially-applied replay. This
// test pins the contract by gating the replay, calling `shutdown()`,
// asserting it does not settle while the gate is held, and then
// releasing the gate and asserting `shutdown()` completes.
//
// The replay is gated through an inboxPrimitives wrapper: the test
// returns a custom `replayProcessingToInbox` that awaits a
// test-controlled promise before delegating to the in-memory
// implementation, and exposes a `release()` callback the test fires
// to let the replay proceed.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

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
  ControlPayload,
  SignedEnvelope,
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
    subscribeMailForAddress(address, handler) {
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

type MemoryEntry = {
  messageId: string;
  receivedAt: number;
  mailAuditRef: { store: string; path: string };
};

type MemoryAddressState = {
  inbox: Map<string, MemoryEntry>;
  processing: Map<string, MemoryEntry>;
  consumed: Map<string, MemoryEntry>;
};

/**
 * Memory inbox primitives with a test-controllable gate on
 * `replayProcessingToInbox`. The gate is a manually-released
 * promise; `replayProcessingToInbox` awaits it before performing
 * the in-memory move. The harness also exposes `snapshot()` so the
 * test can directly inspect the queue partitioning while the gate
 * is held.
 */
function createGatedInboxPrimitives(): {
  primitives: InboxPrimitives;
  release: () => void;
  released: () => boolean;
  replaySettled: () => Promise<void>;
  snapshot: (address: string) => {
    inbox: string[];
    processing: string[];
    consumed: string[];
  };
} {
  const state = new Map<string, MemoryAddressState>();
  function getOrCreate(address: string): MemoryAddressState {
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
  let resolveGate: (() => void) | null = null;
  let gateReleased = false;
  const gate = new Promise<void>((resolve) => {
    resolveGate = resolve;
  });
  let resolveReplaySettled: (() => void) | null = null;
  const replaySettledPromise = new Promise<void>((resolve) => {
    resolveReplaySettled = resolve;
  });
  const primitives: InboxPrimitives = {
    async enqueueInbox(_store, _principal, _repoId, args) {
      const s = getOrCreate(args.address);
      const k = key(args.receivedAt, args.messageId);
      const envelope: MemoryEntry = {
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
      let envelope: MemoryEntry | null = null;
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
      await gate;
      const s = getOrCreate(address);
      const replayedKeys: string[] = [];
      for (const [k, value] of s.processing) {
        s.inbox.set(k, value);
        replayedKeys.push(k);
      }
      s.processing.clear();
      if (resolveReplaySettled !== null) {
        resolveReplaySettled();
        resolveReplaySettled = null;
      }
      return { commitSha: "memory", replayedKeys };
    },
  };
  return {
    primitives,
    release(): void {
      if (gateReleased) return;
      gateReleased = true;
      if (resolveGate !== null) {
        resolveGate();
        resolveGate = null;
      }
    },
    released(): boolean {
      return gateReleased;
    },
    async replaySettled(): Promise<void> {
      await replaySettledPromise;
    },
    snapshot(address: string) {
      const s = getOrCreate(address);
      return {
        inbox: [...s.inbox.values()].map((e) => e.messageId),
        processing: [...s.processing.values()].map((e) => e.messageId),
        consumed: [...s.consumed.values()].map((e) => e.messageId),
      };
    },
  };
}

/**
 * Pre-seed an orphaned `processing/` entry on the workflow-run
 * substrate. The test calls this BEFORE `spawn()`; the supervisor's
 * spawn-time `replayProcessingToInbox` is supposed to move the entry
 * back to `inbox/` before the dispatch loop's first dequeue. We
 * route through `enqueueInbox` + `dequeueToProcessing` so the seed
 * uses the same primitives the supervisor exercises -- a structural
 * miss surfaces here rather than via a divergent fixture.
 */
async function seedOrphanedProcessing(
  primitives: InboxPrimitives,
  store: RepoStore,
  repoId: RepoId,
  address: string,
  messageId: string,
  receivedAt: number,
): Promise<void> {
  // The seed only needs the messageId; the supervisor's primitives
  // do not validate the auditRef beyond passing it through.
  await primitives.enqueueInbox(store, { kind: "supervisor" }, repoId, {
    address,
    messageId,
    receivedAt,
    mailAuditRef: { store: "memory", path: `audit/${messageId}` },
  });
  const dequeued = await primitives.dequeueToProcessing(
    store,
    { kind: "supervisor" },
    repoId,
    address,
  );
  if (dequeued === null) {
    throw new Error("seed: dequeueToProcessing returned null");
  }
  if (dequeued.envelope.messageId !== messageId) {
    throw new Error(
      `seed: dequeueToProcessing returned ${dequeued.envelope.messageId}, expected ${messageId}`,
    );
  }
}

function readPayloadsOfType<T extends string>(
  lines: readonly string[],
  type_: T,
): Extract<typeof ControlPayload.infer, { type: T }>[] {
  const out: Extract<typeof ControlPayload.infer, { type: T }>[] = [];
  for (const line of lines) {
    let raw: unknown;
    try {
      raw = JSON.parse(line);
    } catch {
      continue;
    }
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) continue;
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) continue;
    if (payload.type !== type_) continue;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- arktype narrow above pins the discriminator
    out.push(payload as Extract<typeof ControlPayload.infer, { type: T }>);
  }
  return out;
}

type Harness = {
  supervisor: ReturnType<typeof createWorkflowSupervisor>;
  childSender: ReturnType<typeof createControlChannelSender>;
  childPublicKey: string;
  supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
  mailBus: ReturnType<typeof createMockMailBus>;
  gatedInbox: ReturnType<typeof createGatedInboxPrimitives>;
  bindings: WorkflowSupervisorBindings;
  workflowRunRepoId: RepoId;
  deploymentMailAddress: string;
};

/**
 * Boot a supervisor wired against gated inbox primitives. The caller
 * is responsible for seeding ORPHAN before `spawn()` is invoked --
 * the seed must land on the substrate via `gatedInbox.primitives`
 * BEFORE `boot()` runs (the supervisor reads the primitives by
 * reference, so seeding either before or after the wiring works
 * provided the seed completes before the spawn-time replay does).
 *
 * `boot()` does NOT drive the child's `ready` -- the caller does
 * that, since the H-S1 test wants to interleave gated-replay
 * inspections with the post-ready flow.
 */
async function boot(opts: { prefix: string }): Promise<
  Harness & {
    spawnPromise: Promise<unknown>;
    observedEnv: () => Record<string, string> | undefined;
  }
> {
  const baseDir = await makeTempDir(opts.prefix);
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
    return handle;
  };

  const mailBus = createMockMailBus();
  const gatedInbox = createGatedInboxPrimitives();
  const repoStore = createStubRepoStore(baseDir);
  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: "deployment-x",
  };
  const deploymentMailAddress = "deployment-x@example.com";

  const bindings: WorkflowSupervisorBindings = {
    repoStore,
    signAsPrincipal: async (): Promise<SignedPayload> => ({
      sig: new Uint8Array(64),
      principalKind: "supervisor",
    }),
    mailBus,
    subprocessSpawner: spawner,
    binaryPath: "/fake/bin/workflow-child",
    substrateEnv: { DATA_DIR: baseDir },
    workflowRunRepoId,
    workflowRunRef: "refs/heads/main",
    deploymentId: "deployment-x",
    deploymentMailAddress,
    readPrincipal: { kind: "supervisor" },
    deriveStepAddress: ({ deploymentId, stepId }) =>
      `${deploymentId}-${stepId}@example.com`,
    trivialLaunch: () => {
      throw new Error("trivialLaunch must not run in this test");
    },
    ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
    inboxPrimitives: gatedInbox.primitives,
  };

  const supervisor = createWorkflowSupervisor(bindings);
  const spawnPromise = supervisor.spawn({
    stepOrder: ["step-1"],
    definitionHash: "def-hash-abc",
    warmKeep: false,
    onInferenceEvent: () => {
      /* unused */
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
  // Wait until the mail bus has been registered so the test's
  // `deliver()` will route into the supervisor's onMailMessage.
  while (!mailBus.registered().includes(deploymentMailAddress)) {
    await new Promise((r) => setTimeout(r, 1));
  }
  const childPublicKey = hexEncode(childIpcKeyPair.publicKey);
  return {
    supervisor,
    childSender,
    childPublicKey,
    supervisorToChild,
    mailBus,
    gatedInbox,
    bindings,
    workflowRunRepoId,
    deploymentMailAddress,
    spawnPromise,
    observedEnv: () => observedEnv,
  };
}

async function sendReady(
  childSender: ReturnType<typeof createControlChannelSender>,
  childPublicKey: string,
  childPid: number,
): Promise<void> {
  await childSender.send({
    type: "ready",
    data: {
      childPid,
      childPublicKey,
    },
  });
}

describe("supervisor spawn-time replay FIFO contract", () => {
  test("H-S1: a fresh mail that lands during the spawn-time replay window cannot ship to the child ahead of an orphaned processing/ entry", async () => {
    // Boot the supervisor with the replay gated. The orphan must be
    // seeded BEFORE `spawn()` invokes `replayProcessingToInbox` --
    // because the gate awaits a promise the replay itself blocks on,
    // we seed via the same memory primitives before the boot returns
    // control to the test. The seed runs synchronously against the
    // in-memory state map; the supervisor's spawn-time replay (now
    // pending behind the gate) will reclaim it on release.
    const ORPHAN_ID = "orphan-msg-id";
    const ORPHAN_RECEIVED_AT = 1_000;
    const harness = await boot({ prefix: "hs1-fifo-" });
    // Pre-seed the orphan against the gated primitives' memory state.
    // The supervisor has already invoked the spawner by now (boot
    // waits for the spawner env to be observed), and the spawn-time
    // replay is queued behind the gate; the seed simply mutates the
    // in-memory state map under the address so the replay's eventual
    // iteration over `processing/` picks ORPHAN up.
    await seedOrphanedProcessing(
      harness.gatedInbox.primitives,
      harness.bindings.repoStore,
      harness.workflowRunRepoId,
      harness.deploymentMailAddress,
      ORPHAN_ID,
      ORPHAN_RECEIVED_AT,
    );
    // Confirm the seed lives in `processing/` -- if this fails,
    // we never had a real orphan to race against.
    {
      const snap = harness.gatedInbox.snapshot(harness.deploymentMailAddress);
      expect(snap.processing).toEqual([ORPHAN_ID]);
      expect(snap.inbox).toEqual([]);
    }

    // Drive `ready` so the supervisor reaches `running`. The dispatch
    // loop is started inside `spawn()` and is now blocked on the
    // gated replayDone awaited as its first step.
    await sendReady(harness.childSender, harness.childPublicKey, 7777);
    await harness.spawnPromise;

    // While the replay is still gated, inject a fresh mail. The
    // mail bus dispatch synchronously fans the message into
    // `onMailMessage`, which schedules `enqueueInboundMail` off the
    // event loop. Wait until the fresh mail lands in `inbox/` so the
    // race condition is fully armed before we inspect.
    const freshMessage = new TextEncoder().encode("fresh@example.com");
    harness.mailBus.deliver(harness.deploymentMailAddress, freshMessage);
    const armDeadline = Date.now() + 2_000;
    while (Date.now() < armDeadline) {
      const snap = harness.gatedInbox.snapshot(harness.deploymentMailAddress);
      if (snap.inbox.length === 1) break;
      await new Promise((r) => setTimeout(r, 1));
    }

    // CRITICAL ASSERTION (H-S1): the orphan is still in `processing/`
    // and the fresh mail is in `inbox/`. The dispatch loop's first
    // `dequeueToProcessing` MUST NOT have run yet -- without the
    // `await replayGate` guarding it, the loop would have already
    // dequeued the fresh mail (the only entry currently in `inbox/`)
    // and forwarded it to the child as a `trigger.fire`.
    const gatedSnap = harness.gatedInbox.snapshot(
      harness.deploymentMailAddress,
    );
    expect(gatedSnap.processing).toEqual([ORPHAN_ID]);
    expect(gatedSnap.inbox.length).toBe(1);
    expect(gatedSnap.inbox[0]).not.toBe(ORPHAN_ID);
    const freshId = gatedSnap.inbox[0];
    if (freshId === undefined) throw new Error("freshId undefined");

    // Also assert nothing has been forwarded to the child yet --
    // any `trigger.fire` on the wire here would prove the dispatch
    // loop skipped past the gate.
    {
      const triggers = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "trigger.fire",
      );
      expect(triggers).toEqual([]);
    }

    // Release the gate. The replay moves ORPHAN_ID from `processing/`
    // back into `inbox/`; the dispatch loop's first `dequeueToProcessing`
    // runs against an inbox that now contains both messages, and the
    // FIFO sort (receivedAt ascending) ships ORPHAN_ID first because
    // its receivedAt (1_000) precedes the fresh mail's wall-clock
    // receivedAt by many orders of magnitude.
    harness.gatedInbox.release();
    await harness.gatedInbox.replaySettled();

    // Wait for the first `trigger.fire` and assert it carries
    // ORPHAN_ID. The supervisor blocks the loop on the run's
    // terminal event after the first forward, so we observe the
    // first forward and then drive the terminal event so the loop
    // can advance to the second.
    const firstTriggerDeadline = Date.now() + 2_000;
    let firstTriggerRunId: string | null = null;
    while (firstTriggerRunId === null && Date.now() < firstTriggerDeadline) {
      const triggers = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "trigger.fire",
      );
      if (triggers.length > 0) {
        const first = triggers[0];
        if (first !== undefined) firstTriggerRunId = first.data.runId;
      }
      if (firstTriggerRunId === null) {
        await new Promise((r) => setTimeout(r, 1));
      }
    }
    expect(firstTriggerRunId).toBe(ORPHAN_ID);

    // Drive ORPHAN's terminal event back through the child sender so
    // the dispatch loop's `waitForRunTerminal` resolves and the next
    // iteration runs. The fresh mail must follow.
    await harness.childSender.send({
      type: "terminal.event",
      data: {
        runId: ORPHAN_ID,
        kind: "RunCompleted",
        seq: 0,
        at: "test",
      },
    });

    const secondTriggerDeadline = Date.now() + 2_000;
    let secondTriggerRunId: string | null = null;
    while (secondTriggerRunId === null && Date.now() < secondTriggerDeadline) {
      const triggers = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "trigger.fire",
      );
      if (triggers.length >= 2) {
        const second = triggers[1];
        if (second !== undefined) secondTriggerRunId = second.data.runId;
      }
      if (secondTriggerRunId === null) {
        await new Promise((r) => setTimeout(r, 1));
      }
    }
    expect(secondTriggerRunId).toBe(freshId);

    // Drive the fresh run's terminal so the loop unwinds cleanly,
    // then tear the supervisor down.
    await harness.childSender.send({
      type: "terminal.event",
      data: {
        runId: freshId,
        kind: "RunCompleted",
        seq: 0,
        at: "test",
      },
    });
    await harness.supervisor.shutdown();
  });

  test("H-S3: shutdown() called during phase=starting awaits the spawn-time replay before tearing the bindings down", async () => {
    // Boot with the replay gated but DO NOT drive `ready`. The
    // supervisor remains in phase `starting`: the dispatch loop has
    // not been started yet (`state.dispatchLoop === null`), so the
    // only thing that can serialize shutdown against the in-flight
    // replay is the explicit `await prior.replayDone` inside
    // `shutdownInternal`. Without that await (H-S3), the shutdown
    // path tears the bindings down while the replay's substrate
    // write is still in flight, and a subsequent boot could observe
    // a partially-applied replay.
    //
    // Asserting on phase `starting` specifically eliminates the
    // confounder where `await prior.dispatchLoop` would also
    // happen to block on the gated replay (because the loop's first
    // step also awaits the same gate). In `starting` the loop is
    // null, so the contract under test is uniquely load-bearing.
    const ORPHAN_ID = "orphan-msg-id";
    const ORPHAN_RECEIVED_AT = 1_000;
    const harness = await boot({ prefix: "hs3-shutdown-await-" });
    // Swallow the inevitable spawn() rejection -- when shutdown
    // kills the child handle, `waitForReady` throws "control channel
    // ended before child emitted ready" and surfaces as a spawn()
    // rejection (Gap B behavior). The H-S3 test is orthogonal to
    // that; we only care about the shutdown ordering vs replayDone.
    harness.spawnPromise.catch(() => {
      /* swallowed: orthogonal to H-S3. */
    });
    await seedOrphanedProcessing(
      harness.gatedInbox.primitives,
      harness.bindings.repoStore,
      harness.workflowRunRepoId,
      harness.deploymentMailAddress,
      ORPHAN_ID,
      ORPHAN_RECEIVED_AT,
    );

    // Concurrently call `shutdown()`. Capture when its promise
    // settles. A `shutdownSettledAt` of `-1` while the gate is
    // still held proves the shutdown path is blocked on
    // `replayDone`.
    let shutdownSettledAt = -1;
    let replaySettledAt = -1;
    const shutdownPromise = harness.supervisor.shutdown().then(() => {
      shutdownSettledAt = Date.now();
    });
    void harness.gatedInbox.replaySettled().then(() => {
      replaySettledAt = Date.now();
    });

    // Wait ~20ms and assert shutdown is still pending.
    await new Promise((r) => setTimeout(r, 20));
    expect(shutdownSettledAt).toBe(-1);
    expect(harness.gatedInbox.released()).toBe(false);

    // Release the gate and let everything unwind.
    harness.gatedInbox.release();
    await harness.gatedInbox.replaySettled();
    await shutdownPromise;

    // Shutdown must have settled AFTER the replay -- i.e. it waited.
    expect(shutdownSettledAt).toBeGreaterThanOrEqual(0);
    expect(replaySettledAt).toBeGreaterThanOrEqual(0);
    expect(shutdownSettledAt).toBeGreaterThanOrEqual(replaySettledAt);
  });
});
