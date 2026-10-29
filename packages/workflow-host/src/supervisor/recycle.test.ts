// Recycle path tests against fake bindings.
//
// The recycle path's six-step sequence (drain -> kill -> respawn ->
// self-discover -> resume -> drain-buffer) is exercised here through
// the supervisor's caller-facing `recycle()` API plus direct calls
// into `createRecyclePolicy` for the policy-origin coverage.
//
// Strictly orthogonal to redeploy: every test below operates against
// the same fake definition across the recycle. None of these tests
// re-seeds the workflow definition or mutates step credentials in a
// way that would conflate the recycle path with a redeploy.

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  test,
  expect,
} from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto";
import { base64Encode, hexEncode } from "@intx/types";
import { deriveDeliveryKey, deriveMessageId } from "@intx/types/message-id";
import { claimCheckIdentity } from "@intx/hub-sessions";
import type { CredentialDelivery } from "@intx/types/credential-delivery";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { signalName } from "@intx/types/signals";
import type { RepoStore } from "@intx/hub-sessions";
import { NotInProcessingError } from "@intx/hub-sessions";
import {
  createLogCapture,
  createMemoryFrameStream,
  createMemoryNdjsonStream,
  createMockMailBus,
  parseTriggerFireRunIds,
  createChangeNotifier,
  waitForTriggerFireRunIds,
  waitForUpstreamPayload,
  waitForUpstreamPayloads,
  createStubRepoStore,
  sendDefinition,
  TEST_DEFINITION,
  TEST_DEFINITION_HASH,
  inboundDelivery,
} from "@intx/workflow-host/testing";

import {
  createWorkflowSupervisor,
  StandbyAbandonedError,
  StandbyDeferredError,
  StandbyRefusedError,
  type InboxPrimitives,
  type MailBusBindings,
  type RecordedStandby,
  type SignedPayload,
  type SubprocessHandle,
  type SubprocessSpawner,
  type WorkflowSupervisor,
  type WorkflowSupervisorBindings,
} from "./index";
import {
  DEFAULT_KILL_TIMEOUT_MS,
  defaultClearTimer,
} from "./child-termination";
import {
  createRecyclePolicy,
  DEFAULT_POLICY_INTERVAL_MS,
  type RecyclePolicyBounds,
} from "./recycle";
import { waitUntil } from "@intx/types/testing";
import { mergeCredentialDelivery } from "../child/credential-cell";
import { parseSpawnTimeEnv } from "../child/env-bootstrap";
import {
  createControlChannelSender,
  ControlPayload,
  SignedEnvelope,
} from "../ipc/index";
import { seedStepGrants, seededOnRunStart } from "./step-grants.test-helpers";
import type { ParkedCorrelation } from "../ipc/control-payloads";

function parseSignalDeliverRunIds(lines: readonly string[]): string[] {
  const ids: string[] = [];
  for (const line of lines) {
    if (!line.includes("signal.deliver")) continue;
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) continue;
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) continue;
    if (payload.type !== "signal.deliver") continue;
    ids.push(payload.data.runId);
  }
  return ids;
}

// Every downstream frame on a child's stream, in send order.
function parseControlFrames(lines: readonly string[]): ControlPayload[] {
  return lines.flatMap((line) => {
    const raw: unknown = JSON.parse(line);
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) return [];
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) return [];
    return [payload];
  });
}

function authoritySetFrames(lines: readonly string[]) {
  return parseControlFrames(lines).flatMap((frame) =>
    frame.type === "authority.set" ? [frame.data] : [],
  );
}

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

afterAll(async () => {
  for (const dir of tempDirs.splice(0)) {
    await fs.rm(dir, { recursive: true, force: true });
  }
});

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
  spawnEnvs: Record<string, string>[];
  totalSpawns: number;
  /**
   * Resolve once the spawner has produced at least `count` children. A
   * recycle spawns the replacement asynchronously, so a test driving the new
   * child's handshake has to wait for it to exist.
   */
  awaitChildren(count: number): Promise<void>;
};

function createSpawnTracker(opts: {
  sigtermExits?: boolean;
  killThrows?: boolean;
  // Fails the supervisor's control write of a line, as a closing pipe does.
  controlWriteFails?: (line: string) => boolean;
  // Holds the supervisor's control write of a line until the promise it
  // returns settles.
  holdControlWrite?: (line: string) => Promise<undefined> | undefined;
}): SpawnTracker {
  const children: FakeChild[] = [];
  const spawnEnvs: Record<string, string>[] = [];
  // Reports each spawn so a test can wait for the child rather than re-read
  // the array on a timer.
  const changes = createChangeNotifier();
  const spawner: SubprocessSpawner = ({ env }) => {
    spawnEnvs.push(env);
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
    changes.notify();
    const { controlWriteFails, holdControlWrite } = opts;
    const handle: SubprocessHandle = {
      pid: child.pid,
      controlWriter:
        controlWriteFails === undefined && holdControlWrite === undefined
          ? supervisorToChild.writer
          : {
              write: (line) => {
                if (controlWriteFails?.(line) === true)
                  throw new Error("the child's control pipe is closed");
                const held = holdControlWrite?.(line);
                return held === undefined
                  ? supervisorToChild.writer.write(line)
                  : held.then(() => supervisorToChild.writer.write(line));
              },
            },
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
        // Settle the exit above BEFORE throwing so a shutdown awaiting
        // `exited` does not hang; the throw exercises the kill-step guard.
        if (opts.killThrows === true) {
          throw new Error("child kill boom");
        }
      },
      exited,
    };
    return handle;
  };
  return {
    spawner,
    children,
    spawnEnvs,
    get totalSpawns() {
      return children.length;
    },
    awaitChildren: (count: number) =>
      changes.until(() => children.length >= count),
  };
}

// The sender each fake child reported its definition on, so its `ready`
// follows on the same sender and sequence.
const definitionSenders = new WeakMap<
  FakeChild,
  ReturnType<typeof createControlChannelSender>
>();

// Report the child's definition, the first frame a real child sends.
async function driveDefinition(
  child: FakeChild,
  ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array },
  projection: WorkflowProjectionDefinition = TEST_DEFINITION,
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
  await sendDefinition(childSender, {
    childPid: child.pid,
    childPublicKey: hexEncode(ipcKeypair.publicKey),
    projection,
  });
  definitionSenders.set(child, childSender);
  return childSender;
}

async function driveReady(
  child: FakeChild,
  ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array },
): Promise<ReturnType<typeof createControlChannelSender>> {
  const childSender =
    definitionSenders.get(child) ?? (await driveDefinition(child, ipcKeypair));
  await childSender.send({ type: "ready" });
  return childSender;
}

function createMemoryInboxPrimitives(): InboxPrimitives & {
  messageIds(address: string): {
    inbox: string[];
    processing: string[];
    consumed: string[];
  };
} {
  type Entry = {
    messageId: string;
    authenticatedSender?: string;
    deliveryKey?: string;
    receivedAt: number;
    rawMessage?: string;
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
      const deliveryKey = await deriveDeliveryKey(
        args.authenticatedSender,
        args.messageId,
      );
      const k = key(args.receivedAt, deliveryKey);
      const envelope: Entry = {
        messageId: args.messageId,
        authenticatedSender: args.authenticatedSender,
        deliveryKey,
        receivedAt: args.receivedAt,
        ...(args.rawMessage !== undefined
          ? { rawMessage: args.rawMessage }
          : {}),
      };
      s.inbox.set(k, envelope);
      return {
        outcome: "enqueued",
        commitSha: "memory",
        inboxKey: k,
        envelope: {
          messageId: args.messageId,
          authenticatedSender: args.authenticatedSender,
          deliveryKey,
          receivedAt: args.receivedAt,
          address: args.address,
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
          ...(envelope.authenticatedSender !== undefined
            ? { authenticatedSender: envelope.authenticatedSender }
            : {}),
          ...(envelope.deliveryKey !== undefined
            ? { deliveryKey: envelope.deliveryKey }
            : {}),
          receivedAt: envelope.receivedAt,
          address,
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
        if (claimCheckIdentity(value) === args.identity) {
          foundKey = k;
          envelope = value;
          break;
        }
      }
      if (s.consumed.has(args.identity)) {
        throw new NotInProcessingError(
          `claim_check_already_consumed: ${args.identity}`,
        );
      }
      if (foundKey === null || envelope === null) {
        throw new NotInProcessingError(
          `claim_check_processing_not_found: ${args.identity}`,
        );
      }
      s.processing.delete(foundKey);
      s.consumed.set(args.identity, envelope);
      return {
        commitSha: "memory",
        envelope: {
          messageId: envelope.messageId,
          ...(envelope.authenticatedSender !== undefined
            ? { authenticatedSender: envelope.authenticatedSender }
            : {}),
          ...(envelope.deliveryKey !== undefined
            ? { deliveryKey: envelope.deliveryKey }
            : {}),
          receivedAt: envelope.receivedAt,
          address: args.address,
          runId: args.runId,
          consumedAt: args.consumedAt,
        },
        watermark: 0,
        prunedIdentities: [],
      };
    },
    async hasInboxEntries(_store, _principal, _repoId, address) {
      return getOrCreate(address).inbox.size > 0;
    },
    async replayProcessingToInbox(_store, _principal, _repoId, address, opts) {
      const s = getOrCreate(address);
      const replayedKeys: string[] = [];
      const retainedIdentities: string[] = [];
      for (const [k, value] of s.processing) {
        if (opts?.onlyIdentities?.has(claimCheckIdentity(value)) === false) {
          continue;
        }
        if (opts?.ownedIdentities?.has(claimCheckIdentity(value)) === true) {
          retainedIdentities.push(claimCheckIdentity(value));
          continue;
        }
        s.processing.delete(k);
        s.inbox.set(k, value);
        replayedKeys.push(k);
      }
      return { commitSha: "memory", replayedKeys, retainedIdentities };
    },
    messageIds(address) {
      const s = getOrCreate(address);
      const ids = (entries: Map<string, Entry>) =>
        [...entries.values()].map((entry) => entry.messageId);
      return {
        inbox: ids(s.inbox),
        processing: ids(s.processing),
        consumed: ids(s.consumed),
      };
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
  onRunStart?: WorkflowSupervisorBindings["onRunStart"];
  credentialDelivery?: WorkflowSupervisorBindings["credentialDelivery"];
}): Promise<WorkflowSupervisorBindings> {
  const repoStore = createStubRepoStore(opts.baseDir, { writeTree: true });
  return {
    repoStore,
    signAsPrincipal: async (): Promise<SignedPayload> => ({
      sig: new Uint8Array(64),
      principalKind: "supervisor",
    }),
    mailBus: opts.mailBus,
    subprocessSpawner: opts.spawner,
    binaryPath: "/fake/bin/workflow-child",
    // Source-ref is the only deploy lineage, so the child requires
    // CLOSURE_PACKAGE_DIR to evaluate its pinned closure. In production the
    // sidecar threads it through the frozen substrate env; the fake spawner
    // never runs the real child, so a placeholder dir satisfies the
    // parseSpawnTimeEnv contract this suite asserts on the captured envs.
    substrateEnv: {
      DATA_DIR: opts.baseDir,
      CLOSURE_PACKAGE_DIR: "/fake/closure/package",
    },
    dynamicSpawnEnv: () => ({}),
    workflowRunRepoId: { kind: "workflow-run", id: "run_deployment-x" },
    workflowRunRef: "refs/heads/main",
    anchorRunId: "run_deployment-x",
    deploymentMailAddress: "run_deployment-x@example.com",
    onRunStart: seededOnRunStart(opts.baseDir),
    layoutSteps: () => Promise.resolve(["step-1"]),
    ipcKeyPairFactory: () => Promise.resolve(opts.ipcKeypair),
    inboxPrimitives: opts.inboxPrimitives ?? createMemoryInboxPrimitives(),
    ...(opts.recyclePolicy !== undefined
      ? { recyclePolicy: opts.recyclePolicy }
      : {}),
    ...(opts.onRunStart !== undefined ? { onRunStart: opts.onRunStart } : {}),
    ...(opts.credentialDelivery !== undefined
      ? { credentialDelivery: opts.credentialDelivery }
      : {}),
  };
}

async function spawnSupervisor(opts: {
  baseDir: string;
  tracker: SpawnTracker;
  mailBus: MailBusBindings;
  ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
  recyclePolicy?: RecyclePolicyBounds;
  onRunStart?: WorkflowSupervisorBindings["onRunStart"];
  credentialDelivery?: WorkflowSupervisorBindings["credentialDelivery"];
  overrides?: Partial<WorkflowSupervisorBindings>;
}) {
  await seedStepGrants(opts.baseDir, "step-1", [
    { resource: "thing", action: "read" },
  ]);
  const bindings = await buildBindings({
    baseDir: opts.baseDir,
    spawner: opts.tracker.spawner,
    mailBus: opts.mailBus,
    ipcKeypair: opts.ipcKeypair,
    ...(opts.recyclePolicy !== undefined
      ? { recyclePolicy: opts.recyclePolicy }
      : {}),
    ...(opts.onRunStart !== undefined ? { onRunStart: opts.onRunStart } : {}),
    ...(opts.credentialDelivery !== undefined
      ? { credentialDelivery: opts.credentialDelivery }
      : {}),
  });
  const supervisor = createWorkflowSupervisor({
    ...bindings,
    ...opts.overrides,
  });
  const spawnPromise = supervisor.spawn({
    definitionHash: TEST_DEFINITION_HASH,

    onInferenceEvent: () => undefined,
  });
  // Wait until the spawner has been invoked and the channelId is
  // known, then drive the synthetic child's `ready` frame.
  await opts.tracker.awaitChildren(1);
  const first = opts.tracker.children[0];
  if (first === undefined) {
    throw new Error("tracker.children[0] missing");
  }
  const firstSender = await driveReady(first, opts.ipcKeypair);
  const result = await spawnPromise;
  return { supervisor, spawnResult: result, firstSender };
}

// A new child's handshake arms one deadline for its definition and, once
// that arrives, one for its `ready`. A test fires them by hand rather than
// racing a real clock. Any other timer, such as a kill escalation, fires on
// the next macrotask unless it is cleared first.
function createHandshakeDeadlines(readyTimeoutMs: number) {
  const deadlines: (() => void)[] = [];
  const cleared = new Set<unknown>();
  const changes = createChangeNotifier();
  return {
    timers: {
      readyTimeoutMs,
      setTimer: (cb: () => void, ms: number): unknown => {
        const handle = {};
        if (ms === readyTimeoutMs) {
          deadlines.push(cb);
          changes.notify();
        } else {
          setTimeout(() => {
            if (!cleared.has(handle)) cb();
          }, 0);
        }
        return handle;
      },
      clearTimer: (handle: unknown) => {
        cleared.add(handle);
      },
    },
    fire: async (nth: number): Promise<void> => {
      await changes.until(() => deadlines.length > nth);
      deadlines[nth]?.();
    },
  };
}

describe("supervisor spawn: failure cleanup", () => {
  test("a spawn that times out releases the mail subscription and address", async () => {
    // Regression: spawn registers + subscribes the deployment mail address
    // before the ready handshake. A handshake that times out (or the child
    // exiting mid-handshake) must release that subscription and unregister
    // the address; otherwise a live subscription lingers with no dispatch
    // loop to service it and a redeploy adds a second one.
    const baseDir = await makeTempDir("spawn-timeout-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    // sigtermExits so the timeout path's kill settles the fake child.
    const tracker = createSpawnTracker({ sigtermExits: true });
    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus,
      ipcKeypair,
    });
    const deadlines = createHandshakeDeadlines(4_242);
    const supervisor = createWorkflowSupervisor({
      ...bindings,
      ...deadlines.timers,
    });

    // The child reports its definition but never sends `ready`, so the
    // handshake times out after the address is registered.
    const spawning = supervisor.spawn({
      definitionHash: TEST_DEFINITION_HASH,
      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
    const child = tracker.children[0];
    if (child === undefined) throw new Error("tracker.children[0] missing");
    await driveDefinition(child, ipcKeypair);
    await deadlines.fire(1);
    await expect(spawning).rejects.toThrow(/did not emit ready/);

    // The address must not remain registered, and the history must carry
    // both the register and its matching unregister.
    expect(mailBus.registered()).not.toContain("run_deployment-x@example.com");
    expect(mailBus.registrationHistory()).toContain(
      "register:run_deployment-x@example.com",
    );
    expect(mailBus.registrationHistory()).toContain(
      "unregister:run_deployment-x@example.com",
    );
  });

  test("a throwing teardown step during spawn cleanup is absorbed", async () => {
    // On a failed spawn the spawn-catch unwinds the cohort through
    // shutdownInternal. Its teardown steps are individually guarded and the
    // child kill lives in its `finally`, so a throwing step (here the mail
    // unsubscribe) is absorbed end to end: the spawn still rejects with the
    // real ready-timeout cause rather than the secondary teardown error,
    // and the child is still reaped. The spawn-catch guard that preserves
    // the cause is now defense-in-depth -- shutdownInternal is total by
    // construction -- so the discriminating coverage of the teardown guards
    // themselves lives in the "supervisor shutdown: teardown robustness"
    // block below, which drives shutdownInternal directly.
    const baseDir = await makeTempDir("spawn-teardown-mask-");
    const ipcKeypair = await generateKeyPair();
    const baseMailBus = createMockMailBus();
    // The mail unsubscribe throws during the spawn-failure unwind.
    const mailBus: MailBusBindings = {
      ...baseMailBus,
      subscribeMailForAddress(address, handler) {
        baseMailBus.subscribeMailForAddress(address, handler);
        return () => {
          throw new Error("mail unsubscribe boom during teardown");
        };
      },
    };
    // sigtermExits so the ready-timeout reap settles the fake child.
    const tracker = createSpawnTracker({ sigtermExits: true });
    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus,
      ipcKeypair,
    });
    const deadlines = createHandshakeDeadlines(4_242);
    const supervisor = createWorkflowSupervisor({
      ...bindings,
      ...deadlines.timers,
    });

    // Report the definition but never drive `ready`, so the handshake
    // times out after the mail subscription exists. The spawn cause (ready
    // timeout) must surface even though the teardown then throws its own
    // error.
    const spawning = supervisor.spawn({
      definitionHash: TEST_DEFINITION_HASH,
      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
    const child = tracker.children[0];
    if (child === undefined) throw new Error("tracker.children[0] missing");
    await driveDefinition(child, ipcKeypair);
    await deadlines.fire(1);
    await expect(spawning).rejects.toThrow(/did not emit ready/);

    // The child was reaped during the unwind despite the throwing
    // unsubscribe -- a throwing teardown step leaves nothing orphaned.
    expect(child.killSignals.length).toBeGreaterThan(0);
  });
});

describe("supervisor spawn: dynamic env", () => {
  test("a recycle respawn re-pulls dynamicSpawnEnv so a host revision survives", async () => {
    // Mechanism behind a source rotation surviving a recycle: the spawn-env
    // builder pulls dynamicSpawnEnv() on every spawn AND respawn, so a value
    // the host revised between the two reaches the respawned child instead
    // of reverting to the frozen substrateEnv value.
    const baseDir = await makeTempDir("recycle-dynenv-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
    let hostRevised = "before-rotation";
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus,
      ipcKeypair,
    });
    const supervisor = createWorkflowSupervisor({
      ...bindings,
      dynamicSpawnEnv: () => ({ DYNAMIC_MARKER: hostRevised }),
    });

    const spawnPromise = supervisor.spawn({
      definitionHash: TEST_DEFINITION_HASH,

      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
    const firstChild = tracker.children[0];
    if (firstChild === undefined) throw new Error("first child missing");
    await driveReady(firstChild, ipcKeypair);
    await spawnPromise;
    expect(tracker.spawnEnvs[0]?.DYNAMIC_MARKER).toBe("before-rotation");

    // The host revises the dynamic value (models a live source rotation),
    // then a recycle respawns the child.
    hostRevised = "after-rotation";
    const recyclePromise = supervisor.recycle({ reason: "dynenv" });
    await tracker.awaitChildren(2);
    const secondChild = tracker.children[1];
    if (secondChild === undefined) throw new Error("second child missing");
    await driveReady(secondChild, ipcKeypair);
    await recyclePromise;

    // The respawn env carries the REVISED value -- the rotation survived.
    expect(tracker.spawnEnvs[1]?.DYNAMIC_MARKER).toBe("after-rotation");
  });
});

describe("supervisor recycle: operator-initiated", () => {
  test("the recycle respawn env satisfies the child spawn-time env contract", async () => {
    // Regression: the recycle respawn env must carry every required
    // spawn-time key. A recycle env that omitted a required key made the
    // respawned child abort in parseSpawnTimeEnv before opening IPC, so
    // every recycle tore the deployment down. The fake spawner does not run
    // parseSpawnTimeEnv, so this asserts the captured env against it
    // directly -- the check the real child binary performs at boot.
    const baseDir = await makeTempDir("recycle-env-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
    });

    const recyclePromise = supervisor.recycle({ reason: "env-contract" });
    await tracker.awaitChildren(2);
    const secondChild = tracker.children[1];
    if (secondChild === undefined) {
      throw new Error("second child missing after recycle spawn");
    }
    await driveReady(secondChild, ipcKeypair);
    await recyclePromise;

    expect(tracker.spawnEnvs).toHaveLength(2);
    const spawnEnv = tracker.spawnEnvs[0];
    const respawnEnv = tracker.spawnEnvs[1];
    if (spawnEnv === undefined || respawnEnv === undefined) {
      throw new Error("expected two captured spawn envs");
    }
    // Both the initial spawn and the recycle respawn must satisfy the
    // child's spawn-time env contract, for the same definition.
    const spawned = parseSpawnTimeEnv(spawnEnv);
    const respawned = parseSpawnTimeEnv(respawnEnv);
    expect(respawned.definitionHash).toBe(spawned.definitionHash);
    expect(respawned.closurePackageDir).toBe(spawned.closurePackageDir);
  });

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
    await tracker.awaitChildren(2);
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
      definitionHash: TEST_DEFINITION_HASH,

      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
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

describe("supervisor recycle: respawn handshake bound", () => {
  // Each frame of the respawn's handshake has its own deadline, so a child
  // that hangs while it loads its closure is told apart from one that hangs
  // resuming its runs.
  test.each([
    {
      frame: "its definition",
      reportsDefinition: false,
      deadline: 0,
      error: /child did not report its definition within 4242ms; killed/,
    },
    {
      frame: "ready",
      reportsDefinition: true,
      deadline: 1,
      error: /child did not emit ready within 4242ms; killed/,
    },
  ])(
    "a respawn that never sends $frame times out, reaps the new child, and reaches a clean stopped state",
    async ({ reportsDefinition, deadline, error }) => {
      const baseDir = await makeTempDir("recycle-handshake-timeout-");
      const ipcKeypair = await generateKeyPair();
      const mailBus = createMockMailBus();
      // sigtermExits:false so the reap escalates SIGTERM -> SIGKILL: a wedged
      // respawn is exactly the child that ignores SIGTERM, and the reap must
      // still guarantee it dies.
      const tracker = createSpawnTracker({ sigtermExits: false });
      await seedStepGrants(baseDir, "step-1", [
        { resource: "thing", action: "read" },
      ]);
      const bindings = await buildBindings({
        baseDir,
        spawner: tracker.spawner,
        mailBus,
        ipcKeypair,
      });
      const selfTerminations: {
        phase: "stopped" | "crash-looping";
        reason: string;
      }[] = [];
      // The recycle path's handshake deadlines fire by hand and its kill
      // escalations on the next macrotask. The spawn path keeps real timers,
      // so the first child readies normally.
      const deadlines = createHandshakeDeadlines(4_242);
      const supervisor = createWorkflowSupervisor({
        ...bindings,
        readyTimeoutMs: deadlines.timers.readyTimeoutMs,
        recyclePolicySetTimer: deadlines.timers.setTimer,
        recyclePolicyClearTimer: deadlines.timers.clearTimer,
        onSelfTerminate: (info) => selfTerminations.push(info),
      });
      const spawnPromise = supervisor.spawn({
        definitionHash: TEST_DEFINITION_HASH,

        onInferenceEvent: () => undefined,
      });
      await tracker.awaitChildren(1);
      const first = tracker.children[0];
      if (first === undefined) throw new Error("first child missing");
      await driveReady(first, ipcKeypair);
      await spawnPromise;

      const recycling = supervisor.recycle({
        reason: "handshake-timeout-test",
      });
      await tracker.awaitChildren(2);
      const second = tracker.children[1];
      if (second === undefined) throw new Error("second child missing");
      if (reportsDefinition) await driveDefinition(second, ipcKeypair);
      await deadlines.fire(deadline);
      await expect(recycling).rejects.toThrow(error);

      // The wedged respawn was reaped through the full SIGTERM -> SIGKILL
      // ladder rather than left running as an orphan.
      expect(second.killSignals).toEqual(["SIGTERM", "SIGKILL"]);

      // The recycle failure routed through shutdownInternal to `stopped`, so
      // a follow-up shutdown is a no-op.
      await supervisor.shutdown();

      // The recycle failure is a self-termination: the supervisor drove itself
      // down without a host `shutdown()` request, so the sink fired once with
      // the terminal `stopped` phase. The no-op follow-up shutdown above must
      // not add a second fire.
      expect(selfTerminations).toHaveLength(1);
      expect(selfTerminations[0]?.phase).toBe("stopped");
      expect(selfTerminations[0]?.reason).toMatch(/recycle failed/);
    },
  );

  test("a respawn whose control channel ends before ready rejects and reaps the new child", async () => {
    const baseDir = await makeTempDir("recycle-ready-failed-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    // sigtermExits:true so the reap's SIGTERM settles the fake child; this
    // path proves the child is killed even though the control channel end
    // does not itself terminate the process.
    const tracker = createSpawnTracker({ sigtermExits: true });
    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus,
      ipcKeypair,
    });
    // Default (real, 30s) ready timer: the deadline must NOT fire, so the
    // control-channel-end failure wins the handshake race deterministically.
    const supervisor = createWorkflowSupervisor(bindings);
    const spawnPromise = supervisor.spawn({
      definitionHash: TEST_DEFINITION_HASH,

      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await driveReady(first, ipcKeypair);
    await spawnPromise;

    const recyclePromise = supervisor.recycle({ reason: "ready-failed-test" });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    // End the new child's control channel after it reports its definition
    // but before it emits ready: the handshake fails rather than hangs.
    await driveDefinition(second, ipcKeypair);
    second.childToSupervisor.close();

    let caught: unknown;
    try {
      await recyclePromise;
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(caught instanceof Error && caught.message).toMatch(
      /control channel ended before child emitted ready/,
    );
    expect(second.killSignals).toContain("SIGTERM");

    await supervisor.shutdown();
  });

  test("a respawn whose definition does not hash to the approved hash rejects and reaps the new child", async () => {
    const baseDir = await makeTempDir("recycle-definition-mismatch-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({ sigtermExits: true });
    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus: createMockMailBus(),
      ipcKeypair,
    });
    let layoutCalls = 0;
    const supervisor = createWorkflowSupervisor({
      ...bindings,
      layoutSteps: (definition) => {
        layoutCalls += 1;
        return bindings.layoutSteps(definition);
      },
    });
    const spawnPromise = supervisor.spawn({
      definitionHash: TEST_DEFINITION_HASH,
      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await driveReady(first, ipcKeypair);
    await spawnPromise;

    const recyclePromise = supervisor.recycle({ reason: "mismatch-test" });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveDefinition(second, ipcKeypair, {
      ...TEST_DEFINITION,
      id: "a-different-workflow",
    });

    await expect(recyclePromise).rejects.toThrow(/not the approved/);
    expect(second.killSignals).toContain("SIGTERM");
    // As on first spawn, a child whose definition does not check out never
    // receives the run's authority.
    expect(authoritySetFrames(second.supervisorToChild.flushed())).toEqual([]);
    // The layout stays the one the first child's report fixed.
    expect(layoutCalls).toBe(1);

    await supervisor.shutdown();
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
      definitionHash: TEST_DEFINITION_HASH,

      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
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
    await tracker.awaitChildren(2);
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
    await tracker.awaitChildren(2);
    mailBus.deliver(
      "run_deployment-x@example.com",
      inboundDelivery(new TextEncoder().encode("gap-1")),
    );
    mailBus.deliver(
      "run_deployment-x@example.com",
      inboundDelivery(new TextEncoder().encode("gap-2")),
    );

    const secondChild = tracker.children[1];
    if (secondChild === undefined) {
      throw new Error("second child missing for buffer test");
    }
    // The supervisor must NOT forward the buffered mail until `ready`
    // lands; pre-ready, the only frame the new child gets is its
    // `authority.set`. Recycle's drain step ran against the FIRST
    // child's controlSender, not this one.
    expect(
      parseControlFrames(secondChild.supervisorToChild.flushed())
        .map((frame) => frame.type)
        .filter((frameType) => frameType !== "authority.set"),
    ).toEqual([]);

    const secondChildSender = await driveReady(secondChild, ipcKeypair);
    const attempt = await recyclePromise;
    expect(attempt.origin).toBe("operator");

    // After ready, the new child's dispatch loop drains the inbox claim-check
    // queue in FIFO order. The first message fires the deployment's stable
    // top-level run. Once that run parks, the second message resumes it as a
    // signal; it must not fire a second top-level run.
    await waitForTriggerFireRunIds(secondChild.supervisorToChild, 1);
    const flushedTriggerRunIds = (): string[] =>
      parseTriggerFireRunIds(secondChild.supervisorToChild.flushed());
    const firstRunIds = flushedTriggerRunIds();
    expect(firstRunIds.length).toBeGreaterThanOrEqual(1);
    const firstRunId = firstRunIds[0];
    if (firstRunId === undefined) throw new Error("missing first runId");
    await secondChildSender.send({
      type: "park.notify",
      data: {
        runId: firstRunId,
        correlationId: "corr-recycle-gap-input-1",
        parkKind: "input",
      },
    });

    await waitForUpstreamPayload(
      secondChild.supervisorToChild,
      "signal.deliver",
    );
    const flushedSignalRunIds = (): string[] =>
      parseSignalDeliverRunIds(secondChild.supervisorToChild.flushed());
    expect(flushedTriggerRunIds()).toEqual([firstRunId]);
    expect(flushedSignalRunIds()).toEqual([firstRunId]);

    await secondChildSender.send({
      type: "terminal.event",
      data: {
        runId: firstRunId,
        seq: 0,
        kind: "RunCompleted",
        at: "test",
      },
    });

    await supervisor.shutdown();
  });
});

// A supervisor's recycle policy with its clock and ticks driven by hand. Other
// timers on the same bindings run for real.
function manualRecyclePolicy(maxUptimeMs: number) {
  let nowMs = 1_700_000_000_000;
  const ticks = new Map<symbol, () => void>();
  const timers = new Map<symbol, ReturnType<typeof setTimeout>>();
  let armedNext: PromiseWithResolvers<undefined> | null = null;
  const bindings: Partial<WorkflowSupervisorBindings> = {
    recyclePolicy: { maxUptimeMs },
    recyclePolicyNow: () => nowMs,
    recyclePolicySetTimer: (cb, ms) => {
      const handle = Symbol("timer");
      if (ms === DEFAULT_POLICY_INTERVAL_MS) {
        ticks.set(handle, cb);
        armedNext?.resolve(undefined);
        armedNext = null;
      } else {
        const timer = setTimeout(() => {
          timers.delete(handle);
          cb();
        }, ms);
        timers.set(handle, timer);
      }
      return handle;
    },
    recyclePolicyClearTimer: (handle) => {
      if (typeof handle !== "symbol") return;
      ticks.delete(handle);
      clearTimeout(timers.get(handle));
      timers.delete(handle);
    },
  };
  return {
    bindings,
    advance(ms: number): void {
      nowMs += ms;
    },
    armed: () => ticks.size,
    // Resolves when a policy next arms its tick.
    nextArm(): Promise<undefined> {
      armedNext = Promise.withResolvers<undefined>();
      return armedNext.promise;
    },
    tick(): void {
      const due = [...ticks.values()];
      ticks.clear();
      for (const cb of due) cb();
    },
  };
}

describe("supervisor recycle: policy-initiated (max-uptime trip)", () => {
  test("each replacement child's uptime counts from its own start", async () => {
    const baseDir = await makeTempDir("recycle-policy-child-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({ sigtermExits: true });
    const policy = manualRecyclePolicy(60_000);
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus: createMockMailBus(),
      ipcKeypair,
      overrides: policy.bindings,
    });
    expect(policy.armed()).toBe(1);

    policy.advance(61_000);
    const replaced = policy.nextArm();
    policy.tick();
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await replaced;
    expect(policy.armed()).toBe(1);

    const ticked = policy.nextArm();
    policy.tick();
    await ticked;
    expect(tracker.totalSpawns).toBe(2);

    await supervisor.shutdown();
  });

  test("the policy's max-uptime threshold trips recycle with a policy-origin reason", async () => {
    const baseDir = await makeTempDir("recycle-policy-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
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
    await tracker.awaitChildren(2);
    const secondChild = tracker.children[1];
    if (secondChild === undefined) {
      throw new Error("second child missing for self-recycle test");
    }
    await driveReady(secondChild, ipcKeypair);

    // The new child runs under a fresh channelId.
    expect(secondChild.channelId).not.toBe(firstChild.channelId);
    expect(secondChild.channelId).toMatch(/^[0-9a-f]{32}$/);
    // The mail-bus registration is held across the recycle.
    expect(mailBus.registered()).toContain("run_deployment-x@example.com");

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
      await tracker.awaitChildren(i + 2);
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

    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
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
      definitionHash: TEST_DEFINITION_HASH,

      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await driveReady(first, ipcKeypair);
    await spawnPromise;

    // Deliver one mail so an in-flight run exists, then drain so the
    // supervisor arms an accumulator. Each accumulator subscribes to
    // the cohort's terminal broadcaster for its tracked runId.
    mailBus.deliver(
      "run_deployment-x@example.com",
      inboundDelivery(new TextEncoder().encode("cohort-pre-recycle")),
    );
    // The drain below only arms an accumulator for a run already in flight,
    // and the forwarded trigger is what says the run exists. The pause this
    // replaces was a guess at the same thing.
    await waitForTriggerFireRunIds(first.supervisorToChild, 1);
    await supervisor.drain({ deadlineMs: 5_000 });

    // Recycle. The installNewChild path aborts the prior cohort,
    // disposes the prior broadcaster (which settles every minted
    // iterator with `done: true`), and stops every armed accumulator
    // -- which fires `return()` on each watcher iterator. The
    // observable invariant is structural: after the recycle returns,
    // the supervisor accepts a fresh inbound mail and the new
    // cohort's broadcaster handles its dispatch end-to-end.
    const recyclePromise = supervisor.recycle({ reason: "cohort-finalise" });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recyclePromise;

    // The new cohort still operates: deliver a fresh mail and
    // confirm the supervisor forwards a trigger.fire through the
    // new child's controlSender. A wedged prior cohort would have
    // left the dispatch loop blocked on the disposed broadcaster
    // and the trigger.fire would never reach the new child's stream.
    mailBus.deliver(
      "run_deployment-x@example.com",
      inboundDelivery(new TextEncoder().encode("cohort-post-recycle")),
    );
    // Decoded rather than substring-matched: the text "trigger.fire"
    // appearing in an unrelated payload would have satisfied the filter this
    // replaces.
    await waitForTriggerFireRunIds(second.supervisorToChild, 1);
    const triggerFires = second.supervisorToChild
      .flushed()
      .filter((f) => f.includes("trigger.fire"));
    expect(triggerFires.length).toBeGreaterThanOrEqual(1);

    await supervisor.shutdown();
  });
});

describe("supervisor recycle: drain-side processing replay", () => {
  test("triggerRecycle invokes replayProcessingToInbox after the kill", async () => {
    // This is a focused unit test against `triggerRecycle`'s ordering
    // contract: the `replayProcessingToInbox` callback must fire once
    // `killChildHandle` has landed, when the old child can no longer take
    // mail. The test observes the order via call recording.
    const baseDir = await makeTempDir("recycle-replay-order-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const calls: string[] = [];
    const spawnReplayed = Promise.withResolvers<undefined>();
    const recordingInbox: InboxPrimitives = {
      ...createMemoryInboxPrimitives(),
      async replayProcessingToInbox(_store, _principal, _repoId, _address) {
        calls.push("replayProcessingToInbox");
        spawnReplayed.resolve(undefined);
        return {
          commitSha: "memory",
          replayedKeys: [],
          retainedIdentities: [],
        };
      },
    };
    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus,
      ipcKeypair,
      inboxPrimitives: recordingInbox,
    });
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
      definitionHash: TEST_DEFINITION_HASH,

      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await driveReady(first, ipcKeypair);
    await spawnPromise;
    // Clear out the spawn-time replay call, which can land after the spawn
    // resolves, so we can isolate the recycle-time one.
    await spawnReplayed.promise;
    calls.length = 0;

    const recyclePromise = supervisor.recycle({ reason: "ordering-test" });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recyclePromise;
    // The recycle path invokes `replayProcessingToInbox` AFTER the kill.
    // The order recorded in `calls` reflects that.
    const replayIdx = calls.indexOf("replayProcessingToInbox");
    const killIdx = calls.indexOf("kill");
    expect(replayIdx).toBeGreaterThanOrEqual(0);
    expect(killIdx).toBeGreaterThanOrEqual(0);
    expect(killIdx).toBeLessThan(replayIdx);
    await supervisor.shutdown();
  });
});

async function sendChildWrite(
  childSender: ReturnType<typeof createControlChannelSender>,
  requestId: string,
): Promise<void> {
  await childSender.send({
    type: "substrate.write.request",
    data: {
      requestId,
      repoId: { kind: "workflow-run", id: "run_deployment-x" },
      ref: "refs/heads/main",
      preservePrefix: "runs/",
      message: `child write ${requestId}`,
    },
  });
}

describe("supervisor recycle: the old child's last writes", () => {
  test.each([1, 2])(
    "%d write(s) the old child left waiting on a merge do not hold up the replay",
    async (writes) => {
      const baseDir = await makeTempDir("recycle-merge-left-waiting-");
      const ipcKeypair = await generateKeyPair();
      const tracker = createSpawnTracker({});
      await seedStepGrants(baseDir, "step-1", [
        { resource: "thing", action: "read" },
      ]);
      const bindings = await buildBindings({
        baseDir,
        spawner: tracker.spawner,
        mailBus: createMockMailBus(),
        ipcKeypair,
      });
      // Writes run their merge, which the child computes, one at a time as
      // under the repo lock.
      let lock: Promise<unknown> = Promise.resolve();
      const repoStore = new Proxy(bindings.repoStore, {
        get(target, prop, receiver) {
          if (prop !== "writeTreePreservingPrefix") {
            return Reflect.get(target, prop, receiver);
          }
          return (
            ...args: Parameters<RepoStore["writeTreePreservingPrefix"]>
          ) => {
            const write = lock.then(async () => {
              await args[3].merge(new Map());
              return target.writeTreePreservingPrefix(...args);
            });
            lock = write.catch(() => undefined);
            return write;
          };
        },
      });
      const supervisor = createWorkflowSupervisor({ ...bindings, repoStore });
      const spawned = supervisor.spawn({
        definitionHash: TEST_DEFINITION_HASH,
        onInferenceEvent: () => undefined,
      });
      await tracker.awaitChildren(1);
      const first = tracker.children[0];
      if (first === undefined) throw new Error("first child missing");
      const firstSender = await driveReady(first, ipcKeypair);
      await spawned;
      for (let write = 0; write < writes; write++) {
        await sendChildWrite(firstSender, `left-waiting-${String(write)}`);
      }
      await waitForUpstreamPayload(
        first.supervisorToChild,
        "substrate.merge.request",
      );

      const recycled = supervisor.recycle({ reason: "merge left waiting" });
      await tracker.awaitChildren(2);
      const second = tracker.children[1];
      if (second === undefined) throw new Error("second child missing");
      await driveReady(second, ipcKeypair);
      await recycled;
      // A write that reached its merge after the old child was cut off never
      // asked that child.
      expect(
        parseControlFrames(first.supervisorToChild.flushed()).filter(
          (frame) => frame.type === "substrate.merge.request",
        ),
      ).toHaveLength(1);
      await supervisor.shutdown();
    },
  );

  test("a crash respawn is not held up by a boot replay queued behind the dead child's merge", async () => {
    const baseDir = await makeTempDir("respawn-boot-lock-");
    const inbox = createMemoryInboxPrimitives();
    const tracker = createSpawnTracker({});
    const ipcKeypair = await generateKeyPair();
    // The boot replay's claim-check write and a child's write take the same
    // repo lock, and the child's write holds it through its merge.
    let lock: Promise<unknown> = Promise.resolve();
    function withLock<T>(fn: () => Promise<T>): Promise<T> {
      const run = lock.then(fn);
      lock = run.catch(() => undefined);
      return run;
    }
    const bootReplayHeld = Promise.withResolvers<undefined>();
    let replays = 0;
    const repoStore = createStubRepoStore(baseDir, { writeTree: true });
    const { supervisor, firstSender } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus: createMockMailBus(),
      ipcKeypair,
      overrides: {
        respawnBackoffInitialMs: 0,
        repoStore: new Proxy(repoStore, {
          get(target, prop, receiver) {
            if (prop !== "writeTreePreservingPrefix") {
              return Reflect.get(target, prop, receiver);
            }
            return (
              ...args: Parameters<RepoStore["writeTreePreservingPrefix"]>
            ) =>
              withLock(async () => {
                await args[3].merge(new Map());
                return target.writeTreePreservingPrefix(...args);
              });
          },
        }),
        inboxPrimitives: {
          ...inbox,
          async replayProcessingToInbox(...args) {
            replays += 1;
            if (replays === 1) await bootReplayHeld.promise;
            return withLock(() => inbox.replayProcessingToInbox(...args));
          },
        },
      },
    });
    await sendChildWrite(firstSender, "dies-in-merge");
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await waitForUpstreamPayload(
      first.supervisorToChild,
      "substrate.merge.request",
    );
    bootReplayHeld.resolve(undefined);
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(1);

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await supervisor.shutdown();
  });
});

describe("supervisor recycle: shutdown during the kill/respawn gap", () => {
  test.each([false, true])(
    "concurrent shutdowns wait for the replacement's exit (late ready: %s)",
    async (lateReady) => {
      const baseDir = await makeTempDir("recycle-shutdown-gap-");
      const ipcKeypair = await generateKeyPair();
      const tracker = createSpawnTracker({});
      const replacementKill = Promise.withResolvers<undefined>();
      let finishReplacement: (() => void) | undefined;
      const bindings = await buildBindings({
        baseDir,
        spawner: (args) => {
          const handle = tracker.spawner(args);
          if (tracker.totalSpawns !== 2) return handle;
          finishReplacement = () => handle.kill("SIGTERM");
          return {
            ...handle,
            kill: () => replacementKill.resolve(undefined),
          };
        },
        mailBus: createMockMailBus(),
        ipcKeypair,
      });
      const supervisor = createWorkflowSupervisor(bindings);
      const spawning = supervisor.spawn({
        definitionHash: TEST_DEFINITION_HASH,
        onInferenceEvent: () => undefined,
      });
      await tracker.awaitChildren(1);
      const first = tracker.children[0];
      if (first === undefined) throw new Error("first child missing");
      await driveReady(first, ipcKeypair);
      await spawning;

      const recycled = supervisor.recycle({ reason: "race-test" }).then(
        () => null,
        (cause: unknown) => cause,
      );
      await tracker.awaitChildren(2);
      const shutdownPromise = supervisor.shutdown();
      let concurrentShutdownFinished = false;
      const concurrentShutdown = supervisor.shutdown().then(() => {
        concurrentShutdownFinished = true;
      });
      try {
        expect(
          await Promise.race([
            replacementKill.promise.then(() => "kill"),
            shutdownPromise.then(() => "shutdown"),
          ]),
        ).toBe("kill");
        // The replacement's exit is held explicitly; drain this event-loop turn.
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(concurrentShutdownFinished).toBe(false);
        if (lateReady) {
          const second = tracker.children[1];
          if (second === undefined)
            throw new Error("replacement child missing");
          await driveReady(second, ipcKeypair);
          expect(await recycled).toBeNull();
          await expect(
            supervisor.deliverSignal({
              runId: "run_deployment-x",
              signalName: "test",
              signalId: "late-ready",
              payload: null,
            }),
          ).rejects.toThrow("phase stopping");
          expect(concurrentShutdownFinished).toBe(false);
        }
      } finally {
        finishReplacement?.();
        await Promise.all([shutdownPromise, concurrentShutdown]);
        const outcome = await recycled;
        if (!lateReady) expect(outcome).toBeInstanceOf(Error);
        await fs.rm(baseDir, { recursive: true, force: true });
      }
    },
  );

  test("shutdown fences a replacement whose key generation is still pending", async () => {
    const baseDir = await makeTempDir("recycle-shutdown-before-spawn-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({});
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus: createMockMailBus(),
      ipcKeypair,
    });
    const generating = Promise.withResolvers<undefined>();
    const generated = Promise.withResolvers<typeof ipcKeypair>();
    let generations = 0;
    const supervisor = createWorkflowSupervisor({
      ...bindings,
      ipcKeyPairFactory: () => {
        generations += 1;
        if (generations === 1) return Promise.resolve(ipcKeypair);
        generating.resolve(undefined);
        return generated.promise;
      },
    });
    const spawning = supervisor.spawn({
      definitionHash: TEST_DEFINITION_HASH,
      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await driveReady(first, ipcKeypair);
    await spawning;
    const recycled = supervisor.recycle({ reason: "race-test" }).then(
      () => null,
      (cause: unknown) => cause,
    );
    await generating.promise;
    await supervisor.shutdown();
    generated.resolve(ipcKeypair);
    try {
      expect(
        await Promise.race([
          recycled.then(() => "finished"),
          tracker.awaitChildren(2).then(() => "spawned"),
        ]),
      ).toBe("finished");
      expect(await recycled).toBeInstanceOf(Error);
      expect(tracker.totalSpawns).toBe(1);
    } finally {
      for (const child of tracker.children) {
        child.childToSupervisor.close();
        child.eventChildToSupervisor.close();
        child.resolveExit?.(0);
      }
      await recycled;
      await supervisor.shutdown();
      await fs.rm(baseDir, { recursive: true, force: true });
    }
  });
});

describe("supervisor recycle: shutdown owns the retiring dispatch loop", () => {
  test("shutdown during a recycle waits for the retired cohort's consumption write", async () => {
    const baseDir = await makeTempDir("recycle-retiring-loop-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const inbox = createMemoryInboxPrimitives();
    const consuming = Promise.withResolvers<undefined>();
    const releaseConsumption = Promise.withResolvers<undefined>();
    let consumed = false;
    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus,
      ipcKeypair,
      inboxPrimitives: {
        ...inbox,
        async markConsumed(store, principal, repoId, args) {
          const result = await inbox.markConsumed(
            store,
            principal,
            repoId,
            args,
          );
          consuming.resolve(undefined);
          await releaseConsumption.promise;
          consumed = true;
          return result;
        },
      },
    });
    const supervisor = createWorkflowSupervisor(bindings);
    const spawning = supervisor.spawn({
      definitionHash: TEST_DEFINITION_HASH,
      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    const firstSender = await driveReady(first, ipcKeypair);
    await spawning;

    mailBus.deliver(
      "run_deployment-x@example.com",
      inboundDelivery(new TextEncoder().encode("consumed-during-recycle")),
    );
    await waitForTriggerFireRunIds(first.supervisorToChild, 1);
    const [runId] = parseTriggerFireRunIds(first.supervisorToChild.flushed());
    if (runId === undefined) throw new Error("trigger.fire runId missing");
    await firstSender.send({
      type: "terminal.event",
      data: { runId, seq: 0, kind: "RunCompleted", at: "test" },
    });
    await consuming.promise;

    const recycled = supervisor.recycle({ reason: "retiring-loop" }).then(
      () => null,
      (cause: unknown) => cause,
    );
    await tracker.awaitChildren(2);
    let shutDown = false;
    const shutdown = supervisor.shutdown().then(() => {
      shutDown = true;
    });
    try {
      // The consumption write is held, so a shutdown that owns the retired
      // loop cannot finish, whatever this turn runs.
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(shutDown).toBe(false);
    } finally {
      releaseConsumption.resolve(undefined);
      await shutdown;
      await recycled;
      await fs.rm(baseDir, { recursive: true, force: true });
    }
    expect(consumed).toBe(true);
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
      definitionHash: TEST_DEFINITION_HASH,

      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);
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
    await tracker.awaitChildren(2);
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

describe("supervisor recycle: shutdown during the replay", () => {
  test("a shutdown during a respawn's replay resolves once its write lands", async () => {
    const baseDir = await makeTempDir("recycle-replay-shutdown-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({ sigtermExits: true });
    const memory = createMemoryInboxPrimitives();
    const replaying = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const order: string[] = [];
    let replays = 0;
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus: createMockMailBus(),
      ipcKeypair,
      overrides: {
        onShutdownWait: (waiting) => {
          if (waiting === "replay") release.resolve(undefined);
        },
        inboxPrimitives: {
          ...memory,
          async replayProcessingToInbox(...args) {
            replays += 1;
            if (replays === 1) return memory.replayProcessingToInbox(...args);
            replaying.resolve(undefined);
            await release.promise;
            const replayed = await memory.replayProcessingToInbox(...args);
            order.push("replayed");
            return replayed;
          },
        },
      },
    });

    const recycled = supervisor
      .recycle({ reason: "replay-shutdown-test" })
      .catch(() => undefined);
    await replaying.promise;
    const stopped = supervisor.shutdown().then(() => order.push("stopped"));
    await stopped;
    expect(order).toEqual(["replayed", "stopped"]);
    await recycled;
  });

  test("a respawn's replay that has not begun when shutdown starts does not run", async () => {
    const baseDir = await makeTempDir("recycle-replay-not-begun-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({ sigtermExits: true });
    const memory = createMemoryInboxPrimitives();
    const bootReplayHeld = Promise.withResolvers<undefined>();
    const respawnWaits = Promise.withResolvers<undefined>();
    let replays = 0;
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus: createMockMailBus(),
      ipcKeypair,
      overrides: {
        onRespawnWait: (waiting) => {
          if (waiting === "replay") respawnWaits.resolve(undefined);
        },
        inboxPrimitives: {
          ...memory,
          async replayProcessingToInbox(...args) {
            replays += 1;
            if (replays === 1) await bootReplayHeld.promise;
            return memory.replayProcessingToInbox(...args);
          },
        },
      },
    });

    // The respawn waits for the boot replay, so its own has not begun.
    const recycled = supervisor
      .recycle({ reason: "replay-not-begun-test" })
      .catch(() => undefined);
    await respawnWaits.promise;
    const stopped = supervisor.shutdown();
    bootReplayHeld.resolve(undefined);
    await stopped;
    await recycled;
    expect(replays).toBe(1);
  });
});

describe("supervisor shutdown: teardown robustness", () => {
  test("shutdown still kills the child and reaches stopped when mail unsubscribe throws", async () => {
    const baseDir = await makeTempDir("shutdown-unsub-throw-");
    const ipcKeypair = await generateKeyPair();
    const baseMailBus = createMockMailBus();
    // The mail unsubscribe runs BEFORE the child kill in shutdown. Left
    // unguarded, a throw here skips the kill (child leak) and leaves the
    // supervisor wedged in `stopping`.
    const mailBus: MailBusBindings = {
      ...baseMailBus,
      subscribeMailForAddress(address, handler) {
        baseMailBus.subscribeMailForAddress(address, handler);
        return () => {
          throw new Error("mail unsubscribe boom during shutdown");
        };
      },
    };
    const tracker = createSpawnTracker({ sigtermExits: true });
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
    });
    const child = tracker.children[0];
    if (child === undefined) {
      throw new Error("tracker.children[0] missing");
    }

    // Shutdown must resolve despite the throwing unsubscribe.
    await supervisor.shutdown();

    // The kill step was reached even though the unsubscribe (which runs
    // first) threw.
    expect(child.killSignals.length).toBe(1);

    // The supervisor reached `stopped`, not wedged in `stopping`: a second
    // shutdown is an idempotent no-op that returns without re-running the
    // (throwing) teardown, so no further kill is issued.
    await supervisor.shutdown();
    expect(child.killSignals.length).toBe(1);
  });

  test("shutdown reaches stopped when the child kill throws", async () => {
    const baseDir = await makeTempDir("shutdown-kill-throw-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    // The child kill throws (after settling exit). The `finally` must still
    // drive the supervisor to `stopped` rather than leaving it in
    // `stopping`.
    const tracker = createSpawnTracker({
      sigtermExits: true,
      killThrows: true,
    });
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
    });
    const child = tracker.children[0];
    if (child === undefined) {
      throw new Error("tracker.children[0] missing");
    }

    // Shutdown must resolve even though kill threw.
    await supervisor.shutdown();
    expect(child.killSignals.length).toBe(1);

    // Reached `stopped`: the second shutdown no-ops rather than re-entering
    // teardown (which would call the throwing kill again).
    await supervisor.shutdown();
    expect(child.killSignals.length).toBe(1);
  });
});

describe("supervisor recycle: the new child's authority", () => {
  const runSnapshot = {
    steps: [
      {
        stepId: "step-1",
        grants: [{ id: "run-grant" }],
        contentHash: "run-hash",
      },
    ],
  };
  const material = (credentialId: string) => ({
    credentialId,
    providerKey: "http",
    origin: "https://api.example.test",
    secret: `sk-${credentialId}`,
  });

  const versioned = (contentHash: string) => ({
    steps: [
      {
        stepId: "step-1",
        grants: [{ id: `grant-${contentHash}` }],
        contentHash,
      },
    ],
  });

  // A one-shot gate: `reached` resolves when the gated read arrives, and the
  // read waits until `release()`.
  function readGate() {
    let markReached: () => void = () => undefined;
    const reached = new Promise<void>((resolve) => {
      markReached = resolve;
    });
    let release: () => void = () => undefined;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    return {
      reached,
      release,
      arrive: () => {
        markReached();
        return released;
      },
    };
  }

  // A supervisor whose spawn is started and whose child has reported its
  // definition but not readied.
  async function startSpawn(opts: {
    baseDir: string;
    tracker: SpawnTracker;
    ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
    onRunStart: NonNullable<WorkflowSupervisorBindings["onRunStart"]>;
  }) {
    await seedStepGrants(opts.baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
    const bindings = await buildBindings({
      baseDir: opts.baseDir,
      spawner: opts.tracker.spawner,
      mailBus: createMockMailBus(),
      ipcKeypair: opts.ipcKeypair,
      onRunStart: opts.onRunStart,
    });
    const supervisor = createWorkflowSupervisor(bindings);
    const spawnPromise = supervisor.spawn({
      definitionHash: TEST_DEFINITION_HASH,
      onInferenceEvent: () => undefined,
    });
    // The supervisor reads the run's grants once the child has reported
    // its definition.
    await opts.tracker.awaitChildren(1);
    const child = opts.tracker.children[0];
    if (child === undefined) throw new Error("child missing");
    await driveDefinition(child, opts.ipcKeypair);
    return { supervisor, spawnPromise };
  }

  // The grants hash of the last grants-bearing frame a child applies.
  function lastGrantsHash(lines: readonly string[]): string | undefined {
    const last = parseControlFrames(lines)
      .filter(
        (frame) =>
          frame.type === "authority.set" || frame.type === "grants-updated",
      )
      .at(-1);
    if (last?.type === "authority.set") {
      return last.data.snapshot?.steps[0]?.contentHash;
    }
    if (last?.type === "grants-updated") {
      return last.data.snapshot.steps[0]?.contentHash;
    }
    return undefined;
  }

  test("a grants refresh during the spawn's grants read reaches the starting child", async () => {
    const baseDir = await makeTempDir("recycle-authority-spawn-refresh-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({});
    const gate = readGate();
    let reads = 0;
    const { supervisor, spawnPromise } = await startSpawn({
      baseDir,
      tracker,
      ipcKeypair,
      onRunStart: async () => {
        reads += 1;
        if (reads === 1) {
          await gate.arrive();
          return versioned("old");
        }
        return versioned("new");
      },
    });
    try {
      await gate.reached;
      // The supervisor is still idle, so the refresh reaches no child.
      expect(await supervisor.deliverGrants("run_deployment-x")).toBe(
        "skipped",
      );
      gate.release();
      await tracker.awaitChildren(1);
      const child = tracker.children[0];
      if (child === undefined) throw new Error("child missing");
      await driveReady(child, ipcKeypair);
      await spawnPromise;

      await waitUntil(
        () => authoritySetFrames(child.supervisorToChild.flushed()).length >= 2,
      );
      expect(lastGrantsHash(child.supervisorToChild.flushed())).toBe("new");
    } finally {
      gate.release();
      await spawnPromise.catch(() => undefined);
      await supervisor.shutdown();
    }
  });

  test("a grants refresh skipped during a recycle reaches the new child after install", async () => {
    const baseDir = await makeTempDir("recycle-authority-recycle-refresh-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({});
    const gate = readGate();
    let reads = 0;
    const { supervisor, spawnPromise } = await startSpawn({
      baseDir,
      tracker,
      ipcKeypair,
      onRunStart: async () => {
        reads += 1;
        if (reads === 2) {
          await gate.arrive();
          return versioned("old");
        }
        return versioned(reads === 1 ? "first" : "new");
      },
    });
    await tracker.awaitChildren(1);
    const first = tracker.children[0];
    if (first === undefined) throw new Error("child missing");
    await driveReady(first, ipcKeypair);
    await spawnPromise;

    const recyclePromise = supervisor.recycle({ reason: "refresh" });
    try {
      await gate.reached;
      // The supervisor is recycling, so the refresh reaches no child.
      expect(await supervisor.deliverGrants("run_deployment-x")).toBe(
        "skipped",
      );
      gate.release();
      const second = tracker.children[1];
      if (second === undefined) throw new Error("second child missing");
      await driveReady(second, ipcKeypair);
      await recyclePromise;

      await waitUntil(
        () =>
          authoritySetFrames(second.supervisorToChild.flushed()).length >= 2,
      );
      expect(lastGrantsHash(second.supervisorToChild.flushed())).toBe("new");
    } finally {
      gate.release();
      await recyclePromise.catch(() => undefined);
      await supervisor.shutdown();
    }
  });

  test("a resend whose read began before a newer grants refresh does not send the older grants", async () => {
    const baseDir = await makeTempDir("recycle-authority-stale-resend-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({});
    const spawnRead = readGate();
    const resendRead = readGate();
    let reads = 0;
    const { supervisor, spawnPromise } = await startSpawn({
      baseDir,
      tracker,
      ipcKeypair,
      onRunStart: async () => {
        reads += 1;
        if (reads === 1) {
          await spawnRead.arrive();
          return versioned("v0");
        }
        if (reads === 2) {
          // The resend's read saw the file before the v2 write landed.
          await resendRead.arrive();
          return versioned("v1");
        }
        return versioned("v2");
      },
    });
    try {
      await spawnRead.reached;
      // A skipped refresh during the spawn's read makes the spawn resend.
      await supervisor.deliverGrants("run_deployment-x");
      spawnRead.release();
      await resendRead.reached;
      // The grants file moves to v2, and the refresh reaches the child.
      expect(await supervisor.deliverGrants("run_deployment-x")).toBe("pushed");
      resendRead.release();
      await tracker.awaitChildren(1);
      const child = tracker.children[0];
      if (child === undefined) throw new Error("child missing");
      await driveReady(child, ipcKeypair);
      await spawnPromise;

      await waitUntil(
        () => authoritySetFrames(child.supervisorToChild.flushed()).length >= 2,
      );
      expect(lastGrantsHash(child.supervisorToChild.flushed())).toBe("v2");
    } finally {
      spawnRead.release();
      resendRead.release();
      await spawnPromise.catch(() => undefined);
      await supervisor.shutdown();
    }
  });

  test("a resend whose grants read fails keeps the child's grants", async () => {
    const baseDir = await makeTempDir("recycle-authority-resend-fail-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({});
    const gate = readGate();
    let reads = 0;
    const { supervisor, spawnPromise } = await startSpawn({
      baseDir,
      tracker,
      ipcKeypair,
      onRunStart: async () => {
        reads += 1;
        if (reads === 1) {
          await gate.arrive();
          return versioned("good");
        }
        throw new Error("synthetic grants read failure");
      },
    });
    try {
      await gate.reached;
      // A skipped refresh during the spawn's read makes the spawn resend.
      await supervisor.deliverGrants("run_deployment-x");
      gate.release();
      await tracker.awaitChildren(1);
      const child = tracker.children[0];
      if (child === undefined) throw new Error("child missing");
      await driveReady(child, ipcKeypair);
      await spawnPromise;

      await waitUntil(
        () => authoritySetFrames(child.supervisorToChild.flushed()).length >= 2,
      );
      const frames = authoritySetFrames(child.supervisorToChild.flushed());
      expect(frames[0]?.snapshot?.steps[0]?.contentHash).toBe("good");
      // The resend omits the snapshot, so the child keeps "good".
      expect(frames[1]).toBeDefined();
      expect(frames[1] !== undefined && "snapshot" in frames[1]).toBe(false);
    } finally {
      gate.release();
      await spawnPromise.catch(() => undefined);
      await supervisor.shutdown();
    }
  });

  // A supervisor whose keypair factory waits for `release()`.
  async function startGatedKeypairSpawn(opts: {
    baseDir: string;
    tracker: SpawnTracker;
    ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
    keypair: () => Promise<{ privateKey: Uint8Array; publicKey: Uint8Array }>;
  }) {
    const bindings = await buildBindings({
      baseDir: opts.baseDir,
      spawner: opts.tracker.spawner,
      mailBus: createMockMailBus(),
      ipcKeypair: opts.ipcKeypair,
    });
    const supervisor = createWorkflowSupervisor({
      ...bindings,
      ipcKeyPairFactory: opts.keypair,
    });
    const spawnPromise = supervisor.spawn({
      definitionHash: TEST_DEFINITION_HASH,
      onInferenceEvent: () => undefined,
    });
    return { supervisor, spawnPromise };
  }

  test("a spawner that throws leaves no pending spawn for a later shutdown", async () => {
    const baseDir = await makeTempDir("recycle-authority-spawner-throws-");
    const ipcKeypair = await generateKeyPair();
    const bindings = await buildBindings({
      baseDir,
      spawner: () => {
        throw new Error("synthetic spawn ENOENT");
      },
      mailBus: createMockMailBus(),
      ipcKeypair,
    });
    const supervisor = createWorkflowSupervisor(bindings);
    await expect(
      supervisor.spawn({
        definitionHash: TEST_DEFINITION_HASH,
        onInferenceEvent: () => undefined,
      }),
    ).rejects.toThrow(/synthetic spawn ENOENT/);
    // A stale pending spawn would make this wait forever.
    await supervisor.shutdown();
  });

  // The child applies `authority.set` as a replace and `credentials-updated`
  // as a merge. The credential ids it holds after the given frames.
  function heldCredentialIds(lines: readonly string[]): string[] {
    let held = new Set<string>();
    for (const frame of parseControlFrames(lines)) {
      if (frame.type === "authority.set") {
        held = new Set(
          (frame.data.delivery?.materials ?? []).map((m) => m.credentialId),
        );
      } else if (frame.type === "credentials-updated") {
        for (const id of frame.data.revoke ?? []) held.delete(id);
        for (const m of frame.data.delivery.materials) {
          held.add(m.credentialId);
        }
      }
    }
    return [...held].sort();
  }

  function oneMaterial(credentialId: string) {
    return {
      bindings: [],
      materials: [
        {
          credentialId,
          providerKey: "http",
          origin: "https://api.example.com",
          secret: `sk-${credentialId}`,
        },
      ],
    };
  }

  const inFlightUpdates: [
    string,
    Parameters<WorkflowSupervisor["deliverCredentials"]>[0],
    string[],
  ][] = [
    [
      "adds a credential",
      { delivery: oneMaterial("cred-b") },
      ["cred-a", "cred-b"],
    ],
    [
      "revokes a credential",
      { delivery: { bindings: [], materials: [] }, revoke: ["cred-a"] },
      [],
    ],
  ];
  test.each(inFlightUpdates)(
    "a credentials update that %s while its frame waits on the child is not undone by a resend",
    async (_case, update, expected) => {
      const baseDir = await makeTempDir("recycle-authority-cred-race-");
      const ipcKeypair = await generateKeyPair();
      const inner = createSpawnTracker({});
      // Hold the child's next stdin write once armed, and report when a
      // write reaches the hold.
      let hold: PromiseWithResolvers<undefined> | null = null;
      const writeHeld = Promise.withResolvers<undefined>();
      const tracker: SpawnTracker = {
        ...inner,
        spawner: (args) => {
          const handle = inner.spawner(args);
          return {
            ...handle,
            controlWriter: {
              write: async (line: string) => {
                if (hold !== null) {
                  writeHeld.resolve(undefined);
                  await hold.promise;
                }
                return handle.controlWriter.write(line);
              },
            },
          };
        },
      };
      const spawnRead = readGate();
      const resendRead = readGate();
      let reads = 0;
      const { supervisor, spawnPromise } = await startSpawn({
        baseDir,
        tracker,
        ipcKeypair,
        onRunStart: async () => {
          reads += 1;
          if (reads === 1) await spawnRead.arrive();
          if (reads === 2) await resendRead.arrive();
          return versioned("v");
        },
      });
      const release = Promise.withResolvers<undefined>();
      try {
        await spawnRead.reached;
        // The supervisor is idle, so this reaches only the mirror and makes
        // the spawn resend.
        await supervisor.deliverCredentials({
          delivery: oneMaterial("cred-a"),
        });
        spawnRead.release();
        await resendRead.reached;
        // The child's stdin is full: the update's frame waits on the write.
        hold = release;
        const updating = supervisor.deliverCredentials(update);
        await writeHeld.promise;
        resendRead.release();
        release.resolve(undefined);
        hold = null;
        await updating;
        await inner.awaitChildren(1);
        const child = inner.children[0];
        if (child === undefined) throw new Error("child missing");
        await driveReady(child, ipcKeypair);
        await spawnPromise;
        // The resend sends its frame after the update's.
        await waitUntil(
          () =>
            authoritySetFrames(child.supervisorToChild.flushed()).length >= 2,
        );

        expect(heldCredentialIds(child.supervisorToChild.flushed())).toEqual(
          expected,
        );
      } finally {
        spawnRead.release();
        resendRead.release();
        release.resolve(undefined);
        await spawnPromise.catch(() => undefined);
        await supervisor.shutdown();
      }
    },
  );

  test("a shutdown during the spawn's keypair await stops the spawn before it starts a child", async () => {
    const baseDir = await makeTempDir("recycle-authority-keypair-shutdown-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({ sigtermExits: true });
    const gate = readGate();
    const { supervisor, spawnPromise } = await startGatedKeypairSpawn({
      baseDir,
      tracker,
      ipcKeypair,
      keypair: async () => {
        await gate.arrive();
        return ipcKeypair;
      },
    });
    await gate.reached;
    // The shutdown waits for the spawn, which waits for the keypair.
    const shutdown = supervisor.shutdown();
    gate.release();
    await shutdown;

    await expect(spawnPromise).rejects.toThrow(
      /shut down before the spawn started its child/,
    );
    expect(tracker.children).toHaveLength(0);
  });

  test("a failed keypair leaves no pending spawn for a later shutdown", async () => {
    const baseDir = await makeTempDir("recycle-authority-keypair-fail-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({ sigtermExits: true });
    const { supervisor, spawnPromise } = await startGatedKeypairSpawn({
      baseDir,
      tracker,
      ipcKeypair,
      keypair: () => Promise.reject(new Error("synthetic keypair failure")),
    });
    await expect(spawnPromise).rejects.toThrow(/synthetic keypair failure/);
    // A stale pending spawn would make this wait forever.
    await supervisor.shutdown();
    expect(tracker.children).toHaveLength(0);
  });

  test("a shutdown during the spawn's grants read kills the new child and fails the spawn", async () => {
    const baseDir = await makeTempDir("recycle-authority-shutdown-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({ sigtermExits: true });
    const gate = readGate();
    const { supervisor, spawnPromise } = await startSpawn({
      baseDir,
      tracker,
      ipcKeypair,
      onRunStart: async () => {
        await gate.arrive();
        return versioned("v0");
      },
    });
    await gate.reached;
    // The shutdown waits for the spawn, which waits for the read.
    const shutdown = supervisor.shutdown();
    gate.release();
    await shutdown;

    await expect(spawnPromise).rejects.toThrow(
      /shut down while the spawn read the run's authority/,
    );
    const child = tracker.children[0];
    expect(child?.killSignals.length).toBeGreaterThan(0);
  });

  test("a shutdown during the spawn's definition wait kills the new child without waiting for the deadline", async () => {
    const baseDir = await makeTempDir("recycle-definition-wait-shutdown-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({ sigtermExits: true });
    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
    const bindings = await buildBindings({
      baseDir,
      spawner: tracker.spawner,
      mailBus: createMockMailBus(),
      ipcKeypair,
    });
    // The definition deadline only fires by hand, and this test never fires
    // it, so only the shutdown can end the wait.
    const deadlines = createHandshakeDeadlines(4_242);
    const supervisor = createWorkflowSupervisor({
      ...bindings,
      readyTimeoutMs: deadlines.timers.readyTimeoutMs,
      setTimer: deadlines.timers.setTimer,
      clearTimer: deadlines.timers.clearTimer,
    });
    const spawnPromise = supervisor.spawn({
      definitionHash: TEST_DEFINITION_HASH,
      onInferenceEvent: () => undefined,
    });
    await tracker.awaitChildren(1);

    await supervisor.shutdown();
    await expect(spawnPromise).rejects.toThrow(
      /shut down while the spawn waited for the child's definition/,
    );
    const child = tracker.children[0];
    expect(child?.killSignals.length).toBeGreaterThan(0);
  });

  test("a recycled child's first frame is authority.set with the run's grants and the current credentials", async () => {
    const baseDir = await makeTempDir("recycle-authority-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({});
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus: createMockMailBus(),
      ipcKeypair,
      onRunStart: async () => runSnapshot,
      credentialDelivery: { bindings: [], materials: [material("cred_a")] },
    });
    // A credentials update reaches the supervisor while the first child runs.
    await supervisor.deliverCredentials({
      delivery: { bindings: [], materials: [material("cred_b")] },
    });

    const recyclePromise = supervisor.recycle({ reason: "authority" });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recyclePromise;

    const frames = parseControlFrames(second.supervisorToChild.flushed());
    expect(frames[0]?.type).toBe("authority.set");
    const [authority] = authoritySetFrames(second.supervisorToChild.flushed());
    expect(authority?.snapshot?.steps[0]?.grants).toEqual([
      { id: "run-grant" },
    ]);
    expect(
      authority?.delivery?.materials.map((m) => m.credentialId).sort(),
    ).toEqual(["cred_a", "cred_b"]);

    await supervisor.shutdown();
  });

  test("an older secret that arrives after a newer one leaves the child and the next child on the newer one", async () => {
    const baseDir = await makeTempDir("recycle-secret-order-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({});
    // The deploy delivery carries no version, as a record sealed before
    // versions existed restores it.
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus: createMockMailBus(),
      ipcKeypair,
      onRunStart: async () => runSnapshot,
      credentialDelivery: { bindings: [], materials: [material("cred_a")] },
    });
    const rotated = (secret: string, secretVersion: number) => ({
      delivery: {
        bindings: [],
        materials: [{ ...material("cred_a"), secret, secretVersion }],
      },
    });
    await supervisor.deliverCredentials(rotated("sk-v3", 3));
    await supervisor.deliverCredentials(rotated("sk-v2", 2));

    // The child applies `authority.set` as a replace and each
    // `credentials-updated` frame through the same merge.
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    let cell: CredentialDelivery | null = null;
    for (const frame of parseControlFrames(first.supervisorToChild.flushed())) {
      if (frame.type === "authority.set") {
        cell = frame.data.delivery ?? null;
      } else if (frame.type === "credentials-updated") {
        cell = mergeCredentialDelivery(
          cell,
          frame.data.delivery,
          frame.data.revoke,
        );
      }
    }
    expect(cell?.materials.map((m) => m.secret)).toEqual(["sk-v3"]);

    const recyclePromise = supervisor.recycle({ reason: "authority" });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recyclePromise;
    const [authority] = authoritySetFrames(second.supervisorToChild.flushed());
    expect(authority?.delivery?.materials.map((m) => m.secret)).toEqual([
      "sk-v3",
    ]);

    await supervisor.shutdown();
  });

  test("a credentials update that arrives during the recycle reaches the new child after install", async () => {
    const baseDir = await makeTempDir("recycle-authority-late-");
    const ipcKeypair = await generateKeyPair();
    const tracker = createSpawnTracker({});
    // The first read is the spawn's; the second is the recycle's, which
    // waits until the test has delivered an update.
    let reads = 0;
    let markReached: () => void = () => undefined;
    const reached = new Promise<void>((resolve) => {
      markReached = resolve;
    });
    let release: () => void = () => undefined;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus: createMockMailBus(),
      ipcKeypair,
      onRunStart: async () => {
        reads += 1;
        if (reads === 2) {
          markReached();
          await released;
        }
        return runSnapshot;
      },
      credentialDelivery: { bindings: [], materials: [material("cred_a")] },
    });

    const recyclePromise = supervisor.recycle({ reason: "authority-late" });
    try {
      await reached;
      // The supervisor is recycling: the update reaches only its mirror.
      await supervisor.deliverCredentials({
        delivery: { bindings: [], materials: [material("cred_c")] },
      });
      release();
      const second = tracker.children[1];
      if (second === undefined) throw new Error("second child missing");
      await driveReady(second, ipcKeypair);
      await recyclePromise;

      // The update moved the revision after the frame's read began, so
      // the installed child gets its whole authority again.
      await waitUntil(
        () =>
          authoritySetFrames(second.supervisorToChild.flushed()).length >= 2,
      );
      const frames = authoritySetFrames(second.supervisorToChild.flushed());
      expect(
        frames
          .at(-1)
          ?.delivery?.materials.map((m) => m.credentialId)
          .sort(),
      ).toEqual(["cred_a", "cred_c"]);
    } finally {
      release();
      await recyclePromise.catch(() => undefined);
      await supervisor.shutdown();
    }
  });
});

const RUN = "run_deployment-x";
const ADDRESS = "run_deployment-x@example.com";
const SENDER = "sender@example.com";

// The identity the inbox files a mail from `SENDER` under, and the run's log
// names it by.
function keyOf(messageId: string): Promise<string> {
  return deriveDeliveryKey(SENDER, messageId);
}

// The tip of the run's history that the standby tests' store reports.
const HISTORY_TIP = "tip-at-stop";
const GATE_PARK: RecordedStandby["parked"][number] = {
  runId: RUN,
  correlationId: "corr-gate",
  parkKind: "approval",
  snapshot: {
    name: "charge_card",
    description: "Charge the customer's card",
    inputSchema: { type: "object" },
    arguments: { amount: 100 },
  },
};

// The run's log in the stub store's directory, which the stub serves as its
// committed history.
async function writeWorkingRunLog(
  baseDir: string,
  events: readonly Record<string, unknown>[],
): Promise<void> {
  const dir = path.join(baseDir, "workflow-run", RUN, "runs", RUN, "events");
  await fs.mkdir(dir, { recursive: true });
  for (const [index, event] of events.entries()) {
    const seq = index + 1;
    await fs.writeFile(
      path.join(dir, `${String(seq)}.json`),
      JSON.stringify({ seq, at: "2026-10-01T00:00:00.000Z", ...event }),
    );
  }
}

function runStarted(): Record<string, unknown> {
  return {
    type: "RunStarted",
    runId: RUN,
    definitionHash: "def-hash-abc",
    trigger: { type: "manual", payload: null },
  };
}

// Move a mail to processing/, as a dispatch that took it would.
async function takeMail(
  inbox: ReturnType<typeof createMemoryInboxPrimitives>,
  baseDir: string,
  messageId: string,
  receivedAt: number,
): Promise<void> {
  const store = createStubRepoStore(baseDir);
  const repoId = { kind: "workflow-run", id: RUN } as const;
  const principal = { kind: "supervisor", anchorRunId: RUN } as const;
  await inbox.enqueueInbox(store, principal, repoId, {
    address: ADDRESS,
    messageId,
    authenticatedSender: SENDER,
    receivedAt,
  });
  await inbox.dequeueToProcessing(store, principal, repoId, ADDRESS);
}

// A respawned child resumes the run from its log, so mail the run already
// took stays in processing/ with it instead of being delivered again.
describe("supervisor respawn: mail a resumed run took", () => {
  async function setupOwned(opts: {
    inbox: ReturnType<typeof createMemoryInboxPrimitives>;
    inboxPrimitives?: Partial<InboxPrimitives>;
    setTimer?: WorkflowSupervisorBindings["setTimer"];
    onSuspensionRegister?: WorkflowSupervisorBindings["onSuspensionRegister"];
    mailBus?: MailBusBindings;
    sigtermExits?: boolean;
    repoStore?: (baseDir: string) => RepoStore;
    respawnBackoffInitialMs?: number;
  }) {
    const baseDir = await makeTempDir("owned-");
    await writeWorkingRunLog(baseDir, [
      { ...runStarted(), consumedDeliveryKey: await keyOf("mail-owned") },
    ]);
    await takeMail(opts.inbox, baseDir, "mail-owned", 1);
    const replays: Awaited<
      ReturnType<InboxPrimitives["replayProcessingToInbox"]>
    >[] = [];
    const replayed = createChangeNotifier();
    const tracker = createSpawnTracker(
      opts.sigtermExits !== undefined
        ? { sigtermExits: opts.sigtermExits }
        : {},
    );
    const ipcKeypair = await generateKeyPair();
    const harness = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus: opts.mailBus ?? createMockMailBus(),
      ipcKeypair,
      overrides: {
        inboxPrimitives: {
          ...opts.inbox,
          async replayProcessingToInbox(...args) {
            const result = await opts.inbox.replayProcessingToInbox(...args);
            replays.push(result);
            replayed.notify();
            return result;
          },
          ...opts.inboxPrimitives,
        },
        ...(opts.setTimer !== undefined ? { setTimer: opts.setTimer } : {}),
        ...(opts.onSuspensionRegister !== undefined
          ? { onSuspensionRegister: opts.onSuspensionRegister }
          : {}),
        ...(opts.repoStore !== undefined
          ? { repoStore: opts.repoStore(baseDir) }
          : {}),
        ...(opts.respawnBackoffInitialMs !== undefined
          ? { respawnBackoffInitialMs: opts.respawnBackoffInitialMs }
          : {}),
      },
    });
    await replayed.until(() => replays.length === 1);
    return { ...harness, baseDir, tracker, ipcKeypair, replays };
  }

  test("a respawn during the boot replay waits for it, so mail the replacement takes stays taken", async () => {
    const baseDir = await makeTempDir("boot-replay-");
    const inbox = createMemoryInboxPrimitives();
    const tracker = createSpawnTracker({});
    const ipcKeypair = await generateKeyPair();
    const bootScanning = Promise.withResolvers<undefined>();
    const releaseBoot = Promise.withResolvers<undefined>();
    const respawnWaits = Promise.withResolvers<undefined>();
    let opened = 0;
    const { supervisor } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus: createMockMailBus(),
      ipcKeypair,
      overrides: {
        inboxPrimitives: inbox,
        onRespawnWait: (waiting) => {
          if (waiting === "replay") respawnWaits.resolve(undefined);
        },
        // The boot scan reads the history from before any run started, and
        // stalls on it.
        repoStore: new Proxy(
          createStubRepoStore(baseDir, { writeTree: true }),
          {
            get(target, prop, receiver) {
              if (prop !== "openCommittedReads") {
                return Reflect.get(target, prop, receiver);
              }
              return async (
                ...args: Parameters<RepoStore["openCommittedReads"]>
              ) => {
                const reads = await target.openCommittedReads(...args);
                opened += 1;
                if (opened !== 1 || reads === null) return reads;
                return {
                  ...reads,
                  async listDir(relPath: string) {
                    const entries = await reads.listDir(relPath);
                    bootScanning.resolve(undefined);
                    await releaseBoot.promise;
                    return entries;
                  },
                };
              };
            },
          },
        ),
      },
    });
    await bootScanning.promise;
    const principal = { kind: "supervisor", anchorRunId: RUN } as const;
    await inbox.enqueueInbox(
      createStubRepoStore(baseDir),
      principal,
      { kind: "workflow-run", id: RUN },
      {
        address: ADDRESS,
        messageId: "mail-during-boot",
        authenticatedSender: SENDER,
        receivedAt: 1,
        rawMessage: base64Encode(
          new TextEncoder().encode("Subject: during boot\r\n\r\ninput"),
        ),
      },
    );
    const recycling = supervisor.recycle({ reason: "during the boot replay" });
    // A respawn that does not wait spawns the replacement first, which then
    // takes mail the boot replay puts back.
    expect(
      await Promise.race([
        respawnWaits.promise.then(() => "waited"),
        tracker.awaitChildren(2).then(() => "spawned"),
      ]),
    ).toBe("waited");

    releaseBoot.resolve(undefined);
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recycling;
    await waitForUpstreamPayload(second.supervisorToChild, "trigger.fire");
    expect(inbox.messageIds(ADDRESS)).toEqual({
      inbox: [],
      processing: ["mail-during-boot"],
      consumed: [],
    });

    await supervisor.shutdown();
  });

  test("stays in processing/ and is consumed when the run next parks", async () => {
    const inbox = createMemoryInboxPrimitives();
    const consumed = Promise.withResolvers<undefined>();
    const { supervisor, firstSender } = await setupOwned({
      inbox,
      inboxPrimitives: {
        async markConsumed(...args) {
          const result = await inbox.markConsumed(...args);
          consumed.resolve(undefined);
          return result;
        },
      },
    });
    expect(inbox.messageIds(ADDRESS).processing).toEqual(["mail-owned"]);

    await firstSender.send({
      type: "park.notify",
      data: { runId: RUN, correlationId: "corr-next", parkKind: "input" },
    });
    await consumed.promise;
    expect(inbox.messageIds(ADDRESS)).toEqual({
      inbox: [],
      processing: [],
      consumed: ["mail-owned"],
    });

    await supervisor.shutdown();
  });

  test("a park read once shutdown has waited for its writes starts no consume", async () => {
    const inbox = createMemoryInboxPrimitives();
    let consumes = 0;
    const markerRegistered = Promise.withResolvers<undefined>();
    // Shutdown unregisters the address once it has waited for the last write.
    const bus = createMockMailBus();
    const unregistered = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, tracker } = await setupOwned({
      inbox,
      inboxPrimitives: {
        async markConsumed(...args) {
          consumes += 1;
          return inbox.markConsumed(...args);
        },
      },
      onSuspensionRegister: ({ correlationId }) => {
        if (correlationId === "corr-marker")
          markerRegistered.resolve(undefined);
      },
      mailBus: {
        ...bus,
        unregisterAddress(address) {
          bus.unregisterAddress(address);
          unregistered.resolve(undefined);
        },
      },
      // A child that ignores SIGTERM keeps sending until shutdown's SIGKILL.
      sigtermExits: false,
    });
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");

    const shuttingDown = supervisor.shutdown();
    await unregistered.promise;
    // The supervisor takes frames in order, so once it registers the park sent
    // next, it has read the run's own.
    await firstSender.send({
      type: "park.notify",
      data: { runId: RUN, correlationId: "corr-next", parkKind: "input" },
    });
    await firstSender.send({
      type: "park.notify",
      data: {
        runId: "run-marker",
        correlationId: "corr-marker",
        parkKind: "approval",
      },
    });
    await markerRegistered.promise;
    expect(consumes).toBe(0);

    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(0);
    await shuttingDown;
    expect(inbox.messageIds(ADDRESS).processing).toEqual(["mail-owned"]);
  });

  test("is consumed when the run ended before the replay kept it", async () => {
    const inbox = createMemoryInboxPrimitives();
    const baseDir = await makeTempDir("owned-ended-");
    await writeWorkingRunLog(baseDir, [
      { ...runStarted(), consumedDeliveryKey: await keyOf("mail-owned") },
    ]);
    await takeMail(inbox, baseDir, "mail-owned", 1);
    const replaying = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const registered = Promise.withResolvers<undefined>();
    const consumed = Promise.withResolvers<undefined>();
    const { supervisor, firstSender } = await spawnSupervisor({
      baseDir,
      tracker: createSpawnTracker({}),
      mailBus: createMockMailBus(),
      ipcKeypair: await generateKeyPair(),
      overrides: {
        onSuspensionRegister: () => registered.resolve(undefined),
        inboxPrimitives: {
          ...inbox,
          async replayProcessingToInbox(...args) {
            replaying.resolve(undefined);
            await release.promise;
            return inbox.replayProcessingToInbox(...args);
          },
          async markConsumed(...args) {
            const result = await inbox.markConsumed(...args);
            consumed.resolve(undefined);
            return result;
          },
        },
      },
    });
    await replaying.promise;

    // The run ends while the replay still decides what it took. The park sent
    // next shows the supervisor has taken the end.
    await firstSender.send({
      type: "terminal.event",
      data: { runId: RUN, seq: 1, kind: "RunCompleted", at: "test" },
    });
    await firstSender.send({
      type: "park.notify",
      data: {
        runId: "run-marker",
        correlationId: "corr-marker",
        parkKind: "approval",
      },
    });
    await registered.promise;
    release.resolve(undefined);
    await consumed.promise;
    expect(inbox.messageIds(ADDRESS)).toEqual({
      inbox: [],
      processing: [],
      consumed: ["mail-owned"],
    });

    await supervisor.shutdown();
  });

  test("is consumed when the run parked before the replay kept it", async () => {
    const inbox = createMemoryInboxPrimitives();
    const baseDir = await makeTempDir("owned-parked-");
    await writeWorkingRunLog(baseDir, [
      { ...runStarted(), consumedDeliveryKey: await keyOf("mail-owned") },
    ]);
    await takeMail(inbox, baseDir, "mail-owned", 1);
    const replaying = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const registered = Promise.withResolvers<undefined>();
    const consumed = Promise.withResolvers<undefined>();
    const { supervisor, firstSender } = await spawnSupervisor({
      baseDir,
      tracker: createSpawnTracker({}),
      mailBus: createMockMailBus(),
      ipcKeypair: await generateKeyPair(),
      overrides: {
        onSuspensionRegister: () => registered.resolve(undefined),
        inboxPrimitives: {
          ...inbox,
          async replayProcessingToInbox(...args) {
            replaying.resolve(undefined);
            await release.promise;
            return inbox.replayProcessingToInbox(...args);
          },
          async markConsumed(...args) {
            const result = await inbox.markConsumed(...args);
            consumed.resolve(undefined);
            return result;
          },
        },
      },
    });
    await replaying.promise;

    // The run parks while the replay still decides what it took, and the
    // approval park sent next shows the supervisor has taken it.
    await firstSender.send({
      type: "park.notify",
      data: { runId: RUN, correlationId: "corr-next", parkKind: "input" },
    });
    await firstSender.send({
      type: "park.notify",
      data: {
        runId: "run-marker",
        correlationId: "corr-marker",
        parkKind: "approval",
      },
    });
    await registered.promise;
    release.resolve(undefined);
    await consumed.promise;
    expect(inbox.messageIds(ADDRESS)).toEqual({
      inbox: [],
      processing: [],
      consumed: ["mail-owned"],
    });

    await supervisor.shutdown();
  });

  test("a shutdown waits for the consume of mail a replay it waited for kept", async () => {
    const inbox = createMemoryInboxPrimitives();
    const baseDir = await makeTempDir("owned-shutdown-");
    await writeWorkingRunLog(baseDir, [
      { ...runStarted(), consumedDeliveryKey: await keyOf("mail-owned") },
    ]);
    await takeMail(inbox, baseDir, "mail-owned", 1);
    const replaying = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const registered = Promise.withResolvers<undefined>();
    const writesWaited = Promise.withResolvers<undefined>();
    const { supervisor, firstSender } = await spawnSupervisor({
      baseDir,
      tracker: createSpawnTracker({}),
      mailBus: createMockMailBus(),
      ipcKeypair: await generateKeyPair(),
      overrides: {
        onSuspensionRegister: () => registered.resolve(undefined),
        onShutdownWait: (waiting) => {
          if (waiting === "writes") writesWaited.resolve(undefined);
        },
        inboxPrimitives: {
          ...inbox,
          async replayProcessingToInbox(...args) {
            replaying.resolve(undefined);
            await release.promise;
            return inbox.replayProcessingToInbox(...args);
          },
          async markConsumed(...args) {
            // The consume lands only once the shutdown waits for it.
            await writesWaited.promise;
            return inbox.markConsumed(...args);
          },
        },
      },
    });
    await replaying.promise;
    await firstSender.send({
      type: "terminal.event",
      data: { runId: RUN, seq: 1, kind: "RunCompleted", at: "test" },
    });
    await firstSender.send({
      type: "park.notify",
      data: {
        runId: "run-marker",
        correlationId: "corr-marker",
        parkKind: "approval",
      },
    });
    await registered.promise;

    const shutdown = supervisor.shutdown();
    release.resolve(undefined);
    await shutdown;
    expect(inbox.messageIds(ADDRESS).consumed).toEqual(["mail-owned"]);
  });

  test("a turn the run took stays in processing/ across a recycle", async () => {
    const inbox = createMemoryInboxPrimitives();
    const { supervisor, baseDir, tracker, ipcKeypair, replays } =
      await setupOwned({ inbox });
    // The run took a follow-up mail as its next turn.
    await writeWorkingRunLog(baseDir, [
      { ...runStarted(), consumedDeliveryKey: await keyOf("mail-owned") },
      {
        type: "SignalReceived",
        signalName: "turn",
        signalId: await keyOf("mail-turn"),
        payload: null,
      },
    ]);
    await takeMail(inbox, baseDir, "mail-turn", 2);

    const recycled = supervisor.recycle({ reason: "owned-mail" });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recycled;
    expect(replays.at(-1)).toMatchObject({
      replayedKeys: [],
      retainedIdentities: [await keyOf("mail-owned"), await keyOf("mail-turn")],
    });

    await supervisor.shutdown();
  });

  test("a respawn that cannot read the run's log leaves the mail the run took in processing/", async () => {
    const inbox = createMemoryInboxPrimitives();
    let readFails = false;
    const { supervisor, tracker, ipcKeypair, replays } = await setupOwned({
      inbox,
      repoStore: (baseDir) =>
        new Proxy(createStubRepoStore(baseDir, { writeTree: true }), {
          get(target, prop, receiver) {
            if (prop !== "openCommittedReads") {
              return Reflect.get(target, prop, receiver);
            }
            return async (
              ...args: Parameters<RepoStore["openCommittedReads"]>
            ) => {
              const reads = await target.openCommittedReads(...args);
              if (reads === null) return null;
              return {
                ...reads,
                async listDir(relPath: string) {
                  if (readFails && relPath === `runs/${RUN}`) {
                    throw new Error("EIO: read failed");
                  }
                  return reads.listDir(relPath);
                },
              };
            };
          },
        }),
    });

    readFails = true;
    const recycled = supervisor.recycle({ reason: "unreadable-run" });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await recycled;

    // The replay runs before the replacement spawns, and it did not.
    expect(replays).toHaveLength(1);
    expect(inbox.messageIds(ADDRESS)).toMatchObject({
      inbox: [],
      processing: ["mail-owned"],
    });

    await supervisor.shutdown();
  });

  const respawns: [
    string,
    (
      supervisor: WorkflowSupervisor,
      first: FakeChild,
    ) => Promise<unknown> | undefined,
  ][] = [
    ["recycle", (supervisor) => supervisor.recycle({ reason: "owned-mail" })],
    [
      "crash",
      (_supervisor, first) => {
        first.eventChildToSupervisor.close();
        first.childToSupervisor.close();
        first.resolveExit?.(1);
        return undefined;
      },
    ],
  ];
  test.each(respawns)(
    "a turn kept across a %s is consumed when the run parks in the new child",
    async (_case, respawn) => {
      const inbox = createMemoryInboxPrimitives();
      let consumed = 0;
      const consumes = createChangeNotifier();
      const { supervisor, baseDir, tracker, ipcKeypair } = await setupOwned({
        inbox,
        respawnBackoffInitialMs: 0,
        inboxPrimitives: {
          async markConsumed(...args) {
            const result = await inbox.markConsumed(...args);
            consumed += 1;
            consumes.notify();
            return result;
          },
        },
      });
      const first = tracker.children[0];
      if (first === undefined) throw new Error("first child missing");
      // Only the respawn's replay finds the turn the run took.
      await writeWorkingRunLog(baseDir, [
        { ...runStarted(), consumedDeliveryKey: await keyOf("mail-owned") },
        {
          type: "SignalReceived",
          signalName: "turn",
          signalId: await keyOf("mail-turn"),
          payload: null,
        },
      ]);
      await takeMail(inbox, baseDir, "mail-turn", 2);

      const respawned = respawn(supervisor, first);
      await tracker.awaitChildren(2);
      const second = tracker.children[1];
      if (second === undefined) throw new Error("second child missing");
      const secondSender = await driveReady(second, ipcKeypair);
      await respawned;
      await secondSender.send({
        type: "park.notify",
        data: { runId: RUN, correlationId: "corr-next", parkKind: "input" },
      });
      await consumes.until(() => consumed === 2);
      expect(inbox.messageIds(ADDRESS)).toEqual({
        inbox: [],
        processing: [],
        consumed: ["mail-owned", "mail-turn"],
      });

      await supervisor.shutdown();
    },
  );

  test("a turn the crashed child's last write records stays in processing/", async () => {
    const inbox = createMemoryInboxPrimitives();
    const baseDir = await makeTempDir("owned-late-write-");
    const writing = Promise.withResolvers<undefined>();
    const land = Promise.withResolvers<undefined>();
    const respawnWaits = Promise.withResolvers<undefined>();
    const replays: Awaited<
      ReturnType<InboxPrimitives["replayProcessingToInbox"]>
    >[] = [];
    const replayed = createChangeNotifier();
    const tracker = createSpawnTracker({});
    const ipcKeypair = await generateKeyPair();
    const base = createStubRepoStore(baseDir, { writeTree: true });
    const { supervisor, firstSender } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus: createMockMailBus(),
      ipcKeypair,
      overrides: {
        respawnBackoffInitialMs: 0,
        onRespawnWait: (waiting) => {
          if (waiting === "writes") respawnWaits.resolve(undefined);
        },
        // The child's write that records the run took its next turn.
        repoStore: new Proxy(base, {
          get(target, prop, receiver) {
            if (prop !== "writeTreePreservingPrefix")
              return Reflect.get(target, prop, receiver);
            return async () => {
              writing.resolve(undefined);
              await land.promise;
              await writeWorkingRunLog(baseDir, [
                runStarted(),
                {
                  type: "SignalReceived",
                  signalName: "turn",
                  signalId: await keyOf("mail-turn"),
                  payload: null,
                },
              ]);
              return { commitSha: "deadbeefcafef00d", newlyTerminalRuns: [] };
            };
          },
        }),
        inboxPrimitives: {
          ...inbox,
          async replayProcessingToInbox(...args) {
            const result = await inbox.replayProcessingToInbox(...args);
            replays.push(result);
            replayed.notify();
            return result;
          },
        },
      },
    });
    await replayed.until(() => replays.length === 1);
    await takeMail(inbox, baseDir, "mail-turn", 1);
    await sendChildWrite(firstSender, "turn-write");
    await writing.promise;

    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(1);
    await respawnWaits.promise;
    land.resolve(undefined);
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await replayed.until(() => replays.length === 2);
    expect(replays[1]).toMatchObject({
      replayedKeys: [],
      retainedIdentities: [await keyOf("mail-turn")],
    });
    expect(inbox.messageIds(ADDRESS)).toEqual({
      inbox: [],
      processing: ["mail-turn"],
      consumed: [],
    });

    await supervisor.shutdown();
  });

  // A run that has ended never parks again, so a failed consume is retried
  // on a timer; one already consumed is dropped.
  describe("a failed consume", () => {
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

    async function setupFailing(
      consumeWith: (
        inbox: ReturnType<typeof createMemoryInboxPrimitives>,
      ) => InboxPrimitives["markConsumed"],
    ) {
      const inbox = createMemoryInboxPrimitives();
      const markConsumed = consumeWith(inbox);
      const retries: (() => void)[] = [];
      const retried = createChangeNotifier();
      const harness = await setupOwned({
        inbox,
        inboxPrimitives: { markConsumed: (...args) => markConsumed(...args) },
        // The retry backoffs are a second and then four; other timers stay
        // real.
        setTimer: (callback, ms) => {
          if (ms !== 1_000 && ms !== 4_000) return setTimeout(callback, ms);
          retries.push(callback);
          retried.notify();
          return callback;
        },
      });
      await harness.firstSender.send({
        type: "park.notify",
        data: { runId: RUN, correlationId: "corr-next", parkKind: "input" },
      });
      return { ...harness, inbox, retries, retried };
    }

    test("is retried without the run parking again", async () => {
      let calls = 0;
      const consumed = Promise.withResolvers<undefined>();
      const owned = await setupFailing((inbox) => async (...args) => {
        calls += 1;
        if (calls === 1) throw new Error("the claim-check write failed");
        const result = await inbox.markConsumed(...args);
        consumed.resolve(undefined);
        return result;
      });
      await owned.retried.until(() => owned.retries.length === 1);
      owned.retries[0]?.();
      await consumed.promise;
      expect(owned.inbox.messageIds(ADDRESS).consumed).toEqual(["mail-owned"]);

      await owned.supervisor.shutdown();
    });

    test("gives up after its last retry and leaves the mail in processing/", async () => {
      const owned = await setupFailing(() => async () => {
        throw new Error("the claim-check write keeps failing");
      });
      await owned.retried.until(() => owned.retries.length === 1);
      owned.retries[0]?.();
      await owned.retried.until(() => owned.retries.length === 2);
      owned.retries[1]?.();
      await logs.waitForError(`gave up consuming ${await keyOf("mail-owned")}`);
      expect(owned.retries).toHaveLength(2);
      expect(owned.inbox.messageIds(ADDRESS)).toEqual({
        inbox: [],
        processing: ["mail-owned"],
        consumed: [],
      });

      await owned.supervisor.shutdown();
    });

    test("is not retried for mail something else already consumed", async () => {
      let calls = 0;
      const called = Promise.withResolvers<undefined>();
      const owned = await setupFailing((inbox) => async (...args) => {
        calls += 1;
        // Something else consumes it first.
        await inbox.markConsumed(...args);
        try {
          return await inbox.markConsumed(...args);
        } finally {
          called.resolve(undefined);
        }
      });
      await called.promise;
      // A respawn replays, and so starts its child, only once the old child's
      // writes, this consume among them, have settled.
      const recycled = owned.supervisor.recycle({ reason: "consume settled" });
      await owned.tracker.awaitChildren(2);
      const second = owned.tracker.children[1];
      if (second === undefined) throw new Error("second child missing");
      expect(calls).toBe(1);
      expect(owned.retries).toEqual([]);
      await driveReady(second, owned.ipcKeypair);
      await recycled;

      await owned.supervisor.shutdown();
    });
  });
});

// Answer the `count`th correlation query on the child's channel. Every spawn
// sends one of its own, so a standby attempt's query is the second.
async function answerParkedQuery(
  child: FakeChild,
  childSender: ReturnType<typeof createControlChannelSender>,
  count: number,
  parked: ParkedCorrelation[] = [],
): Promise<void> {
  const queries = await waitForUpstreamPayloads(
    child.supervisorToChild,
    "parked-correlations.request",
    count,
  );
  const query = queries[count - 1];
  if (query === undefined) throw new Error("correlation query missing");
  await childSender.send({
    type: "parked-correlations.response",
    data: { requestId: query.data.requestId, parked },
  });
}

// Answer the `count`th quiet query on the child's channel, and return it.
async function answerQuiet(
  child: FakeChild,
  childSender: ReturnType<typeof createControlChannelSender>,
  count: number,
  answer: { ended: true } | { quiet: boolean; wakeAt?: string; error?: string },
): Promise<{ wait: boolean }> {
  const queries = await waitForUpstreamPayloads(
    child.supervisorToChild,
    "quiet.request",
    count,
  );
  const query = queries[count - 1];
  if (query === undefined) throw new Error("quiet query missing");
  await childSender.send({
    type: "quiet.response",
    data: { requestId: query.data.requestId, ...answer },
  });
  return query.data;
}

// The fake child reports quiet for the wait and the final check, answers the
// standby attempt's correlation query, and exits on its `shutdown`, as the real
// child's control loop does. `firstQuery` numbers the wait's query.
async function actAsChildEnteringStandby(
  child: FakeChild,
  childSender: ReturnType<typeof createControlChannelSender>,
  opts: { firstQuery?: number; wakeAt?: string } = {},
): Promise<void> {
  const first = opts.firstQuery ?? 1;
  const answer = {
    quiet: true,
    ...(opts.wakeAt !== undefined ? { wakeAt: opts.wakeAt } : {}),
  };
  expect((await answerQuiet(child, childSender, first, answer)).wait).toBe(
    true,
  );
  expect((await answerQuiet(child, childSender, first + 1, answer)).wait).toBe(
    false,
  );
  await answerParkedQuery(child, childSender, 2);
  await waitForUpstreamPayload(child.supervisorToChild, "shutdown");
  child.eventChildToSupervisor.close();
  child.childToSupervisor.close();
  child.resolveExit?.(0);
}

function sentFrameTypes(child: FakeChild): string[] {
  return parseControlFrames(child.supervisorToChild.flushed()).map(
    (frame) => frame.type,
  );
}

// The fake child commits the trigger the supervisor fired at it.
async function takeTrigger(
  child: FakeChild,
  childSender: ReturnType<typeof createControlChannelSender>,
): Promise<void> {
  const fire = await waitForUpstreamPayload(
    child.supervisorToChild,
    "trigger.fire",
  );
  const requestId = fire.data.requestId;
  if (requestId === undefined) throw new Error("the trigger has no requestId");
  await childSender.send({
    type: "input.taken",
    data: { requestId, committed: true },
  });
}

// The fake child commits the signal the supervisor delivered to it.
async function confirmSignal(
  child: FakeChild,
  childSender: ReturnType<typeof createControlChannelSender>,
): Promise<void> {
  const delivered = await waitForUpstreamPayload(
    child.supervisorToChild,
    "signal.deliver",
  );
  const requestId = delivered.data.requestId;
  if (requestId === undefined) throw new Error("the signal has no requestId");
  await childSender.send({
    type: "input.taken",
    data: { requestId, committed: true },
  });
}

describe("supervisor standby and wake", () => {
  async function setup(
    opts: {
      onWake?: WorkflowSupervisorBindings["onWake"];
      recordStandby?: (standby: RecordedStandby) => Promise<void>;
      inboxPrimitives?: InboxPrimitives;
      beforeSpawn?: (baseDir: string) => Promise<void>;
      repoStore?: (baseDir: string) => RepoStore;
      onSuspensionRegister?: WorkflowSupervisorBindings["onSuspensionRegister"];
      onStandbyWait?: WorkflowSupervisorBindings["onStandbyWait"];
      onShutdownWait?: WorkflowSupervisorBindings["onShutdownWait"];
      onDispatchWait?: WorkflowSupervisorBindings["onDispatchWait"];
      parkedQueryWatchdogMs?: number;
      timers?: Pick<
        WorkflowSupervisorBindings,
        "setTimer" | "clearTimer" | "now"
      >;
      recyclePolicy?: Partial<WorkflowSupervisorBindings>;
      drainTimeoutAccumulatorFactory?: WorkflowSupervisorBindings["drainTimeoutAccumulatorFactory"];
      respawnBackoffInitialMs?: number;
      dispatchRetryBackoffMs?: number;
      dispatchWaitBackstopMs?: number;
      controlWriteFails?: (line: string) => boolean;
      holdControlWrite?: (line: string) => Promise<undefined> | undefined;
    } = {},
  ) {
    const baseDir = await makeTempDir("standby-");
    await opts.beforeSpawn?.(baseDir);
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({
      ...(opts.controlWriteFails !== undefined
        ? { controlWriteFails: opts.controlWriteFails }
        : {}),
      ...(opts.holdControlWrite !== undefined
        ? { holdControlWrite: opts.holdControlWrite }
        : {}),
    });
    const { supervisor, firstSender } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      overrides: {
        repoStore:
          opts.repoStore?.(baseDir) ??
          createStubRepoStore(baseDir, {
            writeTree: true,
            refTip: HISTORY_TIP,
          }),
        ...(opts.onSuspensionRegister !== undefined
          ? { onSuspensionRegister: opts.onSuspensionRegister }
          : {}),
        ...(opts.onWake !== undefined ? { onWake: opts.onWake } : {}),
        ...(opts.recordStandby !== undefined
          ? { recordStandby: opts.recordStandby }
          : {}),
        ...(opts.onStandbyWait !== undefined
          ? { onStandbyWait: opts.onStandbyWait }
          : {}),
        ...(opts.onShutdownWait !== undefined
          ? { onShutdownWait: opts.onShutdownWait }
          : {}),
        ...(opts.onDispatchWait !== undefined
          ? { onDispatchWait: opts.onDispatchWait }
          : {}),
        ...(opts.parkedQueryWatchdogMs !== undefined
          ? { parkedQueryWatchdogMs: opts.parkedQueryWatchdogMs }
          : {}),
        ...(opts.inboxPrimitives !== undefined
          ? { inboxPrimitives: opts.inboxPrimitives }
          : {}),
        ...(opts.drainTimeoutAccumulatorFactory !== undefined
          ? {
              drainTimeoutAccumulatorFactory:
                opts.drainTimeoutAccumulatorFactory,
            }
          : {}),
        ...(opts.respawnBackoffInitialMs !== undefined
          ? { respawnBackoffInitialMs: opts.respawnBackoffInitialMs }
          : {}),
        ...(opts.dispatchRetryBackoffMs !== undefined
          ? { dispatchRetryBackoffMs: opts.dispatchRetryBackoffMs }
          : {}),
        ...(opts.dispatchWaitBackstopMs !== undefined
          ? { dispatchWaitBackstopMs: opts.dispatchWaitBackstopMs }
          : {}),
        ...opts.timers,
        ...opts.recyclePolicy,
      },
    });
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    return {
      supervisor,
      firstSender,
      first,
      tracker,
      mailBus,
      ipcKeypair,
    };
  }

  test("a child in standby has no recycle policy, and the woken child's uptime counts from its start", async () => {
    const woke = Promise.withResolvers<string>();
    const policy = manualRecyclePolicy(60_000);
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup({
        onWake: ({ reason }) => woke.resolve(reason),
        recyclePolicy: policy.bindings,
      });
    expect(policy.armed()).toBe(1);

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});
    expect(policy.armed()).toBe(0);

    policy.advance(120_000);
    const armed = policy.nextArm();
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("wake up")),
    );
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    expect(await woke.promise).toBe("mail");
    await armed;

    const ticked = policy.nextArm();
    policy.tick();
    await ticked;
    expect(tracker.totalSpawns).toBe(2);

    await supervisor.shutdown();
  });

  test("stops the child once it reports quiet, and mail respawns it", async () => {
    const woke = Promise.withResolvers<string>();
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup({ onWake: ({ reason }) => woke.resolve(reason) });

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});
    expect(tracker.totalSpawns).toBe(1);

    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("wake up")),
    );
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    expect(await woke.promise).toBe("mail");

    await supervisor.shutdown();
  });

  test("the child's exit for standby does not count as a crash", async () => {
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup({ recyclePolicy: { crashLoopMaxCount: 1 } });

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});

    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("wake up")),
    );
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);

    await supervisor.shutdown();
  });

  test("a bad frame from the child going into standby does not stop the supervisor", async () => {
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup();

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await waitForUpstreamPayload(first.supervisorToChild, "shutdown");
    first.childToSupervisor.inject("not-json{{{");
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(0);
    expect(await inStandby).toEqual({});

    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("wake up")),
    );
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);

    await supervisor.shutdown();
  });

  // The inbox as the standby attempt reads it right before the stop, empty, and
  // as `after` once the child stopped.
  function emptyUntilStopped(
    after: InboxPrimitives["hasInboxEntries"],
  ): InboxPrimitives["hasInboxEntries"] {
    let reads = 0;
    return async (...args) => {
      reads += 1;
      return reads === 1 ? false : after(...args);
    };
  }

  // Mail enqueued between the last read before the stop and the stop is in the
  // inbox but was never dispatched.
  const waitingInboxes: [string, InboxPrimitives["hasInboxEntries"]][] = [
    ["holds mail", async () => true],
    [
      "cannot be read",
      async () => {
        throw new Error("the inbox is unreadable");
      },
    ],
  ];
  test.each(waitingInboxes)(
    "a child whose inbox %s once it stops is respawned",
    async (_case, hasInboxEntries) => {
      const woke = Promise.withResolvers<string>();
      const { supervisor, firstSender, first, tracker, ipcKeypair } =
        await setup({
          onWake: ({ reason }) => woke.resolve(reason),
          inboxPrimitives: {
            ...createMemoryInboxPrimitives(),
            hasInboxEntries: emptyUntilStopped(hasInboxEntries),
          },
        });

      const inStandby = supervisor.enterStandby();
      await actAsChildEnteringStandby(first, firstSender);
      expect(await inStandby).toEqual({});

      await tracker.awaitChildren(2);
      const second = tracker.children[1];
      if (second === undefined) throw new Error("second child missing");
      const secondSender = await driveReady(second, ipcKeypair);
      await answerParkedQuery(second, secondSender, 1);
      expect(await woke.promise).toBe("mail");

      await supervisor.shutdown();
    },
  );

  test("mail that lands while the last read before the stop runs wakes the stopped child", async () => {
    const woke = Promise.withResolvers<string>();
    const memory = createMemoryInboxPrimitives();
    let deliverMail = (): Promise<void> => Promise.resolve();
    let reads = 0;
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup({
        onWake: ({ reason }) => woke.resolve(reason),
        inboxPrimitives: {
          ...memory,
          // The read began before the mail landed, and the mail's arrival
          // finds the child still running, so only the read after the stop
          // can wake it.
          async hasInboxEntries(...args) {
            reads += 1;
            if (reads > 1) return memory.hasInboxEntries(...args);
            await deliverMail();
            return false;
          },
        },
      });
    deliverMail = () =>
      mailBus.deliverSettled(
        ADDRESS,
        inboundDelivery(new TextEncoder().encode("late mail")),
      );

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    expect(await woke.promise).toBe("mail");

    await supervisor.shutdown();
  });

  test("a standby attempt is refused while mail waits in the inbox, and the child keeps running", async () => {
    const { supervisor, firstSender, first, tracker } = await setup({
      inboxPrimitives: {
        ...createMemoryInboxPrimitives(),
        // Mail behind a run that waits on an approval.
        async hasInboxEntries() {
          return true;
        },
      },
    });

    const inStandby = supervisor.enterStandby();
    expect(
      (await answerQuiet(first, firstSender, 1, { quiet: true })).wait,
    ).toBe(true);
    expect(
      (await answerQuiet(first, firstSender, 2, { quiet: true })).wait,
    ).toBe(false);
    await answerParkedQuery(first, firstSender, 2);
    await expect(inStandby).rejects.toThrow(
      "mail is waiting in the deployment's inbox",
    );
    expect(sentFrameTypes(first)).not.toContain("shutdown");
    expect(tracker.totalSpawns).toBe(1);

    await supervisor.shutdown();
  });

  test("a standby attempt is refused when the inbox cannot be read before the stop, and the child keeps running", async () => {
    const { supervisor, firstSender, first, tracker } = await setup({
      inboxPrimitives: {
        ...createMemoryInboxPrimitives(),
        async hasInboxEntries() {
          throw new Error("the inbox is unreadable");
        },
      },
    });

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await expect(inStandby).rejects.toThrow(
      "the deployment's inbox could not be read: the inbox is unreadable",
    );
    await expect(inStandby).rejects.toBeInstanceOf(StandbyRefusedError);
    expect(sentFrameTypes(first)).not.toContain("shutdown");
    expect(tracker.totalSpawns).toBe(1);

    await supervisor.shutdown();
  });

  test("a standby attempt is refused when the run's history cannot be read, and nothing is recorded", async () => {
    const records: RecordedStandby[] = [];
    const { supervisor, firstSender, first, tracker } = await setup({
      recordStandby: async (standby) => {
        records.push(standby);
      },
      repoStore: (baseDir) => {
        const base = createStubRepoStore(baseDir, { writeTree: true });
        return new Proxy(base, {
          get(target, prop, receiver) {
            if (prop !== "resolveRef")
              return Reflect.get(target, prop, receiver);
            return async () => {
              throw new Error("the history ref is unreadable");
            };
          },
        });
      },
    });

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await expect(inStandby).rejects.toThrow(
      "the run's history could not be read: the history ref is unreadable",
    );
    await expect(inStandby).rejects.toBeInstanceOf(StandbyRefusedError);
    expect(sentFrameTypes(first)).not.toContain("shutdown");
    expect(records).toEqual([]);
    expect(tracker.totalSpawns).toBe(1);

    await supervisor.shutdown();
  });

  // A repo store whose write of a mail's parts fails while `fails` says so:
  // the dispatch then fails before any input reaches the child.
  function failingPartWrites(fails: () => boolean) {
    return (baseDir: string): RepoStore =>
      new Proxy(createStubRepoStore(baseDir, { writeTree: true }), {
        get(target, prop, receiver) {
          if (prop !== "writeTreePreservingPrefix")
            return Reflect.get(target, prop, receiver);
          return async (
            ...args: Parameters<RepoStore["writeTreePreservingPrefix"]>
          ) => {
            if (args[3].preservePrefix.includes("/parts/") && fails()) {
              throw new Error("the mail's parts could not be written");
            }
            return target.writeTreePreservingPrefix(...args);
          };
        },
      });
  }

  function textMail() {
    return inboundDelivery(
      new TextEncoder().encode(
        `From: a@example.com\r\nTo: ${ADDRESS}\r\nContent-Type: text/plain\r\n\r\ninput`,
      ),
    );
  }

  test("a dispatch that fails before the child takes its mail puts the mail back and tries it again", async () => {
    let failures = 0;
    const inbox = createMemoryInboxPrimitives();
    const { supervisor, firstSender, first, mailBus } = await setup({
      inboxPrimitives: inbox,
      dispatchRetryBackoffMs: 0,
      repoStore: failingPartWrites(() => (failures += 1) === 1),
    });
    mailBus.deliver(ADDRESS, textMail());

    // The second try reaches the child, with no respawn in between.
    await takeTrigger(first, firstSender);
    expect(failures).toBe(2);
    expect(inbox.messageIds(ADDRESS).inbox).toEqual([]);

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});

    await supervisor.shutdown();
  });

  test.each([
    ["a trigger", '"trigger.fire"', false],
    ["a signal", '"signal.deliver"', true],
  ])(
    "mail is put back and sent again when %s could not be sent to the child",
    async (_case, frame, liveRun) => {
      let sends = 0;
      const inbox = createMemoryInboxPrimitives();
      const { supervisor, firstSender, first, mailBus } = await setup({
        inboxPrimitives: inbox,
        dispatchRetryBackoffMs: 0,
        // The first send of the input fails, and the child stays alive.
        controlWriteFails: (line) => line.includes(frame) && (sends += 1) === 1,
        ...(liveRun
          ? {
              beforeSpawn: (baseDir: string) =>
                writeWorkingRunLog(baseDir, [runStarted()]),
            }
          : {}),
      });
      if (liveRun) {
        await answerParkedQuery(first, firstSender, 1, [
          { runId: RUN, correlationId: "corr-unsent", parkKind: "input" },
        ]);
      }
      mailBus.deliver(ADDRESS, textMail());

      if (liveRun) {
        await confirmSignal(first, firstSender);
      } else {
        await takeTrigger(first, firstSender);
      }
      expect(sends).toBe(2);
      expect(inbox.messageIds(ADDRESS).inbox).toEqual([]);

      const inStandby = supervisor.enterStandby();
      await actAsChildEnteringStandby(first, firstSender, {
        firstQuery: 1,
      });
      expect(await inStandby).toEqual({});

      await supervisor.shutdown();
    },
  );

  test("mail put back after a failed dispatch refuses a standby attempt while it waits for its next try", async () => {
    const putBack = Promise.withResolvers<undefined>();
    const inbox = createMemoryInboxPrimitives();
    const { supervisor, firstSender, first, mailBus } = await setup({
      inboxPrimitives: {
        ...inbox,
        replayProcessingToInbox: async (...args) => {
          const replayed = await inbox.replayProcessingToInbox(...args);
          if (args[4]?.onlyIdentities !== undefined) {
            putBack.resolve(undefined);
          }
          return replayed;
        },
      },
      // Longer than the test runs, so the next try never starts.
      dispatchRetryBackoffMs: 60 * 60_000,
      repoStore: failingPartWrites(() => true),
    });
    mailBus.deliver(ADDRESS, textMail());
    await putBack.promise;
    expect(inbox.messageIds(ADDRESS)).toMatchObject({
      processing: [],
      consumed: [],
    });
    expect(inbox.messageIds(ADDRESS).inbox).toHaveLength(1);

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await expect(inStandby).rejects.toThrow(
      "mail is waiting in the deployment's inbox",
    );
    expect(sentFrameTypes(first)).not.toContain("shutdown");

    await supervisor.shutdown();
  });

  test("mail whose dispatch keeps failing is rejected once its tries are spent", async () => {
    let failures = 0;
    const rejected =
      Promise.withResolvers<Parameters<InboxPrimitives["markConsumed"]>[3]>();
    const inbox = createMemoryInboxPrimitives();
    const { supervisor, firstSender, first, mailBus } = await setup({
      inboxPrimitives: {
        ...inbox,
        markConsumed: async (...args) => {
          const consumed = await inbox.markConsumed(...args);
          rejected.resolve(args[3]);
          return consumed;
        },
      },
      dispatchRetryBackoffMs: 0,
      repoStore: failingPartWrites(() => {
        failures += 1;
        return true;
      }),
    });
    mailBus.deliver(ADDRESS, textMail());

    expect((await rejected.promise).rejection).toMatchObject({
      code: "workflow_dispatch_failed",
    });
    expect(failures).toBe(5);
    expect(inbox.messageIds(ADDRESS)).toMatchObject({
      inbox: [],
      processing: [],
    });
    expect(inbox.messageIds(ADDRESS).consumed).toHaveLength(1);
    expect(sentFrameTypes(first)).not.toContain("trigger.fire");

    // Nothing is left to refuse a standby attempt.
    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});

    await supervisor.shutdown();
  });

  test("a write that cannot put failed mail back is tried again, and a standby attempt is refused until it lands", async () => {
    let failPutBack = true;
    const putBackFailed = Promise.withResolvers<undefined>();
    const inbox = createMemoryInboxPrimitives();
    const { supervisor, firstSender, first, mailBus } = await setup({
      inboxPrimitives: {
        ...inbox,
        replayProcessingToInbox: async (...args) => {
          if (args[4]?.onlyIdentities !== undefined && failPutBack) {
            putBackFailed.resolve(undefined);
            throw new Error("the inbox could not be written");
          }
          return inbox.replayProcessingToInbox(...args);
        },
      },
      // Longer than the test runs, so the put-back is not tried again.
      dispatchRetryBackoffMs: 60 * 60_000,
      repoStore: failingPartWrites(() => true),
    });
    mailBus.deliver(ADDRESS, textMail());
    await putBackFailed.promise;
    failPutBack = false;

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await expect(inStandby).rejects.toThrow(
      "mail a failed dispatch left in processing/ is not back in the inbox or with its run yet",
    );
    expect(sentFrameTypes(first)).not.toContain("shutdown");
    expect(inbox.messageIds(ADDRESS).processing).toHaveLength(1);

    await supervisor.shutdown();
  });

  test("a standby attempt is refused after a failed boot replay, which can leave mail behind", async () => {
    const inbox = createMemoryInboxPrimitives();
    const { supervisor, firstSender, first } = await setup({
      inboxPrimitives: {
        ...inbox,
        replayProcessingToInbox: async () => {
          throw new Error("the boot replay failed");
        },
      },
    });

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await expect(inStandby).rejects.toThrow(
      "mail a failed replay left waits in processing/ for the child to restart",
    );
    expect(sentFrameTypes(first)).not.toContain("shutdown");

    await supervisor.shutdown();
  });

  test("a standby attempt is refused after a failed respawn replay, and the next replay clears it", async () => {
    const inbox = createMemoryInboxPrimitives();
    let replays = 0;
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup({
        respawnBackoffInitialMs: 1,
        inboxPrimitives: {
          ...inbox,
          replayProcessingToInbox: async (...args) => {
            replays += 1;
            if (replays === 2) throw new Error("the respawn replay failed");
            return inbox.replayProcessingToInbox(...args);
          },
        },
      });
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("hello")),
    );
    await takeTrigger(first, firstSender);

    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(1);
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    expect(inbox.messageIds(ADDRESS).processing).toHaveLength(1);

    const refused = supervisor.enterStandby();
    await answerQuiet(second, secondSender, 1, { quiet: true });
    await answerQuiet(second, secondSender, 2, { quiet: true });
    await answerParkedQuery(second, secondSender, 2);
    await expect(refused).rejects.toThrow(
      "mail a failed replay left waits in processing/ for the child to restart",
    );
    expect(sentFrameTypes(second)).not.toContain("shutdown");

    second.eventChildToSupervisor.close();
    second.childToSupervisor.close();
    second.resolveExit?.(1);
    await tracker.awaitChildren(3);
    const third = tracker.children[2];
    if (third === undefined) throw new Error("third child missing");
    const thirdSender = await driveReady(third, ipcKeypair);
    await answerParkedQuery(third, thirdSender, 1);
    await takeTrigger(third, thirdSender);
    expect(inbox.messageIds(ADDRESS).inbox).toEqual([]);

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(third, thirdSender);
    expect(await inStandby).toEqual({});

    await supervisor.shutdown();
  });

  test("a child that crashes during the final check puts the standby attempt off, and the respawned child takes mail", async () => {
    const waits: string[] = [];
    const waited = createChangeNotifier();
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup({
        respawnBackoffInitialMs: 0,
        onStandbyWait: (waiting) => {
          waits.push(waiting);
          waited.notify();
        },
      });

    const inStandby = supervisor.enterStandby();
    await waitForUpstreamPayload(first.supervisorToChild, "quiet.request");
    // A signal the child has not taken yet holds the final check back.
    await supervisor.deliverSignal({
      runId: RUN,
      signalName: "gate",
      signalId: "sig-1",
      payload: { approved: true },
    });
    const before = waits.length;
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await waited.until(() => waits.slice(before).includes("standby"));
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(1);

    await expect(inStandby).rejects.toBeInstanceOf(StandbyDeferredError);
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("hello")),
    );
    await takeTrigger(second, secondSender);

    await supervisor.shutdown();
  });

  test("a policy recycle during the final check puts the standby attempt off, and the new child takes mail", async () => {
    const policy = manualRecyclePolicy(60_000);
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup({ recyclePolicy: policy.bindings });

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await waitForUpstreamPayloads(first.supervisorToChild, "quiet.request", 2);
    policy.advance(61_000);
    policy.tick();

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await expect(inStandby).rejects.toBeInstanceOf(StandbyDeferredError);
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("hello")),
    );
    await takeTrigger(second, secondSender);

    await supervisor.shutdown();
  });

  test("an operator recycle while the standby stop waits on its correlation query puts the attempt off", async () => {
    const { supervisor, firstSender, first, tracker, ipcKeypair } =
      await setup();

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await waitForUpstreamPayloads(
      first.supervisorToChild,
      "parked-correlations.request",
      2,
    );
    const recycled = supervisor.recycle({ reason: "operator" });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await recycled;
    await expect(inStandby).rejects.toBeInstanceOf(StandbyDeferredError);

    await supervisor.shutdown();
  });

  test("a standby attempt waits while the child works, and mail still reaches it", async () => {
    const { supervisor, firstSender, first, mailBus } = await setup();

    const inStandby = supervisor.enterStandby();
    expect(
      (await answerQuiet(first, firstSender, 1, { quiet: false })).wait,
    ).toBe(true);
    // Not quiet yet: the child is asked again, and dispatch goes on.
    await waitForUpstreamPayloads(first.supervisorToChild, "quiet.request", 2);
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("hello")),
    );
    await takeTrigger(first, firstSender);
    expect(first.killSignals).toEqual([]);

    await actAsChildEnteringStandby(first, firstSender, { firstQuery: 2 });
    expect(await inStandby).toEqual({});

    await supervisor.shutdown();
  });

  test("a child working again at the final check lets held mail through and waits for quiet once more", async () => {
    const held = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first, mailBus } = await setup({
      onStandbyWait: (waiting) => {
        if (waiting === "input") held.resolve(undefined);
      },
    });

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await waitForUpstreamPayloads(first.supervisorToChild, "quiet.request", 2);
    // Mail during the final check is held back.
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("hello")),
    );
    await held.promise;
    expect(sentFrameTypes(first)).not.toContain("trigger.fire");

    expect(
      (await answerQuiet(first, firstSender, 2, { quiet: false })).wait,
    ).toBe(false);
    await takeTrigger(first, firstSender);
    expect(first.killSignals).toEqual([]);

    await actAsChildEnteringStandby(first, firstSender, { firstQuery: 3 });
    expect(await inStandby).toEqual({});

    await supervisor.shutdown();
  });

  test("a write that lands after the child reports quiet sends the standby attempt back to waiting", async () => {
    // The first write the child asks for waits until the test lets it land.
    const firstWrite = Promise.withResolvers<undefined>();
    const registered = Promise.withResolvers<undefined>();
    let writes = 0;
    const { supervisor, firstSender, first } = await setup({
      onSuspensionRegister: () => registered.resolve(undefined),
      repoStore: (baseDir) => {
        const base = createStubRepoStore(baseDir, { writeTree: true });
        return new Proxy(base, {
          get(target, prop, receiver) {
            if (prop !== "writeTreePreservingPrefix") {
              return Reflect.get(target, prop, receiver);
            }
            return async (
              ...args: Parameters<RepoStore["writeTreePreservingPrefix"]>
            ) => {
              writes += 1;
              if (writes === 1) await firstWrite.promise;
              return target.writeTreePreservingPrefix(...args);
            };
          },
        });
      },
    });

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await waitForUpstreamPayloads(first.supervisorToChild, "quiet.request", 2);
    // A write still landing when the child answers holds the final check.
    await sendChildWrite(firstSender, "landing");
    await answerQuiet(first, firstSender, 2, { quiet: true });
    // A write after the answer is held by the fence and found by the check,
    // which waits for the landing write. The supervisor takes frames in order,
    // so once it registers the park sent next, it has taken that write.
    await sendChildWrite(firstSender, "after-quiet");
    await firstSender.send({
      type: "park.notify",
      data: { runId: RUN, correlationId: "corr-marker", parkKind: "approval" },
    });
    await registered.promise;
    firstWrite.resolve(undefined);

    expect(
      (await answerQuiet(first, firstSender, 3, { quiet: true })).wait,
    ).toBe(true);
    expect(first.killSignals).toEqual([]);
    expect(sentFrameTypes(first)).not.toContain("shutdown");

    await supervisor.wake();
    await expect(inStandby).rejects.toBeInstanceOf(StandbyRefusedError);
    await supervisor.shutdown();
  });

  test("writes a crashed child left behind the fence never land", async () => {
    // The first write the child asks for waits until the test lets it land.
    const firstWrite = Promise.withResolvers<undefined>();
    const registered = Promise.withResolvers<undefined>();
    const messages: string[] = [];
    const { supervisor, firstSender, first } = await setup({
      onSuspensionRegister: () => registered.resolve(undefined),
      repoStore: (baseDir) => {
        const base = createStubRepoStore(baseDir, { writeTree: true });
        return new Proxy(base, {
          get(target, prop, receiver) {
            if (prop !== "writeTreePreservingPrefix") {
              return Reflect.get(target, prop, receiver);
            }
            return async (
              ...args: Parameters<RepoStore["writeTreePreservingPrefix"]>
            ) => {
              if (messages.length === 0) await firstWrite.promise;
              messages.push(args[3].message);
              return target.writeTreePreservingPrefix(...args);
            };
          },
        });
      },
    });

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await waitForUpstreamPayloads(first.supervisorToChild, "quiet.request", 2);
    await sendChildWrite(firstSender, "landing");
    await answerQuiet(first, firstSender, 2, { quiet: true });
    // Held by the fence; the park sent next shows the supervisor took it.
    await sendChildWrite(firstSender, "after-quiet");
    await firstSender.send({
      type: "park.notify",
      data: { runId: RUN, correlationId: "corr-marker", parkKind: "approval" },
    });
    await registered.promise;
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(1);
    // The supervisor watched this exit first, so it has seen it once this
    // await returns, before the landing write lets the standby attempt go on.
    await first.exited;
    firstWrite.resolve(undefined);

    await expect(inStandby).rejects.toBeInstanceOf(StandbyDeferredError);
    expect(messages).toEqual(["child write landing"]);
    await supervisor.shutdown();
  });

  test("a child that writes after the stop began is woken for that work once it stops", async () => {
    const woke = Promise.withResolvers<string>();
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      { onWake: ({ reason }) => woke.resolve(reason) },
    );

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await waitForUpstreamPayloads(
      first.supervisorToChild,
      "parked-correlations.request",
      2,
    );
    await sendChildWrite(firstSender, "after-stop");
    await answerParkedQuery(first, firstSender, 2);
    await waitForUpstreamPayload(first.supervisorToChild, "shutdown");
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(0);
    expect(await inStandby).toEqual({});

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    expect(await woke.promise).toBe("work it began after it went quiet");

    await supervisor.shutdown();
  });

  test("a standby attempt waits for a mail the child has not taken yet", async () => {
    const waits = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first, mailBus } = await setup({
      onStandbyWait: (waiting) => {
        if (waiting === "standby") waits.resolve(undefined);
      },
    });
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("hello")),
    );
    const fire = await waitForUpstreamPayload(
      first.supervisorToChild,
      "trigger.fire",
    );
    const requestId = fire.data.requestId;
    if (requestId === undefined)
      throw new Error("the trigger has no requestId");

    const inStandby = supervisor.enterStandby();
    await waits.promise;
    expect(sentFrameTypes(first)).not.toContain("quiet.request");
    await firstSender.send({
      type: "input.taken",
      data: { requestId, committed: true },
    });
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});

    await supervisor.shutdown();
  });

  test("a run in standby keeps the mail it took", async () => {
    const inbox = createMemoryInboxPrimitives();
    const { supervisor, firstSender, first, mailBus } = await setup({
      inboxPrimitives: inbox,
    });
    const mail = new TextEncoder().encode("hello");
    mailBus.deliver(ADDRESS, inboundDelivery(mail));
    const fire = await waitForUpstreamPayload(
      first.supervisorToChild,
      "trigger.fire",
    );
    const requestId = fire.data.requestId;
    if (requestId === undefined)
      throw new Error("the trigger has no requestId");
    await firstSender.send({
      type: "input.taken",
      data: { requestId, committed: true },
    });

    // The dispatch for a mail waits until its run parks or ends; it does not
    // hold up a standby attempt, since the run keeps that mail across a
    // respawn.
    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});
    expect(inbox.messageIds(ADDRESS)).toEqual({
      inbox: [],
      processing: [await deriveMessageId(mail)],
      consumed: [],
    });

    await supervisor.shutdown();
  });

  test("a redelivery of mail a run in standby took does not wake the child", async () => {
    const inbox = createMemoryInboxPrimitives();
    const wakes: boolean[] = [];
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup({
        inboxPrimitives: {
          ...inbox,
          // As the durable inbox does, mail a run took is not enqueued again.
          enqueueInbox: async (store, principal, repoId, args) =>
            inbox.messageIds(args.address).processing.includes(args.messageId)
              ? { outcome: "already-present", reason: "processing" }
              : inbox.enqueueInbox(store, principal, repoId, args),
        },
        onWake: ({ requested }) => wakes.push(requested),
      });
    const mail = new TextEncoder().encode("hello");
    mailBus.deliver(ADDRESS, inboundDelivery(mail));
    const fire = await waitForUpstreamPayload(
      first.supervisorToChild,
      "trigger.fire",
    );
    const requestId = fire.data.requestId;
    if (requestId === undefined)
      throw new Error("the trigger has no requestId");
    await firstSender.send({
      type: "input.taken",
      data: { requestId, committed: true },
    });
    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});

    await mailBus.deliverSettled(ADDRESS, inboundDelivery(mail));
    // A wake the redelivery started would be the one this joins.
    const woken = supervisor.wake();
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await woken;
    expect(wakes).toEqual([true]);

    await supervisor.shutdown();
  });

  test("a standby attempt is refused while a mail the child declined waits for it", async () => {
    const { supervisor, firstSender, first, tracker, mailBus } = await setup();
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("hello")),
    );
    const fire = await waitForUpstreamPayload(
      first.supervisorToChild,
      "trigger.fire",
    );
    const requestId = fire.data.requestId;
    if (requestId === undefined)
      throw new Error("the trigger has no requestId");
    await firstSender.send({
      type: "input.taken",
      data: { requestId, committed: false },
    });

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await expect(inStandby).rejects.toThrow(
      "mail the dispatch took is still waiting for the child",
    );
    expect(first.killSignals).toEqual([]);
    expect(tracker.totalSpawns).toBe(1);

    await supervisor.shutdown();
  });

  // The dispatch's holds on mail for a live run, in order.
  function dispatchWaits() {
    const held: PromiseWithResolvers<undefined>[] = [];
    const at = (n: number) => {
      while (held.length < n) held.push(Promise.withResolvers());
      const entry = held[n - 1];
      if (entry === undefined) throw new Error("no dispatch hold");
      return entry;
    };
    let count = 0;
    return {
      onDispatchWait: () => {
        count += 1;
        at(count).resolve(undefined);
      },
      nth: (n: number) => at(n).promise,
    };
  }

  // A run that is live on the first child.
  async function setupLiveRun(
    opts: {
      inboxPrimitives?: InboxPrimitives;
      respawnBackoffInitialMs?: number;
      dispatchRetryBackoffMs?: number;
      dispatchWaitBackstopMs?: number;
    } = {},
  ) {
    const waits = dispatchWaits();
    const harness = await setup({
      ...opts,
      beforeSpawn: (baseDir) => writeWorkingRunLog(baseDir, [runStarted()]),
      onDispatchWait: waits.onDispatchWait,
    });
    return { ...harness, waits };
  }

  // The child's answer to the `count`th `signal.deliver` it was sent.
  async function confirmDelivery(
    child: FakeChild,
    childSender: ReturnType<typeof createControlChannelSender>,
    count: number,
    committed: boolean,
  ): Promise<void> {
    const deliveries = await waitForUpstreamPayloads(
      child.supervisorToChild,
      "signal.deliver",
      count,
    );
    const requestId = deliveries[count - 1]?.data.requestId;
    if (requestId === undefined) {
      throw new Error("the delivery asked for no confirmation");
    }
    await childSender.send({
      type: "input.taken",
      data: { requestId, committed },
    });
  }

  test("mail that wakes the child reaches a run the new child resumed parked on input", async () => {
    const {
      supervisor,
      firstSender,
      first,
      tracker,
      mailBus,
      ipcKeypair,
      waits,
    } = await setupLiveRun();
    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});

    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("next turn")),
    );
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    // The dispatch holds the mail before the new child says what its resumed
    // run is parked on, and a park the run already holds sends no notice.
    await waits.nth(1);
    await answerParkedQuery(second, secondSender, 1, [
      { runId: RUN, correlationId: "corr-next-turn", parkKind: "input" },
    ]);
    const delivered = await waitForUpstreamPayload(
      second.supervisorToChild,
      "signal.deliver",
    );
    expect(delivered.data).toMatchObject({
      runId: RUN,
      signalName: signalName("corr-next-turn"),
    });

    await supervisor.shutdown();
  });

  test("a later re-registration delivers mail waiting for the run's input", async () => {
    const { supervisor, firstSender, first, mailBus, waits } =
      await setupLiveRun();
    // The child's first answer shows no park, so the mail waits.
    await answerParkedQuery(first, firstSender, 1);
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("next turn")),
    );
    await waits.nth(1);

    const reEmitted = supervisor.reEmitParkedCorrelations();
    await answerParkedQuery(first, firstSender, 2, [
      { runId: RUN, correlationId: "corr-next", parkKind: "input" },
    ]);
    await reEmitted;
    const delivered = await waitForUpstreamPayload(
      first.supervisorToChild,
      "signal.deliver",
    );
    expect(delivered.data).toMatchObject({
      runId: RUN,
      signalName: signalName("corr-next"),
    });

    await supervisor.shutdown();
  });

  // History the child answers from can lag an input it took, so a
  // re-registration can still show it, before or after the child confirms it.
  const takenOrders: ["confirmation" | "re-registration"][] = [
    ["confirmation"],
    ["re-registration"],
  ];
  test.each(takenOrders)(
    "an input the child took is not registered again when its %s comes first",
    async (comesFirst) => {
      const { supervisor, firstSender, first, mailBus, waits } =
        await setupLiveRun();
      await answerParkedQuery(first, firstSender, 1, [
        { runId: RUN, correlationId: "corr-taken", parkKind: "input" },
      ]);
      mailBus.deliver(
        ADDRESS,
        inboundDelivery(new TextEncoder().encode("turn one")),
      );
      const reRegister = async () => {
        const reEmitted = supervisor.reEmitParkedCorrelations();
        await answerParkedQuery(first, firstSender, 2, [
          { runId: RUN, correlationId: "corr-taken", parkKind: "input" },
        ]);
        await reEmitted;
      };
      if (comesFirst === "confirmation") {
        await confirmDelivery(first, firstSender, 1, true);
        await reRegister();
      } else {
        await waitForUpstreamPayload(first.supervisorToChild, "signal.deliver");
        await reRegister();
        await confirmDelivery(first, firstSender, 1, true);
      }

      // The run then waits on an approval, which shows no input channel, so
      // the next mail waits for the run's next input.
      mailBus.deliver(
        ADDRESS,
        inboundDelivery(new TextEncoder().encode("turn two")),
      );
      await firstSender.send({ type: "park.notify", data: GATE_PARK });
      await waits.nth(1);
      await firstSender.send({
        type: "park.notify",
        data: { runId: RUN, correlationId: "corr-next", parkKind: "input" },
      });
      const deliveries = await waitForUpstreamPayloads(
        first.supervisorToChild,
        "signal.deliver",
        2,
      );
      expect(deliveries[1]?.data).toMatchObject({
        runId: RUN,
        signalName: signalName("corr-next"),
      });

      await supervisor.shutdown();
    },
  );

  test("an input the child declined is put back and delivered again, ahead of newer mail", async () => {
    const inbox = createMemoryInboxPrimitives();
    // Mail that arrives in one millisecond has no order of its own.
    let arrivals = 0;
    const { supervisor, firstSender, first, mailBus } = await setupLiveRun({
      inboxPrimitives: {
        ...inbox,
        enqueueInbox: (store, principal, repoId, args) =>
          inbox.enqueueInbox(store, principal, repoId, {
            ...args,
            receivedAt: (arrivals += 1),
          }),
      },
      dispatchRetryBackoffMs: 0,
    });
    const parkedOnInput: ParkedCorrelation[] = [
      { runId: RUN, correlationId: "corr-declined", parkKind: "input" },
    ];
    await answerParkedQuery(first, firstSender, 1, parkedOnInput);
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("turn one")),
    );
    const firstDelivery = await waitForUpstreamPayload(
      first.supervisorToChild,
      "signal.deliver",
    );
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("turn two")),
    );
    await confirmDelivery(first, firstSender, 1, false);

    // The decline used up the input channel the dispatch knew, so the
    // supervisor asks the child what its run is parked on. The run still waits
    // on that input, and turn one goes to it again.
    await answerParkedQuery(first, firstSender, 2, parkedOnInput);
    const deliveries = await waitForUpstreamPayloads(
      first.supervisorToChild,
      "signal.deliver",
      2,
    );
    expect(deliveries[1]?.data).toMatchObject({
      runId: RUN,
      signalName: signalName("corr-declined"),
      signalId: firstDelivery.data.signalId,
    });
    expect(inbox.messageIds(ADDRESS).consumed).toEqual([]);
    expect(inbox.messageIds(ADDRESS).inbox).toHaveLength(1);

    await supervisor.shutdown();
  });

  test("a dispatch wait that runs out puts its mail back without counting against it", async () => {
    const inbox = createMemoryInboxPrimitives();
    let putBacks = 0;
    const putBackOften = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first, mailBus } = await setupLiveRun({
      inboxPrimitives: {
        ...inbox,
        replayProcessingToInbox: async (...args) => {
          const replayed = await inbox.replayProcessingToInbox(...args);
          if (args[4]?.onlyIdentities !== undefined && (putBacks += 1) === 8) {
            putBackOften.resolve(undefined);
          }
          return replayed;
        },
      },
      dispatchRetryBackoffMs: 0,
      dispatchWaitBackstopMs: 1,
    });
    await answerParkedQuery(first, firstSender, 1);
    // The run shows no input channel, so the mail waits for one and each wait
    // runs out.
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("turn one")),
    );
    await putBackOften.promise;
    expect(inbox.messageIds(ADDRESS).consumed).toEqual([]);

    // More waits than a mail has tries, and it still reaches the run.
    await firstSender.send({
      type: "park.notify",
      data: {
        runId: RUN,
        correlationId: "corr-late",
        parkKind: "input",
      },
    });
    const delivered = await waitForUpstreamPayload(
      first.supervisorToChild,
      "signal.deliver",
    );
    expect(delivered.data).toMatchObject({
      runId: RUN,
      signalName: signalName("corr-late"),
    });

    await supervisor.shutdown();
  });

  // A dispatch that gave up on the child's answer for turn one, with turn two
  // waiting for the run's next input channel.
  async function setupUnansweredInput() {
    const inbox = createMemoryInboxPrimitives();
    const consumed: string[] = [];
    const consumedOne = Promise.withResolvers<undefined>();
    let arrivals = 0;
    const harness = await setupLiveRun({
      inboxPrimitives: {
        ...inbox,
        enqueueInbox: (store, principal, repoId, args) =>
          inbox.enqueueInbox(store, principal, repoId, {
            ...args,
            receivedAt: (arrivals += 1),
          }),
        markConsumed: async (...args) => {
          const result = await inbox.markConsumed(...args);
          consumed.push(args[3].identity);
          consumedOne.resolve(undefined);
          return result;
        },
      },
      dispatchRetryBackoffMs: 0,
      dispatchWaitBackstopMs: 1,
    });
    const { first, firstSender, mailBus, waits } = harness;
    await answerParkedQuery(first, firstSender, 1, [
      { runId: RUN, correlationId: "corr-unanswered", parkKind: "input" },
    ]);
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("turn one")),
    );
    const turnOne = await waitForUpstreamPayload(
      first.supervisorToChild,
      "signal.deliver",
    );
    // Turn two waits for an input channel only once turn one's dispatch has
    // given up.
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("turn two")),
    );
    await waits.nth(1);
    return { ...harness, inbox, turnOne, consumed, consumedOne };
  }

  test("mail whose dispatch gave up on the child's answer is consumed once the child commits it", async () => {
    const { supervisor, firstSender, first, turnOne, consumed, consumedOne } =
      await setupUnansweredInput();
    await confirmDelivery(first, firstSender, 1, true);
    // The run took turn one, and its next park says it is past it.
    await firstSender.send({
      type: "park.notify",
      data: { runId: RUN, correlationId: "corr-next", parkKind: "input" },
    });
    await consumedOne.promise;
    expect(consumed).toEqual([turnOne.data.signalId]);

    const deliveries = await waitForUpstreamPayloads(
      first.supervisorToChild,
      "signal.deliver",
      2,
    );
    expect(deliveries[1]?.data.signalName).toBe(signalName("corr-next"));
    expect(deliveries[1]?.data.signalId).not.toBe(turnOne.data.signalId);

    await supervisor.shutdown();
  });

  test("mail whose dispatch gave up on the child's answer is put back once the child declines it", async () => {
    const { supervisor, firstSender, first, turnOne, consumed } =
      await setupUnansweredInput();
    await confirmDelivery(first, firstSender, 1, false);
    // The run still waits on the input the child declined.
    await answerParkedQuery(first, firstSender, 2, [
      { runId: RUN, correlationId: "corr-unanswered", parkKind: "input" },
    ]);
    const deliveries = await waitForUpstreamPayloads(
      first.supervisorToChild,
      "signal.deliver",
      2,
    );
    expect(deliveries[1]?.data).toMatchObject({
      signalName: signalName("corr-unanswered"),
      signalId: turnOne.data.signalId,
    });
    expect(consumed).toEqual([]);

    await supervisor.shutdown();
  });

  test("an input whose child crashed before committing it goes to the next child", async () => {
    const {
      supervisor,
      firstSender,
      first,
      tracker,
      mailBus,
      ipcKeypair,
      waits,
    } = await setupLiveRun({ respawnBackoffInitialMs: 0 });
    await answerParkedQuery(first, firstSender, 1, [
      { runId: RUN, correlationId: "corr-lost", parkKind: "input" },
    ]);
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("turn one")),
    );
    await waitForUpstreamPayload(first.supervisorToChild, "signal.deliver");
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(1);

    // The replay puts the mail back, and the new child's run still waits on
    // the input the first child never committed.
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await waits.nth(1);
    await answerParkedQuery(second, secondSender, 1, [
      { runId: RUN, correlationId: "corr-lost", parkKind: "input" },
    ]);
    const delivered = await waitForUpstreamPayload(
      second.supervisorToChild,
      "signal.deliver",
    );
    expect(delivered.data).toMatchObject({
      runId: RUN,
      signalName: signalName("corr-lost"),
    });

    await supervisor.shutdown();
  });

  test("mail the child refused because its run ended is rejected", async () => {
    const inbox = createMemoryInboxPrimitives();
    const consumed: unknown[] = [];
    const { supervisor, firstSender, first, mailBus } = await setupLiveRun({
      inboxPrimitives: {
        ...inbox,
        async markConsumed(...args) {
          consumed.push(args[3]);
          return inbox.markConsumed(...args);
        },
      },
    });
    await answerParkedQuery(first, firstSender, 1, [
      { runId: RUN, correlationId: "corr-ended", parkKind: "input" },
    ]);
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("late turn")),
    );
    await waitForUpstreamPayload(first.supervisorToChild, "signal.deliver");
    await firstSender.send({
      type: "terminal.event",
      data: { runId: RUN, kind: "RunCancelled", seq: 9, at: "test" },
    });
    await confirmDelivery(first, firstSender, 1, false);
    await waitUntil(() => inbox.messageIds(ADDRESS).consumed.length === 1);
    expect(consumed[0]).toMatchObject({
      rejection: { code: "workflow_run_terminal" },
    });

    await supervisor.shutdown();
  });

  test("mail declined before the terminal frame of a run that ended is rejected", async () => {
    const inbox = createMemoryInboxPrimitives();
    const consumed: unknown[] = [];
    let logDir = "";
    const { supervisor, firstSender, first, mailBus } = await setup({
      beforeSpawn: async (baseDir) => {
        logDir = baseDir;
        await writeWorkingRunLog(baseDir, [runStarted()]);
      },
      inboxPrimitives: {
        ...inbox,
        async markConsumed(...args) {
          consumed.push(args[3]);
          return inbox.markConsumed(...args);
        },
      },
    });
    await answerParkedQuery(first, firstSender, 1, [
      { runId: RUN, correlationId: "corr-ended", parkKind: "input" },
    ]);
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("late turn")),
    );
    await waitForUpstreamPayload(first.supervisorToChild, "signal.deliver");
    // The run's end is committed, and its terminal frame has not gone out.
    await writeWorkingRunLog(logDir, [
      runStarted(),
      { type: "RunCancelled", reason: "test" },
    ]);
    await confirmDelivery(first, firstSender, 1, false);
    await waitUntil(() => inbox.messageIds(ADDRESS).consumed.length === 1);
    expect(consumed[0]).toMatchObject({
      rejection: { code: "workflow_run_terminal" },
    });

    await supervisor.shutdown();
  });

  test("a stop whose shutdown frame cannot be written still kills the child", async () => {
    const blocked = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first } = await setup({
      timers: {
        setTimer: (cb, ms) =>
          setTimeout(cb, ms === DEFAULT_KILL_TIMEOUT_MS ? 1 : ms),
        clearTimer: defaultClearTimer,
      },
      holdControlWrite: (line) =>
        parseControlFrames([line])[0]?.type === "shutdown"
          ? blocked.promise
          : undefined,
    });
    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await inStandby;
    expect(first.killSignals).toEqual(["SIGTERM"]);

    blocked.resolve(undefined);
    await supervisor.shutdown();
  });

  test("abandoning a standby attempt ends a quiet check whose send is blocked", async () => {
    const blocked = Promise.withResolvers<undefined>();
    const sending = Promise.withResolvers<undefined>();
    const { supervisor } = await setup({
      holdControlWrite: (line) => {
        if (parseControlFrames([line])[0]?.type !== "quiet.request")
          return undefined;
        sending.resolve(undefined);
        return blocked.promise;
      },
    });
    const inStandby = supervisor.enterStandby();
    await sending.promise;
    supervisor.abandonStandby("the Hub that asked for it disconnected");
    await expect(inStandby).rejects.toThrow(
      "the Hub that asked for it disconnected",
    );

    blocked.resolve(undefined);
    await supervisor.shutdown();
  });

  test("a parked query whose send is blocked still gives up at its watchdog", async () => {
    const blocked = Promise.withResolvers<undefined>();
    let hold = false;
    const { supervisor } = await setup({
      parkedQueryWatchdogMs: 10,
      holdControlWrite: (line) =>
        hold &&
        parseControlFrames([line])[0]?.type === "parked-correlations.request"
          ? blocked.promise
          : undefined,
    });
    hold = true;
    await supervisor.reEmitParkedCorrelations();

    blocked.resolve(undefined);
    await supervisor.shutdown();
  });

  test("a standby attempt is refused while a dispatch holds mail for the run's next input", async () => {
    const inbox = createMemoryInboxPrimitives();
    const dequeued = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first, tracker, mailBus } = await setup({
      // The run is live, but the supervisor knows no input channel for it.
      beforeSpawn: (baseDir) => writeWorkingRunLog(baseDir, [runStarted()]),
      inboxPrimitives: {
        ...inbox,
        async dequeueToProcessing(...args) {
          const entry = await inbox.dequeueToProcessing(...args);
          if (entry !== null) dequeued.resolve(undefined);
          return entry;
        },
      },
    });
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("follow-up")),
    );
    await dequeued.promise;

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await expect(inStandby).rejects.toThrow(
      "mail the dispatch took is still waiting for the child",
    );
    expect(first.killSignals).toEqual([]);
    expect(tracker.totalSpawns).toBe(1);
    // The child was not handed the mail yet.
    expect(
      sentFrameTypes(first).filter(
        (frameType) =>
          frameType === "trigger.fire" || frameType === "signal.deliver",
      ),
    ).toEqual([]);

    await supervisor.shutdown();
  });

  // A write that never lands, and the final-check deadlines for the test to fire.
  async function setupStuckWrite() {
    const stuckWrite = Promise.withResolvers<undefined>();
    const watchdogs: (() => void)[] = [];
    const waits: string[] = [];
    const waited = createChangeNotifier();
    const registered = Promise.withResolvers<undefined>();
    let writes = 0;
    const running = await setup({
      onSuspensionRegister: () => registered.resolve(undefined),
      onStandbyWait: (waiting) => {
        waits.push(waiting);
        waited.notify();
      },
      timers: {
        setTimer: (callback: () => void, ms: number): unknown => {
          if (ms === 30_000) {
            watchdogs.push(callback);
            return null;
          }
          const handle = setTimeout(callback, ms);
          return { clear: () => clearTimeout(handle) };
        },
        clearTimer: (handle: unknown): void => {
          if (
            typeof handle === "object" &&
            handle !== null &&
            "clear" in handle &&
            typeof handle.clear === "function"
          )
            handle.clear();
        },
      },
      parkedQueryWatchdogMs: 45_000,
      repoStore: (baseDir) => {
        const base = createStubRepoStore(baseDir, { writeTree: true });
        return new Proxy(base, {
          get(target, prop, receiver) {
            if (prop !== "writeTreePreservingPrefix") {
              return Reflect.get(target, prop, receiver);
            }
            return async (
              ...args: Parameters<RepoStore["writeTreePreservingPrefix"]>
            ) => {
              writes += 1;
              if (writes === 1) await stuckWrite.promise;
              return target.writeTreePreservingPrefix(...args);
            };
          },
        });
      },
    });
    return { ...running, stuckWrite, watchdogs, waits, waited, registered };
  }

  const heldSignal = {
    runId: RUN,
    signalName: "gate",
    signalId: "sig-held",
    payload: { approved: true },
  };

  test("a write stuck before the final check lets a held signal through once the deadline passes", async () => {
    const {
      supervisor,
      firstSender,
      first,
      stuckWrite,
      watchdogs,
      waits,
      waited,
    } = await setupStuckWrite();

    const inStandby = supervisor.enterStandby();
    await waitForUpstreamPayload(first.supervisorToChild, "quiet.request");
    await sendChildWrite(firstSender, "stuck");
    const before = waits.length;
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await waited.until(() => waits.slice(before).includes("standby"));
    const delivered = supervisor.deliverSignal(heldSignal);
    await waited.until(() => waits.includes("input"));
    expect(sentFrameTypes(first)).not.toContain("signal.deliver");

    watchdogs.at(-1)?.();
    await waitForUpstreamPayload(first.supervisorToChild, "signal.deliver");
    expect(sentFrameTypes(first)).not.toContain("shutdown");

    stuckWrite.resolve(undefined);
    await delivered;
    await supervisor.wake();
    await expect(inStandby).rejects.toBeInstanceOf(StandbyRefusedError);
    await supervisor.shutdown();
  });

  test("a write stuck under the final check's answer lets a held signal through once the deadline passes", async () => {
    const {
      supervisor,
      firstSender,
      first,
      stuckWrite,
      watchdogs,
      waits,
      waited,
      registered,
    } = await setupStuckWrite();

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await waitForUpstreamPayloads(first.supervisorToChild, "quiet.request", 2);
    await sendChildWrite(firstSender, "stuck");
    await answerQuiet(first, firstSender, 2, { quiet: true });
    // The supervisor takes frames in order, so once it registers the park sent
    // next, it has the answer and waits on the write.
    await firstSender.send({
      type: "park.notify",
      data: { runId: RUN, correlationId: "corr-marker", parkKind: "approval" },
    });
    await registered.promise;
    const delivered = supervisor.deliverSignal(heldSignal);
    await waited.until(() => waits.includes("input"));
    expect(sentFrameTypes(first)).not.toContain("signal.deliver");

    watchdogs.at(-1)?.();
    await waitForUpstreamPayload(first.supervisorToChild, "signal.deliver");
    expect(sentFrameTypes(first)).not.toContain("shutdown");

    stuckWrite.resolve(undefined);
    await delivered;
    await supervisor.wake();
    await expect(inStandby).rejects.toBeInstanceOf(StandbyRefusedError);
    await supervisor.shutdown();
  });

  test("a final check the child does not answer in time counts it as working", async () => {
    // Quiet-check watchdogs are the test's to fire; every other timer runs.
    const watchdogs: (() => void)[] = [];
    const timers = {
      setTimer: (callback: () => void, ms: number): unknown => {
        if (ms === 30_000) {
          watchdogs.push(callback);
          return null;
        }
        const handle = setTimeout(callback, ms);
        return { clear: () => clearTimeout(handle) };
      },
      clearTimer: (handle: unknown): void => {
        if (
          typeof handle === "object" &&
          handle !== null &&
          "clear" in handle &&
          typeof handle.clear === "function"
        )
          handle.clear();
      },
    };
    const { supervisor, firstSender, first } = await setup({
      timers,
      parkedQueryWatchdogMs: 45_000,
    });

    const inStandby = supervisor.enterStandby();
    expect(
      (await answerQuiet(first, firstSender, 1, { quiet: true })).wait,
    ).toBe(true);
    const queries = await waitForUpstreamPayloads(
      first.supervisorToChild,
      "quiet.request",
      2,
    );
    expect(queries[1]?.data.wait).toBe(false);
    const watchdog = watchdogs.at(-1);
    if (watchdog === undefined) throw new Error("no quiet-check watchdog");
    watchdog();
    // Back to waiting until the child says it is quiet.
    const again = await waitForUpstreamPayloads(
      first.supervisorToChild,
      "quiet.request",
      3,
    );
    expect(again[2]?.data.wait).toBe(true);
    expect(sentFrameTypes(first)).not.toContain("shutdown");

    const woken = supervisor.wake();
    await expect(inStandby).rejects.toBeInstanceOf(StandbyAbandonedError);
    await woken;
    await supervisor.shutdown();
  });

  test("reports the earliest queued timer, and wakes for it", async () => {
    const hour = 60 * 60 * 1000;
    const now = Date.parse("2026-10-02T00:00:00.000Z");
    const fireAt = new Date(now + hour).toISOString();
    // The wake timer is the test's to fire; every other timer runs as usual.
    let wakeTimer: (() => void) | undefined;
    const timers = {
      now: () => now,
      setTimer: (callback: () => void, ms: number): unknown => {
        if (ms === hour) {
          wakeTimer = callback;
          return null;
        }
        const handle = setTimeout(callback, ms);
        return { clear: () => clearTimeout(handle) };
      },
      clearTimer: (handle: unknown): void => {
        if (
          typeof handle === "object" &&
          handle !== null &&
          "clear" in handle &&
          typeof handle.clear === "function"
        )
          handle.clear();
      },
    };
    const woke = Promise.withResolvers<string>();
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      { onWake: ({ reason }) => woke.resolve(reason), timers },
    );

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender, { wakeAt: fireAt });
    expect(await inStandby).toEqual({ wakeAt: fireAt });
    expect(tracker.totalSpawns).toBe(1);

    if (wakeTimer === undefined) throw new Error("no wake timer was armed");
    wakeTimer();
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    expect(await woke.promise).toBe("timer");

    await supervisor.shutdown();
  });

  test("a child that runs again clears the timer that would have woken it", async () => {
    const hour = 3_600_000;
    const now = Date.parse("2026-10-02T00:00:00.000Z");
    const wakeHandle = { wake: true };
    const cleared: unknown[] = [];
    const timers = {
      now: () => now,
      setTimer: (callback: () => void, ms: number): unknown => {
        if (ms === hour) return wakeHandle;
        const handle = setTimeout(callback, ms);
        return { clear: () => clearTimeout(handle) };
      },
      clearTimer: (handle: unknown): void => {
        cleared.push(handle);
        if (
          typeof handle === "object" &&
          handle !== null &&
          "clear" in handle &&
          typeof handle.clear === "function"
        )
          handle.clear();
      },
    };
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      { timers },
    );

    const inStandby = supervisor.enterStandby();
    const wakeAt = new Date(now + hour).toISOString();
    await actAsChildEnteringStandby(first, firstSender, { wakeAt });
    await inStandby;
    expect(cleared).not.toContain(wakeHandle);

    const woken = supervisor.wake();
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await woken;
    expect(cleared).toContain(wakeHandle);

    await supervisor.shutdown();
  });

  test("a wake further off than a timer can wait is armed again when that timer fires, and wakes the child once due", async () => {
    // `setTimeout` fires at once past this many ms.
    const maxTimerMs = 2_147_483_647;
    let now = Date.now();
    const fireAt = new Date(now + 40 * 24 * 60 * 60 * 1000).toISOString();
    const wakeTimers: { callback: () => void; ms: number }[] = [];
    const timers = {
      now: () => now,
      setTimer: (callback: () => void, ms: number): unknown => {
        if (ms > 24 * 60 * 60 * 1000) {
          wakeTimers.push({ callback, ms });
          return null;
        }
        const handle = setTimeout(callback, ms);
        return { clear: () => clearTimeout(handle) };
      },
      clearTimer: (handle: unknown): void => {
        if (
          typeof handle === "object" &&
          handle !== null &&
          "clear" in handle &&
          typeof handle.clear === "function"
        )
          handle.clear();
      },
    };
    const woke = Promise.withResolvers<string>();
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      { timers, onWake: ({ reason }) => woke.resolve(reason) },
    );

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender, { wakeAt: fireAt });
    expect(await inStandby).toEqual({ wakeAt: fireAt });
    expect(wakeTimers.map((timer) => timer.ms)).toEqual([maxTimerMs]);

    now += maxTimerMs;
    wakeTimers[0]?.callback();
    const remainingMs = Date.parse(fireAt) - now;
    expect(wakeTimers.map((timer) => timer.ms)).toEqual([
      maxTimerMs,
      remainingMs,
    ]);
    expect(tracker.totalSpawns).toBe(1);

    now += remainingMs;
    wakeTimers[1]?.callback();
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    expect(await woke.promise).toBe("timer");

    await supervisor.shutdown();
  });

  test("a signal respawns a child in standby and is delivered to the new one", async () => {
    const { supervisor, firstSender, first, tracker, ipcKeypair } =
      await setup();
    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    await inStandby;

    const delivered = supervisor.deliverSignal({
      runId: RUN,
      signalName: "gate",
      signalId: "sig-1",
      payload: { approved: true },
    });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await delivered;
    const signal = await waitForUpstreamPayload(
      second.supervisorToChild,
      "signal.deliver",
    );
    expect(signal.data.signalId).toBe("sig-1");
    expect(parseSignalDeliverRunIds(first.supervisorToChild.flushed())).toEqual(
      [],
    );

    await supervisor.shutdown();
  });

  test("a signal that arrives while another signal's wake respawns the child reaches the new one", async () => {
    const { supervisor, firstSender, first, tracker, ipcKeypair } =
      await setup();
    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    await inStandby;

    const signal = (signalId: string) =>
      supervisor.deliverSignal({
        runId: RUN,
        signalName: "gate",
        signalId,
        payload: { approved: true },
      });
    const waking = signal("sig-1");
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const during = signal("sig-2");
    await driveReady(second, ipcKeypair);
    await Promise.all([waking, during]);
    const signals = await waitForUpstreamPayloads(
      second.supervisorToChild,
      "signal.deliver",
      2,
    );
    expect(signals.map((frame) => frame.data.signalId).sort()).toEqual([
      "sig-1",
      "sig-2",
    ]);

    await supervisor.shutdown();
  });

  test("a cancellation asked for while a wake respawns the child goes to the new one", async () => {
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      { repoStore: (baseDir) => gatedRepoStore(baseDir) },
    );
    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    await inStandby;

    const woken = supervisor.wake();
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const cancelled = supervisor.requestCancel({
      runId: RUN,
      origin: "hub-admin",
      reason: "Stop",
      at: "2026-10-01T00:00:00.000Z",
    });
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await woken;
    const prepare = await waitForUpstreamPayload(
      second.supervisorToChild,
      "cancel.prepare",
    );
    await secondSender.send({
      type: "cancel.prepared",
      data: { requestId: prepare.data.requestId },
    });
    await cancelled;
    expect(sentFrameTypes(first)).not.toContain("cancel.prepare");

    await supervisor.shutdown();
  });

  test("a signal during the final check waits for it and reaches the respawned child", async () => {
    const { supervisor, firstSender, first, tracker, ipcKeypair } =
      await setup();
    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await waitForUpstreamPayloads(first.supervisorToChild, "quiet.request", 2);

    const delivered = supervisor.deliverSignal({
      runId: RUN,
      signalName: "gate",
      signalId: "sig-1",
      payload: { approved: true },
    });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await waitForUpstreamPayload(first.supervisorToChild, "shutdown");
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(0);
    expect(await inStandby).toEqual({});

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    await driveReady(second, ipcKeypair);
    await delivered;
    expect(parseSignalDeliverRunIds(first.supervisorToChild.flushed())).toEqual(
      [],
    );
    const signal = await waitForUpstreamPayload(
      second.supervisorToChild,
      "signal.deliver",
    );
    expect(signal.data.signalId).toBe("sig-1");

    await supervisor.shutdown();
  });

  test("a hub reconnect while the child stops re-registers the correlations it reported", async () => {
    const registered: string[] = [];
    const { supervisor, firstSender, first } = await setup({
      onSuspensionRegister: (registration) => {
        registered.push(registration.correlationId);
      },
    });

    const entering = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    const queries = await waitForUpstreamPayloads(
      first.supervisorToChild,
      "parked-correlations.request",
      2,
    );
    const query = queries[1];
    if (query === undefined) throw new Error("correlation query missing");
    await firstSender.send({
      type: "parked-correlations.response",
      data: {
        requestId: query.data.requestId,
        parked: [
          {
            runId: RUN,
            correlationId: "corr-gate",
            parkKind: "approval",
            snapshot: {
              name: "charge_card",
              description: "Charge the customer's card",
              inputSchema: { type: "object" },
              arguments: { amount: 100 },
            },
          },
        ],
      },
    });
    // The child has been told to stop and has not exited yet.
    await waitForUpstreamPayload(first.supervisorToChild, "shutdown");
    await supervisor.reEmitParkedCorrelations();
    expect(registered).toEqual(["corr-gate"]);

    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(0);
    expect(await entering).toEqual({});
    await supervisor.shutdown();
  });

  test("a standby is recorded once the child stopped, and only once", async () => {
    let childExited = false;
    const records: boolean[] = [];
    const { supervisor, firstSender, first } = await setup({
      recordStandby: async () => {
        records.push(childExited);
      },
    });
    void first.exited.then(() => {
      childExited = true;
    });

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});
    expect(await supervisor.enterStandby()).toEqual({});
    expect(records).toEqual([true]);

    await supervisor.shutdown();
  });

  // A run sleeping on a timer sends no park, so a drain arms a timer for it.
  // The supervisor takes frames in order, so once it registers the park sent
  // next, the run is one it drains.
  async function drainSleepingRun(
    supervisor: WorkflowSupervisor,
    firstSender: ReturnType<typeof createControlChannelSender>,
    registered: Promise<undefined>,
  ): Promise<void> {
    await firstSender.send({ type: "resumed.runs", data: { runIds: [RUN] } });
    await firstSender.send({
      type: "park.notify",
      data: {
        runId: "run-marker",
        correlationId: "corr-marker",
        parkKind: "approval",
      },
    });
    await registered;
    await supervisor.drain({ deadlineMs: 60_000 });
  }

  // A drain timer whose deadline the test passes: it commits its cancellation
  // through the commit the supervisor gave it, as the real one does.
  function passableDrainDeadline() {
    let commitCancel:
      | ((cancel: {
          origin: "supervisor-drain";
          reason: string;
          at: string;
        }) => Promise<unknown>)
      | undefined;
    const factory: WorkflowSupervisorBindings["drainTimeoutAccumulatorFactory"] =
      (opts) => {
        commitCancel = opts.commitCancel;
        return {
          start: () => undefined,
          pause: () => undefined,
          resume: () => undefined,
          stop: () => undefined,
          accumulatedMs: () => 0,
          escalated: false,
          disposed: () => Promise.resolve(),
        };
      };
    return {
      factory,
      pass(): Promise<unknown> {
        if (commitCancel === undefined) throw new Error("no drain was armed");
        return commitCancel({
          origin: "supervisor-drain",
          reason: "drainTimeout expired",
          at: "2026-10-01T00:00:00.000Z",
        });
      },
    };
  }

  test("a standby stop stops the drain timers its child's runs armed", async () => {
    const registered = Promise.withResolvers<undefined>();
    const stopped: string[] = [];
    const { supervisor, firstSender, first } = await setup({
      onSuspensionRegister: () => registered.resolve(undefined),
      drainTimeoutAccumulatorFactory: ({ runId }) => ({
        start: () => undefined,
        pause: () => undefined,
        resume: () => undefined,
        stop: () => {
          stopped.push(runId);
        },
        accumulatedMs: () => 0,
        escalated: false,
        disposed: () => Promise.resolve(),
      }),
    });
    await drainSleepingRun(supervisor, firstSender, registered.promise);

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});
    expect(stopped).toEqual([RUN]);

    await supervisor.shutdown();
  });

  test("a drain sent while a standby stop takes the child arms no drain timer", async () => {
    const registered = Promise.withResolvers<undefined>();
    const started: string[] = [];
    const stoppedOne = Promise.withResolvers<undefined>();
    const drainHeld = Promise.withResolvers<undefined>();
    const releaseDrain = Promise.withResolvers<undefined>();
    const inbox = createMemoryInboxPrimitives();
    let drains = 0;
    let entering = false;
    let lateDrain: Promise<void> | undefined;
    const { supervisor, firstSender, first } = await setup({
      onSuspensionRegister: () => registered.resolve(undefined),
      holdControlWrite: (line) => {
        if (parseControlFrames([line])[0]?.type !== "drain") return undefined;
        drains += 1;
        if (drains !== 2) return undefined;
        drainHeld.resolve(undefined);
        return releaseDrain.promise;
      },
      // The standby attempt reads the inbox last, while the child still runs.
      inboxPrimitives: {
        ...inbox,
        async hasInboxEntries(...args) {
          if (entering) lateDrain ??= supervisor.drain({ deadlineMs: 60_000 });
          return inbox.hasInboxEntries(...args);
        },
      },
      drainTimeoutAccumulatorFactory: ({ runId }) => ({
        start: () => {
          started.push(runId);
        },
        pause: () => undefined,
        resume: () => undefined,
        stop: () => {
          stoppedOne.resolve(undefined);
        },
        accumulatedMs: () => 0,
        escalated: false,
        disposed: () => Promise.resolve(),
      }),
    });
    await drainSleepingRun(supervisor, firstSender, registered.promise);
    expect(started).toEqual([RUN]);

    entering = true;
    const inStandby = supervisor.enterStandby();
    const stopping = actAsChildEnteringStandby(first, firstSender);
    await drainHeld.promise;
    await stoppedOne.promise;
    releaseDrain.resolve(undefined);
    await lateDrain;
    await stopping;
    expect(await inStandby).toEqual({});
    expect(started).toEqual([RUN]);

    await supervisor.shutdown();
  });

  test("a standby attempt waits for a drain deadline's cancellation being committed", async () => {
    const registered = Promise.withResolvers<undefined>();
    const deadline = passableDrainDeadline();
    const committing = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const waiting = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first } = await setup({
      onSuspensionRegister: () => registered.resolve(undefined),
      drainTimeoutAccumulatorFactory: deadline.factory,
      onStandbyWait: (on) => {
        if (on === "standby") waiting.resolve(undefined);
      },
      repoStore: (baseDir) =>
        gatedRepoStore(baseDir, async () => {
          committing.resolve(undefined);
          await release.promise;
        }),
    });
    await drainSleepingRun(supervisor, firstSender, registered.promise);

    const cancelling = deadline.pass();
    await committing.promise;
    const entering = supervisor.enterStandby();
    void entering.catch(() => undefined);
    const asked = waitForUpstreamPayload(
      first.supervisorToChild,
      "quiet.request",
    );
    expect(
      await Promise.race([
        waiting.promise.then(() => "waited"),
        asked.then(() => "asked"),
      ]),
    ).toBe("waited");

    release.resolve(undefined);
    await cancelling;
    await asked;

    await supervisor.shutdown();
  });

  test("a drain deadline that passes during the final check calls the standby attempt off, and the child keeps running", async () => {
    const registered = Promise.withResolvers<undefined>();
    const deadline = passableDrainDeadline();
    let writes = 0;
    const { supervisor, firstSender, first, tracker } = await setup({
      onSuspensionRegister: () => registered.resolve(undefined),
      drainTimeoutAccumulatorFactory: deadline.factory,
      repoStore: (baseDir) =>
        gatedRepoStore(baseDir, async () => {
          writes += 1;
        }),
    });
    await drainSleepingRun(supervisor, firstSender, registered.promise);

    const inStandby = supervisor.enterStandby();
    expect(
      (await answerQuiet(first, firstSender, 1, { quiet: true })).wait,
    ).toBe(true);
    await waitForUpstreamPayloads(first.supervisorToChild, "quiet.request", 2);
    const committed = deadline.pass();
    await expect(inStandby).rejects.toThrow("the drain deadline passed");
    await committed;
    expect(writes).toBe(1);
    expect(sentFrameTypes(first)).not.toContain("shutdown");
    expect(tracker.totalSpawns).toBe(1);

    await supervisor.shutdown();
  });

  test("a drain deadline that passes once the standby stop is under way wakes the child for its cancellation", async () => {
    const registered = Promise.withResolvers<undefined>();
    const deadline = passableDrainDeadline();
    const reasons: string[] = [];
    const woke = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      {
        onWake: ({ reason }) => {
          reasons.push(reason);
          woke.resolve(undefined);
        },
        onSuspensionRegister: () => registered.resolve(undefined),
        drainTimeoutAccumulatorFactory: deadline.factory,
        repoStore: (baseDir) => gatedRepoStore(baseDir),
      },
    );
    await drainSleepingRun(supervisor, firstSender, registered.promise);

    const inStandby = supervisor.enterStandby();
    expect(
      (await answerQuiet(first, firstSender, 1, { quiet: true })).wait,
    ).toBe(true);
    expect(
      (await answerQuiet(first, firstSender, 2, { quiet: true })).wait,
    ).toBe(false);
    await answerParkedQuery(first, firstSender, 2);
    await waitForUpstreamPayload(first.supervisorToChild, "shutdown");
    // Past the point a stop can be called off.
    const committed = deadline.pass();
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(0);
    await expect(inStandby).rejects.toThrow("the drain deadline passed");
    await committed;

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await woke.promise;
    expect(reasons).toEqual(["cancel"]);

    await supervisor.shutdown();
  });

  test("a shutdown waits for a drain deadline's cancellation the standby stop left in flight", async () => {
    const registered = Promise.withResolvers<undefined>();
    const deadline = passableDrainDeadline();
    const shutdownWaits = Promise.withResolvers<undefined>();
    let landed = false;
    const { supervisor, firstSender, first, tracker } = await setup({
      onSuspensionRegister: () => registered.resolve(undefined),
      onShutdownWait: (waiting) => {
        if (waiting === "cancellation") shutdownWaits.resolve(undefined);
      },
      drainTimeoutAccumulatorFactory: deadline.factory,
      repoStore: (baseDir) =>
        gatedRepoStore(baseDir, async () => {
          await shutdownWaits.promise;
          landed = true;
        }),
    });
    await drainSleepingRun(supervisor, firstSender, registered.promise);

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await waitForUpstreamPayload(first.supervisorToChild, "shutdown");
    const committed = deadline.pass();

    // The shutdown kills the child the stop waits on.
    await supervisor.shutdown();
    expect(landed).toBe(true);
    await expect(inStandby).rejects.toThrow();
    await committed;
    expect(tracker.totalSpawns).toBe(1);
  });

  test("mail that lands while the standby is recorded wakes the child once the record lands", async () => {
    const memory = createMemoryInboxPrimitives();
    const recording = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const wakeWaits = Promise.withResolvers<undefined>();
    const woke = Promise.withResolvers<string>();
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup({
        onWake: ({ reason }) => {
          woke.resolve(reason);
        },
        recordStandby: async () => {
          recording.resolve(undefined);
          await release.promise;
        },
        onStandbyWait: (waiting) => {
          if (waiting === "attempt") wakeWaits.resolve(undefined);
        },
        inboxPrimitives: memory,
      });

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    await recording.promise;
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("wake up")),
    );
    await wakeWaits.promise;
    expect(tracker.totalSpawns).toBe(1);
    release.resolve(undefined);
    expect(await inStandby).toEqual({});

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    expect(await woke.promise).toBe("mail");

    await supervisor.shutdown();
  });

  test("records the parks, the timer and the history tip it read before the child reported its parks", async () => {
    const records: RecordedStandby[] = [];
    const parkQueriesAtTipRead: number[] = [];
    // The correlation queries sent to the child so far, once it exists.
    let parkQueries = (): number => -1;
    const { supervisor, firstSender, first } = await setup({
      recordStandby: async (standby) => {
        records.push(standby);
      },
      repoStore: (baseDir) => {
        const base = createStubRepoStore(baseDir, { writeTree: true });
        return new Proxy(base, {
          get(target, prop, receiver) {
            if (prop !== "resolveRef")
              return Reflect.get(target, prop, receiver);
            return async () => {
              parkQueriesAtTipRead.push(parkQueries());
              return HISTORY_TIP;
            };
          },
        });
      },
    });
    parkQueries = () =>
      sentFrameTypes(first).filter(
        (type) => type === "parked-correlations.request",
      ).length;
    const wakeAt = new Date(Date.now() + 3_600_000).toISOString();

    const entering = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true, wakeAt });
    await answerQuiet(first, firstSender, 2, { quiet: true, wakeAt });
    const queries = await waitForUpstreamPayloads(
      first.supervisorToChild,
      "parked-correlations.request",
      2,
    );
    const query = queries[1];
    if (query === undefined) throw new Error("correlation query missing");
    await firstSender.send({
      type: "parked-correlations.response",
      data: { requestId: query.data.requestId, parked: [GATE_PARK] },
    });
    await waitForUpstreamPayload(first.supervisorToChild, "shutdown");
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(0);
    expect(await entering).toEqual({ wakeAt });

    // Only the spawn's own correlation query had gone out.
    expect(parkQueriesAtTipRead).toEqual([1]);
    expect(records).toEqual([
      {
        parked: [GATE_PARK],
        wakeAt,
        tip: HISTORY_TIP,
        definition: TEST_DEFINITION,
      },
    ]);
    await supervisor.shutdown();
  });

  test("a shutdown while the standby is recorded says the shutdown called it off", async () => {
    const recording = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first } = await setup({
      recordStandby: async () => {
        recording.resolve(undefined);
        await release.promise;
      },
    });

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    await recording.promise;
    const shutdown = supervisor.shutdown();
    release.resolve(undefined);
    const cause = await inStandby.then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(cause).toBeInstanceOf(StandbyAbandonedError);
    expect(cause).toHaveProperty(
      "message",
      "the workflow supervisor shut down while entering standby",
    );
    await shutdown;
  });

  test("a shutdown resolves once the standby it called off is recorded", async () => {
    const recording = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const shutdownWaits = Promise.withResolvers<undefined>();
    let recorded = false;
    const { supervisor, firstSender, first } = await setup({
      recordStandby: async () => {
        recording.resolve(undefined);
        await release.promise;
        recorded = true;
      },
      onShutdownWait: (waiting) => {
        if (waiting === "attempt") shutdownWaits.resolve(undefined);
      },
    });

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    await recording.promise;
    const shutdown = supervisor.shutdown();
    const recordedWhenShutDown = shutdown.then(() => recorded);
    await shutdownWaits.promise;
    release.resolve(undefined);
    expect(await recordedWhenShutDown).toBe(true);
    await expect(inStandby).rejects.toBeInstanceOf(StandbyAbandonedError);
  });

  test("a standby attempt its host cannot record runs the child again", async () => {
    const woke = Promise.withResolvers<string>();
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      {
        onWake: ({ reason }) => woke.resolve(reason),
        recordStandby: () => Promise.reject(new Error("disk full")),
      },
    );

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    await expect(inStandby).rejects.toThrow(
      "the standby could not be recorded: disk full",
    );
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    expect(await woke.promise).toBe("a standby that was not recorded");

    await supervisor.shutdown();
  });

  test("a child that does not report its parked correlations is not in standby", async () => {
    const { supervisor, firstSender, first } = await setup({
      parkedQueryWatchdogMs: 1,
    });
    const inStandby = supervisor.enterStandby();
    expect(
      (await answerQuiet(first, firstSender, 1, { quiet: true })).wait,
    ).toBe(true);
    expect(
      (await answerQuiet(first, firstSender, 2, { quiet: true })).wait,
    ).toBe(false);
    await expect(inStandby).rejects.toThrow(
      "the workflow child did not report the correlations it is parked on",
    );
    expect(sentFrameTypes(first)).not.toContain("shutdown");

    await supervisor.shutdown();
  });

  test("a standby attempt abandoned while its child stops runs the child again", async () => {
    const woke = Promise.withResolvers<string>();
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      { onWake: ({ reason }) => woke.resolve(reason) },
    );

    const inStandby = supervisor.enterStandby();
    expect(
      (await answerQuiet(first, firstSender, 1, { quiet: true })).wait,
    ).toBe(true);
    expect(
      (await answerQuiet(first, firstSender, 2, { quiet: true })).wait,
    ).toBe(false);
    await answerParkedQuery(first, firstSender, 2);
    await waitForUpstreamPayload(first.supervisorToChild, "shutdown");
    // Past the point a stop can be called off.
    supervisor.abandonStandby("the Hub that asked for it disconnected");
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(0);
    await expect(inStandby).rejects.toThrow(
      "the Hub that asked for it disconnected",
    );

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    expect(await woke.promise).toBe("an abandoned standby attempt");

    await supervisor.shutdown();
  });

  // A store whose delta writes, such as a cancellation's commit, wait for
  // `beforeWrite` first.
  function gatedRepoStore(
    baseDir: string,
    beforeWrite: () => Promise<void> = async () => undefined,
  ): RepoStore {
    const base = createStubRepoStore(baseDir, { writeTree: true });
    return new Proxy(base, {
      get(target, prop, receiver) {
        if (prop !== "writeTreeDelta") {
          return Reflect.get(target, prop, receiver);
        }
        return async (...args: Parameters<RepoStore["writeTreeDelta"]>) => {
          await beforeWrite();
          return target.writeTreeDelta(...args);
        };
      },
    });
  }

  test("a cancellation that abandons a standby attempt while its child stops runs the child itself", async () => {
    const reasons: string[] = [];
    const woke = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      {
        onWake: ({ reason }) => {
          reasons.push(reason);
          woke.resolve(undefined);
        },
        repoStore: (baseDir) => gatedRepoStore(baseDir),
      },
    );

    const inStandby = supervisor.enterStandby();
    expect(
      (await answerQuiet(first, firstSender, 1, { quiet: true })).wait,
    ).toBe(true);
    expect(
      (await answerQuiet(first, firstSender, 2, { quiet: true })).wait,
    ).toBe(false);
    await answerParkedQuery(first, firstSender, 2);
    await waitForUpstreamPayload(first.supervisorToChild, "shutdown");
    // Past the point a stop can be called off.
    const cancelled = supervisor.requestCancel({
      runId: RUN,
      origin: "hub-admin",
      reason: "Stop",
      at: "2026-10-01T00:00:00.000Z",
    });
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(0);
    await expect(inStandby).rejects.toThrow("a cancellation was requested");
    await cancelled;

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await woke.promise;
    expect(reasons).toEqual(["cancel"]);

    await supervisor.shutdown();
  });

  test("a standby attempt waiting on a cancellation commit goes on once it lands", async () => {
    const committing = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const waiting = Promise.withResolvers<undefined>();
    let hold = true;
    const { supervisor, firstSender, first } = await setup({
      onStandbyWait: (on) => {
        if (on === "standby") waiting.resolve(undefined);
      },
      repoStore: (baseDir) =>
        gatedRepoStore(baseDir, async () => {
          if (!hold) return;
          hold = false;
          committing.resolve(undefined);
          await release.promise;
        }),
    });

    const cancelled = supervisor.requestCancel({
      runId: RUN,
      origin: "hub-admin",
      reason: "Stop",
      at: "2026-10-01T00:00:00.000Z",
    });
    // The running child prepares the cancellation before it is committed.
    const prepare = await waitForUpstreamPayload(
      first.supervisorToChild,
      "cancel.prepare",
    );
    await firstSender.send({
      type: "cancel.prepared",
      data: { requestId: prepare.data.requestId },
    });
    await committing.promise;
    const entering = supervisor.enterStandby();
    void entering.catch(() => undefined);
    await waiting.promise;
    expect(sentFrameTypes(first)).not.toContain("quiet.request");

    release.resolve(undefined);
    await cancelled;
    await waitForUpstreamPayload(first.supervisorToChild, "quiet.request");

    await supervisor.shutdown();
  });

  test("a standby attempt waiting on a cancellation the child has not prepared goes on once it lands", async () => {
    const waiting = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first } = await setup({
      onStandbyWait: (on) => {
        if (on === "standby") waiting.resolve(undefined);
      },
      repoStore: (baseDir) => gatedRepoStore(baseDir),
    });

    const cancelled = supervisor.requestCancel({
      runId: RUN,
      origin: "hub-admin",
      reason: "Stop",
      at: "2026-10-01T00:00:00.000Z",
    });
    const prepare = await waitForUpstreamPayload(
      first.supervisorToChild,
      "cancel.prepare",
    );
    const entering = supervisor.enterStandby();
    void entering.catch(() => undefined);
    await waiting.promise;
    expect(sentFrameTypes(first)).not.toContain("quiet.request");

    await firstSender.send({
      type: "cancel.prepared",
      data: { requestId: prepare.data.requestId },
    });
    await cancelled;
    await waitForUpstreamPayload(first.supervisorToChild, "quiet.request");

    await supervisor.shutdown();
  });

  test("a wake waits for a drain deadline's cancellation that waits on the standby attempt", async () => {
    const baseDir = await makeTempDir("standby-drain-wake-");
    const tracker = createSpawnTracker({});
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const registered = Promise.withResolvers<undefined>();
    const wakeWaitsOnCancel = Promise.withResolvers<undefined>();
    const signing = Promise.withResolvers<undefined>();
    const allowSign = Promise.withResolvers<undefined>();
    const deadline = passableDrainDeadline();
    const { supervisor, firstSender } = await spawnSupervisor({
      baseDir,
      tracker,
      mailBus,
      ipcKeypair,
      overrides: {
        repoStore: gatedRepoStore(baseDir),
        onSuspensionRegister: () => registered.resolve(undefined),
        onStandbyWait: (waiting) => {
          if (waiting === "cancellation") wakeWaitsOnCancel.resolve(undefined);
        },
        // Holds the drain deadline's cancellation before it commits.
        signAsPrincipal: async () => {
          signing.resolve(undefined);
          await allowSign.promise;
          return { sig: new Uint8Array(64), principalKind: "supervisor" };
        },
        drainTimeoutAccumulatorFactory: deadline.factory,
      },
    });
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    await drainSleepingRun(supervisor, firstSender, registered.promise);

    const entering = supervisor.enterStandby().then(
      () => null,
      (cause: unknown) => cause,
    );
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await answerParkedQuery(first, firstSender, 2);
    await waitForUpstreamPayload(first.supervisorToChild, "shutdown");
    // The wake waits for the stop first, so the drain deadline's cancellation
    // waits behind it.
    const woken = supervisor.wake();
    const cancelling = deadline.pass();
    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(0);
    expect(await entering).toBeInstanceOf(StandbyAbandonedError);
    await signing.promise;
    expect(
      await Promise.race([
        wakeWaitsOnCancel.promise.then(() => "waited"),
        tracker.awaitChildren(2).then(() => "spawned"),
      ]),
    ).toBe("waited");

    allowSign.resolve(undefined);
    await cancelling;
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await woken;

    await supervisor.shutdown();
  });

  test("a child in standby runs again only once a cancellation being committed lands", async () => {
    const committing = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const waiting = Promise.withResolvers<undefined>();
    let hold = false;
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      {
        onStandbyWait: (on) => {
          if (on === "cancellation") waiting.resolve(undefined);
        },
        repoStore: (baseDir) =>
          gatedRepoStore(baseDir, async () => {
            if (!hold) return;
            hold = false;
            committing.resolve(undefined);
            await release.promise;
          }),
      },
    );

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    await inStandby;
    hold = true;
    const cancelled = supervisor.requestCancel({
      runId: RUN,
      origin: "hub-admin",
      reason: "Stop",
      at: "2026-10-01T00:00:00.000Z",
    });
    await committing.promise;
    const woken = supervisor.wake();
    await waiting.promise;
    expect(tracker.totalSpawns).toBe(1);

    release.resolve(undefined);
    await cancelled;
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await woken;
    expect(tracker.totalSpawns).toBe(2);

    await supervisor.shutdown();
  });

  test("a standby attempt asked for while an abandoned stop finishes is put off, not answered as done", async () => {
    const inboxRead = Promise.withResolvers<undefined>();
    const releaseRead = Promise.withResolvers<undefined>();
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      {
        inboxPrimitives: {
          ...createMemoryInboxPrimitives(),
          hasInboxEntries: emptyUntilStopped(async () => {
            inboxRead.resolve(undefined);
            await releaseRead.promise;
            return false;
          }),
        },
      },
    );

    const abandoned = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    // The child is stopped and recorded; the stop still reads the inbox.
    await inboxRead.promise;
    supervisor.abandonStandby("the Hub that asked for it disconnected");
    await expect(supervisor.enterStandby()).rejects.toBeInstanceOf(
      StandbyDeferredError,
    );
    releaseRead.resolve(undefined);
    await expect(abandoned).rejects.toThrow(
      "the Hub that asked for it disconnected",
    );

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);

    await supervisor.shutdown();
  });

  test("a standby attempt asked for while an abandoned one settles is put off, not refused", async () => {
    const { supervisor, firstSender, first, tracker } = await setup();

    const abandoned = supervisor.enterStandby();
    expect(
      (await answerQuiet(first, firstSender, 1, { quiet: true })).wait,
    ).toBe(true);
    expect(
      (await answerQuiet(first, firstSender, 2, { quiet: true })).wait,
    ).toBe(false);
    await waitForUpstreamPayloads(
      first.supervisorToChild,
      "parked-correlations.request",
      2,
    );
    supervisor.abandonStandby("the Hub that asked for it disconnected");
    await expect(supervisor.enterStandby()).rejects.toBeInstanceOf(
      StandbyDeferredError,
    );

    await answerParkedQuery(first, firstSender, 2);
    await expect(abandoned).rejects.toThrow(
      "the Hub that asked for it disconnected",
    );
    expect(first.killSignals).toEqual([]);
    expect(tracker.totalSpawns).toBe(1);

    await supervisor.shutdown();
  });

  test("a standby attempt asked for while the child wakes is put off, not refused", async () => {
    const woke = Promise.withResolvers<string>();
    const { supervisor, firstSender, first, tracker, ipcKeypair } = await setup(
      { onWake: ({ reason }) => woke.resolve(reason) },
    );

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});

    const woken = supervisor.wake();
    await expect(supervisor.enterStandby()).rejects.toBeInstanceOf(
      StandbyDeferredError,
    );

    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await woken;
    expect(await woke.promise).toBe("wake");

    await supervisor.shutdown();
  });

  // A child that crashed once, so the backoff is raised, with the replacement
  // running. Only that crash's backoff runs out, and the stable-run reset is the
  // test's to fire.
  async function setupRaisedBackoff() {
    const backoffMs = 1_234;
    const stableResetMs = 65_432;
    const backoffs: number[] = [];
    const resets: (() => void)[] = [];
    const armed = createChangeNotifier();
    const running = await setup({
      respawnBackoffInitialMs: backoffMs,
      recyclePolicy: { crashLoopStableResetMs: stableResetMs },
      timers: {
        setTimer: (callback, ms) => {
          if (ms === stableResetMs) {
            resets.push(callback);
            armed.notify();
            return callback;
          }
          if (ms === backoffMs || ms === backoffMs * 2) {
            backoffs.push(ms);
            armed.notify();
            return backoffs.length === 1 ? setTimeout(callback, 0) : callback;
          }
          return setTimeout(callback, ms);
        },
      },
    });
    const { first, tracker, ipcKeypair } = running;
    const crash = (child: FakeChild) => {
      child.eventChildToSupervisor.close();
      child.childToSupervisor.close();
      child.resolveExit?.(1);
    };
    crash(first);
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await armed.until(() => resets.length === 1);
    const wakeThirdChild = async () => {
      const woken = running.supervisor.wake();
      await tracker.awaitChildren(3);
      const third = tracker.children[2];
      if (third === undefined) throw new Error("third child missing");
      const thirdSender = await driveReady(third, ipcKeypair);
      await answerParkedQuery(third, thirdSender, 1);
      await woken;
      return third;
    };
    const backoffAfterCrash = async (child: FakeChild) => {
      crash(child);
      await armed.until(() => backoffs.length === 2);
      return backoffs[1];
    };
    return {
      ...running,
      second,
      secondSender,
      backoffMs,
      resets,
      wakeThirdChild,
      backoffAfterCrash,
    };
  }

  test("a raised respawn backoff resets when its time passes with the child in standby", async () => {
    const {
      supervisor,
      second,
      secondSender,
      backoffMs,
      resets,
      wakeThirdChild,
      backoffAfterCrash,
    } = await setupRaisedBackoff();

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(second, secondSender);
    await inStandby;
    resets.at(-1)?.();

    const third = await wakeThirdChild();
    expect(await backoffAfterCrash(third)).toBe(backoffMs);

    await supervisor.shutdown();
  });

  test("a raised respawn backoff resets once a child woken from standby lasts", async () => {
    const {
      supervisor,
      second,
      secondSender,
      backoffMs,
      resets,
      wakeThirdChild,
      backoffAfterCrash,
    } = await setupRaisedBackoff();

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(second, secondSender);
    await inStandby;

    const third = await wakeThirdChild();
    expect(resets).toHaveLength(2);
    resets.at(-1)?.();
    expect(await backoffAfterCrash(third)).toBe(backoffMs);

    await supervisor.shutdown();
  });

  test("a child woken from standby before its reset comes does not reset the backoff if it crashes first", async () => {
    const {
      supervisor,
      second,
      secondSender,
      backoffMs,
      wakeThirdChild,
      backoffAfterCrash,
    } = await setupRaisedBackoff();

    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(second, secondSender);
    await inStandby;

    const third = await wakeThirdChild();
    expect(await backoffAfterCrash(third)).toBe(backoffMs * 2);

    await supervisor.shutdown();
  });

  test("a standby request while a crashed child waits out its respawn backoff is put off", async () => {
    // A backoff no other timer here shares, which is never let fire.
    const backoffMs = 54_321;
    const backoffArmed = Promise.withResolvers<undefined>();
    const { supervisor, first } = await setup({
      respawnBackoffInitialMs: backoffMs,
      timers: {
        setTimer: (callback, ms) => {
          if (ms !== backoffMs) return setTimeout(callback, ms);
          backoffArmed.resolve(undefined);
          return callback;
        },
      },
    });

    first.eventChildToSupervisor.close();
    first.childToSupervisor.close();
    first.resolveExit?.(1);
    await backoffArmed.promise;
    await expect(supervisor.enterStandby()).rejects.toBeInstanceOf(
      StandbyDeferredError,
    );
    expect(sentFrameTypes(first)).not.toContain("quiet.request");

    await supervisor.shutdown();
  });

  test("a quiet check the child can no longer be sent puts the standby attempt off", async () => {
    let pipeClosed = false;
    const { supervisor } = await setup({
      controlWriteFails: () => pipeClosed,
    });

    pipeClosed = true;
    await expect(supervisor.enterStandby()).rejects.toBeInstanceOf(
      StandbyDeferredError,
    );

    pipeClosed = false;
    await supervisor.shutdown();
  });

  test("a correlation query the child can no longer be sent puts the standby attempt off", async () => {
    let queryFails = false;
    const { supervisor, firstSender, first } = await setup({
      controlWriteFails: (line) =>
        queryFails && line.includes('"parked-correlations.request"'),
    });

    queryFails = true;
    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await expect(inStandby).rejects.toBeInstanceOf(StandbyDeferredError);
    expect(sentFrameTypes(first)).not.toContain("shutdown");

    queryFails = false;
    await supervisor.shutdown();
  });

  test("a standby attempt is refused once the run has ended", async () => {
    const { supervisor, firstSender, first, tracker } = await setup();

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, { ended: true });
    await expect(inStandby).rejects.toBeInstanceOf(StandbyRefusedError);
    expect(first.killSignals).toEqual([]);
    expect(tracker.totalSpawns).toBe(1);

    await supervisor.shutdown();
  });

  test("a standby attempt is refused when the child cannot check that it is quiet", async () => {
    const { supervisor, firstSender, first, tracker } = await setup();

    const inStandby = supervisor.enterStandby();
    await answerQuiet(first, firstSender, 1, {
      quiet: false,
      error: "store read failed",
    });
    await expect(inStandby).rejects.toThrow(/store read failed/);
    await expect(inStandby).rejects.toBeInstanceOf(StandbyRefusedError);
    // Refused at the first failed check, not asked again.
    expect(
      sentFrameTypes(first).filter((type) => type === "quiet.request"),
    ).toHaveLength(1);
    expect(first.killSignals).toEqual([]);
    expect(tracker.totalSpawns).toBe(1);

    await supervisor.shutdown();
  });

  test("wake abandons a standby attempt the child has not reported quiet for", async () => {
    const { supervisor, first, tracker } = await setup();

    const inStandby = supervisor.enterStandby();
    await waitForUpstreamPayload(first.supervisorToChild, "quiet.request");
    await supervisor.wake();
    await expect(inStandby).rejects.toBeInstanceOf(StandbyRefusedError);
    expect(first.killSignals).toEqual([]);
    expect(tracker.totalSpawns).toBe(1);

    await supervisor.shutdown();
  });

  test("a standby attempt waits out the new dispatch loop when the one a recycle retired finishes late", async () => {
    const memory = createMemoryInboxPrimitives();
    const consumeHeld = Promise.withResolvers<undefined>();
    const releaseConsume = Promise.withResolvers<undefined>();
    let holdConsume = true;
    let holdDequeue = false;
    const dequeueHeld = Promise.withResolvers<undefined>();
    const releaseDequeue = Promise.withResolvers<undefined>();
    const standbyWaits = Promise.withResolvers<undefined>();
    let firstIdentity: string | null = null;
    const { supervisor, firstSender, first, tracker, mailBus, ipcKeypair } =
      await setup({
        onStandbyWait: (waiting) => {
          if (waiting === "standby") standbyWaits.resolve(undefined);
        },
        inboxPrimitives: {
          ...memory,
          async markConsumed(...args) {
            if (holdConsume) {
              holdConsume = false;
              consumeHeld.resolve(undefined);
              await releaseConsume.promise;
            }
            return memory.markConsumed(...args);
          },
          async replayProcessingToInbox(
            store,
            principal,
            repoId,
            address,
            opts,
          ) {
            // Keeps the mail the run took, as the production scan does.
            const owned = new Set(opts?.ownedIdentities ?? []);
            if (firstIdentity !== null) owned.add(firstIdentity);
            return memory.replayProcessingToInbox(
              store,
              principal,
              repoId,
              address,
              { ...opts, ownedIdentities: owned },
            );
          },
          async dequeueToProcessing(...args) {
            const taken = await memory.dequeueToProcessing(...args);
            if (holdDequeue && taken !== null) {
              holdDequeue = false;
              dequeueHeld.resolve(undefined);
              await releaseDequeue.promise;
            }
            return taken;
          },
        },
      });

    // The first loop's consume is held after its run parks.
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("first")),
    );
    const fired = await waitForUpstreamPayload(
      first.supervisorToChild,
      "trigger.fire",
    );
    const taken = fired.data.mailIdentity;
    firstIdentity =
      "deliveryKey" in taken ? taken.deliveryKey : taken.messageId;
    await takeTrigger(first, firstSender);
    await firstSender.send({
      type: "park.notify",
      data: { runId: RUN, correlationId: "corr-1", parkKind: "input" },
    });
    await consumeHeld.promise;

    // A recycle retires that loop, and the new loop holds the replayed mail
    // right after moving it out of the inbox.
    await mailBus.deliverSettled(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("second")),
    );
    holdDequeue = true;
    const recycled = supervisor.recycle({ reason: "operator" });
    await tracker.awaitChildren(2);
    const second = tracker.children[1];
    if (second === undefined) throw new Error("second child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await dequeueHeld.promise;
    releaseConsume.resolve(undefined);
    await recycled;

    const inStandby = supervisor.enterStandby();
    await standbyWaits.promise;
    releaseDequeue.resolve(undefined);
    await takeTrigger(second, secondSender);
    await secondSender.send({
      type: "park.notify",
      data: { runId: RUN, correlationId: "corr-2", parkKind: "input" },
    });
    await actAsChildEnteringStandby(second, secondSender);
    await inStandby;
    await supervisor.shutdown();
    expect(memory.messageIds(ADDRESS).processing).toEqual([]);
  });

  test("a refusal answers the writes a final check held back", async () => {
    const { supervisor, firstSender, first, tracker } = await setup({
      inboxPrimitives: (() => {
        const inbox = createMemoryInboxPrimitives();
        return { ...inbox, hasInboxEntries: async () => true };
      })(),
    });
    const inStandby = supervisor.enterStandby();
    void inStandby.catch(() => undefined);
    await answerQuiet(first, firstSender, 1, { quiet: true });
    await answerQuiet(first, firstSender, 2, { quiet: true });
    await waitForUpstreamPayloads(
      first.supervisorToChild,
      "parked-correlations.request",
      2,
    );
    await sendChildWrite(firstSender, "held-by-fence");
    await answerParkedQuery(first, firstSender, 2);
    await expect(inStandby).rejects.toThrow("mail is waiting");
    expect(tracker.totalSpawns).toBe(1);
    const responses = await waitForUpstreamPayloads(
      first.supervisorToChild,
      "substrate.write.response",
      1,
    );
    expect(responses[0]?.data.requestId).toBe("held-by-fence");
    await supervisor.shutdown();
  });

  test("a signal mid delivery holds a standby attempt's final check back", async () => {
    const reached = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const waits = Promise.withResolvers<undefined>();
    let holding = false;
    const { supervisor, firstSender, first } = await setup({
      onStandbyWait: (on) => {
        if (holding && on === "standby") waits.resolve(undefined);
      },
      recyclePolicy: {
        onRunStart: async () => {
          if (holding) {
            reached.resolve(undefined);
            await release.promise;
          }
          return null;
        },
      },
    });
    const inStandby = supervisor.enterStandby();
    void inStandby.catch(() => undefined);
    await waitForUpstreamPayload(first.supervisorToChild, "quiet.request");
    holding = true;
    const delivering = supervisor.deliverSignal({
      runId: RUN,
      signalName: "go",
      signalId: "sig-held",
      payload: null,
    });
    await reached.promise;
    await answerQuiet(first, firstSender, 1, { quiet: true });
    expect(
      await Promise.race([
        waits.promise.then(() => "waited"),
        waitForUpstreamPayloads(
          first.supervisorToChild,
          "quiet.request",
          2,
        ).then(() => "asked"),
      ]),
    ).toBe("waited");
    release.resolve(undefined);
    await delivering;
    await supervisor.shutdown();
  });

  test("a boot replay still running holds a standby attempt back", async () => {
    const inbox = createMemoryInboxPrimitives();
    const release = Promise.withResolvers<undefined>();
    const waits = Promise.withResolvers<undefined>();
    const { supervisor, first } = await setup({
      inboxPrimitives: {
        ...inbox,
        replayProcessingToInbox: async (...args) => {
          await release.promise;
          return inbox.replayProcessingToInbox(...args);
        },
      },
      onStandbyWait: (on) => {
        if (on === "standby") waits.resolve(undefined);
      },
    });
    const inStandby = supervisor.enterStandby();
    void inStandby.catch(() => undefined);
    const seen = await Promise.race([
      waits.promise.then(() => "waited"),
      waitForUpstreamPayload(first.supervisorToChild, "quiet.request").then(
        () => "asked",
      ),
    ]);
    expect(seen).toBe("waited");
    release.resolve(undefined);
    await supervisor.shutdown();
  });

  test("mail waking a live standby whose respawn fails stops the supervisor and says so", async () => {
    const stopped = Promise.withResolvers<string>();
    const baseDir = await makeTempDir("standby-wake-fails-");
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    let spawns = 0;
    const { supervisor, firstSender } = await spawnSupervisor({
      baseDir,
      tracker: {
        ...tracker,
        spawner: (args) => {
          spawns += 1;
          if (spawns > 1)
            throw new Error("the workflow-child binary is missing");
          return tracker.spawner(args);
        },
      },
      mailBus,
      ipcKeypair,
      overrides: {
        repoStore: createStubRepoStore(baseDir, {
          writeTree: true,
          refTip: HISTORY_TIP,
        }),
        onSelfTerminate: (info) => stopped.resolve(info.reason),
      },
    });
    const first = tracker.children[0];
    if (first === undefined) throw new Error("first child missing");
    const inStandby = supervisor.enterStandby();
    await actAsChildEnteringStandby(first, firstSender);
    expect(await inStandby).toEqual({});
    mailBus.deliver(
      ADDRESS,
      inboundDelivery(new TextEncoder().encode("wake up")),
    );
    expect(await stopped.promise).toBe(
      "recycle failed: the workflow-child binary is missing",
    );
  });
});

describe("supervisor restored in standby", () => {
  const SPAWN_OPTS = {
    definitionHash: TEST_DEFINITION_HASH,
    onInferenceEvent: () => undefined,
  };

  // A store over a run whose history tip is `tip`, and whose writes merge, so
  // a cancellation can be committed. `undefined` leaves the tip unreadable.
  // `beforeTipRead` runs ahead of each read of the tip.
  function historyStore(
    baseDir: string,
    tip: string | null | undefined,
    beforeTipRead?: () => Promise<void>,
  ) {
    const base = createStubRepoStore(baseDir, {
      writeTree: true,
      ...(tip !== undefined ? { refTip: tip } : {}),
    });
    return new Proxy(base, {
      get(target, prop, receiver) {
        if (prop === "resolveRef" && beforeTipRead !== undefined) {
          return async (...args: Parameters<RepoStore["resolveRef"]>) => {
            await beforeTipRead();
            return target.resolveRef(...args);
          };
        }
        if (prop !== "writeTreePreservingPrefix") {
          return Reflect.get(target, prop, receiver);
        }
        return async (
          ...args: Parameters<RepoStore["writeTreePreservingPrefix"]>
        ) => {
          await args[3].merge(new Map());
          return target.writeTreePreservingPrefix(...args);
        };
      },
    });
  }

  // Restore a supervisor from `standby` over a run whose history tip is
  // `tip`, the recorded one unless a test moves it.
  async function restore(
    standby: RecordedStandby,
    opts: {
      tip?: string | null | undefined;
      onWake?: WorkflowSupervisorBindings["onWake"];
      onSelfTerminate?: WorkflowSupervisorBindings["onSelfTerminate"];
      onSuspensionRegister?: WorkflowSupervisorBindings["onSuspensionRegister"];
      inbox?: ReturnType<typeof createMemoryInboxPrimitives>;
      inboxPrimitives?: Partial<InboxPrimitives>;
      beforeRestore?: (
        baseDir: string,
        inbox: ReturnType<typeof createMemoryInboxPrimitives>,
      ) => Promise<void>;
      spawner?: (tracker: SpawnTracker) => SubprocessSpawner;
      recyclePolicy?: Partial<WorkflowSupervisorBindings>;
      beforeTipRead?: () => Promise<void>;
    } = {},
  ) {
    const baseDir = await makeTempDir("standby-restore-");
    await seedStepGrants(baseDir, "step-1", [
      { resource: "thing", action: "read" },
    ]);
    const inbox = opts.inbox ?? createMemoryInboxPrimitives();
    await opts.beforeRestore?.(baseDir, inbox);
    const ipcKeypair = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker({});
    const bindings = await buildBindings({
      baseDir,
      spawner: opts.spawner?.(tracker) ?? tracker.spawner,
      mailBus,
      ipcKeypair,
      inboxPrimitives: { ...inbox, ...opts.inboxPrimitives },
    });
    const supervisor = createWorkflowSupervisor({
      ...bindings,
      repoStore: historyStore(
        baseDir,
        "tip" in opts ? opts.tip : standby.tip,
        opts.beforeTipRead,
      ),
      ...(opts.onWake !== undefined ? { onWake: opts.onWake } : {}),
      ...(opts.onSelfTerminate !== undefined
        ? { onSelfTerminate: opts.onSelfTerminate }
        : {}),
      ...(opts.onSuspensionRegister !== undefined
        ? { onSuspensionRegister: opts.onSuspensionRegister }
        : {}),
      ...opts.recyclePolicy,
    });
    const restoring = supervisor.restoreInStandby(SPAWN_OPTS, standby);
    return { supervisor, restoring, tracker, mailBus, ipcKeypair, inbox };
  }

  // Drive the `count`th child through its handshake and the correlation query
  // its install sends.
  async function driveStartedChild(
    tracker: SpawnTracker,
    ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array },
    count: number,
  ) {
    await tracker.awaitChildren(count);
    const child = tracker.children[count - 1];
    if (child === undefined) throw new Error(`child ${String(count)} missing`);
    const sender = await driveReady(child, ipcKeypair);
    await answerParkedQuery(child, sender, 1);
    return { child, sender };
  }

  const recorded: RecordedStandby = {
    parked: [GATE_PARK],
    tip: HISTORY_TIP,
    definition: TEST_DEFINITION,
  };

  test("a shutdown while the restore reads the history tip waits for it, and the restore stops", async () => {
    const reading = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const { supervisor, restoring, tracker, mailBus } = await restore(
      recorded,
      {
        beforeTipRead: async () => {
          reading.resolve(undefined);
          await release.promise;
        },
      },
    );
    await reading.promise;

    const shutdown = supervisor.shutdown();
    release.resolve(undefined);
    await expect(restoring).rejects.toThrow("shut down before the restore");
    await shutdown;
    expect(mailBus.registered()).toEqual([]);
    expect(tracker.totalSpawns).toBe(0);
  });

  test("a wake whose child a shutdown leaves uninstalled reports no wake", async () => {
    const killRequested = Promise.withResolvers<undefined>();
    let finishChild: (() => void) | undefined;
    const wakes: string[] = [];
    const { supervisor, restoring, tracker, ipcKeypair } = await restore(
      recorded,
      {
        onWake: ({ reason }) => wakes.push(reason),
        // The shutdown's kill is held, so the child's ready arrives after it.
        spawner: (tracker) => (args) => {
          const handle = tracker.spawner(args);
          finishChild = () => handle.kill("SIGKILL");
          return { ...handle, kill: () => killRequested.resolve(undefined) };
        },
      },
    );
    await restoring;

    const woken = supervisor.wake();
    await tracker.awaitChildren(1);
    const stopped = supervisor.shutdown();
    await killRequested.promise;
    const child = tracker.children[0];
    if (child === undefined) throw new Error("child missing");
    await driveReady(child, ipcKeypair);
    await expect(woken).rejects.toThrow("shut down before the woken child ran");
    finishChild?.();
    await stopped;
    expect(wakes).toEqual([]);
  });

  test("a restored standby arms no recycle policy before its first child, and measures that child from its start", async () => {
    const policy = manualRecyclePolicy(60_000);
    const { supervisor, restoring, tracker, ipcKeypair } = await restore(
      recorded,
      { recyclePolicy: policy.bindings },
    );
    expect(await restoring).toEqual({ inStandby: true });
    expect(policy.armed()).toBe(0);

    policy.advance(120_000);
    const armed = policy.nextArm();
    const woken = supervisor.wake();
    await driveStartedChild(tracker, ipcKeypair, 1);
    await woken;
    await armed;

    const ticked = policy.nextArm();
    policy.tick();
    await ticked;
    expect(tracker.totalSpawns).toBe(1);

    await supervisor.shutdown();
  });

  test("the first child of a restored standby whose boot replay went through does not replay again", async () => {
    const inbox = createMemoryInboxPrimitives();
    let replays = 0;
    const { supervisor, restoring, tracker, ipcKeypair } = await restore(
      recorded,
      {
        inbox,
        inboxPrimitives: {
          async replayProcessingToInbox(...args) {
            replays += 1;
            return inbox.replayProcessingToInbox(...args);
          },
        },
      },
    );
    expect(await restoring).toEqual({ inStandby: true });

    const woken = supervisor.wake();
    await driveStartedChild(tracker, ipcKeypair, 1);
    await woken;
    expect(replays).toBe(1);

    await supervisor.shutdown();
  });

  test("a wake that reaches its replay after a shutdown began does not replay", async () => {
    const inbox = createMemoryInboxPrimitives();
    const bootReplay = Promise.withResolvers<undefined>();
    let replays = 0;
    const { supervisor, restoring } = await restore(recorded, {
      inbox,
      inboxPrimitives: {
        async replayProcessingToInbox(...args) {
          replays += 1;
          if (replays > 1) return inbox.replayProcessingToInbox(...args);
          await bootReplay.promise;
          throw new Error("the boot replay failed");
        },
      },
    });
    expect(await restoring).toEqual({ inStandby: true });

    const woken = supervisor.wake().catch(() => undefined);
    const stopped = supervisor.shutdown();
    bootReplay.resolve(undefined);
    await stopped;
    await woken;
    expect(replays).toBe(1);
  });

  test("a standby restored over unchanged history starts no child and re-sends what it was parked on", async () => {
    const registered: string[] = [];
    const { supervisor, restoring, tracker, mailBus } = await restore(
      recorded,
      {
        onSuspensionRegister: (registration) => {
          registered.push(registration.correlationId);
        },
      },
    );
    expect(await restoring).toEqual({ inStandby: true });
    expect(mailBus.registered()).toEqual([ADDRESS]);

    await supervisor.reEmitParkedCorrelations();
    expect(registered).toEqual(["corr-gate"]);
    expect(await supervisor.enterStandby()).toEqual({});

    await supervisor.shutdown();
    expect(mailBus.registered()).toEqual([]);
    expect(tracker.totalSpawns).toBe(0);
  });

  const wakes: [
    string,
    Partial<RecordedStandby>,
    (setup: Awaited<ReturnType<typeof restore>>) => Promise<unknown>,
  ][] = [
    [
      "mail",
      {},
      async ({ mailBus }) => {
        mailBus.deliver(
          ADDRESS,
          inboundDelivery(new TextEncoder().encode("wake up")),
        );
      },
    ],
    [
      "timer",
      { wakeAt: new Date(Date.now() - 1_000).toISOString() },
      async () => undefined,
    ],
    ["wake", {}, ({ supervisor }) => supervisor.wake()],
    [
      "signal",
      {},
      ({ supervisor }) =>
        supervisor.deliverSignal({
          runId: RUN,
          signalName: "gate",
          signalId: "sig-1",
          payload: { approved: true },
        }),
    ],
    [
      "cancel",
      {},
      ({ supervisor }) =>
        supervisor.requestCancel({
          runId: RUN,
          origin: "hub-admin",
          reason: "Stop",
          at: "2026-10-01T00:00:00.000Z",
        }),
    ],
  ];
  test.each(wakes)(
    "%s starts the first child of a restored standby",
    async (reason, standby, trigger) => {
      const woke = Promise.withResolvers<{
        reason: string;
        requested: boolean;
      }>();
      const restored = await restore(
        { ...recorded, ...standby },
        { onWake: (info) => woke.resolve(info) },
      );
      expect(await restored.restoring).toEqual({ inStandby: true });
      const triggered = trigger(restored);
      const { child } = await driveStartedChild(
        restored.tracker,
        restored.ipcKeypair,
        1,
      );
      await triggered;
      // Only a wake asked for through `wake()` is requested.
      expect(await woke.promise).toEqual({
        reason,
        requested: reason === "wake",
      });
      expect(sentFrameTypes(child)[0]).toBe("authority.set");
      if (reason === "signal") {
        expect(
          parseSignalDeliverRunIds(child.supervisorToChild.flushed()),
        ).toEqual([RUN]);
      }

      await restored.supervisor.shutdown();
    },
  );

  const startedAwake: [string, string | null | undefined, string][] = [
    [
      "written since it stopped",
      "tip-after-a-cancellation",
      "its restart, which found history written after it stopped",
    ],
    [
      "that cannot be read",
      undefined,
      "its restart, which could not read its history: stub RepoStore: resolveRef not implemented for this test",
    ],
  ];
  test.each(startedAwake)(
    "a standby restored over history %s starts the child, and says why",
    async (_case, tip, reason) => {
      const { supervisor, restoring, tracker, ipcKeypair } = await restore(
        recorded,
        { tip },
      );
      await tracker.awaitChildren(1);
      const child = tracker.children[0];
      if (child === undefined) throw new Error("child missing");
      const sender = await driveReady(child, ipcKeypair);
      expect(await restoring).toEqual({ inStandby: false, reason });

      // A running child is asked whether it is quiet, not answered for.
      const entering = supervisor.enterStandby();
      await answerQuiet(child, sender, 1, { ended: true });
      await expect(entering).rejects.toThrow("the workflow run has ended");

      await supervisor.shutdown();
    },
  );

  test("a standby restored with a definition that is not the approved one starts the child, and says why", async () => {
    const { supervisor, restoring, tracker, ipcKeypair } = await restore({
      ...recorded,
      definition: { ...TEST_DEFINITION, id: "another-workflow" },
    });
    await tracker.awaitChildren(1);
    const child = tracker.children[0];
    if (child === undefined) throw new Error("child missing");
    await driveReady(child, ipcKeypair);
    expect(await restoring).toEqual({
      inStandby: false,
      reason:
        "its restart, which found a recorded definition that is not the approved one",
    });

    await supervisor.shutdown();
  });

  test("a restored standby whose boot replay puts mail back in the inbox wakes for it", async () => {
    const woke = Promise.withResolvers<string>();
    const { restoring, tracker, ipcKeypair, supervisor } = await restore(
      recorded,
      {
        onWake: (info) => woke.resolve(info.reason),
        beforeRestore: (baseDir, memory) =>
          takeMail(memory, baseDir, "mail-orphan", 1),
      },
    );
    expect(await restoring).toEqual({ inStandby: true });
    await driveStartedChild(tracker, ipcKeypair, 1);
    expect(await woke.promise).toBe("mail");

    await supervisor.shutdown();
  });

  test("a restored standby keeps mail a live run took, and stays stopped", async () => {
    const replayed = Promise.withResolvers<undefined>();
    const { restoring, tracker, supervisor, inbox } = await restore(recorded, {
      beforeRestore: async (baseDir, memory) => {
        await writeWorkingRunLog(baseDir, [
          { ...runStarted(), consumedDeliveryKey: await keyOf("mail-owned") },
        ]);
        await takeMail(memory, baseDir, "mail-owned", 1);
      },
      inboxPrimitives: {
        async hasInboxEntries(...args) {
          replayed.resolve(undefined);
          return createMemoryInboxPrimitives().hasInboxEntries(...args);
        },
      },
    });
    expect(await restoring).toEqual({ inStandby: true });
    await replayed.promise;
    expect(inbox.messageIds(ADDRESS)).toEqual({
      inbox: [],
      processing: ["mail-owned"],
      consumed: [],
    });

    await supervisor.shutdown();
    expect(tracker.totalSpawns).toBe(0);
  });

  test("a restored standby whose boot replay fails wakes in case it left mail behind", async () => {
    const woke = Promise.withResolvers<string>();
    const { restoring, tracker, ipcKeypair, supervisor } = await restore(
      recorded,
      {
        onWake: (info) => woke.resolve(info.reason),
        inboxPrimitives: {
          async replayProcessingToInbox() {
            throw new Error("the claim-check tree is unreadable");
          },
        },
      },
    );
    expect(await restoring).toEqual({ inStandby: true });
    await driveStartedChild(tracker, ipcKeypair, 1);
    expect(await woke.promise).toBe(
      "its restart, which could not recover its mail",
    );

    await supervisor.shutdown();
  });

  test("a credential update while restored reaches the first child's authority.set", async () => {
    const { restoring, tracker, ipcKeypair, supervisor } =
      await restore(recorded);
    expect(await restoring).toEqual({ inStandby: true });
    const delivery = {
      bindings: [],
      materials: [
        {
          credentialId: "cred-1",
          providerKey: "provider",
          origin: "tenant",
          secret: "s3cret",
        },
      ],
    };
    await supervisor.deliverCredentials({ delivery });

    const woken = supervisor.wake();
    const { child } = await driveStartedChild(tracker, ipcKeypair, 1);
    await woken;
    const authority = await waitForUpstreamPayload(
      child.supervisorToChild,
      "authority.set",
    );
    expect(authority.data.delivery).toEqual(delivery);

    await supervisor.shutdown();
  });

  test("calls that need a child do nothing to a restored standby, or are refused", async () => {
    const { restoring, tracker, supervisor } = await restore(recorded);
    expect(await restoring).toEqual({ inStandby: true });

    await supervisor.drain({ deadlineMs: 1_000 });
    await supervisor.deliverSources({
      sources: [
        {
          id: "primary",
          provider: "anthropic",
          baseURL: "https://api.anthropic.com",
          credentialId: "sk-x",
          model: "claude-test",
        },
      ],
      defaultSource: "primary",
    });
    expect(await supervisor.deliverGrants(RUN)).toBe("skipped");
    await expect(
      supervisor.recycle({ reason: "policy", origin: "operator" }),
    ).rejects.toThrow("recycle called in phase standby");
    supervisor.abandonStandby("the Hub went away");
    expect(tracker.totalSpawns).toBe(0);

    await supervisor.shutdown();
  });

  test("a shutdown while a restored standby's first child starts stops that child", async () => {
    const { restoring, tracker, supervisor } = await restore(recorded);
    expect(await restoring).toEqual({ inStandby: true });
    const woken = supervisor.wake();
    await tracker.awaitChildren(1);
    const child = tracker.children[0];
    if (child === undefined) throw new Error("child missing");

    await supervisor.shutdown();
    await expect(woken).rejects.toThrow();
    expect(child.killSignals.length).toBeGreaterThan(0);
  });

  test("a channel crash while a restored standby's first child starts stops the supervisor and says so", async () => {
    const stopped = Promise.withResolvers<string>();
    const { restoring, tracker, supervisor } = await restore(recorded, {
      onSelfTerminate: (info) => stopped.resolve(info.reason),
    });
    expect(await restoring).toEqual({ inStandby: true });
    const woken = supervisor.wake();
    await tracker.awaitChildren(1);
    const child = tracker.children[0];
    if (child === undefined) throw new Error("child missing");
    child.childToSupervisor.inject("not-json{{{");

    await expect(woken).rejects.toThrow();
    expect(await stopped.promise).toContain(
      "control channel received non-JSON line",
    );
  });

  test("a restored standby whose first child cannot start stops the supervisor and says so", async () => {
    const stopped = Promise.withResolvers<string>();
    const { restoring, supervisor } = await restore(recorded, {
      onSelfTerminate: (info) => stopped.resolve(info.reason),
      spawner: () => () => {
        throw new Error("the workflow-child binary is missing");
      },
    });
    expect(await restoring).toEqual({ inStandby: true });

    await expect(supervisor.wake()).rejects.toThrow(
      "the workflow-child binary is missing",
    );
    expect(await stopped.promise).toBe(
      "recycle failed: the workflow-child binary is missing",
    );
  });

  test("a standby attempt while a restored standby's first child starts is put off", async () => {
    const { restoring, supervisor, tracker, ipcKeypair } =
      await restore(recorded);
    expect(await restoring).toEqual({ inStandby: true });
    const woken = supervisor.wake();
    await tracker.awaitChildren(1);
    const attempt = await supervisor.enterStandby().then(
      (outcome) => ({ answered: outcome }),
      (cause: unknown) => ({ failed: cause }),
    );
    expect(
      "failed" in attempt && attempt.failed instanceof StandbyDeferredError,
    ).toBe(true);
    const second = tracker.children[0];
    if (second === undefined) throw new Error("child missing");
    const secondSender = await driveReady(second, ipcKeypair);
    await answerParkedQuery(second, secondSender, 1);
    await woken;
    await supervisor.shutdown();
  });
});
