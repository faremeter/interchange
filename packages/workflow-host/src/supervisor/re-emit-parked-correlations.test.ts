// Supervisor `reEmitParkedCorrelations` driver and its Trigger A wiring.
//
// On a re-establishment the supervisor queries the child for its currently-
// parked correlations (`parked-correlations.request`) and re-registers each
// through `onSuspensionRegister` -- recovering a `park.notify` register the hub
// may have missed while it was down at suspend. The supervisor fires this
// automatically whenever a fresh child becomes addressable: after a spawn and
// after a recycle's `installNewChild`. The driver itself is best-effort: it
// no-ops when the child is not addressable, and a query that times out or whose
// send fails is dropped for the next re-establishment to re-drive.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import type { ApprovalSnapshot } from "@intx/types/runtime";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { RepoId, RepoStore } from "@intx/hub-sessions";

import {
  createWorkflowSupervisor,
  type InboxPrimitives,
  type SubprocessHandle,
  type SubprocessSpawner,
  type SuspensionRegistration,
  type WorkflowSupervisor,
} from "./index";
import { wrapHubTransportAsMailBus } from "../mail-bus/index";
import {
  createControlChannelSender,
  receiveControlChannel,
  type ControlPayload,
  type NdjsonReader,
  type NdjsonWriter,
  type FrameReader,
} from "../ipc/index";

const AGENT_ADDRESS = "ins_reemit-agent@integration.example";
const DEPLOYMENT_ID = "reemit-dep";

const SNAPSHOT: ApprovalSnapshot = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

type ParkedEntry = {
  runId: string;
  correlationId: string;
  kind: "approval";
  snapshot: ApprovalSnapshot;
};

function registrationFor(entry: ParkedEntry): SuspensionRegistration {
  return {
    runId: entry.runId,
    correlationId: entry.correlationId,
    kind: entry.kind,
    deploymentId: DEPLOYMENT_ID,
    agentAddress: AGENT_ADDRESS,
    approvalSnapshot: entry.snapshot,
  };
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
            if (next === undefined) throw new Error("buffer shift undefined");
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
            if (next === undefined) throw new Error("frame shift undefined");
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

/**
 * Minimal `RepoStore` stub: `spawn` consults `getRepoDir` for credentials
 * assembly. No re-emit path touches the substrate, so every other method
 * throws to surface an accidental untested code path.
 */
function createStubRepoStore(baseDir: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; only getRepoDir is exercised and any other method throws via the proxy
  return new Proxy(stub as RepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(`stub RepoStore: ${String(prop)} not implemented`);
      };
    },
  });
}

function createNoopInboxPrimitives(): InboxPrimitives {
  return {
    async enqueueInbox() {
      throw new Error("enqueueInbox not exercised in the re-emit test");
    },
    async dequeueToProcessing() {
      return null;
    },
    async markConsumed() {
      throw new Error("markConsumed not exercised in the re-emit test");
    },
    async replayProcessingToInbox() {
      return { commitSha: "noop", replayedKeys: [] };
    },
  };
}

type FakeChild = {
  pid: number;
  channelId: string | undefined;
  s2c: ReturnType<typeof createMemoryNdjsonStream>;
  c2s: ReturnType<typeof createMemoryNdjsonStream>;
  events: ReturnType<typeof createMemoryFrameStream>;
  // When true, the supervisor's downstream writes to this child throw,
  // simulating a closing pipe so the driver's send-failure path is exercised.
  failWrites: boolean;
};

/**
 * A subprocess spawner that records one `FakeChild` per spawn, so the test can
 * drive the initial cohort and every recycle's replacement cohort.
 */
function createSpawnTracker() {
  const children: FakeChild[] = [];
  const spawner: SubprocessSpawner = ({ env }) => {
    const s2c = createMemoryNdjsonStream();
    const c2s = createMemoryNdjsonStream();
    const events = createMemoryFrameStream();
    const child: FakeChild = {
      pid: 9300 + children.length,
      channelId: env.IPC_CHANNEL_ID,
      s2c,
      c2s,
      events,
      failWrites: false,
    };
    children.push(child);
    const handle: SubprocessHandle = {
      pid: child.pid,
      controlWriter: {
        write(line: string) {
          if (child.failWrites) throw new Error("controlWriter send boom");
          return s2c.writer.write(line);
        },
      },
      controlReader: c2s.reader,
      eventReader: events.reader,
      kill: () => {
        events.close();
        c2s.close();
      },
      exited: Promise.resolve(0),
    };
    return handle;
  };
  return { spawner, children };
}

/**
 * Drive a cohort's `ready` handshake and start a mock child loop that answers
 * `parked-correlations.request` from the shared `nextReply` ref (a `null` ref
 * means never reply, to exercise the watchdog). Calls `onReply` after each
 * answer so a test can await the fire-and-forget Trigger A effect
 * deterministically.
 */
async function driveReadyAndAnswer(
  child: FakeChild,
  ipcKp: { privateKey: Uint8Array; publicKey: Uint8Array },
  nextReply: { current: ParkedEntry[] | null },
  onReply: () => void,
): Promise<void> {
  if (child.channelId === undefined) throw new Error("no channelId");
  const childSender = createControlChannelSender({
    privateKeySeed: ipcKp.privateKey,
    channelId: child.channelId,
    writer: {
      write(line: string) {
        child.c2s.inject(line);
      },
    },
  });
  await childSender.send({
    type: "ready",
    data: { childPid: child.pid, childPublicKey: hexEncode(ipcKp.publicKey) },
  });

  const receiver = receiveControlChannel({
    publicKey: ipcKp.publicKey,
    channelId: child.channelId,
    reader: child.s2c.reader,
    onCrash: () => undefined,
  });
  void (async () => {
    for await (const payload of receiver) {
      if (payload.type !== "parked-correlations.request") continue;
      const reply = nextReply.current;
      if (reply === null) continue; // watchdog path: never answer
      const response: ControlPayload = {
        type: "parked-correlations.response",
        data: { requestId: payload.data.requestId, parked: reply },
      };
      await childSender.send(response);
      onReply();
    }
  })();
}

interface Harness {
  supervisor: WorkflowSupervisor;
  registrations: SuspensionRegistration[];
  nextReply: { current: ParkedEntry[] | null };
  children: FakeChild[];
  ipcKp: { privateKey: Uint8Array; publicKey: Uint8Array };
  waitForRegistrations: (n: number) => Promise<void>;
  cleanup: () => Promise<void>;
}

async function poll(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 2));
  }
  throw new Error("condition not met in time");
}

/**
 * Spawn a supervisor with the mock cohort tracker and drive the initial
 * cohort's `ready`. `initialReply` is what the first cohort reports for the
 * spawn-seam Trigger A query; `setup` awaits that first reply so the fire-and-
 * forget auto-emit has settled before the test proceeds (no race with a later
 * `nextReply` change).
 */
async function setup(opts: {
  initialReply: ParkedEntry[] | null;
  parkedQueryWatchdogMs?: number;
}): Promise<Harness> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "reemit-parked-"));
  const hostTransport = createInMemoryTransport();
  const agentKeyPair = await generateKeyPair();
  hostTransport.register(AGENT_ADDRESS, createEd25519Crypto(agentKeyPair));
  const mailBus = wrapHubTransportAsMailBus(hostTransport);

  const ipcKp = await generateKeyPair();
  const tracker = createSpawnTracker();

  const registrations: SuspensionRegistration[] = [];
  const nextReply: { current: ParkedEntry[] | null } = {
    current: opts.initialReply,
  };
  let replies = 0;

  const supervisor = createWorkflowSupervisor({
    repoStore: createStubRepoStore(baseDir),
    signAsPrincipal: async () => ({
      sig: new Uint8Array(64),
      principalKind: "supervisor",
    }),
    mailBus,
    onSuspensionRegister: (registration) => {
      registrations.push(registration);
    },
    subprocessSpawner: tracker.spawner,
    binaryPath: "/fake/bin/workflow-child",
    substrateEnv: {},
    dynamicSpawnEnv: () => ({}),
    workflowRunRepoId: { kind: "workflow-run", id: DEPLOYMENT_ID },
    workflowRunRef: "refs/heads/main",
    deploymentId: DEPLOYMENT_ID,
    stepCount: 1,
    deploymentMailAddress: AGENT_ADDRESS,
    readPrincipal: { kind: "supervisor" },
    deriveStepAddress: () => AGENT_ADDRESS,
    deriveStepRepoId: () => ({ kind: "agent-state", id: DEPLOYMENT_ID }),
    inboxPrimitives: createNoopInboxPrimitives(),
    ipcKeyPairFactory: () => Promise.resolve(ipcKp),
    drainTimeoutMs: 200,
    ...(opts.parkedQueryWatchdogMs !== undefined
      ? { parkedQueryWatchdogMs: opts.parkedQueryWatchdogMs }
      : {}),
  });

  const spawnPromise = supervisor.spawn({
    stepOrder: ["step-1"],
    definitionHash: "def-hash",
    warmKeep: false,
    onInferenceEvent: () => undefined,
  });

  await poll(() => tracker.children.length >= 1);
  const childA = tracker.children[0];
  if (childA === undefined) throw new Error("cohort A missing");
  await driveReadyAndAnswer(childA, ipcKp, nextReply, () => {
    replies += 1;
  });
  await spawnPromise;
  // Await the spawn-seam Trigger A round-trip (unless the cohort withholds its
  // reply) so a later `nextReply` change cannot race the auto-emit.
  if (nextReply.current !== null) {
    await poll(() => replies >= 1);
  }

  return {
    supervisor,
    registrations,
    nextReply,
    children: tracker.children,
    ipcKp,
    waitForRegistrations: (n) => poll(() => registrations.length >= n),
    cleanup: async () => {
      await supervisor.shutdown();
      for (const child of tracker.children) {
        child.s2c.close();
        child.c2s.close();
        child.events.close();
      }
      await fs.rm(baseDir, { recursive: true, force: true });
    },
  };
}

describe("supervisor reEmitParkedCorrelations", () => {
  test("re-registers the parked set on spawn (Trigger A)", async () => {
    const parked: ParkedEntry[] = [
      {
        runId: "run-a",
        correlationId: "corr-a",
        kind: "approval",
        snapshot: SNAPSHOT,
      },
      {
        runId: "run-b",
        correlationId: "corr-b",
        kind: "approval",
        snapshot: SNAPSHOT,
      },
    ];
    const harness = await setup({ initialReply: parked });

    // The spawn seam fired the driver automatically; each parked correlation is
    // re-registered with the deployment identity stamped on.
    await harness.waitForRegistrations(2);
    expect(harness.registrations).toEqual(parked.map(registrationFor));

    await harness.cleanup();
  });

  test("re-registers the parked set on recycle against the new cohort (Trigger A)", async () => {
    // Cohort A reports nothing on spawn; the recycle's fresh cohort reports a
    // parked correlation, and the supervisor must re-emit it -- proving the
    // recycle seam fires the driver against the NEW cohort's controlSender.
    const harness = await setup({ initialReply: [] });
    expect(harness.registrations).toEqual([]);

    const recycled: ParkedEntry = {
      runId: "run-c",
      correlationId: "corr-c",
      kind: "approval",
      snapshot: SNAPSHOT,
    };
    harness.nextReply.current = [recycled];

    const recycleP = harness.supervisor.recycle({ reason: "test-recycle" });
    await poll(() => harness.children.length >= 2);
    const childB = harness.children[1];
    if (childB === undefined) throw new Error("cohort B missing");
    let cohortBReplies = 0;
    await driveReadyAndAnswer(childB, harness.ipcKp, harness.nextReply, () => {
      cohortBReplies += 1;
    });
    await recycleP;

    // The recycle-seam re-emit registered the new cohort's parked correlation.
    // Only cohort B could have reported `corr-c` (cohort A reported nothing), so
    // this proves the re-emit queried the fresh cohort.
    await harness.waitForRegistrations(1);
    expect(harness.registrations).toEqual([registrationFor(recycled)]);
    expect(cohortBReplies).toBeGreaterThanOrEqual(1);

    await harness.cleanup();
  });

  test("re-registers what the child reports on an explicit call", async () => {
    // The spawn auto-emit reported nothing; an explicit call then re-registers
    // the freshly-reported set.
    const harness = await setup({ initialReply: [] });
    expect(harness.registrations).toEqual([]);

    const parked: ParkedEntry = {
      runId: "run-x",
      correlationId: "corr-x",
      kind: "approval",
      snapshot: SNAPSHOT,
    };
    harness.nextReply.current = [parked];

    await harness.supervisor.reEmitParkedCorrelations();

    await harness.waitForRegistrations(1);
    expect(harness.registrations).toEqual([registrationFor(parked)]);

    await harness.cleanup();
  });

  test("registers nothing when the child reports no parked correlations", async () => {
    const harness = await setup({ initialReply: [] });

    await harness.supervisor.reEmitParkedCorrelations();

    expect(harness.registrations).toEqual([]);

    await harness.cleanup();
  });

  test("returns without a register when the query times out", async () => {
    // The child never answers; the watchdog fires and the driver returns.
    const harness = await setup({
      initialReply: [],
      parkedQueryWatchdogMs: 50,
    });
    harness.nextReply.current = null;

    await harness.supervisor.reEmitParkedCorrelations();

    expect(harness.registrations).toEqual([]);

    await harness.cleanup();
  });

  test("returns without a register when the query send fails", async () => {
    // The downstream send throws (a closing pipe). The driver swallows it,
    // registers nothing, and leaves no unhandled rejection.
    const harness = await setup({ initialReply: [] });
    const childA = harness.children[0];
    if (childA === undefined) throw new Error("cohort A missing");
    childA.failWrites = true;
    harness.nextReply.current = [
      {
        runId: "run-x",
        correlationId: "corr-x",
        kind: "approval",
        snapshot: SNAPSHOT,
      },
    ];

    await harness.supervisor.reEmitParkedCorrelations();

    expect(harness.registrations).toEqual([]);

    await harness.cleanup();
  });

  test("an in-flight query settles via sentinel when shutdown aborts it", async () => {
    // Guards the reject->settle refactor: a teardown that aborts an in-flight
    // query must settle it with the null sentinel, not reject it (which would
    // surface as an unhandled rejection since the auto-emit fires concurrently
    // with arbitrary shutdown/recycle).
    const harness = await setup({ initialReply: [] });
    harness.nextReply.current = null; // the child answers no further query

    let settled = false;
    let rejected = false;
    const inflight = harness.supervisor
      .reEmitParkedCorrelations()
      .then(() => {
        settled = true;
      })
      .catch(() => {
        rejected = true;
      });

    await new Promise((r) => setTimeout(r, 20));
    expect(settled).toBe(false); // still pending: proves the query was in flight

    await harness.supervisor.shutdown();

    await Promise.race([
      inflight,
      new Promise((_r, reject) =>
        setTimeout(
          () => reject(new Error("in-flight query hung after shutdown")),
          1000,
        ),
      ),
    ]);

    expect(settled).toBe(true);
    expect(rejected).toBe(false);
    expect(harness.registrations).toEqual([]);

    for (const child of harness.children) {
      child.s2c.close();
      child.c2s.close();
      child.events.close();
    }
  });

  test("no-ops without throwing when the child is not addressable", async () => {
    // Before spawn the supervisor is idle; the driver must skip, not throw, and
    // the spawner must never be invoked.
    const registrations: SuspensionRegistration[] = [];
    const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "reemit-idle-"));
    const supervisor = createWorkflowSupervisor({
      repoStore: createStubRepoStore(baseDir),
      signAsPrincipal: async () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus: wrapHubTransportAsMailBus(createInMemoryTransport()),
      onSuspensionRegister: (registration) => {
        registrations.push(registration);
      },
      subprocessSpawner: () => {
        throw new Error("spawner must not be called for the idle no-op path");
      },
      binaryPath: "/fake/bin/workflow-child",
      substrateEnv: {},
      dynamicSpawnEnv: () => ({}),
      workflowRunRepoId: { kind: "workflow-run", id: DEPLOYMENT_ID },
      workflowRunRef: "refs/heads/main",
      deploymentId: DEPLOYMENT_ID,
      stepCount: 1,
      deploymentMailAddress: AGENT_ADDRESS,
      readPrincipal: { kind: "supervisor" },
      deriveStepAddress: () => AGENT_ADDRESS,
      deriveStepRepoId: () => ({ kind: "agent-state", id: DEPLOYMENT_ID }),
      inboxPrimitives: createNoopInboxPrimitives(),
    });

    await supervisor.reEmitParkedCorrelations();

    expect(registrations).toEqual([]);
    await fs.rm(baseDir, { recursive: true, force: true });
  });
});
