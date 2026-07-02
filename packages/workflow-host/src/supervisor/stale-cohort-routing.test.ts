// H-S2 routing pinch-point verification.
//
// The supervisor's `pumpUpstreamControl` resolves the broadcaster
// DYNAMICALLY on every `terminal.event` via `activeTerminalBroadcaster()`,
// which returns whatever broadcaster is currently in `state`. During a
// recycle, the supervisor mints a new broadcaster on the new cohort
// before the OLD pump's iterator finishes draining. A `terminal.event`
// frame the OLD child emitted before kill landed sits in the OLD
// iterator's buffer; when the OLD pump eventually consumes it, the
// dynamic resolution returns the NEW cohort's broadcaster, and the
// frame is fan-out to the NEW cohort's listeners.
//
// The realistic scenario:
//   - Run M is in-flight on cohort A.
//   - Cohort A crashes mid-run before markConsumed → recycle.
//   - The processing/ entry for M is replayed back to inbox/.
//   - Cohort B dispatches M as runId = M. Cohort B subscribes its
//     broadcaster for runId M.
//   - Meanwhile, cohort A's iterator still has a buffered
//     `terminal.event` for runId M from the previous incarnation.
//   - The OLD pump dequeues the buffered frame, sees that the cohort A
//     iterator hasn't terminated yet, and calls
//     activeTerminalBroadcaster().notify("M", staleEvent) — which
//     routes to COHORT B's broadcaster, settling B's listener early.
//   - Cohort B's dispatch loop falsely believes the run has completed
//     and prematurely calls markConsumed on M, while the run is still
//     running on cohort B.

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
            if (next === undefined) throw new Error("undef");
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

function createMockMailBus() {
  const registered: string[] = [];
  const subs = new Map<string, Set<(b: Uint8Array) => void>>();
  return {
    registerAddress(a: string) {
      registered.push(a);
    },
    unregisterAddress(a: string) {
      const i = registered.lastIndexOf(a);
      if (i >= 0) registered.splice(i, 1);
      subs.delete(a);
    },
    subscribeMailForAddress(a: string, h: (b: Uint8Array) => void) {
      let s = subs.get(a);
      if (s === undefined) {
        s = new Set();
        subs.set(a, s);
      }
      s.add(h);
      return () => {
        subs.get(a)?.delete(h);
      };
    },
    sendOutbound() {
      throw new Error("sendOutbound not exercised in this test");
    },
    registered(): readonly string[] {
      return registered.slice();
    },
    deliver(a: string, m: Uint8Array) {
      for (const h of subs.get(a) ?? []) h(m);
    },
  } as MailBusBindings & {
    registered(): readonly string[];
    deliver(a: string, m: Uint8Array): void;
  };
}

function createStubRepoStore(baseDir: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix() {
      return { commitSha: "deadbeefcafef00d", newlyTerminalRuns: [] };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; missing methods surface as a precise failure via the proxy
  return new Proxy(stub as RepoStore, {
    get(t, p, r) {
      const v = Reflect.get(t, p, r);
      if (v !== undefined) return v;
      return () => {
        throw new Error(`stub: ${String(p)}`);
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

function createMemoryInbox(): InboxPrimitives & {
  seedProcessing: (
    address: string,
    messageId: string,
    receivedAt: number,
  ) => void;
} {
  type E = {
    messageId: string;
    receivedAt: number;
    mailAuditRef: { store: string; path: string };
  };
  const st = new Map<
    string,
    {
      inbox: Map<string, E>;
      processing: Map<string, E>;
      consumed: Map<string, E>;
    }
  >();
  function get(a: string) {
    let e = st.get(a);
    if (e === undefined) {
      e = { inbox: new Map(), processing: new Map(), consumed: new Map() };
      st.set(a, e);
    }
    return e;
  }
  function k(r: number, m: string): string {
    return `${String(r)}-${m}`;
  }
  return {
    async enqueueInbox(_s, _p, _r, args) {
      const s = get(args.address);
      const key = k(args.receivedAt, args.messageId);
      const env: E = {
        messageId: args.messageId,
        receivedAt: args.receivedAt,
        mailAuditRef: args.mailAuditRef,
      };
      s.inbox.set(key, env);
      return {
        commitSha: "memory",
        inboxKey: key,
        envelope: {
          messageId: args.messageId,
          receivedAt: args.receivedAt,
          address: args.address,
          mailAuditRef: args.mailAuditRef,
        },
      };
    },
    async dequeueToProcessing(_s, _p, _r, address) {
      const s = get(address);
      const sorted = [...s.inbox.entries()].sort(
        ([, a], [, b]) => a.receivedAt - b.receivedAt,
      );
      const head = sorted[0];
      if (head === undefined) return null;
      const [key, env] = head;
      s.inbox.delete(key);
      s.processing.set(key, env);
      return {
        commitSha: "memory",
        key,
        envelope: {
          messageId: env.messageId,
          receivedAt: env.receivedAt,
          address,
          mailAuditRef: env.mailAuditRef,
        },
      };
    },
    async markConsumed(_s, _p, _r, args) {
      const s = get(args.address);
      for (const [key, value] of s.processing) {
        if (value.messageId === args.messageId) {
          s.processing.delete(key);
          s.consumed.set(args.messageId, value);
          return {
            commitSha: "memory",
            envelope: {
              messageId: value.messageId,
              receivedAt: value.receivedAt,
              address: args.address,
              runId: args.runId,
              consumedAt: args.consumedAt,
              mailAuditRef: value.mailAuditRef,
            },
            watermark: 0,
            prunedMessageIds: [],
          };
        }
      }
      throw new Error("processing not found");
    },
    async replayProcessingToInbox(_s, _p, _r, address) {
      const s = get(address);
      for (const [key, value] of s.processing) {
        s.inbox.set(key, value);
      }
      s.processing.clear();
      return { commitSha: "memory", replayedKeys: [] };
    },
    seedProcessing(address, messageId, receivedAt) {
      const s = get(address);
      const key = k(receivedAt, messageId);
      s.processing.set(key, {
        messageId,
        receivedAt,
        mailAuditRef: { store: "memory", path: messageId },
      });
    },
  };
}

type FakeChild = {
  pid: number;
  channelId: string | undefined;
  s2c: ReturnType<typeof createMemoryNdjsonStream>;
  c2s: ReturnType<typeof createMemoryNdjsonStream>;
  events: ReturnType<typeof createMemoryFrameStream>;
  resolveExit: ((c: number) => void) | undefined;
  exited: Promise<number>;
  killSignals: string[];
  closeOnKill: boolean;
};

function createSpawnTracker() {
  const children: FakeChild[] = [];
  const spawner: SubprocessSpawner = ({ env }) => {
    const s2c = createMemoryNdjsonStream();
    const c2s = createMemoryNdjsonStream();
    const events = createMemoryFrameStream();
    let resolveExit: ((c: number) => void) | undefined;
    const exited = new Promise<number>((r) => {
      resolveExit = r;
    });
    const child: FakeChild = {
      pid: 4000 + children.length,
      channelId: env.IPC_CHANNEL_ID,
      s2c,
      c2s,
      events,
      resolveExit,
      exited,
      killSignals: [],
      closeOnKill: true,
    };
    children.push(child);
    const handle: SubprocessHandle = {
      pid: child.pid,
      controlWriter: s2c.writer,
      controlReader: c2s.reader,
      eventReader: events.reader,
      kill: (signal) => {
        const sig = typeof signal === "string" ? signal : String(signal ?? "");
        child.killSignals.push(sig);
        if (child.closeOnKill) {
          events.close();
          c2s.close();
        }
        child.resolveExit?.(0);
      },
      exited,
    };
    return handle;
  };
  return { spawner, children };
}

async function driveReady(
  child: FakeChild,
  ipcKp: { privateKey: Uint8Array; publicKey: Uint8Array },
) {
  if (child.channelId === undefined) throw new Error("no channelId");
  const sender = createControlChannelSender({
    privateKeySeed: ipcKp.privateKey,
    channelId: child.channelId,
    writer: {
      write(line: string) {
        child.c2s.inject(line);
      },
    },
  });
  await sender.send({
    type: "ready",
    data: {
      childPid: child.pid,
      childPublicKey: hexEncode(ipcKp.publicKey),
    },
  });
  return sender;
}

describe("H-S2 stale-cohort routing pinch-point", () => {
  test("a terminal.event from OLD child for a runId being dispatched on NEW cohort leaks across cohorts via dynamic activeTerminalBroadcaster() resolution", async () => {
    const baseDir = await makeTempDir("hs2-leak-");
    const ipcKp = await generateKeyPair();
    const mailBus = createMockMailBus();
    const tracker = createSpawnTracker();
    const inbox = createMemoryInbox();

    await seedStepGrants(
      baseDir,
      defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
      [{ resource: "thing", action: "read" }],
    );

    const consumedRecord: string[] = [];
    const wrappedInbox: InboxPrimitives = {
      ...inbox,
      async markConsumed(s, p, r, args) {
        consumedRecord.push(args.messageId);
        return inbox.markConsumed(s, p, r, args);
      },
    };

    const bindings: WorkflowSupervisorBindings = {
      repoStore: createStubRepoStore(baseDir),
      signAsPrincipal: async (): Promise<SignedPayload> => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
      subprocessSpawner: tracker.spawner,
      binaryPath: "/fake/bin",
      substrateEnv: { DATA_DIR: baseDir },
      workflowRunRepoId: { kind: "workflow-run", id: "deployment-x" },
      workflowRunRef: "refs/heads/main",
      deploymentId: "deployment-x",
      stepCount: 1,
      deploymentMailAddress: "deployment-x@example.com",
      readPrincipal: { kind: "supervisor" },
      deriveStepAddress: ({ deploymentId, stepId }) =>
        `${deploymentId}-${stepId}@example.com`,
      trivialLaunch: () => {
        throw new Error("not used");
      },
      ipcKeyPairFactory: () => Promise.resolve(ipcKp),
      inboxPrimitives: wrappedInbox,
    };

    const supervisor = createWorkflowSupervisor(bindings);
    const spawnP = supervisor.spawn({
      stepOrder: ["step-1"],
      definitionHash: "def-hash",
      warmKeep: false,
      onInferenceEvent: () => undefined,
    });
    while (tracker.children.length === 0)
      await new Promise((r) => setTimeout(r, 1));
    const childA = tracker.children[0];
    if (childA === undefined) throw new Error("tracker.children[0] missing");
    // Disable closeOnKill on cohort A so we can inject stale frames AFTER kill.
    childA.closeOnKill = false;
    const senderA = await driveReady(childA, ipcKp);
    await spawnP;

    // Begin recycle; cohort A's iterator does NOT terminate on kill in
    // this test (we want to keep it alive so the OLD pump can still
    // dequeue frames after the recycle returns).
    const recycleP = supervisor.recycle({ reason: "h-s2-probe" });
    while (tracker.children.length < 2)
      await new Promise((r) => setTimeout(r, 1));
    const childB = tracker.children[1];
    if (childB === undefined) throw new Error("tracker.children[1] missing");
    await driveReady(childB, ipcKp);
    await recycleP;

    // The recycle finished. State is now `running` with cohort B's
    // broadcaster. Cohort A's pump is STILL ALIVE (we left c2s open)
    // and waiting on the iterator.
    //
    // Deliver a fresh mail to cohort B and let it land in the inbox.
    // The runtime body of cohort B has NOT processed it yet; the
    // dispatch loop will dequeue it, subscribe cohort B's broadcaster
    // for the runId, then forward trigger.fire. At that point, cohort
    // B's broadcaster has a listener for that runId.
    const TEST_MESSAGE =
      "Message-ID: <stale-routing-probe@example.com>\r\n\r\nbody";
    const TEST_MESSAGE_ID = "<stale-routing-probe@example.com>";
    mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode(TEST_MESSAGE),
    );

    // Wait until cohort B has forwarded the trigger.fire (which means
    // the cohort B broadcaster has a listener for runId TEST_MESSAGE_ID).
    const deadline = Date.now() + 1_000;
    while (Date.now() < deadline) {
      const flushed = childB.s2c.flushed();
      if (flushed.some((f) => f.includes("trigger.fire"))) break;
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(childB.s2c.flushed().some((f) => f.includes("trigger.fire"))).toBe(
      true,
    );

    // Now inject a STALE `terminal.event` for that runId from the OLD
    // child (cohort A). The OLD pump (still alive on A's iterator)
    // will pick it up. Inside `pumpUpstreamControl`, the code resolves
    // `activeTerminalBroadcaster()` — which returns cohort B's
    // broadcaster — and routes the notify there.
    await senderA.send({
      type: "terminal.event",
      data: {
        runId: TEST_MESSAGE_ID,
        kind: "RunCompleted",
        seq: 0,
        at: "stale-from-cohort-A",
      },
    });

    // Wait for the OLD pump to drain the stale frame.
    await new Promise((r) => setTimeout(r, 50));

    // If H-S2 is real, cohort B's dispatch loop will have observed the
    // stale terminal event, exited waitForRunTerminal, and called
    // markConsumed for TEST_MESSAGE_ID — even though cohort B never
    // actually completed the run on the child side.
    //
    // If H-S2 is mitigated, the cohort B dispatch loop is STILL waiting
    // for the real terminal.event from cohort B's child.
    expect(consumedRecord).not.toContain(TEST_MESSAGE_ID);

    // Cleanup: settle the run legitimately so shutdown is clean.
    if (childB.channelId === undefined)
      throw new Error("childB.channelId missing");
    const senderB = createControlChannelSender({
      privateKeySeed: ipcKp.privateKey,
      channelId: childB.channelId,
      writer: {
        write(line: string) {
          childB.c2s.inject(line);
        },
      },
    });
    await senderB.send({
      type: "terminal.event",
      data: {
        runId: TEST_MESSAGE_ID,
        kind: "RunCompleted",
        seq: 0,
        at: "real-from-cohort-B",
      },
    });
    childA.c2s.close();
    await supervisor.shutdown();
  });
});
