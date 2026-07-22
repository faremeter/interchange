// Supervisor-level coverage for the substrate.write IPC layer.
//
// Three review concerns are pinned here:
//
//   1. The terminal-write watchdog timeout. The supervisor holds the
//      `substrate.write.response` back to the child until the
//      dispatch loop's `markConsumed` settles for a matching terminal
//      event. A bug in `markConsumed`, a torn-down cohort, or a
//      stalled inbox primitive would deadlock the child's write, the
//      runtime body, and the dispatch loop itself. The watchdog
//      surfaces such a stall as a structured `{ ok: false, reason }`
//      response rather than an unbounded wait.
//
//   2. Authz under the new IPC. The supervisor executes substrate
//      writes on behalf of the child under
//      `WorkflowRunWorkflowProcessPrincipal`; nothing in the existing
//      tests pinned the supervisor's behaviour when the child claims
//      a principal it shouldn't have or a deploymentId that doesn't
//      match the supervisor's. The supervisor's handler intentionally
//      ignores any principal/deploymentId the child might smuggle in:
//      the principal is constructed by the supervisor at write time
//      from `bindings.deploymentId`. Pin that override so a future
//      change does not quietly let the child influence the on-disk
//      audit subject.
//
//   3. A repoId.kind other than `workflow-run` -- the only kind the
//      child's proxy is supposed to forward through this IPC -- must
//      be rejected at the handler boundary. Pin that here.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto";
import { base64Encode, hexEncode } from "@intx/types";
import type { NewlyTerminalRun, RepoId, RepoStore } from "@intx/hub-sessions";

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
  let done = false;
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
          await new Promise<void>((resolve) => setTimeout(resolve, 1));
        }
      })();
    },
  };
  return {
    reader,
    close() {
      done = true;
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

type WriteCapture = {
  principal: { kind: string; deploymentId?: string };
  repoId: RepoId;
  ref: string;
  preservePrefix: string;
  message: string;
};

// Mirror the workflow-run kind handler's terminal detection for the stub
// substrate: a run is newly terminal when the merge produced a terminal
// event blob under its `events/` prefix. The real handler scopes this to
// events newly added against the prior tree; the stub's merge always runs
// against an empty prior, so every terminal event it emits is new.
const STUB_TERMINAL_EVENT_STATUS = new Map<
  string,
  "completed" | "failed" | "cancelled"
>([
  ["RunCompleted", "completed"],
  ["RunFailed", "failed"],
  ["RunCancelled", "cancelled"],
]);
const STUB_RUN_EVENT_PATH_RE = /^runs\/([^/]+)\/events\/[0-9]+\.json$/;
function deriveNewlyTerminalRuns(
  merged: Record<string, string | Uint8Array>,
): NewlyTerminalRun[] {
  const out: NewlyTerminalRun[] = [];
  for (const [blobPath, content] of Object.entries(merged)) {
    const match = STUB_RUN_EVENT_PATH_RE.exec(blobPath);
    if (match === null || match[1] === undefined) continue;
    const json =
      typeof content === "string" ? content : new TextDecoder().decode(content);
    let parsed: unknown;
    try {
      parsed = JSON.parse(json);
    } catch {
      continue;
    }
    if (typeof parsed !== "object" || parsed === null || !("type" in parsed)) {
      continue;
    }
    const eventType = (parsed as { type?: unknown }).type;
    if (typeof eventType !== "string") continue;
    const status = STUB_TERMINAL_EVENT_STATUS.get(eventType);
    if (status !== undefined) {
      out.push({ runId: match[1], status, terminalEventJson: json });
    }
  }
  return out;
}

function createStubRepoStore(opts: {
  baseDir: string;
  /**
   * When supplied, every write invokes this callback with the principal
   * and routing the supervisor presented to the substrate. The test
   * captures the principal here so it can pin the override behaviour.
   */
  onWriteAttempt?: (cap: WriteCapture) => void;
  /**
   * When true the merge callback is invoked synchronously with an empty
   * `existing` map; on supervisor-side writes that need to round-trip
   * the child the supervisor's merge callback drives the IPC and the
   * substrate's per-repo lock is held until the response arrives.
   * Default is true (single-shot merge).
   */
  invokeMerge?: boolean;
}): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(opts.baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(principal, repoId, ref, args) {
      opts.onWriteAttempt?.({
        principal: principal as { kind: string; deploymentId?: string },
        repoId,
        ref,
        preservePrefix: args.preservePrefix,
        message: args.message,
      });
      let newlyTerminalRuns: NewlyTerminalRun[] = [];
      if (opts.invokeMerge !== false) {
        const merged = await args.merge(new Map());
        // Stand in for the real workflow-run kind handler's terminal
        // detection: surface any run whose terminal event the merge
        // produced, the way the handler's validation walk would.
        newlyTerminalRuns = deriveNewlyTerminalRuns(merged);
      }
      return { commitSha: "deadbeefcafef00d", newlyTerminalRuns };
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

type MemoryEntry = {
  messageId: string;
  receivedAt: number;
  mailAuditRef: { store: string; path: string };
};

function createMemoryInboxPrimitives(opts: {
  /**
   * When supplied, `markConsumed` awaits this promise before resolving.
   * The watchdog tests inject a never-resolving promise so the
   * dispatch loop's `markConsumed` hangs and the supervisor's
   * watchdog has to surface the deadlock as a structured response.
   */
  blockMarkConsumed?: Promise<void>;
}): InboxPrimitives & { snapshotConsumed(): readonly string[] } {
  const state = new Map<
    string,
    {
      inbox: Map<string, MemoryEntry>;
      processing: Map<string, MemoryEntry>;
      consumed: Map<string, MemoryEntry>;
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
    snapshotConsumed() {
      const out: string[] = [];
      for (const s of state.values()) {
        for (const v of s.consumed.values()) {
          out.push(v.messageId);
        }
      }
      return out;
    },
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
      if (opts.blockMarkConsumed !== undefined) {
        await opts.blockMarkConsumed;
      }
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

type SupervisorHarness = {
  supervisor: ReturnType<typeof createWorkflowSupervisor>;
  channelId: string;
  childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
  supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
  childSender: ReturnType<typeof createControlChannelSender>;
  mailBus: ReturnType<typeof createMockMailBus>;
  inboxPrimitives: ReturnType<typeof createMemoryInboxPrimitives>;
  bindings: WorkflowSupervisorBindings;
  spawnResult: { pid: number; channelId: string };
};

async function bootSupervisor(opts: {
  prefix: string;
  inboxOpts?: Parameters<typeof createMemoryInboxPrimitives>[0];
  onWriteAttempt?: (cap: WriteCapture) => void;
  terminalWriteWatchdogMs?: number;
  /**
   * The merge callback the substrate runs is the supervisor's
   * IPC-bridging closure; set this to `false` so the stub does not
   * pre-invoke the merge callback synchronously with an empty existing
   * map. Tests that exercise the merge round-trip set this to true
   * (default false here for the watchdog test which writes the
   * terminal-event blob directly via the merge body).
   */
  invokeMerge?: boolean;
}): Promise<SupervisorHarness> {
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
  const inboxPrimitives = createMemoryInboxPrimitives(opts.inboxOpts ?? {});
  const repoStore = createStubRepoStore({
    baseDir,
    ...(opts.onWriteAttempt !== undefined
      ? { onWriteAttempt: opts.onWriteAttempt }
      : {}),
    ...(opts.invokeMerge !== undefined
      ? { invokeMerge: opts.invokeMerge }
      : {}),
  });

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
    dynamicSpawnEnv: () => ({}),
    workflowRunRepoId: { kind: "workflow-run", id: "deployment-x" },
    workflowRunRef: "refs/heads/main",
    deploymentId: "deployment-x",
    stepCount: 1,
    deploymentMailAddress: "deployment-x@example.com",
    readPrincipal: { kind: "supervisor" },
    deriveStepAddress: ({ deploymentId, stepId }) =>
      `${deploymentId}-${stepId}@example.com`,
    ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
    inboxPrimitives,
    ...(opts.terminalWriteWatchdogMs !== undefined
      ? { terminalWriteWatchdogMs: opts.terminalWriteWatchdogMs }
      : {}),
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
  while (!mailBus.registered().includes("deployment-x@example.com")) {
    await new Promise((r) => setTimeout(r, 1));
  }
  await childSender.send({
    type: "ready",
    data: {
      childPid: 7777,
      childPublicKey: hexEncode(childIpcKeyPair.publicKey),
    },
  });
  const spawnResult = await spawnPromise;
  return {
    supervisor,
    channelId,
    childToSupervisor,
    supervisorToChild,
    childSender,
    mailBus,
    inboxPrimitives,
    bindings,
    spawnResult,
  };
}

/**
 * Parse every signed envelope on the supervisor-to-child stream that
 * decodes to a payload of the supplied type. Returns the matching
 * payloads in arrival order. Skips frames that fail envelope/payload
 * validation so a test asserting on a specific payload kind is not
 * fooled by an unrelated frame (e.g. an unrelated `grants-updated`).
 */
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
    // The `Extract` narrow above pins T to a known discriminator; the
    // payload's runtime type matches by construction.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- arktype narrow above pins the discriminator; the cast walks the union to the matching branch
    out.push(payload as Extract<typeof ControlPayload.infer, { type: T }>);
  }
  return out;
}

describe("substrate-write watchdog", () => {
  test("a stalled markConsumed forces a structured failure response within the watchdog window", async () => {
    // Inject a never-resolving `markConsumed` so the dispatch loop's
    // post-terminal step hangs forever. The watchdog inside
    // `synchronouslyDispatchTerminalWrite` has to surface that stall
    // as a `{ ok: false, reason: "terminal-write watchdog timeout: ..." }`
    // response within the configured window rather than wedging.
    const blockMarkConsumed = new Promise<void>(() => {
      /* never resolve */
    });
    const harness = await bootSupervisor({
      prefix: "watchdog-",
      inboxOpts: { blockMarkConsumed },
      terminalWriteWatchdogMs: 60,
    });

    // Deliver a mail so the dispatch loop dequeues it, forwards a
    // trigger.fire, and adds the runId to inFlightRuns. The runId
    // equals the messageId in the supervisor's dispatch loop.
    harness.mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("watchdog-msg"),
    );
    // Wait until the supervisor has forwarded the trigger.fire so we
    // can extract the runId it allocated.
    const triggerDeadline = Date.now() + 2_000;
    let runId: string | null = null;
    while (runId === null && Date.now() < triggerDeadline) {
      const triggers = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "trigger.fire",
      );
      if (triggers.length > 0) {
        const first = triggers[0];
        if (first !== undefined) runId = first.data.runId;
      }
      if (runId === null) await new Promise((r) => setTimeout(r, 1));
    }
    if (runId === null) {
      throw new Error("supervisor did not forward trigger.fire in time");
    }

    // Now drive the child's `substrate.write.request` for a terminal
    // event blob targeting that same runId. The supervisor's stub
    // RepoStore invokes the merge round-trip; we satisfy it with a
    // valid `substrate.merge.response` carrying the prospective
    // RunCompleted blob. Once the merge round-trip completes the
    // supervisor enters `synchronouslyDispatchTerminalWrite`, which
    // notifies the broadcaster (the dispatch loop's iterator wakes
    // and calls markConsumed -- but markConsumed is wedged on the
    // injected promise). The watchdog should surface the deadlock
    // within `terminalWriteWatchdogMs`.
    const requestId = "watchdog-req-1";
    await harness.childSender.send({
      type: "substrate.write.request",
      data: {
        requestId,
        repoId: { kind: "workflow-run", id: "deployment-x" },
        ref: "refs/heads/main",
        preservePrefix: `runs/${runId}/events/`,
        message: "child terminal write",
      },
    });
    // Wait for the supervisor's `substrate.merge.request` to land,
    // then reply with the terminal-event blob.
    const mergeDeadline = Date.now() + 2_000;
    let mergeRequestSeen = false;
    while (!mergeRequestSeen && Date.now() < mergeDeadline) {
      const merges = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.merge.request",
      );
      if (merges.some((m) => m.data.requestId === requestId)) {
        mergeRequestSeen = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(mergeRequestSeen).toBe(true);
    const terminalBlob = JSON.stringify({
      type: "RunCompleted",
      seq: 0,
      runId,
      at: "test",
      signature: { principalKind: "workflow-process", sig: "00" },
    });
    await harness.childSender.send({
      type: "substrate.merge.response",
      data: {
        requestId,
        result: {
          ok: true,
          files: [
            {
              path: `runs/${runId}/events/0.json`,
              contentBase64: base64Encode(
                new TextEncoder().encode(terminalBlob),
              ),
            },
          ],
        },
      },
    });

    // The watchdog should surface a structured failure on the
    // downstream control channel within the configured window.
    const responseDeadline = Date.now() + 2_000;
    let watchdogResponse: {
      requestId: string;
      result: { ok: boolean; reason?: string };
    } | null = null;
    while (watchdogResponse === null && Date.now() < responseDeadline) {
      const responses = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.write.response",
      );
      const matched = responses.find((r) => r.data.requestId === requestId);
      if (matched !== undefined) {
        watchdogResponse = {
          requestId: matched.data.requestId,
          result: matched.data.result,
        };
        break;
      }
      await new Promise((r) => setTimeout(r, 5));
    }
    if (watchdogResponse === null) {
      const flushed = harness.supervisorToChild.flushed();
      throw new Error(
        `watchdog did not surface a substrate.write.response in time; flushed lines: ${String(flushed.length)}`,
      );
    }
    expect(watchdogResponse.result.ok).toBe(false);
    expect(watchdogResponse.result.reason).toMatch(
      /terminal-write watchdog timeout/,
    );

    // The dispatch loop is wedged on the injected `blockMarkConsumed`;
    // a normal shutdown would await the loop's settle and hang. The
    // close on `childToSupervisor` from the spawner's `kill` lands the
    // dispatch loop's cohort abort, but the in-flight `markConsumed`
    // awaiter still leaks. The test does not need a clean shutdown
    // here -- the watchdog assertion is the load-bearing observation.
    void harness.supervisor.shutdown();
  });
});

describe("substrate-write authz: supervisor overrides the child's claim", () => {
  test("the supervisor presents its bindings-pinned workflow-process principal regardless of what the child claims", async () => {
    // The substrate.write.request wire frame does not carry a
    // principal field (the supervisor's handler constructs the
    // workflow-process principal from `bindings.deploymentId` at write
    // time). Pin that override here by observing the principal the
    // supervisor presents to the substrate's writeTreePreservingPrefix.
    const writes: WriteCapture[] = [];
    const harness = await bootSupervisor({
      prefix: "authz-override-",
      onWriteAttempt: (cap) => writes.push(cap),
      invokeMerge: true,
    });

    const requestId = "authz-req-1";
    await harness.childSender.send({
      type: "substrate.write.request",
      data: {
        requestId,
        repoId: { kind: "workflow-run", id: "deployment-x" },
        ref: "refs/heads/main",
        preservePrefix: "state/some-step/",
        message: "child write",
      },
    });
    // Wait for the supervisor's substrate write attempt to land on
    // the stub. The substrate's per-repo lock window is held inside
    // the merge round-trip; with `invokeMerge: true` the stub
    // synchronously drives the supervisor's merge closure with an
    // empty existing map, which forwards a `substrate.merge.request`
    // upstream. We satisfy it immediately with an empty file set so
    // the substrate's writeTreePreservingPrefix resolves and the
    // captured principal is observable.
    const mergeDeadline = Date.now() + 2_000;
    while (Date.now() < mergeDeadline) {
      const merges = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.merge.request",
      );
      if (merges.some((m) => m.data.requestId === requestId)) break;
      await new Promise((r) => setTimeout(r, 1));
    }
    await harness.childSender.send({
      type: "substrate.merge.response",
      data: {
        requestId,
        result: { ok: true, files: [] },
      },
    });
    const writeDeadline = Date.now() + 2_000;
    while (writes.length === 0 && Date.now() < writeDeadline) {
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(writes.length).toBeGreaterThanOrEqual(1);
    const first = writes[0];
    if (first === undefined) throw new Error("no write captured");
    // The supervisor's handler constructs the principal as
    // `{ kind: "workflow-process", deploymentId: bindings.deploymentId }`.
    // The on-disk audit subject is therefore pinned to the deployment
    // the supervisor was bound to at construction; the child has no
    // way to influence it through the IPC.
    expect(first.principal.kind).toBe("workflow-process");
    expect(first.principal.deploymentId).toBe("deployment-x");

    await harness.supervisor.shutdown();
  });

  test("a substrate.write.request with a non-workflow-run repoId.kind is rejected", async () => {
    // The supervisor's handler explicitly rejects any write whose
    // repoId.kind is not `workflow-run` -- the only kind the child's
    // proxy is supposed to forward through this IPC. A child that
    // smuggled a `kind: "agent-state"` request through would be
    // attempting to write outside the supervisor's audit boundary;
    // the handler responds with `{ ok: false }` and never reaches the
    // underlying substrate.
    const writes: WriteCapture[] = [];
    const harness = await bootSupervisor({
      prefix: "authz-kind-reject-",
      onWriteAttempt: (cap) => writes.push(cap),
      invokeMerge: false,
    });

    const requestId = "authz-req-2";
    await harness.childSender.send({
      type: "substrate.write.request",
      data: {
        requestId,
        repoId: { kind: "agent-state", id: "deployment-x" },
        ref: "refs/heads/main",
        preservePrefix: "state/x/",
        message: "wrong-kind write",
      },
    });
    const responseDeadline = Date.now() + 2_000;
    let rejection: {
      requestId: string;
      result: { ok: boolean; reason?: string };
    } | null = null;
    while (rejection === null && Date.now() < responseDeadline) {
      const responses = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.write.response",
      );
      const matched = responses.find((r) => r.data.requestId === requestId);
      if (matched !== undefined) {
        rejection = {
          requestId: matched.data.requestId,
          result: matched.data.result,
        };
        break;
      }
      await new Promise((r) => setTimeout(r, 1));
    }
    if (rejection === null) {
      throw new Error(
        "supervisor did not surface a rejection response in time",
      );
    }
    expect(rejection.result.ok).toBe(false);
    expect(rejection.result.reason).toMatch(/workflow-run/);
    expect(writes.length).toBe(0);

    await harness.supervisor.shutdown();
  });
});

describe("substrate-write cohort abort cleanup", () => {
  test("a substrate.write.request mid-merge is rejected through MergeAbortedError when the supervisor shuts down", async () => {
    // The HIGH cleanup the supervisor commits when a cohort tears down:
    // every pending merge round-trip and every markConsumed waiter
    // registered against the dying cohort must reject through
    // `MergeAbortedError` so handler closures awaiting them do not
    // leak past the shutdown. Pin the observable result here by
    // driving a substrate.write.request to mid-merge, then issuing
    // shutdown without sending the matching substrate.merge.response.
    // The supervisor's `substrate.write.response` to the child must
    // surface the abort reason rather than sit forever on the merge
    // resolver the dying control channel will never invoke.
    const harness = await bootSupervisor({
      prefix: "supv-cohort-abort-",
      invokeMerge: true,
    });

    const requestId = "cohort-abort-req-1";
    await harness.childSender.send({
      type: "substrate.write.request",
      data: {
        requestId,
        repoId: { kind: "workflow-run", id: "deployment-x" },
        ref: "refs/heads/main",
        preservePrefix: `runs/some-run/events/`,
        message: "test write that will be aborted",
      },
    });

    // Wait for the supervisor's substrate.merge.request to land in the
    // supervisor-to-child stream, then trigger shutdown without
    // sending the matching response. The merge resolver is what the
    // HIGH cleanup must reject through `rejectCohortAwaiters`.
    const mergeDeadline = Date.now() + 2_000;
    let mergeRequestSeen = false;
    while (!mergeRequestSeen && Date.now() < mergeDeadline) {
      const merges = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.merge.request",
      );
      if (merges.some((m) => m.data.requestId === requestId)) {
        mergeRequestSeen = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(mergeRequestSeen).toBe(true);

    // Issue shutdown. The HIGH cleanup runs inside `shutdownInternal`
    // after the cohort abort; it iterates `pendingMerges` and resolves
    // each entry with `{ ok: false, reason: "cohort aborted: ..." }`.
    // The handler's `await result` returns the failure, the
    // try/catch's `cause` path runs, and the substrate.write.response
    // lands on the supervisor-to-child stream with the abort reason.
    const shutdownPromise = harness.supervisor.shutdown();

    const responseDeadline = Date.now() + 5_000;
    let abortResponse: {
      requestId: string;
      result: { ok: boolean; reason?: string };
    } | null = null;
    while (abortResponse === null && Date.now() < responseDeadline) {
      const responses = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.write.response",
      );
      const matched = responses.find((r) => r.data.requestId === requestId);
      if (matched !== undefined) {
        abortResponse = {
          requestId: matched.data.requestId,
          result: matched.data.result,
        };
        break;
      }
      await new Promise((r) => setTimeout(r, 5));
    }
    if (abortResponse === null) {
      throw new Error(
        "supervisor did not surface a cohort-aborted substrate.write.response in time",
      );
    }
    expect(abortResponse.result.ok).toBe(false);
    expect(abortResponse.result.reason).toMatch(/cohort aborted/);

    await shutdownPromise;
  });

  test("a markConsumed waiter mid-await is rejected through MergeAbortedError when the supervisor shuts down", async () => {
    // Companion to the test above. The HIGH cleanup also covers
    // `markConsumedCompletionWaiters`, which the supervisor populates
    // when a substrate.write.request lands a terminal-event blob: the
    // post-merge `synchronouslyDispatchTerminalWrite` path notifies
    // the broadcaster and then awaits a per-runId completion promise
    // that the dispatch loop's `markConsumed` would normally resolve.
    // If the cohort tears down while a waiter is mid-await, the
    // resolver the dispatch loop would have called never fires; the
    // HIGH cleanup must reject the waiter via MergeAbortedError so
    // the awaiting handler closure unblocks and the
    // substrate.write.response surfaces the abort to the child.
    const blockMarkConsumed = new Promise<void>(() => {
      /* never resolve: lets the markConsumed completion waiter sit
         in `markConsumedCompletionWaiters` for shutdown to find */
    });
    const harness = await bootSupervisor({
      prefix: "supv-cohort-abort-mc-",
      invokeMerge: true,
      inboxOpts: { blockMarkConsumed },
    });

    // Drive a trigger.fire so the supervisor adds the runId to
    // `inFlightRuns` (a precondition for the waiter registration).
    harness.mailBus.deliver(
      "deployment-x@example.com",
      new TextEncoder().encode("cohort-abort-mc-msg"),
    );
    const triggerDeadline = Date.now() + 2_000;
    let runId: string | null = null;
    while (runId === null && Date.now() < triggerDeadline) {
      const triggers = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "trigger.fire",
      );
      if (triggers.length > 0) {
        const first = triggers[0];
        if (first !== undefined) runId = first.data.runId;
      }
      if (runId === null) await new Promise((r) => setTimeout(r, 1));
    }
    if (runId === null) {
      throw new Error("supervisor did not forward trigger.fire in time");
    }

    // Drive a terminal-event write through the substrate-write IPC.
    // The supervisor's `synchronouslyDispatchTerminalWrite` enters the
    // `await completed` line after registering the waiter; the
    // blockMarkConsumed stub guarantees `markConsumed` cannot resolve
    // it, so the await sits in the map for shutdown to find.
    const requestId = "cohort-abort-mc-req-1";
    await harness.childSender.send({
      type: "substrate.write.request",
      data: {
        requestId,
        repoId: { kind: "workflow-run", id: "deployment-x" },
        ref: "refs/heads/main",
        preservePrefix: `runs/${runId}/events/`,
        message: "terminal-event write that will be aborted",
      },
    });
    const mergeDeadline = Date.now() + 2_000;
    let mergeRequestSeen = false;
    while (!mergeRequestSeen && Date.now() < mergeDeadline) {
      const merges = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.merge.request",
      );
      if (merges.some((m) => m.data.requestId === requestId)) {
        mergeRequestSeen = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(mergeRequestSeen).toBe(true);

    // Satisfy the merge round-trip with a real terminal-event blob.
    // The supervisor's post-merge synthesizeTerminalEvent path runs,
    // notifies the broadcaster, and parks on the markConsumed
    // completion waiter (which blockMarkConsumed will never resolve).
    const terminalBlob = JSON.stringify({
      type: "RunCompleted",
      seq: 0,
      runId,
      at: "test",
      signature: { principalKind: "workflow-process", sig: "00" },
    });
    await harness.childSender.send({
      type: "substrate.merge.response",
      data: {
        requestId,
        result: {
          ok: true,
          files: [
            {
              path: `runs/${runId}/events/0.json`,
              contentBase64: base64Encode(
                new TextEncoder().encode(terminalBlob),
              ),
            },
          ],
        },
      },
    });

    // Shutdown. `rejectCohortAwaiters` iterates the waiter map and
    // calls reject(MergeAbortedError) on every entry; the awaiting
    // handler's catch surfaces the rejection as a
    // substrate.write.response with the abort reason.
    const shutdownPromise = harness.supervisor.shutdown();

    const responseDeadline = Date.now() + 5_000;
    let abortResponse: {
      requestId: string;
      result: { ok: boolean; reason?: string };
    } | null = null;
    while (abortResponse === null && Date.now() < responseDeadline) {
      const responses = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.write.response",
      );
      const matched = responses.find((r) => r.data.requestId === requestId);
      if (matched !== undefined) {
        abortResponse = {
          requestId: matched.data.requestId,
          result: matched.data.result,
        };
        break;
      }
      await new Promise((r) => setTimeout(r, 5));
    }
    if (abortResponse === null) {
      throw new Error(
        "supervisor did not surface a cohort-aborted substrate.write.response for the markConsumed waiter in time",
      );
    }
    expect(abortResponse.result.ok).toBe(false);
    expect(abortResponse.result.reason).toMatch(/cohort aborted|markConsumed/);

    await shutdownPromise;
  });
});

describe("substrate-write malformed merge response", () => {
  test("a malformed contentBase64 fails the pending merge without tearing down the upstream pump", async () => {
    // The supervisor decodes the child's `substrate.merge.response`
    // `contentBase64` from inside `pumpUpstreamControl`'s `for await`.
    // A malformed value makes the decoder throw; that throw must be
    // caught and surfaced as a FAILED merge (a structured
    // substrate.write.response) instead of escaping the loop and
    // stopping the supervisor from draining every other upstream
    // control frame for the cohort.
    const harness = await bootSupervisor({
      prefix: "supv-merge-decode-",
      invokeMerge: true,
    });

    // First write: drive its merge round-trip, then answer with a
    // malformed contentBase64.
    const badRequestId = "merge-decode-bad-1";
    await harness.childSender.send({
      type: "substrate.write.request",
      data: {
        requestId: badRequestId,
        repoId: { kind: "workflow-run", id: "deployment-x" },
        ref: "refs/heads/main",
        preservePrefix: "state/some-step/",
        message: "write whose merge response carries malformed base64",
      },
    });
    const badMergeDeadline = Date.now() + 2_000;
    let badMergeSeen = false;
    while (!badMergeSeen && Date.now() < badMergeDeadline) {
      const merges = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.merge.request",
      );
      if (merges.some((m) => m.data.requestId === badRequestId)) {
        badMergeSeen = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(badMergeSeen).toBe(true);
    await harness.childSender.send({
      type: "substrate.merge.response",
      data: {
        requestId: badRequestId,
        result: {
          ok: true,
          files: [
            { path: "state/some-step/x", contentBase64: "@@@not-valid@@@" },
          ],
        },
      },
    });

    const badResponseDeadline = Date.now() + 2_000;
    let badResponse: {
      requestId: string;
      result: { ok: boolean; reason?: string };
    } | null = null;
    while (badResponse === null && Date.now() < badResponseDeadline) {
      const responses = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.write.response",
      );
      const matched = responses.find((r) => r.data.requestId === badRequestId);
      if (matched !== undefined) {
        badResponse = {
          requestId: matched.data.requestId,
          result: matched.data.result,
        };
        break;
      }
      await new Promise((r) => setTimeout(r, 1));
    }
    if (badResponse === null) {
      throw new Error(
        "supervisor did not surface a substrate.write.response for the malformed merge in time",
      );
    }
    expect(badResponse.result.ok).toBe(false);
    expect(badResponse.result.reason).toMatch(/decode failed/);

    // Liveness: a SUBSEQUENT upstream frame must still be processed. If
    // the malformed decode had escaped the `for await`, the pump would
    // be dead and this second write would never receive a merge.request.
    const goodRequestId = "merge-decode-good-1";
    await harness.childSender.send({
      type: "substrate.write.request",
      data: {
        requestId: goodRequestId,
        repoId: { kind: "workflow-run", id: "deployment-x" },
        ref: "refs/heads/main",
        preservePrefix: "state/other-step/",
        message: "subsequent write proving the pump survived",
      },
    });
    const goodMergeDeadline = Date.now() + 2_000;
    let goodMergeSeen = false;
    while (!goodMergeSeen && Date.now() < goodMergeDeadline) {
      const merges = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.merge.request",
      );
      if (merges.some((m) => m.data.requestId === goodRequestId)) {
        goodMergeSeen = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 1));
    }
    expect(goodMergeSeen).toBe(true);
    await harness.childSender.send({
      type: "substrate.merge.response",
      data: {
        requestId: goodRequestId,
        result: { ok: true, files: [] },
      },
    });
    const goodResponseDeadline = Date.now() + 2_000;
    let goodResponse: {
      requestId: string;
      result: { ok: boolean; reason?: string };
    } | null = null;
    while (goodResponse === null && Date.now() < goodResponseDeadline) {
      const responses = readPayloadsOfType(
        harness.supervisorToChild.flushed(),
        "substrate.write.response",
      );
      const matched = responses.find((r) => r.data.requestId === goodRequestId);
      if (matched !== undefined) {
        goodResponse = {
          requestId: matched.data.requestId,
          result: matched.data.result,
        };
        break;
      }
      await new Promise((r) => setTimeout(r, 1));
    }
    if (goodResponse === null) {
      throw new Error(
        "upstream pump did not process the subsequent write; it appears to have torn down",
      );
    }
    expect(goodResponse.result.ok).toBe(true);

    await harness.supervisor.shutdown();
  });
});
