// Supervisor-level coverage for the substrate.write IPC layer.
//
// Four review concerns are pinned here:
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
//      a principal it shouldn't have or a anchorRunId that doesn't
//      match the supervisor's. The supervisor's handler intentionally
//      ignores any principal/anchorRunId the child might smuggle in:
//      the principal is constructed by the supervisor at write time
//      from `bindings.anchorRunId`. Pin that override so a future
//      change does not quietly let the child influence the on-disk
//      audit subject.
//
//   3. A repoId.kind other than `workflow-run` -- the only kind the
//      child's proxy is supposed to forward through this IPC -- must
//      be rejected at the handler boundary. Pin that here.
//
//   4. The supervisor's crash handler must interpolate the crash reason
//      into its log record. A malformed substrate.write.request reaches
//      the crash handler on the live cohort; a non-JSON line during the
//      pre-ready handshake reaches it in a non-running phase. Both log
//      the reason, and both are pinned here because this file already
//      owns the harness that drives a child through the IPC.

import {
  describe,
  test,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
  afterEach,
} from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import type { NewlyTerminalRun, RepoId, RepoStore } from "@intx/hub-sessions";
import {
  createLogCapture,
  createMemoryFrameStream,
  createMemoryNdjsonStream,
  createSupervisorReaper,
  createMockMailBus,
  waitForUpstreamPayload,
} from "@intx/workflow-host/testing";

import {
  createWorkflowSupervisor,
  type InboxPrimitives,
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

type WriteCapture = {
  principal: { kind: string; anchorRunId?: string };
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
        principal: principal as { kind: string; anchorRunId?: string },
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
        outcome: "enqueued",
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

type BootSupervisorOpts = {
  prefix: string;
  inboxOpts?: Parameters<typeof createMemoryInboxPrimitives>[0];
  onWriteAttempt?: (cap: WriteCapture) => void;
  /**
   * The merge callback the substrate runs is the supervisor's
   * IPC-bridging closure; set this to `false` so the stub does not
   * pre-invoke the merge callback synchronously with an empty existing
   * map. Tests that exercise the merge round-trip set this to true.
   */
  invokeMerge?: boolean;
  onSelfTerminate?: (info: {
    phase: "stopped" | "crash-looping";
    reason: string;
  }) => void;
};
const supervisors = createSupervisorReaper();

afterEach(supervisors.reap);

// The supervisor after spawn but before the child's `ready` frame: the
// receiver is already draining the child's control stream, yet the phase
// is still `starting`. Tests that need to observe a pre-ready crash inject
// into `childToSupervisor` and await `spawnPromise`; `sendReady` completes
// the handshake for the running-phase harness below.
type SupervisorSeam = Omit<SupervisorHarness, "spawnResult"> & {
  spawnPromise: Promise<{ pid: number; channelId: string }>;
  sendReady: () => Promise<void>;
};
async function bootSupervisorToReady(
  opts: BootSupervisorOpts,
): Promise<SupervisorSeam> {
  const baseDir = await makeTempDir(opts.prefix);
  await seedStepGrants(
    baseDir,
    defaultStepRepoId({ runId: "deployment-x", stepId: "step-1" }),
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
  // The spawner being called IS the event the boot helper waits for, so it
  // reports the env through this rather than leaving the helper to re-read a
  // mutable binding on a timer until it is populated. A respawn calls the
  // spawner again, so only the first call settles this.
  const spawned = Promise.withResolvers<Record<string, string>>();
  const spawner: SubprocessSpawner = ({ env }) => {
    spawned.resolve(env);
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
    anchorRunId: "deployment-x",
    stepCount: 1,
    deploymentMailAddress: "deployment-x@example.com",
    readPrincipal: { kind: "supervisor" },
    deriveStepAddress: ({ runId, stepId }) => `${runId}-${stepId}@example.com`,
    ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
    inboxPrimitives,
    ...(opts.onSelfTerminate !== undefined
      ? { onSelfTerminate: opts.onSelfTerminate }
      : {}),
  };

  const supervisor = createWorkflowSupervisor(bindings);
  supervisors.track(supervisor);
  const spawnPromise = supervisor.spawn({
    stepOrder: ["step-1"],
    definitionHash: "def-hash-abc",
    warmKeep: false,

    onInferenceEvent: () => {
      /* unused */
    },
  });
  const spawnEnv = await spawned.promise;
  const channelId = spawnEnv.IPC_CHANNEL_ID;
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
  await mailBus.awaitRegistered("deployment-x@example.com");
  const sendReady = (): Promise<void> =>
    childSender.send({
      type: "ready",
      data: {
        childPid: 7777,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
  return {
    supervisor,
    channelId,
    childToSupervisor,
    supervisorToChild,
    childSender,
    mailBus,
    inboxPrimitives,
    bindings,
    spawnPromise,
    sendReady,
  };
}

async function bootSupervisor(
  opts: BootSupervisorOpts,
): Promise<SupervisorHarness> {
  const seam = await bootSupervisorToReady(opts);
  await seam.sendReady();
  const spawnResult = await seam.spawnPromise;
  return {
    supervisor: seam.supervisor,
    channelId: seam.channelId,
    childToSupervisor: seam.childToSupervisor,
    supervisorToChild: seam.supervisorToChild,
    childSender: seam.childSender,
    mailBus: seam.mailBus,
    inboxPrimitives: seam.inboxPrimitives,
    bindings: seam.bindings,
    spawnResult,
  };
}

describe("substrate-write authz: supervisor overrides the child's claim", () => {
  test("the supervisor presents its bindings-pinned workflow-process principal regardless of what the child claims", async () => {
    // The substrate.write.request wire frame does not carry a
    // principal field (the supervisor's handler constructs the
    // workflow-process principal from `bindings.anchorRunId` at write
    // time). Pin that override here by observing the principal the
    // supervisor presents to the substrate's writeTreePreservingPrefix.
    const writes: WriteCapture[] = [];
    // The capture callback is the signal that a write reached the substrate;
    // the test previously inferred it by re-reading `writes.length` on a
    // timer until a deadline expired.
    const firstWrite = Promise.withResolvers<boolean>();
    const harness = await bootSupervisor({
      prefix: "authz-override-",
      onWriteAttempt: (cap) => {
        writes.push(cap);
        firstWrite.resolve(true);
      },
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
    await waitForUpstreamPayload(
      harness.supervisorToChild,
      "substrate.merge.request",
      (m) => m.data.requestId === requestId,
    );
    await harness.childSender.send({
      type: "substrate.merge.response",
      data: {
        requestId,
        result: { ok: true, files: [] },
      },
    });
    await firstWrite.promise;
    expect(writes.length).toBeGreaterThanOrEqual(1);
    const first = writes[0];
    if (first === undefined) throw new Error("no write captured");
    // The supervisor's handler constructs the principal as
    // `{ kind: "workflow-process", anchorRunId: bindings.anchorRunId }`.
    // The on-disk audit subject is therefore pinned to the deployment
    // the supervisor was bound to at construction; the child has no
    // way to influence it through the IPC.
    expect(first.principal.kind).toBe("workflow-process");
    expect(first.principal.anchorRunId).toBe("deployment-x");

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
    const matchedRejection = await waitForUpstreamPayload(
      harness.supervisorToChild,
      "substrate.write.response",
      (r) => r.data.requestId === requestId,
    );
    const rejection: {
      requestId: string;
      result: { ok: boolean; reason?: string };
    } = {
      requestId: matchedRejection.data.requestId,
      result: matchedRejection.data.result,
    };
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
  test("a substrate.write.request mid-merge is rejected with cohort abort reason when the supervisor shuts down", async () => {
    // The HIGH cleanup the supervisor commits when a cohort tears down:
    // every pending merge round-trip registered against the dying cohort
    // must resolve with a failure so handler closures awaiting them do
    // not leak past the shutdown. Pin the observable result here by
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
    // Reaching the next line is the merge request having arrived, so
    // the flag and its assertion have nothing left to add.
    await waitForUpstreamPayload(
      harness.supervisorToChild,
      "substrate.merge.request",
      (m) => m.data.requestId === requestId,
    );

    // Issue shutdown. The HIGH cleanup runs inside `shutdownInternal`
    // after the cohort abort; it iterates `pendingMerges` and resolves
    // each entry with `{ ok: false, reason: "cohort aborted: ..." }`.
    // The handler's `await result` returns the failure, the
    // try/catch's `cause` path runs, and the substrate.write.response
    // lands on the supervisor-to-child stream with the abort reason.
    const shutdownPromise = harness.supervisor.shutdown();

    // Waiting for the response rather than for five seconds to pass. The
    // explicit null check this replaces was the one deadline in the file that
    // failed loudly, but "in time" was still the thing it measured; a
    // response that never comes now ends the run at the lane timeout.
    const abortResponse = await waitForUpstreamPayload(
      harness.supervisorToChild,
      "substrate.write.response",
      (r) => r.data.requestId === requestId,
    );
    const abortResult = abortResponse.data.result;
    if (abortResult.ok) {
      throw new Error("expected the cohort-aborted write to fail");
    }
    expect(abortResult.reason).toMatch(/cohort aborted/);

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
    // Reaching the next line is the merge request having arrived, so
    // the flag and its assertion have nothing left to add.
    await waitForUpstreamPayload(
      harness.supervisorToChild,
      "substrate.merge.request",
      (m) => m.data.requestId === badRequestId,
    );
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

    const matchedBadResponse = await waitForUpstreamPayload(
      harness.supervisorToChild,
      "substrate.write.response",
      (r) => r.data.requestId === badRequestId,
    );
    const badResponse: {
      requestId: string;
      result: { ok: boolean; reason?: string };
    } = {
      requestId: matchedBadResponse.data.requestId,
      result: matchedBadResponse.data.result,
    };
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
    // Reaching the next line is the merge request having arrived, so
    // the flag and its assertion have nothing left to add.
    await waitForUpstreamPayload(
      harness.supervisorToChild,
      "substrate.merge.request",
      (m) => m.data.requestId === goodRequestId,
    );
    await harness.childSender.send({
      type: "substrate.merge.response",
      data: {
        requestId: goodRequestId,
        result: { ok: true, files: [] },
      },
    });
    const matchedGoodResponse = await waitForUpstreamPayload(
      harness.supervisorToChild,
      "substrate.write.response",
      (r) => r.data.requestId === goodRequestId,
    );
    const goodResponse: {
      requestId: string;
      result: { ok: boolean; reason?: string };
    } = {
      requestId: matchedGoodResponse.data.requestId,
      result: matchedGoodResponse.data.result,
    };
    if (goodResponse === null) {
      throw new Error(
        "upstream pump did not process the subsequent write; it appears to have torn down",
      );
    }
    expect(goodResponse.result.ok).toBe(true);

    await harness.supervisor.shutdown();
  });
});

// Capture LogTape records file-wide so the crash-handler tests can assert
// on the interpolated reason, and so a test can await a record the
// supervisor emits rather than poll for its effect. `configureSync` is
// process-global, so the prior configuration is saved and restored around
// this file.
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

describe("onChildCrash logs the interpolated crash reason", () => {
  test("running-phase crash logs the reason, not a literal placeholder", async () => {
    const harness = await bootSupervisor({ prefix: "crash-running-" });

    // A repoId whose kind is outside RepoKind passes the permissive wire
    // schema (kind: string) but fails the handler's RepoId() domain check,
    // driving the direct onChildCrash call on the live cohort.
    await harness.childSender.send({
      type: "substrate.write.request",
      data: {
        requestId: "crash-req-1",
        repoId: { kind: "bogus-kind", id: "deployment-x" },
        ref: "refs/heads/main",
        preservePrefix: "state/x/",
        message: "malformed repoId",
      },
    });

    await logs.waitForError(
      "forcing child down to respawn: substrate.write.request repoId failed validation",
    );
    expect(logs.errors().some((m) => m.includes("{reason}"))).toBe(false);

    // Cancel the respawn backoff the kill() scheduled.
    await harness.supervisor.shutdown();
  });

  test("pre-ready crash logs the reason, not a literal placeholder", async () => {
    const seam = await bootSupervisorToReady({ prefix: "crash-starting-" });

    // A non-JSON line reaches the receiver before the ready handshake,
    // while the phase is still `starting`, driving onChildCrash's
    // non-running branch (shutdownInternal).
    seam.childToSupervisor.inject("not-json{{{");

    await expect(seam.spawnPromise).rejects.toThrow();

    await logs.waitForError(
      "channel crash: control channel received non-JSON line",
    );
    expect(logs.errors().some((m) => m.includes("{reason}"))).toBe(false);

    // `onChildCrash` launches its teardown fire-and-forget, so returning here
    // would leave it unwinding into the next test, where its records land
    // after that test's `reset` and are indistinguishable from the next
    // test's own. Await this teardown's completion so every record it emits
    // belongs to the test that caused it.
    await logs.waitForRecord(
      'supervisor shutdown complete (control channel received non-JSON line: JSON Parse error: Unexpected identifier "not"',
    );
  });

  test("a pre-ready channel crash does not fire onSelfTerminate", async () => {
    const selfTerminations: {
      phase: "stopped" | "crash-looping";
      reason: string;
    }[] = [];
    const seam = await bootSupervisorToReady({
      prefix: "crash-starting-noselfterm-",
      onSelfTerminate: (info) => selfTerminations.push(info),
    });

    // A non-JSON line before the ready handshake drives onChildCrash while the
    // phase is still `starting`. That is the INITIAL spawn failing -- which the
    // deploy unwind owns -- not a self-termination of a registered deployment
    // the host must reclaim, so the sink must stay silent even though the
    // teardown lands in `stopped`.
    // The leading token is what the parse error quotes into the crash reason,
    // and the reason is what distinguishes one teardown's completion record
    // from another's. A token the sibling test does not use keeps the needle
    // below matched by this test's teardown alone.
    seam.childToSupervisor.inject("noselfterm-json{{{");
    await expect(seam.spawnPromise).rejects.toThrow();

    // The rejection above does not license asserting silence: `onChildCrash`
    // launches its teardown fire-and-forget, so the rejection can arrive
    // while that teardown is still unwinding. Wait for the teardown's own
    // completion record instead. `shutdownInternal` fires the
    // `onSelfTerminate` sink and then logs this line in the same synchronous
    // block, sink first, with no await between them -- so the record is a
    // happens-after: had the sink been going to fire for this teardown, it
    // already did.
    //
    // The needle carries the crash's own reason because the failed spawn
    // handshake runs a second, independent `shutdownInternal` and logs the
    // same line under `spawn failed during startup`. That one is awaited
    // before `spawnPromise` rejects, so a needle matching either would be
    // satisfied by the wrong teardown and barrier nothing.
    await logs.waitForRecord(
      'supervisor shutdown complete (control channel received non-JSON line: JSON Parse error: Unexpected identifier "noselfterm"',
    );

    // The sink fires only when `selfTerminated` is set, and `onChildCrash`
    // derives that from an allowlist admitting `recycling` alone. This crash
    // lands in `starting` -- the INITIAL spawn failing, which the deploy
    // unwind owns, not a self-termination of a registered deployment the host
    // must reclaim. That allowlist is what this test pins.
    expect(selfTerminations).toEqual([]);
  });
});
