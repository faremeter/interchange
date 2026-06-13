// End-to-end tests for the supervisor↔child workflow-run pack-push
// IPC bridge. Each test drives the supervisor's `spawn` lifecycle
// against a synthetic child that injects `pack.push.request` frames
// upstream and reads the matching `pack.push.response` frames on the
// supervisor-to-child stream. The bindings the supervisor binds against
// the host's `pushWorkflowRunPack` surface are observed as recording
// stubs.
//
// The tests cover three shapes:
//   1. happy path -- the supervisor forwards the validated request to
//      the binding, awaits its resolve, and sends `{ ok: true }` back.
//   2. binding rejection -- the supervisor catches the rejection and
//      sends `{ ok: false, reason }` to the child.
//   3. binding absent -- the supervisor responds with a structured
//      `{ ok: false, reason }` so the child's pending push surfaces a
//      configured-by-construction error rather than hanging.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto-node";
import type { RepoId, RepoStore } from "@intx/hub-sessions";

import {
  createWorkflowSupervisor,
  type MailBusBindings,
  type SubprocessHandle,
  type SubprocessSpawner,
  type WorkflowSupervisorBindings,
} from "./index";
import { defaultStepRepoId, STEP_GRANTS_PATH } from "./credentials";
import {
  createControlChannelSender,
  receiveControlChannel,
  type FrameReader,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";

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
} {
  const registered: string[] = [];
  return {
    registerAddress(address: string) {
      registered.push(address);
    },
    unregisterAddress(address: string) {
      const idx = registered.lastIndexOf(address);
      if (idx >= 0) registered.splice(idx, 1);
    },
    subscribeMailForAddress() {
      return () => {
        /* unused in pack-push tests */
      };
    },
    registered(): readonly string[] {
      return registered.slice();
    },
  };
}

async function makeTempDir(prefix: string): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

function createStubRepoStore(baseDir: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix() {
      return { commitSha: "deadbeefcafef00d" };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- partial stub; only the methods the supervisor reaches into are populated and the proxy below throws on any other call so an untested code path surfaces precisely
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

async function buildBindings(opts: {
  baseDir: string;
  spawner: SubprocessSpawner;
  mailBus: MailBusBindings;
}): Promise<WorkflowSupervisorBindings> {
  return {
    repoStore: createStubRepoStore(opts.baseDir),
    signAsPrincipal: () => ({
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
      throw new Error("trivialLaunch not provided to this test binding");
    },
  };
}

interface DriveSupervisorOpts {
  supervisorIpcKeyPair: { privateKey: Uint8Array; publicKey: Uint8Array };
  childIpcKeyPair: { privateKey: Uint8Array; publicKey: Uint8Array };
  pushWorkflowRunPack?: WorkflowSupervisorBindings["pushWorkflowRunPack"];
}

async function driveSupervisor(opts: DriveSupervisorOpts): Promise<{
  childToSupervisorStream: ReturnType<typeof createMemoryNdjsonStream>;
  supervisorToChildStream: ReturnType<typeof createMemoryNdjsonStream>;
  channelId: string;
  childSender: ReturnType<typeof createControlChannelSender>;
  shutdown: () => Promise<void>;
}> {
  const baseDir = await makeTempDir("pack-push-bridge-");
  await seedStepGrants(
    baseDir,
    defaultStepRepoId({ deploymentId: "deployment-x", stepId: "step-1" }),
    [{ resource: "thing", action: "read" }],
  );
  const supervisorToChild = createMemoryNdjsonStream();
  const childToSupervisor = createMemoryNdjsonStream();
  const eventStream = createMemoryFrameStream();
  let resolveExit: ((code: number) => void) | undefined;
  const exited = new Promise<number>((resolve) => {
    resolveExit = resolve;
  });
  let observedEnv: Record<string, string> | undefined;
  const spawner: SubprocessSpawner = ({ env }) => {
    observedEnv = env;
    const handle: SubprocessHandle = {
      pid: 9001,
      controlWriter: supervisorToChild.writer,
      controlReader: childToSupervisor.reader,
      eventReader: eventStream.reader,
      kill: () => {
        childToSupervisor.close();
        eventStream.close();
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
    mailBus,
  });
  const bindings: WorkflowSupervisorBindings = {
    ...baseBindings,
    ipcKeyPairFactory: () => Promise.resolve(opts.supervisorIpcKeyPair),
    ...(opts.pushWorkflowRunPack !== undefined
      ? { pushWorkflowRunPack: opts.pushWorkflowRunPack }
      : {}),
  };
  const supervisor = createWorkflowSupervisor(bindings);
  const spawnPromise = supervisor.spawn({
    stepOrder: ["step-1"],
    definitionHash: "def-hash-abc",
    onInferenceEvent: () => {
      /* unused */
    },
  });
  while (observedEnv === undefined) {
    await new Promise((r) => setTimeout(r, 1));
  }
  const channelId = observedEnv.IPC_CHANNEL_ID;
  if (channelId === undefined) {
    throw new Error("IPC_CHANNEL_ID missing in spawn env");
  }
  const childSender = createControlChannelSender({
    privateKeySeed: opts.childIpcKeyPair.privateKey,
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
      childPid: 9001,
      childPublicKey: Buffer.from(opts.childIpcKeyPair.publicKey).toString(
        "hex",
      ),
    },
  });
  await spawnPromise;

  return {
    childToSupervisorStream: childToSupervisor,
    supervisorToChildStream: supervisorToChild,
    channelId,
    childSender,
    shutdown: async () => {
      await supervisor.shutdown();
    },
  };
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

const PackPushResponseFrame = type({
  envelope: {
    seq: "number",
    channelId: "string",
    payload: {
      type: "'pack.push.response'",
      data: {
        pushId: "string",
        result: type(
          {
            ok: "true",
          },
          "|",
          {
            ok: "false",
            reason: "string",
          },
        ),
      },
    },
    "+": "ignore",
  },
  "+": "ignore",
});

type PackPushResponseFrame = typeof PackPushResponseFrame.infer;

async function awaitPackPushResponse(
  stream: ReturnType<typeof createMemoryNdjsonStream>,
  maxIters = 200,
): Promise<PackPushResponseFrame["envelope"]["payload"]["data"]> {
  for (let i = 0; i < maxIters; i += 1) {
    const flushed = stream.flushed();
    for (const raw of flushed) {
      const parsed: unknown = JSON.parse(raw);
      const validated = SignedFrame(parsed);
      if (validated instanceof type.errors) continue;
      if (validated.envelope.payload.type !== "pack.push.response") continue;
      const narrowed = PackPushResponseFrame(parsed);
      if (narrowed instanceof type.errors) {
        throw new Error(
          `pack.push.response frame failed narrow validation: ${narrowed.summary}`,
        );
      }
      return narrowed.envelope.payload.data;
    }
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error(
    `did not observe a pack.push.response frame after ${String(maxIters)} polls`,
  );
}

describe("supervisor pack.push.request handler", () => {
  test("forwards a valid request into pushWorkflowRunPack and replies ok=true", async () => {
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();

    const pushCalls: {
      agentAddress: string;
      repoId: RepoId;
      pack: Uint8Array;
      ref: string;
      commitSha: string;
    }[] = [];
    const handle = await driveSupervisor({
      supervisorIpcKeyPair,
      childIpcKeyPair,
      pushWorkflowRunPack: async (opts) => {
        pushCalls.push(opts);
      },
    });

    const childSender = handle.childSender;
    const pack = new Uint8Array([1, 2, 3, 4, 5, 6]);
    await childSender.send({
      type: "pack.push.request",
      data: {
        pushId: "pp-100",
        agentAddress: "deployment-x@example.com",
        repoId: { kind: "workflow-run", id: "deployment-x" },
        ref: "refs/heads/main",
        commitSha: "cafe1234",
        packBase64: Buffer.from(pack).toString("base64"),
      },
    });

    const response = await awaitPackPushResponse(
      handle.supervisorToChildStream,
    );
    expect(response).toEqual({
      pushId: "pp-100",
      result: { ok: true },
    });
    expect(pushCalls).toHaveLength(1);
    const call = pushCalls[0];
    if (call === undefined) throw new Error("no push call captured");
    expect(call.agentAddress).toBe("deployment-x@example.com");
    expect(call.repoId).toEqual({
      kind: "workflow-run",
      id: "deployment-x",
    });
    expect(call.ref).toBe("refs/heads/main");
    expect(call.commitSha).toBe("cafe1234");
    expect(Array.from(call.pack)).toEqual(Array.from(pack));

    await handle.shutdown();
  });

  test("a binding rejection becomes ok=false on the response frame", async () => {
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const handle = await driveSupervisor({
      supervisorIpcKeyPair,
      childIpcKeyPair,
      pushWorkflowRunPack: () =>
        Promise.reject(new Error("hub rejected pack: rate limited")),
    });

    const childSender = handle.childSender;
    await childSender.send({
      type: "pack.push.request",
      data: {
        pushId: "pp-101",
        agentAddress: "deployment-x@example.com",
        repoId: { kind: "workflow-run", id: "deployment-x" },
        ref: "refs/heads/main",
        commitSha: "cafe5678",
        packBase64: Buffer.from([1, 2]).toString("base64"),
      },
    });

    const response = await awaitPackPushResponse(
      handle.supervisorToChildStream,
    );
    expect(response).toEqual({
      pushId: "pp-101",
      result: { ok: false, reason: "hub rejected pack: rate limited" },
    });

    await handle.shutdown();
  });

  test("an absent pushWorkflowRunPack binding replies ok=false with a configured-by-construction reason", async () => {
    const supervisorIpcKeyPair = await generateKeyPair();
    const childIpcKeyPair = await generateKeyPair();
    const handle = await driveSupervisor({
      supervisorIpcKeyPair,
      childIpcKeyPair,
    });

    const childSender = handle.childSender;
    await childSender.send({
      type: "pack.push.request",
      data: {
        pushId: "pp-200",
        agentAddress: "deployment-x@example.com",
        repoId: { kind: "workflow-run", id: "deployment-x" },
        ref: "refs/heads/main",
        commitSha: "cafe9999",
        packBase64: Buffer.from([1]).toString("base64"),
      },
    });

    const response = await awaitPackPushResponse(
      handle.supervisorToChildStream,
    );
    expect(response.pushId).toBe("pp-200");
    expect(response.result.ok).toBe(false);
    // The reason carries the supervisor's structured "not configured"
    // error so the wrap's caller can surface it through the wrapped
    // RepoStore's commit path.
    if (response.result.ok) {
      throw new Error("expected ok=false result");
    }
    expect(response.result.reason).toMatch(
      /pushWorkflowRunPack binding not configured/,
    );

    await handle.shutdown();
  });
});

void receiveControlChannel;
