// Supervisor `reEmitParkedCorrelations` driver.
//
// On a re-establishment the supervisor queries the child for its currently-
// parked correlations (`parked-correlations.request`) and re-registers each
// through `onSuspensionRegister` -- recovering a `park.notify` register the hub
// may have missed while it was down at suspend. The driver is best-effort: it
// no-ops when the child is not addressable, and a query that times out is
// dropped for the next re-establishment to re-drive.

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

interface Harness {
  supervisor: WorkflowSupervisor;
  registrations: SuspensionRegistration[];
  cleanup: () => Promise<void>;
}

/**
 * Spawn a supervisor wired to a mock child. The child answers a
 * `parked-correlations.request` with `parked` when it is an array, or ignores
 * the request (to exercise the watchdog) when it is `null`.
 */
async function setup(opts: {
  parked: ParkedEntry[] | null;
  parkedQueryWatchdogMs?: number;
  failSendAfterReady?: boolean;
}): Promise<Harness> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "reemit-parked-"));
  const hostTransport = createInMemoryTransport();
  const agentKeyPair = await generateKeyPair();
  hostTransport.register(AGENT_ADDRESS, createEd25519Crypto(agentKeyPair));
  const mailBus = wrapHubTransportAsMailBus(hostTransport);

  const supervisorIpcKeyPair = await generateKeyPair();
  const childIpcKeyPair = await generateKeyPair();

  const supervisorToChild = createMemoryNdjsonStream();
  const childToSupervisor = createMemoryNdjsonStream();
  const eventChildToSupervisor = createMemoryFrameStream();
  let resolveExit: ((code: number) => void) | undefined;
  const exited = new Promise<number>((resolve) => {
    resolveExit = resolve;
  });

  const registrations: SuspensionRegistration[] = [];
  let observedEnv: Record<string, string> | undefined;

  // A downstream writer that starts failing once armed, so the driver's
  // `parked-correlations.request` send rejects. Left disarmed through spawn so
  // the credentials push and `ready` handshake succeed first.
  let failWrites = false;
  const guardedWriter: NdjsonWriter = {
    write(line: string) {
      if (failWrites) throw new Error("controlWriter send boom");
      return supervisorToChild.writer.write(line);
    },
  };

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
    subprocessSpawner: ({ env }) => {
      observedEnv = env;
      return {
        pid: 9300,
        controlWriter: guardedWriter,
        controlReader: childToSupervisor.reader,
        eventReader: eventChildToSupervisor.reader,
        kill: () => {
          childToSupervisor.close();
          eventChildToSupervisor.close();
          resolveExit?.(0);
        },
        exited,
      };
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
    ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
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

  while (observedEnv === undefined) {
    await new Promise((r) => setTimeout(r, 1));
  }
  const channelId = observedEnv.IPC_CHANNEL_ID;
  if (channelId === undefined) throw new Error("IPC_CHANNEL_ID missing");

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
      childPid: 9300,
      childPublicKey: hexEncode(childIpcKeyPair.publicKey),
    },
  });
  await spawnPromise;
  // Arm the write failure only now that spawn's own downstream frames landed.
  if (opts.failSendAfterReady === true) failWrites = true;

  // The mock child: drain the supervisor's downstream frames and answer a
  // parked-correlations query. Verifies the supervisor's real signed frames
  // through the production receiver.
  const childReceiver = receiveControlChannel({
    publicKey: supervisorIpcKeyPair.publicKey,
    channelId,
    reader: supervisorToChild.reader,
    onCrash: (reason) => {
      throw new Error(`mock child control receiver crashed: ${reason}`);
    },
  });
  const childLoop = (async () => {
    for await (const payload of childReceiver) {
      if (payload.type !== "parked-correlations.request") continue;
      if (opts.parked === null) continue; // exercise the watchdog: never reply
      const response: ControlPayload = {
        type: "parked-correlations.response",
        data: { requestId: payload.data.requestId, parked: opts.parked },
      };
      await childSender.send(response);
    }
  })();

  return {
    supervisor,
    registrations,
    cleanup: async () => {
      await supervisor.shutdown();
      supervisorToChild.close();
      await childLoop.catch(() => undefined);
      await fs.rm(baseDir, { recursive: true, force: true });
    },
  };
}

describe("supervisor reEmitParkedCorrelations", () => {
  test("re-registers every parked correlation the child reports", async () => {
    const harness = await setup({
      parked: [
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
      ],
    });

    await harness.supervisor.reEmitParkedCorrelations();

    // Each parked correlation is re-registered with the deployment identity
    // stamped on and its snapshot forwarded.
    expect(harness.registrations).toEqual([
      {
        runId: "run-a",
        correlationId: "corr-a",
        kind: "approval",
        deploymentId: DEPLOYMENT_ID,
        agentAddress: AGENT_ADDRESS,
        approvalSnapshot: SNAPSHOT,
      },
      {
        runId: "run-b",
        correlationId: "corr-b",
        kind: "approval",
        deploymentId: DEPLOYMENT_ID,
        agentAddress: AGENT_ADDRESS,
        approvalSnapshot: SNAPSHOT,
      },
    ]);

    await harness.cleanup();
  });

  test("re-registers nothing when the child reports no parked correlations", async () => {
    const harness = await setup({ parked: [] });

    await harness.supervisor.reEmitParkedCorrelations();

    expect(harness.registrations).toEqual([]);

    await harness.cleanup();
  });

  test("returns without a register when the query times out", async () => {
    // The child never answers; the watchdog fires and the driver returns.
    const harness = await setup({ parked: null, parkedQueryWatchdogMs: 50 });

    await harness.supervisor.reEmitParkedCorrelations();

    expect(harness.registrations).toEqual([]);

    await harness.cleanup();
  });

  test("returns without a register when the query send fails", async () => {
    // The downstream send throws (a closing pipe). The driver swallows it,
    // registers nothing, and leaves no unhandled rejection; the next
    // re-establishment re-drives.
    const harness = await setup({
      parked: [
        {
          runId: "run-a",
          correlationId: "corr-a",
          kind: "approval",
          snapshot: SNAPSHOT,
        },
      ],
      failSendAfterReady: true,
    });

    await harness.supervisor.reEmitParkedCorrelations();

    expect(harness.registrations).toEqual([]);

    await harness.cleanup();
  });

  test("no-ops without throwing when the child is not addressable", async () => {
    // Before spawn the supervisor is idle; the driver must skip, not throw.
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
