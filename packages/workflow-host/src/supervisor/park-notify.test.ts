// Supervisor `park.notify` arm.
//
// A workflow-process child that parks a step on a reserved control-plane
// channel forwards a `park.notify` frame up the control channel. The
// supervisor's upstream-control pump stamps the deployment identity it owns
// (`deploymentId` + the deployment's mail address as `agentAddress`) onto the
// child-supplied `runId`/`correlationId`/`kind` and hands the stamped
// registration to the host's `onSuspensionRegister` sink -- the seam the
// sidecar wires to the hub's `signal.correlation.register` frame.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { RepoId, RepoStore } from "@intx/hub-sessions";

import {
  createWorkflowSupervisor,
  type InboxPrimitives,
  type SuspensionRegistration,
} from "./index";
import { wrapHubTransportAsMailBus } from "../mail-bus/index";
import {
  createControlChannelSender,
  type ControlChannelSender,
  type NdjsonReader,
  type NdjsonWriter,
  type FrameReader,
} from "../ipc/index";

const AGENT_ADDRESS = "ins_park-agent@integration.example";
const DEPLOYMENT_ID = "park-dep";

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
 * Minimal `RepoStore` stub: the supervisor's `spawn` consults `getRepoDir`
 * (credentials assembly). No park.notify path touches the substrate, so
 * every other method throws to surface an accidental untested code path.
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
      throw new Error("enqueueInbox not exercised in the park test");
    },
    async dequeueToProcessing() {
      return null;
    },
    async markConsumed() {
      throw new Error("markConsumed not exercised in the park test");
    },
    async replayProcessingToInbox() {
      return { commitSha: "noop", replayedKeys: [] };
    },
  };
}

describe("supervisor park.notify arm", () => {
  test("stamps deploymentId + agentAddress and hands the registration to the sink", async () => {
    const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "park-notify-"));

    const hostTransport = createInMemoryTransport();
    const agentKeyPair = await generateKeyPair();
    // The park path never signs mail; a bare crypto registration is enough
    // for the mail bus to accept the deployment address.
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
          pid: 9200,
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

    const childSender: ControlChannelSender = createControlChannelSender({
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
        childPid: 9200,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;

    // Drive the park.notify frame the child emits from `env.onPark`.
    await childSender.send({
      type: "park.notify",
      data: {
        runId: "run-parked",
        correlationId: "corr-99",
        kind: "approval",
      },
    });

    const waitForRegistration = async (): Promise<SuspensionRegistration> => {
      const deadline = Date.now() + 2000;
      while (Date.now() < deadline) {
        const found = registrations[0];
        if (found !== undefined) return found;
        await new Promise((r) => setTimeout(r, 2));
      }
      throw new Error("supervisor did not invoke onSuspensionRegister in time");
    };
    const registration = await waitForRegistration();

    // The child-supplied fields ride through verbatim; the supervisor stamped
    // its own deployment identity onto them.
    expect(registration).toEqual({
      runId: "run-parked",
      correlationId: "corr-99",
      kind: "approval",
      deploymentId: DEPLOYMENT_ID,
      agentAddress: AGENT_ADDRESS,
    });

    await supervisor.shutdown();
    await fs.rm(baseDir, { recursive: true, force: true });
  });
});
