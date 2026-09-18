// Supervisor `park.notify` arm.
//
// A workflow-process child that parks a step on a reserved control-plane
// channel forwards a `park.notify` frame up the control channel. The
// supervisor's upstream-control pump stamps the deployment identity it owns
// (`anchorRunId` + the deployment's mail address as `agentAddress`) onto the
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
import {
  createChangeNotifier,
  createMemoryFrameStream,
  createMemoryNdjsonStream,
  createSpawnObserver,
  createStubRepoStore,
} from "@intx/workflow-host/testing";

import {
  createWorkflowSupervisor,
  type InboxPrimitives,
  type SuspensionRegistration,
} from "./index";
import { wrapHubTransportAsMailBus } from "../mail-bus/index";
import {
  createControlChannelSender,
  type ControlChannelSender,
} from "../ipc/index";

const AGENT_ADDRESS = "run_park-agent@integration.example";
const DEPLOYMENT_ID = "park-dep";

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
  test("stamps identity onto the registration and forwards an approval snapshot when present", async () => {
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
    // `onSuspensionRegister` is the signal the supervisor already hands this
    // test: report each arrival rather than re-reading the array on a tick.
    const registered = createChangeNotifier();

    let observedEnv: Record<string, string> | undefined;
    // Scoped here, not to the file: `first()` must resolve with THIS
    // fixture's spawn, not whichever spawn happened earliest in the run.
    const spawnObserver = createSpawnObserver();
    const supervisor = createWorkflowSupervisor({
      repoStore: createStubRepoStore(baseDir),
      signAsPrincipal: async () => ({
        sig: new Uint8Array(64),
        principalKind: "supervisor",
      }),
      mailBus,
      onSuspensionRegister: (registration) => {
        registrations.push(registration);
        registered.notify();
      },
      subprocessSpawner: ({ env }) => {
        observedEnv = env;
        spawnObserver.record(env);
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
      anchorRunId: DEPLOYMENT_ID,
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

    observedEnv = await spawnObserver.first();
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
        parkKind: "approval",
      },
    });

    await registered.until(() => registrations[0] !== undefined);
    const registration = registrations[0];
    if (registration === undefined) {
      throw new Error("supervisor did not invoke onSuspensionRegister");
    }

    // The child-supplied fields ride through verbatim; the supervisor stamped
    // its own deployment identity onto them. A park with no snapshot forwards
    // no `approvalSnapshot` key at all (the omit idiom, not a present
    // `undefined`) -- `toStrictEqual` distinguishes the two, `toEqual` does not.
    expect(registration).toStrictEqual({
      runId: "run-parked",
      correlationId: "corr-99",
      kind: "approval",
      anchorRunId: DEPLOYMENT_ID,
      agentAddress: AGENT_ADDRESS,
    });

    // A second park carrying an approval snapshot forwards it onto the
    // registration as `approvalSnapshot`, alongside the stamped identity.
    const snapshot = {
      name: "charge_card",
      description: "Charge the customer's card",
      inputSchema: { type: "object" },
      arguments: { amount: 100 },
    };
    await childSender.send({
      type: "park.notify",
      data: {
        runId: "run-parked-2",
        correlationId: "corr-100",
        parkKind: "approval",
        snapshot,
      },
    });
    await registered.until(() => registrations[1] !== undefined);
    const withSnapshot = registrations[1];
    if (withSnapshot === undefined) {
      throw new Error("supervisor did not forward the second registration");
    }
    expect(withSnapshot).toEqual({
      runId: "run-parked-2",
      correlationId: "corr-100",
      kind: "approval",
      anchorRunId: DEPLOYMENT_ID,
      agentAddress: AGENT_ADDRESS,
      approvalSnapshot: snapshot,
    });

    await supervisor.shutdown();
    await fs.rm(baseDir, { recursive: true, force: true });
  });
});
