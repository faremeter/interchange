// Phase 4.3 -- OUTBOUND half of mailbox ownership.
//
// Proves the load-bearing 4.3 contract end-to-end across the IPC seam:
// a child-originated outbound mail request (the shape the step agent's
// mail-send tool / reply path produces) flows up the control channel as
// an `outbound.message` frame, the supervisor performs the actual send
// through the HOST transport, and the outbound mail is SIGNED BY THE
// AGENT'S IDENTITY -- the same `CryptoProvider` the host registered for
// the agent's address, with full parity to the in-process path
// (`@intx/mail-memory`'s `executeSend`). The recipient's `fetchFull`
// verifies the signature as `valid` against the agent's key and reports
// `from = <run address>`.
//
// Against the absence of an outbound path (the pre-4.3 supervisor mail
// bus is INBOUND-ONLY -- register / unregister / subscribe /
// routeInbound, no send), there is no `MailBusBindings.sendOutbound`,
// no `outbound.message` control frame, and no supervisor handler: the
// child has no way to drive an outbound send and this test cannot be
// written. It is green only because 4.3 added all three.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import { createInMemoryTransport } from "@intx/mail-memory";
import {
  createMemoryFrameStream,
  createMemoryNdjsonStream,
  createSpawnObserver,
  createStubRepoStore,
  waitForUpstreamPayload,
} from "@intx/workflow-host/testing";

import { createWorkflowSupervisor, type InboxPrimitives } from "./index";
import { wrapHubTransportAsMailBus } from "../mail-bus/index";
import {
  createControlChannelSender,
  type ControlChannelSender,
} from "../ipc/index";

const AGENT_ADDRESS = "run_outbound-agent@integration.example";
const RECIPIENT_ADDRESS = "recipient@integration.example";

/**
 * No-op inbox primitives. This test exercises only the OUTBOUND path,
 * which never touches the inbox claim-check; injecting these keeps the
 * spawn-time replay and dispatch loop off the stub `RepoStore`'s
 * unimplemented `writeTreePreservingPrefix` so a crash there cannot tear
 * the control channel down before the outbound round-trip completes.
 */
function createNoopInboxPrimitives(): InboxPrimitives {
  return {
    async enqueueInbox() {
      throw new Error("enqueueInbox not exercised in the outbound test");
    },
    async dequeueToProcessing() {
      return null;
    },
    async markConsumed() {
      throw new Error("markConsumed not exercised in the outbound test");
    },
    async replayProcessingToInbox() {
      return { commitSha: "noop", replayedKeys: [] };
    },
  };
}

describe("supervisor-backed outbound signed send (Phase 4.3)", () => {
  test("child outbound.message -> supervisor signed send -> signed by the agent's identity", async () => {
    const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "p4-3-outbound-"));

    // Host transport: the real in-memory transport the sidecar owns. The
    // agent's CryptoProvider is registered here against its address --
    // exactly the registration the in-process `startSession` performs and
    // the 4.3 sidecar wiring performs at spawn. The supervisor signs the
    // agent's outbound mail with THIS key.
    const hostTransport = createInMemoryTransport();
    const agentKeyPair = await generateKeyPair();
    const agentCrypto = createEd25519Crypto(agentKeyPair);
    hostTransport.register(AGENT_ADDRESS, agentCrypto);
    // Register the recipient so the send delivers locally (no remote
    // leg) and the test can fetch the signed bytes back out of its INBOX.
    const recipientKeyPair = await generateKeyPair();
    hostTransport.register(
      RECIPIENT_ADDRESS,
      createEd25519Crypto(recipientKeyPair),
    );

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
      subprocessSpawner: ({ env }) => {
        observedEnv = env;
        spawnObserver.record(env);
        return {
          pid: 9100,
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
      workflowRunRepoId: { kind: "workflow-run", id: "outbound-dep" },
      workflowRunRef: "refs/heads/main",
      anchorRunId: "outbound-dep",
      stepCount: 1,
      deploymentMailAddress: AGENT_ADDRESS,
      readPrincipal: { kind: "supervisor" },
      deriveStepAddress: () => AGENT_ADDRESS,
      // Single-step launched agent: its grants live in the legacy
      // agent-state repo keyed by the agent id. The stub repo's
      // getRepoDir resolves the dir; no grants file is needed because the
      // outbound path never reads grants.
      deriveStepRepoId: () => ({ kind: "agent-state", id: "outbound-dep" }),
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

    // The child must announce `ready` (publishing its upstream-signing
    // public key) before the supervisor accepts further upstream frames.
    await childSender.send({
      type: "ready",
      data: {
        childPid: 9100,
        childPublicKey: hexEncode(childIpcKeyPair.publicKey),
      },
    });
    await spawnPromise;

    // Drive the OUTBOUND request the step agent's mail-send tool produces:
    // a structured conversation message addressed to the recipient. The
    // child forwards it up as `outbound.message` carrying the agent's
    // sender address; the supervisor performs the signed send.
    const requestId = "om-test-1";
    await childSender.send({
      type: "outbound.message",
      data: {
        requestId,
        senderAddress: AGENT_ADDRESS,
        message: {
          to: RECIPIENT_ADDRESS,
          type: "conversation.message",
          content: "Reply produced by the step agent.",
        },
      },
    });

    // Wait for the supervisor's `outbound.result` reply. The wait is driven
    // by the stream double reporting each write and re-reading the buffer
    // then, so nothing is re-parsed on a tick and no duration decides the
    // outcome. `waitForUpstreamPayload` validates each frame through the
    // canonical `SignedEnvelope` + `ControlPayload` narrows, as the local
    // parser this replaced did.
    const result = (
      await waitForUpstreamPayload(
        supervisorToChild,
        "outbound.result",
        (payload) => payload.data.requestId === requestId,
      )
    ).data;

    // The supervisor performed the signed send through the host transport.
    if (!result.result.ok) {
      throw new Error(
        `outbound send failed: ${result.result.ok ? "" : result.result.reason}`,
      );
    }
    expect(result.result.ok).toBe(true);
    expect(result.result.messageId.length).toBeGreaterThan(0);
    expect(result.result.status === "delivered").toBe(true);

    // The signed bytes landed in the recipient's INBOX. Fetch the full
    // message: `fetchFull` verifies the PGP/MIME signature against the
    // sender's registered CryptoProvider -- this is the SAME verification
    // the in-process path relies on, so a `valid` status with `from` =
    // the agent's address is the proof that the outbound mail was signed
    // by the AGENT's identity, not the supervisor's principal key.
    const recipientView = hostTransport.getTransportFor(RECIPIENT_ADDRESS);
    const refs = await recipientView.search("INBOX", {});
    expect(refs.length).toBe(1);
    const ref = refs[0];
    if (ref === undefined) throw new Error("no inbox ref");
    const full = await recipientView.fetchFull(ref);
    expect(full.signatureStatus).toBe("valid");
    expect(full.headers.from).toBe(AGENT_ADDRESS);
    expect(full.headers.messageId).toBe(result.result.messageId);

    await supervisor.shutdown();
    await fs.rm(baseDir, { recursive: true, force: true });
  });
});
