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
import { base64Decode, base64Encode } from "@intx/types/base64";
import { verifyMimeSignature } from "@intx/mailbox";
import { parseHeaderSection } from "@intx/mime";
import {
  createInMemoryTransport,
  type InMemoryTransport,
} from "@intx/mail-memory";
import {
  createMemoryFrameStream,
  createMemoryNdjsonStream,
  createSpawnObserver,
  createStubRepoStore,
  waitForUpstreamPayload,
  type MemoryNdjsonStream,
  sendDefinition,
  TEST_DEFINITION_HASH,
} from "@intx/workflow-host/testing";

import { createWorkflowSupervisor, type InboxPrimitives } from "./index";
import { wrapHubTransportAsMailBus } from "../mail-bus/index";
import {
  createControlChannelSender,
  type ControlChannelSender,
} from "../ipc/index";
import { noGrantsOnRunStart } from "./step-grants.test-helpers";
import { createChildMailComposeBridge } from "../child/mail-compose-bridge";

const AGENT_ADDRESS = "run_outbound-agent@integration.example";
const RECIPIENT_ADDRESS = "recipient@integration.example";
const COHOSTED_ADDRESS = "run_cohosted-agent@integration.example";

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

type OutboundHarness = {
  hostTransport: InMemoryTransport;
  agentPublicKey: Uint8Array;
  childSender: ControlChannelSender;
  supervisorToChild: MemoryNdjsonStream;
  shutdown(): Promise<void>;
};

async function startOutboundHarness(
  opts: { composeGate?: Promise<void> } = {},
): Promise<OutboundHarness> {
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

  const transportBus = wrapHubTransportAsMailBus(hostTransport);
  // `composeGate` holds every compose until it resolves, so a test can
  // send a second request while the first is still in flight.
  const composeGate = opts.composeGate;
  const mailBus =
    composeGate === undefined
      ? transportBus
      : {
          ...transportBus,
          async composeSigned(
            ...args: Parameters<typeof transportBus.composeSigned>
          ) {
            await composeGate;
            return transportBus.composeSigned(...args);
          },
        };

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
    deploymentMailAddress: AGENT_ADDRESS,
    // Single-step launched agent. The outbound path never reads grants.
    onRunStart: noGrantsOnRunStart,
    layoutSteps: () => Promise.resolve(["step-1"]),
    inboxPrimitives: createNoopInboxPrimitives(),
    ipcKeyPairFactory: () => Promise.resolve(supervisorIpcKeyPair),
  });

  const spawnPromise = supervisor.spawn({
    definitionHash: TEST_DEFINITION_HASH,

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
  await sendDefinition(childSender, {
    childPid: 9100,
    childPublicKey: hexEncode(childIpcKeyPair.publicKey),
  });
  await childSender.send({ type: "ready" });
  await spawnPromise;

  return {
    hostTransport,
    agentPublicKey: agentKeyPair.publicKey,
    childSender,
    supervisorToChild,
    async shutdown() {
      await supervisor.shutdown();
      await fs.rm(baseDir, { recursive: true, force: true });
    },
  };
}

describe("supervisor-backed outbound signed send (Phase 4.3)", () => {
  test("child outbound.message -> supervisor signed send -> signed by the agent's identity", async () => {
    const { hostTransport, childSender, supervisorToChild, shutdown } =
      await startOutboundHarness();

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

    await shutdown();
  });

  test("child outbound.message naming another registered sender is refused", async () => {
    const { hostTransport, childSender, supervisorToChild, shutdown } =
      await startOutboundHarness();
    // A second deployment on the same host: the host transport holds its
    // signing key, so only the supervisor stands between this child and
    // mail signed as it.
    hostTransport.register(
      COHOSTED_ADDRESS,
      createEd25519Crypto(await generateKeyPair()),
    );

    const requestId = "om-test-spoof";
    await childSender.send({
      type: "outbound.message",
      data: {
        requestId,
        senderAddress: COHOSTED_ADDRESS,
        message: {
          to: RECIPIENT_ADDRESS,
          type: "conversation.message",
          content: "Sent as a deployment this child does not own.",
        },
      },
    });

    const result = (
      await waitForUpstreamPayload(
        supervisorToChild,
        "outbound.result",
        (payload) => payload.data.requestId === requestId,
      )
    ).data;
    expect(result.result).toEqual({
      ok: false,
      reason: `the child sends mail only as ${AGENT_ADDRESS}, not ${COHOSTED_ADDRESS}`,
    });
    const refs = await hostTransport
      .getTransportFor(RECIPIENT_ADDRESS)
      .search("INBOX", {});
    expect(refs).toEqual([]);

    await shutdown();
  });
});

describe("supervisor mail compose", () => {
  test("a compose request returns a message signed as the deployment and sends nothing", async () => {
    const {
      hostTransport,
      agentPublicKey,
      childSender,
      supervisorToChild,
      shutdown,
    } = await startOutboundHarness();
    const bridge = createChildMailComposeBridge({
      upstreamSender: childSender,
      allocateRequestId: () => "mcp-test-1",
    });

    const composed = bridge.compose({
      to: RECIPIENT_ADDRESS,
      content: "Posted to the hub by run code.",
      attachments: [
        {
          mimeType: "text/plain",
          data: base64Encode(new TextEncoder().encode("notes")),
          name: "notes.txt",
        },
      ],
    });
    const response = await waitForUpstreamPayload(
      supervisorToChild,
      "mail.compose.response",
      (payload) => payload.data.requestId === "mcp-test-1",
    );
    bridge.handleResult(response.data);
    const composedMail = await composed;
    const { messageId } = composedMail;
    const rawMessage = base64Decode(composedMail.rawMessage);

    expect(await verifyMimeSignature(rawMessage, agentPublicKey)).toBe("valid");
    const { headers } = parseHeaderSection(rawMessage);
    expect(headers.get("from")).toBe(AGENT_ADDRESS);
    expect(headers.get("to")).toBe(RECIPIENT_ADDRESS);
    expect(headers.get("message-id")).toBe(messageId);
    expect(headers.get("interchange-type")).toBe("conversation.message");
    expect(
      await hostTransport
        .getTransportFor(RECIPIENT_ADDRESS)
        .search("INBOX", {}),
    ).toEqual([]);

    await shutdown();
  });

  test("a disallowed attachment is refused with the structured attachment error", async () => {
    const { childSender, supervisorToChild, shutdown } =
      await startOutboundHarness();

    await childSender.send({
      type: "mail.compose.request",
      data: {
        requestId: "mcp-test-mime",
        message: {
          to: RECIPIENT_ADDRESS,
          content: "with a binary",
          attachments: [
            {
              contentType: "application/x-msdownload",
              dataBase64: base64Encode(new Uint8Array([1, 2, 3])),
            },
          ],
        },
      },
    });
    const { result } = (
      await waitForUpstreamPayload(
        supervisorToChild,
        "mail.compose.response",
        (payload) => payload.data.requestId === "mcp-test-mime",
      )
    ).data;
    if (result.ok) throw new Error("compose unexpectedly succeeded");
    expect(result.attachmentError?.code).toBe("disallowed_mime_type");

    await shutdown();
  });

  test("a compose the transport refuses is answered rather than dropped", async () => {
    const { hostTransport, childSender, supervisorToChild, shutdown } =
      await startOutboundHarness();
    hostTransport.unregister(AGENT_ADDRESS);

    await childSender.send({
      type: "mail.compose.request",
      data: {
        requestId: "mcp-test-unregistered",
        message: { to: RECIPIENT_ADDRESS, content: "no key" },
      },
    });
    const { result } = (
      await waitForUpstreamPayload(
        supervisorToChild,
        "mail.compose.response",
        (payload) => payload.data.requestId === "mcp-test-unregistered",
      )
    ).data;
    expect(result).toEqual({
      ok: false,
      reason: `Sender "${AGENT_ADDRESS}" is not registered with this transport`,
    });

    await shutdown();
  });

  test("a second compose while one is in flight is refused", async () => {
    let openGate: () => void = () => undefined;
    const composeGate = new Promise<void>((resolve) => {
      openGate = resolve;
    });
    const { childSender, supervisorToChild, shutdown } =
      await startOutboundHarness({ composeGate });

    for (const requestId of ["mcp-test-first", "mcp-test-second"]) {
      await childSender.send({
        type: "mail.compose.request",
        data: {
          requestId,
          message: { to: RECIPIENT_ADDRESS, content: requestId },
        },
      });
    }
    const second = (
      await waitForUpstreamPayload(
        supervisorToChild,
        "mail.compose.response",
        (payload) => payload.data.requestId === "mcp-test-second",
      )
    ).data;
    expect(second.result).toEqual({
      ok: false,
      reason: "another mail compose is in flight for this child",
    });

    openGate();
    const first = (
      await waitForUpstreamPayload(
        supervisorToChild,
        "mail.compose.response",
        (payload) => payload.data.requestId === "mcp-test-first",
      )
    ).data;
    expect(first.result.ok).toBe(true);

    await shutdown();
  });

  test("a message too large for a mail request is refused, though its attachments pass", async () => {
    const { childSender, supervisorToChild, shutdown } =
      await startOutboundHarness();
    // 27 MiB of attachments is under the attachment limit, but the signed
    // message, base64-encoded for the request, is over the mail limit.
    const nineMiB = base64Encode(new Uint8Array(9 * 1024 * 1024));

    await childSender.send({
      type: "mail.compose.request",
      data: {
        requestId: "mcp-test-oversized",
        message: {
          to: RECIPIENT_ADDRESS,
          content: "large",
          attachments: [0, 1, 2].map((index) => ({
            contentType: "application/pdf",
            dataBase64: nineMiB,
            name: `part-${String(index)}.pdf`,
          })),
        },
      },
    });
    const { result } = (
      await waitForUpstreamPayload(
        supervisorToChild,
        "mail.compose.response",
        (payload) => payload.data.requestId === "mcp-test-oversized",
      )
    ).data;
    if (result.ok) throw new Error("compose unexpectedly succeeded");
    expect(result.reason).toContain("over the");
    expect(result.attachmentError).toBeUndefined();

    await shutdown();
  }, 60000);
});
