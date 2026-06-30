import { describe, test, expect } from "bun:test";

import { type } from "arktype";

import { base64Decode } from "@intx/types";

import { createChildOutboundMailBridge } from "./outbound-mail-bridge";
import { createSupervisorBackedTransport } from "./supervisor-backed-transport";
import {
  ControlPayload,
  type ControlChannelSender,
} from "../ipc/control-channel";

/**
 * Capture the `outbound.message` frames a bridge emits without standing
 * up the real Ed25519-signed sender. The `seq` accessor is unused by the
 * bridge but required by the `ControlChannelSender` shape.
 */
function createCapturingSender(): ControlChannelSender & {
  sent: Extract<ControlPayload, { type: "outbound.message" }>["data"][];
} {
  const sent: Extract<ControlPayload, { type: "outbound.message" }>["data"][] =
    [];
  return {
    get seq() {
      return sent.length;
    },
    async send(payload: ControlPayload) {
      if (payload.type === "outbound.message") sent.push(payload.data);
    },
    sent,
  };
}

describe("createChildOutboundMailBridge", () => {
  test("submit emits an outbound.message frame and resolves on the matching result", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-1",
    });

    const submitted = bridge.submit("agent@example.com", {
      to: "recipient@example.com",
      type: "conversation.message",
      content: "reply text",
    });
    // The frame carries the sender address and the projected message.
    expect(sender.sent).toHaveLength(1);
    const frame = sender.sent[0];
    if (frame === undefined) throw new Error("no frame emitted");
    expect(frame.requestId).toBe("rid-1");
    expect(frame.senderAddress).toBe("agent@example.com");
    expect(frame.message.to).toBe("recipient@example.com");
    expect(frame.message.content).toBe("reply text");
    // The frame validates against the canonical control payload narrow.
    const validated = ControlPayload({ type: "outbound.message", data: frame });
    expect(validated instanceof type.errors).toBe(false);
    expect(bridge.pendingCount).toBe(1);

    bridge.handleResult({
      requestId: "rid-1",
      result: { ok: true, messageId: "<m-1@example.com>", status: "delivered" },
    });
    const receipt = await submitted;
    expect(receipt.messageId).toBe("<m-1@example.com>");
    expect(receipt.status).toBe("delivered");
    expect(bridge.pendingCount).toBe(0);
  });

  test("a failed result rejects the submit so the mail-tool call fails loudly", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-2",
    });
    const submitted = bridge.submit("agent@example.com", {
      to: "recipient@example.com",
      type: "conversation.message",
      content: "x",
    });
    bridge.handleResult({
      requestId: "rid-2",
      result: { ok: false, reason: "sender not registered" },
    });
    await expect(submitted).rejects.toThrow(/sender not registered/);
  });

  test("cancelAll rejects every pending send", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-3",
    });
    const submitted = bridge.submit("agent@example.com", {
      to: "recipient@example.com",
      type: "conversation.message",
      content: "x",
    });
    bridge.cancelAll("control loop exited");
    await expect(submitted).rejects.toThrow(/cancelled: control loop exited/);
    expect(bridge.pendingCount).toBe(0);
  });

  test("base64-roundtrips attachment bytes through the wire projection", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-4",
    });
    const data = new Uint8Array([1, 2, 3, 250, 251, 252]);
    void bridge.submit("agent@example.com", {
      to: "recipient@example.com",
      type: "conversation.message",
      content: "with attachment",
      attachments: [
        { name: "f.bin", contentType: "application/octet-stream", data },
      ],
    });
    const frame = sender.sent[0];
    if (frame === undefined) throw new Error("no frame emitted");
    const att = frame.message.attachments?.[0];
    if (att === undefined) throw new Error("attachment not projected");
    expect(att.name).toBe("f.bin");
    expect(base64Decode(att.dataBase64)).toEqual(data);
  });
});

describe("createSupervisorBackedTransport", () => {
  test("send routes through the bridge as the agent's address", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-5",
    });
    const transport = createSupervisorBackedTransport(
      bridge,
      "agent@example.com",
    );
    const sendPromise = transport.send({
      to: "recipient@example.com",
      type: "conversation.message",
      content: "via transport",
    });
    expect(sender.sent).toHaveLength(1);
    expect(sender.sent[0]?.senderAddress).toBe("agent@example.com");
    bridge.handleResult({
      requestId: "rid-5",
      result: { ok: true, messageId: "<m-5@example.com>", status: "delivered" },
    });
    const receipt = await sendPromise;
    expect(receipt.messageId).toBe("<m-5@example.com>");
  });

  test("inbound read surface throws (the agent owns no mailbox in the unified host)", async () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({ upstreamSender: sender });
    const transport = createSupervisorBackedTransport(
      bridge,
      "agent@example.com",
    );
    await expect(transport.search("INBOX", {})).rejects.toThrow(
      /not supported for unified-host step agent/,
    );
    await expect(
      transport.fetchFull({ uid: 1, mailbox: "INBOX" }),
    ).rejects.toThrow(/not supported for unified-host step agent/);
  });

  test("watch returns a no-op unsubscribe and never fires", () => {
    const sender = createCapturingSender();
    const bridge = createChildOutboundMailBridge({ upstreamSender: sender });
    const transport = createSupervisorBackedTransport(
      bridge,
      "agent@example.com",
    );
    let fired = false;
    const unsubscribe = transport.watch("INBOX", () => {
      fired = true;
    });
    expect(typeof unsubscribe).toBe("function");
    expect(() => unsubscribe()).not.toThrow();
    expect(fired).toBe(false);
  });
});
