import { describe, test, expect } from "bun:test";

import { type } from "arktype";

import { isMessageTransportError } from "@intx/types/runtime";

import { createChildMailboxCallBridge } from "./mailbox-call-bridge";
import {
  ControlPayload,
  type ControlChannelSender,
} from "../ipc/control-channel";

/**
 * Capture the `mailbox.call.request` frames a bridge emits without standing
 * up the real Ed25519-signed sender. The `seq` accessor is unused by the
 * bridge but required by the `ControlChannelSender` shape.
 */
function createCapturingSender(): ControlChannelSender & {
  sent: Extract<ControlPayload, { type: "mailbox.call.request" }>["data"][];
} {
  const sent: Extract<
    ControlPayload,
    { type: "mailbox.call.request" }
  >["data"][] = [];
  return {
    get seq() {
      return sent.length;
    },
    async send(payload: ControlPayload) {
      if (payload.type === "mailbox.call.request") sent.push(payload.data);
    },
    sent,
  };
}

describe("createChildMailboxCallBridge", () => {
  test("a search emits a request and resolves with the supervisor's value", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxCallBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-1",
    });

    const submitted = bridge.submit({
      runId: "run-1",
      op: "search",
      mailbox: "Drafts",
      query: { from: "alice" },
    });
    expect(sender.sent).toHaveLength(1);
    const frame = sender.sent[0];
    if (frame === undefined) throw new Error("no frame emitted");
    expect(frame).toEqual({
      requestId: "rid-1",
      runId: "run-1",
      op: "search",
      mailbox: "Drafts",
      query: { from: "alice" },
    });
    const validated = ControlPayload({
      type: "mailbox.call.request",
      data: frame,
    });
    expect(validated instanceof type.errors).toBe(false);
    expect(bridge.pendingCount).toBe(1);

    bridge.handleResult({
      requestId: "rid-1",
      ok: true,
      op: "search",
      value: [{ uid: 4, mailbox: "Drafts" }],
    });
    const answer = await submitted;
    expect(answer).toEqual({
      requestId: "rid-1",
      ok: true,
      op: "search",
      value: [{ uid: 4, mailbox: "Drafts" }],
    });
    expect(bridge.pendingCount).toBe(0);
  });

  test("a watch success carries no value", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxCallBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-watch",
    });
    const submitted = bridge.submit({
      runId: "run-1",
      op: "watch",
      mailbox: "INBOX",
    });
    bridge.handleResult({
      requestId: "rid-watch",
      ok: true,
      op: "watch",
    });
    const answer = await submitted;
    expect(answer.op).toBe("watch");
    expect("value" in answer).toBe(false);
  });

  test("a refusal that names a condition rejects with that condition", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxCallBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-cond",
    });
    const submitted = bridge.submit({
      runId: "run-1",
      op: "listMailboxes",
    });
    bridge.handleResult({
      requestId: "rid-cond",
      ok: false,
      op: "listMailboxes",
      reason: "listing refused",
      condition: "CANNOT",
    });
    try {
      await submitted;
      throw new Error("expected the call to refuse");
    } catch (cause) {
      expect(isMessageTransportError(cause)).toBe(true);
      if (!isMessageTransportError(cause)) return;
      expect(cause.condition).toBe("CANNOT");
      expect(cause.message).toBe("listing refused");
    }
    expect(bridge.pendingCount).toBe(0);
  });

  test("a failure that names no condition stays a plain error", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxCallBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-plain",
    });
    const submitted = bridge.submit({
      runId: "run-1",
      op: "fetchFull",
      ref: { uid: 1, mailbox: "INBOX" },
    });
    bridge.handleResult({
      requestId: "rid-plain",
      ok: false,
      op: "fetchFull",
      reason: "message uid 1 not found",
    });
    try {
      await submitted;
      throw new Error("expected the call to fail");
    } catch (cause) {
      expect(isMessageTransportError(cause)).toBe(false);
      expect(cause).toBeInstanceOf(Error);
      if (!(cause instanceof Error)) return;
      expect(cause.message).toContain("message uid 1 not found");
    }
    expect(bridge.pendingCount).toBe(0);
  });

  test("an upstream send failure rejects the submit and leaks no awaiter", async () => {
    const failingSender: ControlChannelSender = {
      get seq() {
        return 0;
      },
      send() {
        return Promise.reject(new Error("pipe closed"));
      },
    };
    const bridge = createChildMailboxCallBridge({
      upstreamSender: failingSender,
      allocateRequestId: () => "rid-send-fail",
    });
    const submitted = bridge.submit({
      runId: "run-1",
      op: "watch",
      mailbox: "INBOX",
    });
    await expect(submitted).rejects.toThrow(
      /upstream send failed for requestId rid-send-fail: pipe closed/,
    );
    expect(bridge.pendingCount).toBe(0);
  });

  test("a result with no pending entry is dropped without throwing", () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxCallBridge({
      upstreamSender: sender,
    });
    expect(() =>
      bridge.handleResult({
        requestId: "stale",
        ok: true,
        op: "watch",
      }),
    ).not.toThrow();
  });
});
