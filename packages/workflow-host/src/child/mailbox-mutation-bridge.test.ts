import { describe, test, expect } from "bun:test";

import { type } from "arktype";

import { createChildMailboxMutationBridge } from "./mailbox-mutation-bridge";
import {
  ControlPayload,
  type ControlChannelSender,
} from "../ipc/control-channel";

/**
 * Capture the `mailbox.mutate.request` frames a bridge emits without
 * standing up the real Ed25519-signed sender. The `seq` accessor is
 * unused by the bridge but required by the `ControlChannelSender` shape.
 */
function createCapturingSender(): ControlChannelSender & {
  sent: Extract<ControlPayload, { type: "mailbox.mutate.request" }>["data"][];
} {
  const sent: Extract<
    ControlPayload,
    { type: "mailbox.mutate.request" }
  >["data"][] = [];
  return {
    get seq() {
      return sent.length;
    },
    async send(payload: ControlPayload) {
      if (payload.type === "mailbox.mutate.request") sent.push(payload.data);
    },
    sent,
  };
}

describe("createChildMailboxMutationBridge", () => {
  test("a flag mutation emits an addFlags request and resolves on the matching result", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxMutationBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-1",
    });

    const submitted = bridge.submit({
      runId: "run-1",
      mailbox: "INBOX",
      op: "addFlags",
      uid: 7,
      flags: ["\\Seen"],
    });
    expect(sender.sent).toHaveLength(1);
    const frame = sender.sent[0];
    if (frame === undefined) throw new Error("no frame emitted");
    expect(frame.requestId).toBe("rid-1");
    expect(frame.op).toBe("addFlags");
    if (frame.op === "expunge") throw new Error("expected a flag frame");
    expect(frame.uid).toBe(7);
    expect(frame.flags).toEqual(["\\Seen"]);
    // The frame validates against the canonical control payload narrow.
    const validated = ControlPayload({
      type: "mailbox.mutate.request",
      data: frame,
    });
    expect(validated instanceof type.errors).toBe(false);
    expect(bridge.pendingCount).toBe(1);

    bridge.handleResult({ requestId: "rid-1", result: { ok: true } });
    const result = await submitted;
    expect(result.expungedUids).toBeUndefined();
    expect(bridge.pendingCount).toBe(0);
  });

  test("a removeFlags mutation carries its uid and flags on the wire", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxMutationBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-rm",
    });
    const submitted = bridge.submit({
      runId: "run-1",
      mailbox: "INBOX",
      op: "removeFlags",
      uid: 4,
      flags: ["\\Seen"],
    });
    const frame = sender.sent[0];
    if (frame === undefined) throw new Error("no frame emitted");
    expect(frame.op).toBe("removeFlags");
    if (frame.op === "expunge") throw new Error("expected a flag frame");
    expect(frame.uid).toBe(4);
    expect(frame.flags).toEqual(["\\Seen"]);
    const validated = ControlPayload({
      type: "mailbox.mutate.request",
      data: frame,
    });
    expect(validated instanceof type.errors).toBe(false);
    bridge.handleResult({ requestId: "rid-rm", result: { ok: true } });
    await submitted;
  });

  test("an expunge request omits uid and flags and carries the swept uids back", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxMutationBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-exp",
    });

    const submitted = bridge.submit({
      runId: "run-1",
      mailbox: "INBOX",
      op: "expunge",
    });
    const frame = sender.sent[0];
    if (frame === undefined) throw new Error("no frame emitted");
    expect(frame.op).toBe("expunge");
    // The op-discriminated wire type carries no uid/flags on an expunge.
    expect("uid" in frame).toBe(false);
    expect("flags" in frame).toBe(false);
    const validated = ControlPayload({
      type: "mailbox.mutate.request",
      data: frame,
    });
    expect(validated instanceof type.errors).toBe(false);

    bridge.handleResult({
      requestId: "rid-exp",
      result: { ok: true, expungedUids: [3, 5] },
    });
    const result = await submitted;
    expect(result.expungedUids).toEqual([3, 5]);
  });

  test("a failed result rejects the submit so the mail-tool call fails loudly", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxMutationBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-2",
    });
    const submitted = bridge.submit({
      runId: "run-1",
      mailbox: "INBOX",
      op: "addFlags",
      uid: 1,
      flags: ["\\Deleted"],
    });
    bridge.handleResult({
      requestId: "rid-2",
      result: { ok: false, reason: "message uid 1 not found" },
    });
    await expect(submitted).rejects.toThrow(/message uid 1 not found/);
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
    const bridge = createChildMailboxMutationBridge({
      upstreamSender: failingSender,
      allocateRequestId: () => "rid-send-fail",
    });
    const submitted = bridge.submit({
      runId: "run-1",
      mailbox: "INBOX",
      op: "expunge",
    });
    await expect(submitted).rejects.toThrow(
      /upstream send failed for requestId rid-send-fail: pipe closed/,
    );
    // The pending entry is removed on send failure so a later cancelAll or
    // stale response finds nothing to act on.
    expect(bridge.pendingCount).toBe(0);
  });

  test("cancelAll rejects every pending mutation", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxMutationBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-3",
    });
    const submitted = bridge.submit({
      runId: "run-1",
      mailbox: "INBOX",
      op: "expunge",
    });
    bridge.cancelAll("control loop exited");
    await expect(submitted).rejects.toThrow(/cancelled: control loop exited/);
    expect(bridge.pendingCount).toBe(0);
  });

  test("a result with no pending entry is dropped without throwing", () => {
    const sender = createCapturingSender();
    const bridge = createChildMailboxMutationBridge({
      upstreamSender: sender,
      allocateRequestId: () => "rid-4",
    });
    // A stale supervisor reply for a requestId with no awaiter must not throw.
    expect(() =>
      bridge.handleResult({ requestId: "stale", result: { ok: true } }),
    ).not.toThrow();
  });
});
