import { describe, test, expect } from "bun:test";

import { base64Encode } from "@intx/types/base64";
import { waitUntil } from "@intx/types/testing";

import { MailComposeError } from "@intx/types/mail-signer";

import { createChildMailComposeBridge } from "./mail-compose-bridge";
import type {
  ControlChannelSender,
  ControlPayload,
} from "../ipc/control-channel";

type ComposeRequest = Extract<
  ControlPayload,
  { type: "mail.compose.request" }
>["data"];

/**
 * Capture the `mail.compose.request` frames a bridge emits without standing
 * up the real Ed25519-signed sender.
 */
function createCapturingSender(): ControlChannelSender & {
  sent: ComposeRequest[];
} {
  const sent: ComposeRequest[] = [];
  return {
    get seq() {
      return sent.length;
    },
    async send(payload: ControlPayload) {
      if (payload.type === "mail.compose.request") sent.push(payload.data);
    },
    sent,
  };
}

function sequentialIds(): () => string {
  let next = 0;
  return () => `mcp-${String(++next)}`;
}

describe("createChildMailComposeBridge", () => {
  test("compose emits a request and resolves with the base64 message", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailComposeBridge({
      upstreamSender: sender,
      allocateRequestId: sequentialIds(),
    });

    const composed = bridge.compose({
      to: "run_target@example.com",
      content: "hello",
      attachments: [
        { mimeType: "text/plain", data: new Uint8Array([104, 105]) },
      ],
    });
    await waitUntil(() => sender.sent.length === 1);
    expect(sender.sent[0]).toEqual({
      requestId: "mcp-1",
      message: {
        to: "run_target@example.com",
        content: "hello",
        attachments: [
          {
            contentType: "text/plain",
            dataBase64: base64Encode(new Uint8Array([104, 105])),
          },
        ],
      },
    });

    const raw = new TextEncoder().encode("From: a@example.com\r\n\r\nbody");
    bridge.handleResult({
      requestId: "mcp-1",
      result: {
        ok: true,
        messageId: "<m1@example.com>",
        rawMessageBase64: base64Encode(raw),
      },
    });
    expect(await composed).toEqual({
      messageId: "<m1@example.com>",
      rawMessage: base64Encode(raw),
    });
  });

  test("keeps one request outstanding and sends the next when it settles", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailComposeBridge({
      upstreamSender: sender,
      allocateRequestId: sequentialIds(),
    });

    const first = bridge.compose({ to: "a@example.com", content: "one" });
    const second = bridge.compose({ to: "b@example.com", content: "two" });
    await waitUntil(() => sender.sent.length === 1);
    // The second request waits for the first to settle, whatever its outcome.
    expect(bridge.pendingCount).toBe(1);

    bridge.handleResult({
      requestId: "mcp-1",
      result: { ok: false, reason: "refused" },
    });
    await expect(first).rejects.toThrow("refused");
    await waitUntil(() => sender.sent.length === 2);
    expect(sender.sent[1]?.message.content).toBe("two");

    bridge.handleResult({
      requestId: "mcp-2",
      result: {
        ok: true,
        messageId: "<m2@example.com>",
        rawMessageBase64: base64Encode(new Uint8Array([120])),
      },
    });
    expect((await second).messageId).toBe("<m2@example.com>");
  });

  test("an upstream send failure rejects that compose and the queue moves on", async () => {
    const sent: ComposeRequest[] = [];
    let failNext = true;
    const sender: ControlChannelSender = {
      get seq() {
        return sent.length;
      },
      async send(payload: ControlPayload) {
        if (payload.type !== "mail.compose.request") return;
        if (failNext) {
          failNext = false;
          throw new Error("pipe closed");
        }
        sent.push(payload.data);
      },
    };
    const bridge = createChildMailComposeBridge({
      upstreamSender: sender,
      allocateRequestId: sequentialIds(),
    });

    const first = bridge.compose({ to: "a@example.com", content: "one" });
    const second = bridge.compose({ to: "b@example.com", content: "two" });
    await expect(first).rejects.toThrow("upstream send failed");
    await waitUntil(() => sent.length === 1);
    expect(sent[0]?.message.content).toBe("two");

    bridge.handleResult({
      requestId: "mcp-2",
      result: { ok: false, reason: "refused" },
    });
    await expect(second).rejects.toBeInstanceOf(MailComposeError);
  });

  test("a refused attachment rejects with the structured attachment error", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailComposeBridge({
      upstreamSender: sender,
      allocateRequestId: sequentialIds(),
    });

    const composed = bridge.compose({ to: "a@example.com", content: "x" });
    await waitUntil(() => sender.sent.length === 1);
    const attachmentError = {
      code: "disallowed_mime_type" as const,
      message: "attachment 0 has disallowed MIME type",
      attachmentIndex: 0,
      mimeType: "application/x-msdownload",
    };
    bridge.handleResult({
      requestId: "mcp-1",
      result: { ok: false, reason: attachmentError.message, attachmentError },
    });

    const failure = await composed.then(
      () => undefined,
      (cause: unknown) => cause,
    );
    if (!(failure instanceof MailComposeError)) {
      throw new Error(`expected a MailComposeError, got ${String(failure)}`);
    }
    expect(failure.attachmentError).toEqual(attachmentError);
  });

  test("cancelAll rejects the outstanding compose and every queued one", async () => {
    const sender = createCapturingSender();
    const bridge = createChildMailComposeBridge({
      upstreamSender: sender,
      allocateRequestId: sequentialIds(),
    });

    const first = bridge.compose({ to: "a@example.com", content: "one" });
    const second = bridge.compose({ to: "b@example.com", content: "two" });
    await waitUntil(() => sender.sent.length === 1);

    bridge.cancelAll("control loop exited");
    const failures = await Promise.all(
      [first, second].map((composed) =>
        composed.then(
          () => undefined,
          (cause: unknown) => cause,
        ),
      ),
    );
    // A cancel is the child's own doing, never a supervisor refusal, for
    // the sent compose and the queued one alike.
    for (const failure of failures) {
      if (!(failure instanceof Error)) {
        throw new Error(`expected an Error, got ${String(failure)}`);
      }
      expect(failure).not.toBeInstanceOf(MailComposeError);
      expect(failure.message).toContain("cancelled: control loop exited");
    }
    expect(sender.sent).toHaveLength(1);
  });
});
