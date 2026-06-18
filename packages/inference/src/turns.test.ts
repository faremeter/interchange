import { describe, test, expect } from "bun:test";
import { base64Encode } from "@intx/types";
import type { InboundMessage, MessageAttachment } from "@intx/types/runtime";
import { createInboundTurn } from "./turns";

function att(contentType: string, bytes: number[]): MessageAttachment {
  return { name: "file", contentType, data: new Uint8Array(bytes) };
}

function msg(opts: {
  content?: string;
  attachments?: MessageAttachment[];
  from?: string;
  subject?: string;
}): InboundMessage {
  return {
    ref: { uid: 1, mailbox: "INBOX" },
    headers: {
      from: opts.from ?? "",
      to: [],
      date: "2026-01-01T00:00:00.000Z",
      messageId: "<m@test>",
      ...(opts.subject !== undefined ? { subject: opts.subject } : {}),
    },
    flags: [],
    signatureStatus: "valid",
    ...(opts.content !== undefined ? { content: opts.content } : {}),
    ...(opts.attachments !== undefined
      ? { attachments: opts.attachments }
      : {}),
  };
}

describe("createInboundTurn", () => {
  const cases: { mimeType: string; blockType: string }[] = [
    { mimeType: "image/png", blockType: "image" },
    { mimeType: "image/heic", blockType: "image" },
    { mimeType: "video/mp4", blockType: "video" },
    { mimeType: "audio/mpeg", blockType: "audio" },
    { mimeType: "application/pdf", blockType: "document" },
    { mimeType: "application/json", blockType: "document" },
    { mimeType: "text/plain", blockType: "document" },
    { mimeType: "text/csv", blockType: "document" },
    { mimeType: "text/markdown", blockType: "document" },
  ];

  for (const { mimeType, blockType } of cases) {
    test(`maps ${mimeType} to a ${blockType} block`, () => {
      const turn = createInboundTurn(
        msg({ attachments: [att(mimeType, [1, 2, 3])] }),
      );
      expect(turn).toMatchObject({
        role: "user",
        content: [
          {
            type: blockType,
            source: {
              kind: "base64",
              mimeType,
              data: base64Encode(new Uint8Array([1, 2, 3])),
            },
          },
        ],
      });
    });
  }

  test("text plus attachments yields the text block then media blocks in order", () => {
    const turn = createInboundTurn(
      msg({
        content: "look at these",
        attachments: [att("image/png", [1]), att("application/pdf", [2])],
      }),
    );
    expect(turn).toMatchObject({
      role: "user",
      content: [
        { type: "text", text: "look at these" },
        { type: "image", source: { mimeType: "image/png" } },
        { type: "document", source: { mimeType: "application/pdf" } },
      ],
    });
  });

  test("image-only message (empty text) yields one image block, not null", () => {
    const turn = createInboundTurn(
      msg({ content: "", attachments: [att("image/png", [9, 8, 7])] }),
    );
    expect(turn).toMatchObject({
      role: "user",
      content: [{ type: "image", source: { mimeType: "image/png" } }],
    });
  });

  test("empty content and no attachments yields null", () => {
    expect(createInboundTurn(msg({ content: "" }))).toBeNull();
    expect(createInboundTurn(msg({}))).toBeNull();
  });

  test("prepends the From/Subject envelope to the text block", () => {
    const turn = createInboundTurn(
      msg({ content: "body", from: "alice@x", subject: "Hi" }),
    );
    expect(turn).toMatchObject({
      content: [
        { type: "text", text: "[From: alice@x]\n[Subject: Hi]\n\nbody" },
      ],
    });
  });

  test("degrades an unmappable attachment type to a text marker, not a throw", () => {
    // Inbound attachments come from remote senders and are not allowlist
    // filtered; an unmappable type must surface visibly rather than throw
    // into the reactor's delivery path.
    const turn = createInboundTurn(
      msg({ attachments: [att("application/zip", [1])] }),
    );
    expect(turn).toMatchObject({
      role: "user",
      content: [
        {
          type: "text",
          text: "[Unsupported attachment: file (application/zip)]",
        },
      ],
    });
  });
});
