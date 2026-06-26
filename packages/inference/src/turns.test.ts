import { describe, test, expect } from "bun:test";
import { base64Encode } from "@intx/types";
import type {
  ConversationTurn,
  InboundMessage,
  MessageAttachment,
} from "@intx/types/runtime";
import { assertWellFormedToolSequence, createInboundTurn } from "./turns";

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

function toolCallTurn(ids: string[]): ConversationTurn {
  return {
    role: "assistant",
    content: ids.map((id) => ({
      type: "tool_call" as const,
      id,
      name: "some_tool",
      arguments: {},
    })),
    model: "test-model",
    timestamp: 0,
  };
}

function toolResultTurn(callIds: string[]): ConversationTurn {
  return {
    role: "user",
    content: callIds.map((callId) => ({
      type: "tool_result" as const,
      callId,
      content: [{ type: "text" as const, text: "ok" }],
    })),
    timestamp: 0,
  };
}

describe("assertWellFormedToolSequence", () => {
  test("accepts a well-formed multi-cycle conversation", () => {
    const turns: ConversationTurn[] = [
      { role: "user", content: [{ type: "text", text: "hi" }], timestamp: 0 },
      toolCallTurn(["c1", "c2"]),
      toolResultTurn(["c1", "c2"]),
      toolCallTurn(["c3"]),
      toolResultTurn(["c3"]),
    ];
    expect(() => assertWellFormedToolSequence(turns)).not.toThrow();
  });

  test("accepts an unanswered tool_call (halt/abort leaves no result)", () => {
    const turns: ConversationTurn[] = [toolCallTurn(["c1"])];
    expect(() => assertWellFormedToolSequence(turns)).not.toThrow();
  });

  test("accepts empty input", () => {
    expect(() => assertWellFormedToolSequence([])).not.toThrow();
  });

  test("throws when a call id is re-emitted after it was answered", () => {
    // The literal INTR-225 overlap shape: a stale batch re-issues a call id
    // that already has a result.
    const turns: ConversationTurn[] = [
      toolCallTurn(["c1"]),
      toolResultTurn(["c1"]),
      toolCallTurn(["c1"]),
    ];
    expect(() => assertWellFormedToolSequence(turns)).toThrow(
      /duplicate tool_call id "c1"/,
    );
  });

  test("throws on two results for one call in the same turn", () => {
    const turns: ConversationTurn[] = [
      toolCallTurn(["c1"]),
      toolResultTurn(["c1", "c1"]),
    ];
    expect(() => assertWellFormedToolSequence(turns)).toThrow(
      /duplicate tool_result for "c1"/,
    );
  });

  test("throws on a duplicate tool_result for one callId", () => {
    const turns: ConversationTurn[] = [
      toolCallTurn(["c1"]),
      toolResultTurn(["c1"]),
      toolResultTurn(["c1"]),
    ];
    expect(() => assertWellFormedToolSequence(turns)).toThrow(
      /duplicate tool_result for "c1"/,
    );
  });

  test("throws on a tool_result with no preceding tool_call", () => {
    const turns: ConversationTurn[] = [toolResultTurn(["c1"])];
    expect(() => assertWellFormedToolSequence(turns)).toThrow(
      /no preceding tool_call/,
    );
  });

  test("throws on a duplicate tool_call id", () => {
    const turns: ConversationTurn[] = [
      toolCallTurn(["c1"]),
      toolCallTurn(["c1"]),
    ];
    expect(() => assertWellFormedToolSequence(turns)).toThrow(
      /duplicate tool_call id "c1"/,
    );
  });
});
