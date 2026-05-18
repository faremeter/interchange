import { describe, test, expect } from "bun:test";
import type {
  InboundMessage,
  MessageAttachment,
  OutboundMessage,
} from "@interchange/types/runtime";

import { createInboundMessage, createOutboundMessage } from "./mail-builder";
import type {
  CreateInboundMessageOpts,
  CreateOutboundMessageOpts,
} from "./mail-builder";

const FROM = "alice@example.com";
const TO = "agent@example.com";

// Test-only escape hatches. The builders' types reject obviously invalid
// inputs at compile time, but the defensive validation also has to surface
// errors when callers bypass the type system (e.g. data deserialised from
// unknown JSON). These helpers route around the type checker so the
// runtime guards can be exercised directly.
function callInboundUnsafe(opts: unknown): unknown {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- exercise runtime validation against type-violating input
  return createInboundMessage(opts as CreateInboundMessageOpts);
}
function callOutboundUnsafe(opts: unknown): unknown {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- exercise runtime validation against type-violating input
  return createOutboundMessage(opts as CreateOutboundMessageOpts);
}

describe("createInboundMessage", () => {
  test("conversation content with default ref/flags/signatureStatus", () => {
    const msg = createInboundMessage({ from: FROM, to: TO, content: "hi" });

    expect(msg.ref).toEqual({ uid: 1, mailbox: "INBOX" });
    expect(msg.flags).toEqual([]);
    expect(msg.signatureStatus).toBe("missing");
    expect(msg.content).toBe("hi");
    expect(msg.payload).toBeUndefined();
    expect(msg.attachments).toBeUndefined();
    expect(msg.headers.from).toBe(FROM);
    expect(msg.headers.to).toEqual([TO]);
    expect(msg.headers.interchangeType).toBeUndefined();
  });

  test("auto-generated messageId derives domain from `from`", () => {
    const msg = createInboundMessage({ from: FROM, to: TO, content: "hi" });
    expect(msg.headers.messageId).toMatch(/^<[^<>\s]+@example\.com>$/);
  });

  test("auto-generated date is a parseable ISO string", () => {
    const msg = createInboundMessage({ from: FROM, to: TO, content: "hi" });
    const parsed = new Date(msg.headers.date);
    expect(Number.isNaN(parsed.getTime())).toBe(false);
    expect(msg.headers.date).toBe(parsed.toISOString());
  });

  test("structured payload sets interchangeType header automatically", () => {
    const msg = createInboundMessage({
      from: FROM,
      to: TO,
      payload: {
        type: "offering.request",
        body: { offeringId: "code-review", parameters: {} },
      },
    });

    expect(msg.payload).toEqual({
      type: "offering.request",
      version: "1",
      body: { offeringId: "code-review", parameters: {} },
    });
    expect(msg.headers.interchangeType).toBe("offering.request");
    expect(msg.content).toBeUndefined();
  });

  test("payload.version override is preserved", () => {
    const msg = createInboundMessage({
      from: FROM,
      to: TO,
      payload: {
        type: "payment.required",
        version: "2",
        body: { amount: "0.50" },
      },
    });
    expect(msg.payload?.version).toBe("2");
  });

  test("attachments are passed through when non-empty", () => {
    const attachments: MessageAttachment[] = [
      {
        name: "report.pdf",
        contentType: "application/pdf",
        data: new Uint8Array([1, 2, 3]),
      },
    ];
    const msg = createInboundMessage({
      from: FROM,
      to: TO,
      content: "see attached",
      attachments,
    });
    expect(msg.attachments).toEqual(attachments);
  });

  test("threading headers are wired through", () => {
    const msg = createInboundMessage({
      from: FROM,
      to: TO,
      content: "reply",
      inReplyTo: "<original@example.com>",
      references: ["<one@example.com>", "<two@example.com>"],
      correlationId: "corr-123",
    });

    expect(msg.headers.inReplyTo).toBe("<original@example.com>");
    expect(msg.headers.references).toEqual([
      "<one@example.com>",
      "<two@example.com>",
    ]);
    expect(msg.headers.interchangeCorrelationId).toBe("corr-123");
  });

  test("all interchange identity headers map through", () => {
    const msg = createInboundMessage({
      from: FROM,
      to: TO,
      content: "hi",
      tenantId: "tenant-1",
      agentId: "agent-1",
      sessionId: "session-1",
      offeringId: "offering-1",
      schemaVersion: "1",
      traceparent: "00-trace-span-01",
      tracestate: "vendor=foo",
      listId: "list-1",
    });

    expect(msg.headers.interchangeTenantId).toBe("tenant-1");
    expect(msg.headers.interchangeAgentId).toBe("agent-1");
    expect(msg.headers.interchangeSessionId).toBe("session-1");
    expect(msg.headers.interchangeOfferingId).toBe("offering-1");
    expect(msg.headers.interchangeSchemaVersion).toBe("1");
    expect(msg.headers.traceparent).toBe("00-trace-span-01");
    expect(msg.headers.tracestate).toBe("vendor=foo");
    expect(msg.headers.listId).toBe("list-1");
  });

  test("ref override merges with synthetic defaults", () => {
    const a = createInboundMessage({
      from: FROM,
      to: TO,
      content: "x",
      ref: { uid: 42 },
    });
    expect(a.ref).toEqual({ uid: 42, mailbox: "INBOX" });

    const b = createInboundMessage({
      from: FROM,
      to: TO,
      content: "x",
      ref: { mailbox: "Trash" },
    });
    expect(b.ref).toEqual({ uid: 1, mailbox: "Trash" });
  });

  test("Date input is normalised to an ISO string", () => {
    const fixed = new Date("2026-01-02T03:04:05.000Z");
    const msg = createInboundMessage({
      from: FROM,
      to: TO,
      content: "x",
      date: fixed,
    });
    expect(msg.headers.date).toBe("2026-01-02T03:04:05.000Z");
  });

  test("flags and signatureStatus overrides are respected", () => {
    const msg = createInboundMessage({
      from: FROM,
      to: TO,
      content: "x",
      flags: ["\\Seen"],
      signatureStatus: "valid",
    });
    expect(msg.flags).toEqual(["\\Seen"]);
    expect(msg.signatureStatus).toBe("valid");
  });

  test("cc accepts a string and normalises to an array", () => {
    const msg = createInboundMessage({
      from: FROM,
      to: TO,
      content: "x",
      cc: "watcher@example.com",
    });
    expect(msg.headers.cc).toEqual(["watcher@example.com"]);
  });

  test("control-frame (no content, no payload) is allowed", () => {
    const msg = createInboundMessage({ from: FROM, to: TO });
    expect(msg.content).toBeUndefined();
    expect(msg.payload).toBeUndefined();
    expect(msg.flags).toEqual([]);
  });

  test("explicit interchangeType is allowed for content-only messages", () => {
    const msg = createInboundMessage({
      from: FROM,
      to: TO,
      content: "join",
      interchangeType: "conversation.join",
    });
    expect(msg.headers.interchangeType).toBe("conversation.join");
  });

  describe("validation", () => {
    test("throws when both content and payload are set", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          payload: { type: "offering.request", body: {} },
        }),
      ).toThrow(/`content` and `payload` are mutually exclusive/);
    });

    test("throws when from is empty", () => {
      expect(() =>
        createInboundMessage({ from: "", to: TO, content: "x" }),
      ).toThrow(/`from` must be a non-empty string/);
    });

    test("throws when to is an empty array", () => {
      expect(() =>
        createInboundMessage({ from: FROM, to: [], content: "x" }),
      ).toThrow(/`to` must contain at least one recipient address/);
    });

    test("throws when to contains an empty string", () => {
      expect(() =>
        createInboundMessage({ from: FROM, to: [""], content: "x" }),
      ).toThrow(/`to\[0\]` must be a non-empty string/);
    });

    test("throws when payload.type is not a known InterchangeType", () => {
      expect(() =>
        callInboundUnsafe({
          from: FROM,
          to: TO,
          payload: {
            type: "not.a.real.type",
            body: {},
          },
        }),
      ).toThrow(/`payload.type` is not a valid InterchangeType/);
    });

    test("throws when interchangeType conflicts with payload.type", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          payload: { type: "offering.request", body: {} },
          interchangeType: "payment.required",
        }),
      ).toThrow(
        /`interchangeType` \(payment\.required\) conflicts with `payload\.type` \(offering\.request\)/,
      );
    });

    test("throws when messageId lacks angle brackets", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          messageId: "missing-brackets@example.com",
        }),
      ).toThrow(/`messageId` must be an RFC 2822 message identifier/);
    });

    test("throws when inReplyTo lacks angle brackets", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          inReplyTo: "bare",
        }),
      ).toThrow(/`inReplyTo` must be an RFC 2822 message identifier/);
    });

    test("throws when references contains a malformed entry", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          references: ["<one@example.com>", "bad"],
        }),
      ).toThrow(/`references\[1\]` must be an RFC 2822 message identifier/);
    });

    test("throws when references is an empty array", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          references: [],
        }),
      ).toThrow(/`references`, when provided, must contain at least one entry/);
    });

    test("throws when date string is not parseable", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          date: "not-a-date",
        }),
      ).toThrow(/`date` is not a parseable date string/);
    });

    test("throws when date is an Invalid Date instance", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          date: new Date("nope"),
        }),
      ).toThrow(/`date` is an Invalid Date/);
    });

    test("throws when optional string fields are empty", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          correlationId: "",
        }),
      ).toThrow(/`correlationId`, when provided, must be a non-empty string/);
    });

    test("throws when flags contains an empty string", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          flags: ["\\Seen", ""],
        }),
      ).toThrow(/`flags\[1\]` must be a non-empty string/);
    });

    test("throws when ref.mailbox is empty", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          ref: { mailbox: "" },
        }),
      ).toThrow(/`ref\.mailbox`.*must be a non-empty string/);
    });

    test("throws when from lacks an @ domain", () => {
      expect(() =>
        createInboundMessage({
          from: "alice",
          to: TO,
          content: "x",
        }),
      ).toThrow(/`from` must be an RFC 5322 address/);
    });

    test("throws when a `to` entry lacks an @ domain", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: ["agent@example.com", "loose-string"],
          content: "x",
        }),
      ).toThrow(/`to\[1\]` must be an RFC 5322 address/);
    });

    test("throws when messageId has angle brackets but no @ host", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          messageId: "<no-at-sign>",
        }),
      ).toThrow(/`messageId` must be an RFC 2822 message identifier/);
    });

    test("throws when payload.type is a conversation type", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          payload: { type: "conversation.message", body: {} },
        }),
      ).toThrow(/conversation types must use `content` instead of `payload`/);
    });

    test("throws when payload.body is null", () => {
      expect(() =>
        callInboundUnsafe({
          from: FROM,
          to: TO,
          payload: { type: "offering.request", body: null },
        }),
      ).toThrow(/`payload\.body` must be a plain object/);
    });

    test("throws when payload.body is an array", () => {
      expect(() =>
        callInboundUnsafe({
          from: FROM,
          to: TO,
          payload: { type: "offering.request", body: [] },
        }),
      ).toThrow(/`payload\.body` must be a plain object/);
    });

    test("throws when payload.version is not a string", () => {
      expect(() =>
        callInboundUnsafe({
          from: FROM,
          to: TO,
          payload: { type: "offering.request", body: {}, version: 7 },
        }),
      ).toThrow(/`payload\.version`.*must be a non-empty string/);
    });

    test("throws when ref.uid is not an integer", () => {
      expect(() =>
        callInboundUnsafe({
          from: FROM,
          to: TO,
          content: "x",
          ref: { uid: "not-a-number" },
        }),
      ).toThrow(/`ref\.uid`.*must be a positive integer/);
    });

    test("throws when content is the empty string", () => {
      expect(() =>
        createInboundMessage({ from: FROM, to: TO, content: "" }),
      ).toThrow(/`content`, when provided, must be a non-empty string/);
    });

    test("throws when signatureStatus is not a recognised value", () => {
      expect(() =>
        callInboundUnsafe({
          from: FROM,
          to: TO,
          content: "x",
          signatureStatus: "bogus",
        }),
      ).toThrow(/`signatureStatus` is not a recognised SignatureStatus/);
    });

    test("throws when ref.uid is zero", () => {
      expect(() =>
        createInboundMessage({
          from: FROM,
          to: TO,
          content: "x",
          ref: { uid: 0 },
        }),
      ).toThrow(/`ref\.uid`.*must be a positive integer/);
    });
  });
});

describe("createOutboundMessage", () => {
  test("conversation content with required fields", () => {
    const msg = createOutboundMessage({
      to: TO,
      type: "conversation.message",
      content: "hi",
    });
    expect(msg).toEqual({
      to: TO,
      type: "conversation.message",
      content: "hi",
    });
  });

  test("structured payload with summary and attachments", () => {
    const attachments: MessageAttachment[] = [
      {
        name: "x.bin",
        contentType: "application/octet-stream",
        data: new Uint8Array([0]),
      },
    ];
    const msg = createOutboundMessage({
      to: TO,
      type: "offering.request",
      payload: { offeringId: "code-review", parameters: {} },
      summary: "Code review request",
      attachments,
    });
    expect(msg.type).toBe("offering.request");
    expect(msg.payload).toEqual({ offeringId: "code-review", parameters: {} });
    expect(msg.summary).toBe("Code review request");
    expect(msg.attachments).toEqual(attachments);
    expect(msg.content).toBeUndefined();
  });

  test("to/cc are passed through without normalisation", () => {
    const single = createOutboundMessage({
      to: "single@example.com",
      type: "conversation.message",
      content: "hi",
      cc: "watcher@example.com",
    });
    expect(single.to).toBe("single@example.com");
    expect(single.cc).toBe("watcher@example.com");

    const many = createOutboundMessage({
      to: ["a@example.com", "b@example.com"],
      type: "conversation.message",
      content: "hi",
      cc: ["c@example.com"],
    });
    expect(many.to).toEqual(["a@example.com", "b@example.com"]);
    expect(many.cc).toEqual(["c@example.com"]);
  });

  test("threading fields are wired through", () => {
    const msg = createOutboundMessage({
      to: TO,
      type: "approval.granted",
      payload: { decision: "approved" },
      inReplyTo: "<request@example.com>",
      correlationId: "corr-abc",
      sessionId: "session-1",
      tenantId: "tenant-1",
    });
    expect(msg.inReplyTo).toBe("<request@example.com>");
    expect(msg.correlationId).toBe("corr-abc");
    expect(msg.sessionId).toBe("session-1");
    expect(msg.tenantId).toBe("tenant-1");
  });

  describe("validation", () => {
    test("throws when type is invalid", () => {
      expect(() =>
        callOutboundUnsafe({
          to: TO,
          type: "not.real",
          content: "x",
        }),
      ).toThrow(/`type` is not a valid InterchangeType/);
    });

    test("throws when content and payload are both set", () => {
      expect(() =>
        createOutboundMessage({
          to: TO,
          type: "offering.request",
          content: "x",
          payload: { offeringId: "x" },
        }),
      ).toThrow(/`content` and `payload` are mutually exclusive/);
    });

    test("throws when to is empty string", () => {
      expect(() =>
        createOutboundMessage({
          to: "",
          type: "conversation.message",
          content: "x",
        }),
      ).toThrow(/`to` must be a non-empty string/);
    });

    test("throws when to is empty array", () => {
      expect(() =>
        createOutboundMessage({
          to: [],
          type: "conversation.message",
          content: "x",
        }),
      ).toThrow(/`to` must contain at least one recipient address/);
    });

    test("throws when to array has empty entry", () => {
      expect(() =>
        createOutboundMessage({
          to: ["valid@example.com", ""],
          type: "conversation.message",
          content: "x",
        }),
      ).toThrow(/`to\[1\]` must be a non-empty string/);
    });

    test("throws when cc array has empty entry", () => {
      expect(() =>
        createOutboundMessage({
          to: TO,
          type: "conversation.message",
          content: "x",
          cc: [""],
        }),
      ).toThrow(/`cc\[0\]` must be a non-empty string/);
    });

    test("throws when inReplyTo lacks angle brackets", () => {
      expect(() =>
        createOutboundMessage({
          to: TO,
          type: "conversation.message",
          content: "x",
          inReplyTo: "bare-id",
        }),
      ).toThrow(/`inReplyTo` must be an RFC 2822 message identifier/);
    });

    test("throws when summary is empty", () => {
      expect(() =>
        createOutboundMessage({
          to: TO,
          type: "offering.request",
          payload: { x: 1 },
          summary: "",
        }),
      ).toThrow(/`summary`, when provided, must be a non-empty string/);
    });

    test("throws when type is conversation.* with payload", () => {
      expect(() =>
        createOutboundMessage({
          to: TO,
          type: "conversation.message",
          payload: { x: 1 },
        }),
      ).toThrow(
        /conversation `type` conversation\.message must use `content` instead of `payload`/,
      );
    });

    test("throws when type is non-conversation with content", () => {
      expect(() =>
        createOutboundMessage({
          to: TO,
          type: "offering.request",
          content: "hi",
        }),
      ).toThrow(
        /non-conversation `type` offering\.request must use `payload` instead of `content`/,
      );
    });

    test("throws when payload is null", () => {
      expect(() =>
        callOutboundUnsafe({
          to: TO,
          type: "offering.request",
          payload: null,
        }),
      ).toThrow(/`payload` must be a plain object/);
    });

    test("throws when payload is an array", () => {
      expect(() =>
        callOutboundUnsafe({
          to: TO,
          type: "offering.request",
          payload: [1, 2, 3],
        }),
      ).toThrow(/`payload` must be a plain object/);
    });

    test("throws when content is the empty string", () => {
      expect(() =>
        createOutboundMessage({
          to: TO,
          type: "conversation.message",
          content: "",
        }),
      ).toThrow(/`content`, when provided, must be a non-empty string/);
    });

    test("throws when conversation type is missing content", () => {
      expect(() =>
        createOutboundMessage({
          to: TO,
          type: "conversation.message",
        }),
      ).toThrow(/conversation `type` conversation\.message requires `content`/);
    });

    test("throws when non-conversation type is missing payload", () => {
      expect(() =>
        createOutboundMessage({
          to: TO,
          type: "offering.request",
        }),
      ).toThrow(/non-conversation `type` offering\.request requires `payload`/);
    });

    test("throws when a to entry lacks an @ domain", () => {
      expect(() =>
        createOutboundMessage({
          to: ["agent@example.com", "loose-string"],
          type: "conversation.message",
          content: "x",
        }),
      ).toThrow(/`to\[1\]` must be an RFC 5322 address/);
    });
  });
});

// Compile-time guards: the public return types of the builders match
// InboundMessage / OutboundMessage exactly. Variables are unused at runtime;
// referenced via the `_` prefix to satisfy lint.
const _inbound: InboundMessage = createInboundMessage({
  from: FROM,
  to: TO,
  content: "x",
});
const _outbound: OutboundMessage = createOutboundMessage({
  to: TO,
  type: "conversation.message",
  content: "x",
});
void _inbound;
void _outbound;
