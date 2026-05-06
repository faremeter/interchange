import { describe, expect, test } from "bun:test";

import {
  extractBodyText,
  formatAddress,
  isAgentAddress,
  mailDeliveryToEvent,
  mailToEvent,
  parseFromHeader,
  resolveAgentAddress,
  resolveAgentRecipient,
  shouldShowMail,
  turnToEvent,
} from "./transforms";

const AGENT_ADDR = "ins_abc123@tenant.example";
const HUMAN_ADDR = "usr_alice@tenant.example";

describe("shouldShowMail", () => {
  test("inbound mail is shown regardless of recipient address", () => {
    expect(
      shouldShowMail({
        direction: "inbound",
        to: [{ name: null, email: AGENT_ADDR }],
      }),
    ).toBe(true);
    expect(
      shouldShowMail({
        direction: "inbound",
        to: [{ name: null, email: HUMAN_ADDR }],
      }),
    ).toBe(true);
  });

  test("outbound mail to another agent is suppressed", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
        to: [{ name: "Other Agent", email: AGENT_ADDR }],
      }),
    ).toBe(false);
  });

  test("outbound inter-agent mail is suppressed", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
        to: [{ name: "Agent B", email: "ins_abc@example.com" }],
      }),
    ).toBe(false);
  });

  test("outbound connector reply to human is suppressed", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
        to: [{ name: "Alice", email: HUMAN_ADDR }],
      }),
    ).toBe(false);
  });

  test("outbound mail with no recipients is suppressed", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
      }),
    ).toBe(false);
  });

  test("outbound mail with empty recipients is suppressed", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
        to: [],
      }),
    ).toBe(false);
  });
});

describe("parseFromHeader", () => {
  test("extracts display name from RFC format", () => {
    expect(parseFromHeader('"Alice Smith" <alice@example.com>')).toBe(
      "Alice Smith",
    );
  });

  test("falls back to local part when no display name", () => {
    expect(parseFromHeader("alice@example.com")).toBe("alice");
  });
});

describe("extractBodyText", () => {
  test("extracts text from bodyValues using textBody reference", () => {
    const bodyValues = { "part-1": { value: "Hello world" } };
    const textBody = [{ partId: "part-1" }];
    expect(extractBodyText(bodyValues, textBody)).toBe("Hello world");
  });

  test("returns empty string when textBody is empty", () => {
    expect(extractBodyText({}, [])).toBe("");
  });

  test("returns empty string when bodyValue has no value field", () => {
    const bodyValues = { "part-1": { other: "stuff" } };
    const textBody = [{ partId: "part-1" }];
    expect(extractBodyText(bodyValues, textBody)).toBe("");
  });
});

describe("formatAddress", () => {
  test("returns name when present", () => {
    expect(formatAddress({ name: "Alice", email: "a@b.com" })).toBe("Alice");
  });

  test("falls back to email when name is null", () => {
    expect(formatAddress({ name: null, email: "a@b.com" })).toBe("a@b.com");
  });
});

describe("isAgentAddress", () => {
  test("returns true for ins_ prefixed addresses", () => {
    expect(isAgentAddress(AGENT_ADDR)).toBe(true);
  });

  test("returns false for non-agent addresses", () => {
    expect(isAgentAddress(HUMAN_ADDR)).toBe(false);
  });
});

describe("resolveAgentAddress", () => {
  test("resolves agent address to instanceId and label", () => {
    const result = resolveAgentAddress({ name: "Bot", email: AGENT_ADDR });
    expect(result).toEqual({ instanceId: "ins_abc123", label: "Bot" });
  });

  test("returns null for non-agent address", () => {
    expect(
      resolveAgentAddress({ name: "Alice", email: HUMAN_ADDR }),
    ).toBeNull();
  });
});

describe("resolveAgentRecipient", () => {
  test("resolves first recipient", () => {
    const result = resolveAgentRecipient([{ name: "Bot", email: AGENT_ADDR }]);
    expect(result).toEqual({ instanceId: "ins_abc123", label: "Bot" });
  });

  test("returns null for empty recipients", () => {
    expect(resolveAgentRecipient([])).toBeNull();
  });
});

describe("mailToEvent", () => {
  const baseMail = {
    id: "mail_1",
    sessionId: "sess_1",
    instanceId: "ins_abc123",
    status: "delivered" as const,
    receivedAt: "2024-01-01T00:00:00Z",
    from: [{ name: "Alice", email: HUMAN_ADDR }],
    to: [{ name: null, email: AGENT_ADDR }],
    subject: "Hello",
    sentAt: null,
    bodyValues: { p1: { value: "Hi there" } },
    textBody: [{ partId: "p1", type: "text/plain" }],
    htmlBody: [],
    attachments: [],
    headers: {},
  };

  test("inbound mail maps to user role", () => {
    const event = mailToEvent({ ...baseMail, direction: "inbound" });
    expect(event.kind).toBe("mail");
    if (event.kind !== "mail") return;
    expect(event.role).toBe("user");
    expect(event.id).toBe("mail_1");
    expect(event.content).toBe("Hi there");
    expect(event.timestamp).toBe("2024-01-01T00:00:00Z");
  });

  test("outbound mail maps to assistant role", () => {
    const event = mailToEvent({
      ...baseMail,
      direction: "outbound",
      from: [{ name: null, email: AGENT_ADDR }],
      to: [{ name: "Alice", email: HUMAN_ADDR }],
    });
    expect(event.kind).toBe("mail");
    if (event.kind !== "mail") return;
    expect(event.role).toBe("assistant");
  });

  test("uses unknown sender when from is empty", () => {
    const event = mailToEvent({ ...baseMail, direction: "inbound", from: [] });
    expect(event.kind).toBe("mail");
    if (event.kind !== "mail") return;
    expect(event.sender).toEqual({ name: null, email: "unknown" });
  });

  test("passes through attachments", () => {
    const attachment = {
      blobId: "blob_1",
      name: "file.txt",
      type: "text/plain",
      size: 100,
    };
    const event = mailToEvent({
      ...baseMail,
      direction: "inbound",
      attachments: [attachment],
    });
    expect(event.kind).toBe("mail");
    if (event.kind !== "mail") return;
    expect(event.attachments).toEqual([attachment]);
  });
});

describe("mailDeliveryToEvent", () => {
  const baseDelivery = {
    id: "mail_2",
    direction: "inbound" as const,
    from: [{ name: "Bob", email: HUMAN_ADDR }],
    to: [{ name: null, email: AGENT_ADDR }],
    bodyValues: { p1: { value: "SSE body" } },
    textBody: [{ partId: "p1", type: "text/plain" }],
    receivedAt: "2024-01-02T00:00:00Z",
  };

  test("inbound delivery maps to user role", () => {
    const event = mailDeliveryToEvent(baseDelivery);
    expect(event.kind).toBe("mail");
    if (event.kind !== "mail") return;
    expect(event.role).toBe("user");
    expect(event.content).toBe("SSE body");
    expect(event.id).toBe("mail_2");
    expect(event.timestamp).toBe("2024-01-02T00:00:00Z");
  });

  test("outbound delivery maps to assistant role", () => {
    const event = mailDeliveryToEvent({
      ...baseDelivery,
      direction: "outbound",
    });
    expect(event.kind).toBe("mail");
    if (event.kind !== "mail") return;
    expect(event.role).toBe("assistant");
  });

  test("uses unknown sender when from is absent", () => {
    const { from: _from, ...withoutFrom } = baseDelivery;
    const event = mailDeliveryToEvent(withoutFrom);
    expect(event.kind).toBe("mail");
    if (event.kind !== "mail") return;
    expect(event.sender).toEqual({ name: null, email: "unknown" });
  });

  test("maps optional attachments", () => {
    const event = mailDeliveryToEvent({
      ...baseDelivery,
      attachments: [
        {
          blobId: "blob_2",
          name: "doc.pdf",
          type: "application/pdf",
          size: 512,
        },
      ],
    });
    expect(event.kind).toBe("mail");
    if (event.kind !== "mail") return;
    expect(event.attachments).toEqual([
      { blobId: "blob_2", name: "doc.pdf", type: "application/pdf", size: 512 },
    ]);
  });

  test("produces empty attachments when none provided", () => {
    const event = mailDeliveryToEvent(baseDelivery);
    expect(event.kind).toBe("mail");
    if (event.kind !== "mail") return;
    expect(event.attachments).toEqual([]);
  });
});

describe("turnToEvent", () => {
  const baseTurn = {
    id: "turn_1",
    sessionId: "sess_1",
    instanceId: "ins_abc123",
    model: "gpt-4",
    status: "completed" as const,
    startedAt: "2024-01-03T00:00:00Z",
    endedAt: "2024-01-03T00:00:01Z",
    parts: [
      {
        id: "part_1",
        type: "text" as const,
        content: "Hello from assistant",
        metadata: null,
        ordinal: 0,
      },
    ],
  };

  test("converts a text turn to an event", () => {
    const event = turnToEvent(baseTurn);
    expect(event).not.toBeNull();
    if (!event) return;
    expect(event.kind).toBe("turn");
    if (event.kind !== "turn") return;
    expect(event.turnId).toBe("turn_1");
    expect(event.content).toBe("Hello from assistant");
    expect(event.timestamp).toBe("2024-01-03T00:00:00Z");
    expect(event.isError).toBeUndefined();
  });

  test("returns null for turns with no displayable parts", () => {
    const event = turnToEvent({
      ...baseTurn,
      parts: [
        {
          id: "part_1",
          type: "reasoning" as const,
          content: "some internal reasoning",
          metadata: null,
          ordinal: 0,
        },
      ],
    });
    expect(event).toBeNull();
  });

  test("sets isError and fallback content for failed turns", () => {
    const event = turnToEvent({
      ...baseTurn,
      status: "failed",
      parts: [
        {
          id: "part_1",
          type: "error" as const,
          content: "Something went wrong",
          metadata: { category: "timeout" },
          ordinal: 0,
        },
      ],
    });
    expect(event).not.toBeNull();
    if (!event) return;
    expect(event.kind).toBe("turn");
    if (event.kind !== "turn") return;
    expect(event.isError).toBe(true);
    expect(event.errors).toEqual([
      { category: "timeout", message: "Something went wrong" },
    ]);
  });

  test("collects tool errors with resolved call names", () => {
    const event = turnToEvent({
      ...baseTurn,
      parts: [
        {
          id: "part_call",
          type: "tool" as const,
          content: null,
          metadata: { kind: "call", callId: "call_1", name: "search" },
          ordinal: 0,
        },
        {
          id: "part_result",
          type: "tool" as const,
          content: null,
          metadata: {
            kind: "result",
            callId: "call_1",
            isError: true,
            content: "Tool failure",
          },
          ordinal: 1,
        },
        {
          id: "part_text",
          type: "text" as const,
          content: "I had an error",
          metadata: null,
          ordinal: 2,
        },
      ],
    });
    expect(event).not.toBeNull();
    if (!event) return;
    expect(event.kind).toBe("turn");
    if (event.kind !== "turn") return;
    expect(event.toolErrors).toEqual([
      { name: "search", content: "Tool failure" },
    ]);
  });

  test("concatenates multiple text parts", () => {
    const event = turnToEvent({
      ...baseTurn,
      parts: [
        {
          id: "p1",
          type: "text" as const,
          content: "Hello",
          metadata: null,
          ordinal: 0,
        },
        {
          id: "p2",
          type: "text" as const,
          content: " world",
          metadata: null,
          ordinal: 1,
        },
      ],
    });
    expect(event).not.toBeNull();
    if (!event) return;
    expect(event.kind).toBe("turn");
    if (event.kind !== "turn") return;
    expect(event.content).toBe("Hello world");
  });
});
