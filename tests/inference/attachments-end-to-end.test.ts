// End-to-end attachment pipe, parameterized over every shipped adapter.
//
// A conversation message with an image attachment is assembled exactly as
// the session service assembles it, delivered through the real in-memory
// transport, parsed back by the real `fetchFull` (NOT a hand-built
// InboundMessage), turned into a ConversationTurn by the real
// createInboundTurn, and marshaled by each real provider adapter. The test
// asserts the original image bytes survive all the way to each adapter's
// wire body.
//
// This is the only place the whole mime -> fetchFull -> turn -> adapter pipe
// runs against real adapters at once: a future @intx/mime change that breaks
// attachment assembly or parsing fails here for every adapter, which
// per-adapter unit tests (mocking the upstream surface) cannot catch.

import { describe, test, expect, beforeAll } from "bun:test";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@intx/mime";
import { createInMemoryTransport } from "@intx/mail-memory";
import { createInboundTurn } from "@intx/inference";
import {
  createAnthropicAdapter,
  createOpenAIAdapter,
  createGoogleGenAIAdapter,
} from "@intx/inference/providers";
import type { ProviderAdapter } from "@intx/inference";
import { base64Encode } from "@intx/types";
import type {
  ConversationTurn,
  InboundMessage,
  LastCycleSource,
  MessageAttachment,
} from "@intx/types/runtime";

const SENDER = "alpha@test.interchange";
const AGENT = "beta@test.interchange";

const imageBytes = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
]);
const expectedBase64 = base64Encode(imageBytes);

function conversationHeaders(): MessageHeaders {
  return {
    from: SENDER,
    to: [AGENT],
    cc: undefined,
    date: new Date("2026-01-15T12:00:00Z"),
    messageId: "<e2e-1@test.interchange>",
    subject: undefined,
    inReplyTo: undefined,
    references: undefined,
    mimeVersion: "1.0",
    interchangeType: "conversation.message",
    interchangeCorrelationId: undefined,
    interchangeTenantId: undefined,
    interchangeAgentId: undefined,
    interchangeSessionId: undefined,
    interchangeOfferingId: undefined,
    interchangeSchemaVersion: undefined,
    traceparent: undefined,
    tracestate: undefined,
  };
}

// Provider adapters and a token that proves the image was marshaled into the
// provider's own image shape (Anthropic image block, OpenAI image_url,
// Google inlineData).
function source(name: string, model: string): LastCycleSource {
  return { sourceId: `e2e-${name}`, provider: name, model };
}

const adapters: {
  name: string;
  adapter: ProviderAdapter;
  model: string;
  marker: string;
}[] = [
  {
    name: "anthropic",
    adapter: createAnthropicAdapter(
      source("anthropic", "claude-3-5-sonnet-20241022"),
    ),
    model: "claude-3-5-sonnet-20241022",
    marker: '"type":"image"',
  },
  {
    name: "openai",
    adapter: createOpenAIAdapter(source("openai", "gpt-5.5")),
    model: "gpt-5.5",
    marker: '"image_url"',
  },
  {
    name: "google-genai",
    adapter: createGoogleGenAIAdapter(
      source("google-genai", "gemini-2.5-flash"),
    ),
    model: "gemini-2.5-flash",
    marker: '"inlineData"',
  },
];

// Assemble a conversation message carrying one attachment exactly as the
// session service does, deliver it through the in-memory transport, and
// reconstruct the inbound message and turn with the real fetchFull and
// createInboundTurn.
async function deliverAndBuild(
  text: string,
  attachment: MessageAttachment,
): Promise<{ inbound: InboundMessage; turn: ConversationTurn }> {
  const transport = createInMemoryTransport();
  const senderCrypto = createEd25519Crypto(await generateKeyPair());
  const agentCrypto = createEd25519Crypto(await generateKeyPair());
  transport.register(SENDER, senderCrypto);
  transport.register(AGENT, agentCrypto);

  const content = assembleSignedContent({
    kind: "conversation",
    text,
    attachments: [attachment],
  });
  const signature = await createDetachedSignatureFromProvider(
    content,
    senderCrypto,
  );
  const raw = assembleMessage(conversationHeaders(), content, signature);

  transport.deliver(AGENT, raw);
  const agentTransport = transport.getTransportFor(AGENT);
  const refs = await agentTransport.search("INBOX", {});
  const ref = refs[0];
  if (refs.length !== 1 || ref === undefined) {
    throw new Error("expected exactly one message");
  }
  const inbound = await agentTransport.fetchFull(ref);
  const built = createInboundTurn(inbound);
  if (built === null) throw new Error("expected a turn");
  return { inbound, turn: built };
}

describe("attachment end-to-end: assemble -> fetchFull -> turn -> adapter", () => {
  let inbound: InboundMessage;
  let turn: ConversationTurn;

  beforeAll(async () => {
    ({ inbound, turn } = await deliverAndBuild("look at this", {
      name: "shot.png",
      contentType: "image/png",
      data: imageBytes,
    }));
  });

  test("fetchFull populates the attachment bytes (real parse)", () => {
    expect(inbound.signatureStatus).toBe("valid");
    expect(inbound.attachments).toEqual([
      {
        name: "shot.png",
        contentType: "image/png",
        data: imageBytes,
      },
    ]);
  });

  test("the turn carries a base64 ImageBlock with the original bytes", () => {
    expect(turn).toMatchObject({
      role: "user",
      content: [
        { type: "text", text: `[From: ${SENDER}]\n\nlook at this` },
        {
          type: "image",
          source: {
            kind: "base64",
            mimeType: "image/png",
            data: expectedBase64,
          },
        },
      ],
    });
  });

  for (const { name, adapter, model, marker } of adapters) {
    test(`${name} adapter marshals the image bytes onto the wire`, () => {
      const req = adapter.buildRequest([turn], model, {});
      // The original bytes reach the provider wire as base64.
      expect(req.body).toContain(expectedBase64);
      // ...inside the provider's own image shape.
      expect(req.body).toContain(marker);
    });
  }
});

// A PDF rides the same pipe as a DocumentBlock. Adapter document support is
// uneven (Anthropic and Google marshal it; OpenAI does not yet emit document
// blocks and rejects loudly) — so this asserts each adapter's actual behavior
// rather than a uniform success.
describe("attachment end-to-end: pdf document", () => {
  const pdfBytes = new Uint8Array([
    0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e, 0x34,
  ]); // "%PDF-1.4"
  const pdfBase64 = base64Encode(pdfBytes);
  let inbound: InboundMessage;
  let turn: ConversationTurn;

  beforeAll(async () => {
    ({ inbound, turn } = await deliverAndBuild("see the report", {
      name: "report.pdf",
      contentType: "application/pdf",
      data: pdfBytes,
    }));
  });

  test("fetchFull populates the pdf bytes (real parse)", () => {
    expect(inbound.attachments).toEqual([
      { name: "report.pdf", contentType: "application/pdf", data: pdfBytes },
    ]);
  });

  test("the turn carries a base64 DocumentBlock with the original pdf bytes", () => {
    expect(turn).toMatchObject({
      role: "user",
      content: [
        { type: "text", text: `[From: ${SENDER}]\n\nsee the report` },
        {
          type: "document",
          source: {
            kind: "base64",
            mimeType: "application/pdf",
            data: pdfBase64,
          },
        },
      ],
    });
  });

  test("anthropic marshals the pdf as a document block", () => {
    const adapter = createAnthropicAdapter(
      source("anthropic", "claude-3-5-sonnet-20241022"),
    );
    const req = adapter.buildRequest([turn], "claude-3-5-sonnet-20241022", {});
    expect(req.body).toContain(pdfBase64);
    expect(req.body).toContain('"type":"document"');
  });

  test("google marshals the pdf as inline data", () => {
    const adapter = createGoogleGenAIAdapter(
      source("google-genai", "gemini-2.5-flash"),
    );
    const req = adapter.buildRequest([turn], "gemini-2.5-flash", {});
    expect(req.body).toContain(pdfBase64);
    expect(req.body).toContain('"inlineData"');
  });

  test("openai rejects document blocks loudly (not yet wired)", () => {
    const adapter = createOpenAIAdapter(source("openai", "gpt-5.5"));
    expect(() => adapter.buildRequest([turn], "gpt-5.5", {})).toThrow(
      /document/i,
    );
  });
});
