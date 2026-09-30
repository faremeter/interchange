import { describe, test, expect } from "bun:test";
import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { createInMemoryTransport } from "@intx/mail-memory";
import type {
  BodyStructure,
  InboundMessage,
  Mailbox,
  MailboxEvent,
  MailboxStatus,
  MessageHeaders,
  MessagePart,
  MessageRef,
  MessageTransport,
  OutboundMessage,
  SearchQuery,
  SendReceipt,
  SyncResult,
  SyncState,
  Thread,
  ListInfo,
  ToolCall,
  ToolResult,
  Unsubscribe,
} from "@intx/types/runtime";
import { MessageTransportError } from "@intx/types/runtime";
import {
  createRuntimeCapabilities,
  type RuntimeCapabilities,
} from "@intx/types/runtime-capabilities";

import { createMailTools, type MailToolErrorCode } from "./index";
import {
  makeMailExpungeHandler,
  makeMailFlagHandler,
  makeMailReadHandler,
  makeMailReplyHandler,
  makeMailSearchHandler,
  makeMailSendHandler,
  makeMailWaitHandler,
  type ToolHandler,
} from "./handlers";

// ---------------------------------------------------------------------------
// Mock transport — minimal MessageTransport with hooks for sent-message
// inspection, watch firing, and message enqueueing.
// ---------------------------------------------------------------------------

type WatchCallback = (event: MailboxEvent) => void;

type MockTransport = MessageTransport & {
  getSentMessages(): OutboundMessage[];
  fireWatch(event: MailboxEvent): void;
  enqueueMessage(ref: MessageRef, msg: InboundMessage): void;
  setSearchResult(refs: MessageRef[]): void;
  getFlagCalls(): { op: "set" | "clear"; ref: MessageRef; flags: string[] }[];
  getExpungeCalls(): string[];
  setExpungeResult(uids: number[]): void;
  failMutations(err: Error | null): void;
};

function makeMockTransport(): MockTransport {
  const sentMessages: OutboundMessage[] = [];
  const watchCallbacks: WatchCallback[] = [];
  const messageStore = new Map<string, InboundMessage>();
  let searchResult: MessageRef[] = [];
  const flagCalls: { op: "set" | "clear"; ref: MessageRef; flags: string[] }[] =
    [];
  const expungeCalls: string[] = [];
  let expungeUids: number[] = [];
  let mutationError: Error | null = null;

  function refKey(ref: MessageRef): string {
    return `${ref.mailbox}:${String(ref.uid)}`;
  }

  const transport: MockTransport = {
    getSentMessages() {
      return sentMessages;
    },
    fireWatch(event: MailboxEvent): void {
      for (const cb of watchCallbacks) {
        cb(event);
      }
    },
    enqueueMessage(ref: MessageRef, msg: InboundMessage): void {
      messageStore.set(refKey(ref), msg);
    },
    setSearchResult(refs: MessageRef[]): void {
      searchResult = refs;
    },
    getFlagCalls() {
      return flagCalls;
    },
    getExpungeCalls() {
      return expungeCalls;
    },
    setExpungeResult(uids: number[]): void {
      expungeUids = uids;
    },
    failMutations(err: Error | null): void {
      mutationError = err;
    },

    async send(message: OutboundMessage): Promise<SendReceipt> {
      sentMessages.push(message);
      return {
        messageId: `<msg-${String(Date.now())}@test>`,
        status: "delivered",
      };
    },

    async append(
      mailbox: string,
      message: InboundMessage,
    ): Promise<MessageRef> {
      const ref = { uid: 999, mailbox };
      messageStore.set(refKey(ref), message);
      return ref;
    },

    async listMailboxes(): Promise<Mailbox[]> {
      return [{ name: "INBOX", role: "\\Inbox" }];
    },

    async createMailbox(name: string): Promise<Mailbox> {
      return { name };
    },

    async deleteMailbox(): Promise<void> {
      /* noop */
    },

    async getMailboxStatus(): Promise<MailboxStatus> {
      return {
        total: 0,
        unseen: 0,
        recent: 0,
        uidNext: 1,
        uidValidity: 1,
        highestModSeq: 0,
      };
    },

    async search(_mailbox: string, _query: SearchQuery): Promise<MessageRef[]> {
      return searchResult;
    },

    async thread(): Promise<Thread[]> {
      return [];
    },

    async fetchHeaders(ref: MessageRef): Promise<MessageHeaders> {
      const msg = messageStore.get(refKey(ref));
      if (msg !== undefined) return msg.headers;
      return {
        from: "sender@test",
        to: ["agent@test"],
        date: new Date().toISOString(),
        messageId: `<${String(ref.uid)}@test>`,
      };
    },

    async fetchStructure(): Promise<BodyStructure> {
      return { contentType: "multipart/signed" };
    },

    async fetchPart(): Promise<MessagePart> {
      return { contentType: "text/plain", content: new Uint8Array() };
    },

    async fetchFull(ref: MessageRef): Promise<InboundMessage> {
      const stored = messageStore.get(refKey(ref));
      if (stored !== undefined) return stored;
      return {
        ref,
        headers: {
          from: "sender@test",
          to: ["agent@test"],
          date: new Date().toISOString(),
          messageId: `<${String(ref.uid)}@test>`,
        },
        flags: [],
        content: "hello",
        signatureStatus: "missing",
      };
    },

    async setFlags(ref: MessageRef, flags: string[]): Promise<void> {
      if (mutationError !== null) throw mutationError;
      flagCalls.push({ op: "set", ref, flags });
    },

    async clearFlags(ref: MessageRef, flags: string[]): Promise<void> {
      if (mutationError !== null) throw mutationError;
      flagCalls.push({ op: "clear", ref, flags });
    },

    async move(): Promise<void> {
      /* noop */
    },

    async copy(): Promise<void> {
      /* noop */
    },

    async expunge(mailbox: string): Promise<{ expungedUids: number[] }> {
      if (mutationError !== null) throw mutationError;
      expungeCalls.push(mailbox);
      return { expungedUids: expungeUids };
    },

    watch(_mailbox: string, callback: WatchCallback): Unsubscribe {
      watchCallbacks.push(callback);
      return () => {
        const idx = watchCallbacks.indexOf(callback);
        if (idx !== -1) watchCallbacks.splice(idx, 1);
      };
    },

    async sync(_mailbox: string, _state: SyncState): Promise<SyncResult> {
      return {
        vanished: [],
        changed: [],
        newMessages: [],
        fullResyncRequired: false,
      };
    },

    async createList(address: string, name: string): Promise<ListInfo> {
      return {
        address,
        name,
        memberCount: 0,
        createdAt: new Date().toISOString(),
      };
    },

    async listMembers(): Promise<string[]> {
      return [];
    },

    async subscribe(): Promise<void> {
      /* noop */
    },

    async unsubscribe(): Promise<void> {
      /* noop */
    },
  };

  return transport;
}

function makeInboundMessage(from = "user@test"): InboundMessage {
  return {
    ref: { uid: 0, mailbox: "INBOX" },
    headers: {
      from,
      to: ["agent@local.interchange"],
      date: new Date().toISOString(),
      messageId: `<inbound-${String(Date.now())}@test>`,
      subject: "Test conversation",
    },
    flags: [],
    content: "Hello, agent!",
    signatureStatus: "missing",
  };
}

function makeCapabilities(transport: MessageTransport): RuntimeCapabilities {
  return createRuntimeCapabilities({ "mail.transport": transport });
}

const signal = AbortSignal.timeout(5000);

// ---------------------------------------------------------------------------
// createMailTools factory surface
// ---------------------------------------------------------------------------

describe("createMailTools", () => {
  test("definitions include all mail tools in registered order", () => {
    const tools = createMailTools({
      capabilities: makeCapabilities(makeMockTransport()),
    });

    expect(tools.definitions.map((d) => d.name)).toEqual([
      "mail_send",
      "mail_reply",
      "mail_search",
      "mail_read",
      "mail_wait",
      "mail_flag",
      "mail_expunge",
    ]);
  });

  test("run dispatches each registered tool name", async () => {
    const transport = makeMockTransport();
    const tools = createMailTools({
      capabilities: makeCapabilities(transport),
    });

    const result = await tools.run(
      {
        id: "c1",
        name: "mail_send",
        arguments: { to: "user@test", content: "hi" },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    expect(transport.getSentMessages().length).toBe(1);
  });

  test("run returns Unknown tool error for an unregistered name", async () => {
    const tools = createMailTools({
      capabilities: makeCapabilities(makeMockTransport()),
    });

    const result = await tools.run(
      { id: "c1", name: "not_a_mail_tool", arguments: {} },
      signal,
    );

    expect(result.callId).toBe("c1");
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["error"]).toBe(`Unknown tool: "not_a_mail_tool"`);
  });

  test("run wraps an error thrown from a handler path lacking its own try/catch", async () => {
    // mail_wait registers its watch subscription inside the promise executor,
    // outside any try/catch. A throw from transport.watch therefore rejects
    // the handler and reaches createMailTools.run, where the top-level wrapper
    // turns it into an isError result coded as this package's own defect.
    const transport = makeMockTransport();
    transport.watch = () => {
      throw new Error("synthetic watch failure");
    };

    const tools = createMailTools({
      capabilities: makeCapabilities(transport),
    });

    const result = await tools.run(
      { id: "c1", name: "mail_wait", arguments: { query: {} } },
      signal,
    );

    expect(result.callId).toBe("c1");
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["error"]).toBe("synthetic watch failure");
    expect(result.content["code"]).toBe("internal_error");
  });

  test("a resolver that throws on mail.transport propagates from createMailTools", () => {
    const capabilities = createRuntimeCapabilities({});

    expect(() => createMailTools({ capabilities })).toThrow(
      /"mail\.transport".*not provided by the host/,
    );
  });

  test("dispose is idempotent", async () => {
    const tools = createMailTools({
      capabilities: makeCapabilities(makeMockTransport()),
    });

    await tools.dispose();
    await tools.dispose();
    // No throw; reaching here is the assertion.
    expect(true).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// mail_send handler
// ---------------------------------------------------------------------------

describe("mail_send handler", () => {
  test("sends a conversation message and returns messageId", async () => {
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    const call: ToolCall = {
      id: "s1",
      name: "mail_send",
      arguments: {
        to: "user@test",
        content: "Hello from agent",
        type: "conversation.message",
      },
    };

    const result = await handler(call, signal);

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(typeof result.content["messageId"]).toBe("string");

    expect(transport.getSentMessages().length).toBe(1);
    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.to).toBe("user@test");
    expect(sent.content).toBe("Hello from agent");
    expect(sent.type).toBe("conversation.message");
  });

  test("returns error when 'to' is missing", async () => {
    const handler = makeMailSendHandler(makeMockTransport());

    const result = await handler(
      { id: "s2", name: "mail_send", arguments: { content: "No recipient" } },
      signal,
    );

    expect(result.isError).toBe(true);
  });

  test("returns error when both content and payload are provided", async () => {
    const handler = makeMailSendHandler(makeMockTransport());

    const result = await handler(
      {
        id: "s3",
        name: "mail_send",
        arguments: {
          to: "user@test",
          content: "text",
          payload: { type: "offering.response", version: "1", body: {} },
        },
      },
      signal,
    );

    expect(result.isError).toBe(true);
  });

  test("rejects a call naming neither content nor payload and sends nothing", async () => {
    // The advertised schema requires only 'to', because either body field
    // satisfies the tool and neither is individually required. Nothing else
    // obliges a caller to name one, and the transport resolves the omission
    // into an empty body rather than refusing it, so an accepted call would
    // sign and deliver an empty message and report the send as a success.
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    const result = await handler(
      { id: "s7", name: "mail_send", arguments: { to: "user@test" } },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_arguments");
    expect(transport.getSentMessages()).toHaveLength(0);
  });

  test("rejects a structured type naming no payload and sends nothing", async () => {
    // The structured half of the same omission: the transport would turn the
    // absent payload into an empty 'body' under the declared type, which a
    // recipient cannot tell from a body the agent meant to send empty.
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    const result = await handler(
      {
        id: "s8",
        name: "mail_send",
        arguments: { to: "user@test", type: "offering.request" },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_arguments");
    expect(transport.getSentMessages()).toHaveLength(0);
  });

  test("a body contradicting the declared type is invalid_arguments, not send_failed", async () => {
    // Both shapes are self-contradictory calls, which is what
    // `invalid_arguments` covers. Reaching the transport with either would code
    // the refusal `send_failed` instead -- "the transport rejected the
    // submission" -- which tells the model its arguments were fine and the peer
    // was at fault, and invites it to retry the same call.
    const contradictions: [
      label: string,
      id: string,
      args: Record<string, unknown>,
    ][] = [
      [
        "a structured type carrying content",
        "s9",
        { to: "user@test", type: "offering.request", content: "text" },
      ],
      [
        "a conversation type carrying a payload",
        "s10",
        { to: "user@test", type: "conversation.message", payload: { a: 1 } },
      ],
    ];

    for (const [label, id, args] of contradictions) {
      const transport = makeMockTransport();
      const handler = makeMailSendHandler(transport);

      const result = await handler(
        { id, name: "mail_send", arguments: args },
        signal,
      );

      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error(`${label}: expected object content`);
      expect(result.content["code"]).toBe("invalid_arguments");
      expect(transport.getSentMessages()).toHaveLength(0);
    }
  });

  test("stamps a correlation id onto the outbound message", async () => {
    // The correlation machinery is reachable only if the id survives the tool
    // boundary: the serializer reads OutboundMessage.correlationId to write
    // Interchange-Correlation-ID, and the reactor's tryCorrelate matches an
    // inbound response on that header alone. Declaring the key is not enough
    // -- the handler builds the outbound from named properties -- so assert on
    // the message the transport received, not on the accepted arguments.
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    const result = await handler(
      {
        id: "s5",
        name: "mail_send",
        arguments: {
          to: "user@test",
          payload: { type: "offering.request", version: "1", body: {} },
          type: "offering.request",
          correlationId: "req-abc123",
        },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();

    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.correlationId).toBe("req-abc123");
  });

  test("refuses a correlation id no header can carry and sends nothing", async () => {
    // The id becomes an Interchange-Correlation-ID field body, and RFC 5322
    // § 2.2 allows no CR and no LF there outside folding. The MIME writer
    // refuses it too, three layers down, but from there it can only reach the
    // caller as `send_failed` -- which blames the peer for the caller's own
    // argument and invites a retry of the same call.
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    for (const correlationId of [
      "req-abc\r\nBcc: victim@test",
      "req-abc\nX-Injected: yes",
      "req-abc\r",
    ]) {
      const result = await handler(
        {
          id: "s5a",
          name: "mail_send",
          arguments: { to: "user@test", content: "hi", correlationId },
        },
        signal,
      );

      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["code"]).toBe("invalid_arguments");
      expect(transport.getSentMessages()).toHaveLength(0);
    }
  });

  test("refuses a subject or an inReplyTo no header can carry", async () => {
    // Both become field bodies, like the correlation id above, and the MIME
    // writer refuses a CR or an LF in one three layers down -- from where it
    // reaches the caller as `send_failed`, naming no argument to fix.
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    for (const [field, value] of [
      ["subject", "hello\r\nBcc: victim@test"],
      ["subject", "hello\n"],
      ["inReplyTo", "<a@test>\r\nBcc: victim@test"],
      ["inReplyTo", "<a@test>\r"],
    ] as const) {
      const result = await handler(
        {
          id: "s5b",
          name: "mail_send",
          arguments: { to: "user@test", content: "hi", [field]: value },
        },
        signal,
      );

      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["code"]).toBe("invalid_arguments");
      expect(String(result.content["error"])).toContain(field);
      expect(transport.getSentMessages()).toHaveLength(0);
    }
  });

  test("refuses a blank inReplyTo and sends nothing", async () => {
    // RFC 5322 § 3.6.4 gives In-Reply-To as `1*msg-id`. The transport refuses a
    // blank one as well, but only as `send_failed`.
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    for (const inReplyTo of ["", "   "]) {
      const result = await handler(
        {
          id: "s5c",
          name: "mail_send",
          arguments: { to: "user@test", content: "hi", inReplyTo },
        },
        signal,
      );

      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["code"]).toBe("invalid_arguments");
      expect(String(result.content["error"])).toContain("inReplyTo");
      expect(transport.getSentMessages()).toHaveLength(0);
    }
  });

  test("refuses an empty recipient list and sends nothing", async () => {
    // An empty list names no destination. The transport refuses it too, as
    // `send_failed`, which reads as a peer that may yet take the same call.
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    const result = await handler(
      { id: "s5d", name: "mail_send", arguments: { to: [], content: "hi" } },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_arguments");
    expect(String(result.content["error"])).toContain("to");
    expect(transport.getSentMessages()).toHaveLength(0);
  });

  test("omits correlationId when the call supplies none", async () => {
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    await handler(
      {
        id: "s6",
        name: "mail_send",
        arguments: { to: "user@test", content: "no correlation" },
      },
      signal,
    );

    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect("correlationId" in sent).toBe(false);
  });

  test("rejects an argument the shape does not declare and sends nothing", async () => {
    // 'attachments' is documented as a parameter of mail.send and is not
    // declared by the shape, so this is the call a caller following the
    // documentation makes. Both halves matter: the caller must see the refusal
    // AND no message may leave, because an accepted key would have gone out as
    // a message without the attachment, reported as a success.
    const transport = makeMockTransport();
    const handler = makeMailSendHandler(transport);

    const result = await handler(
      {
        id: "s4",
        name: "mail_send",
        arguments: {
          to: "user@test",
          content: "Hello from agent",
          attachments: [{ name: "report.pdf", contentType: "application/pdf" }],
        },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("attachments");
    expect(transport.getSentMessages()).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// mail_send over a transport that assembles the message
// ---------------------------------------------------------------------------

// The mock transport above keeps every OutboundMessage it is handed, so no case
// written against it reaches the layer that decides whether a recipient is
// writable as an RFC 5322 field body. These cases bind the handler to
// @intx/mail-memory, which assembles and signs the message as a transport does,
// and they assert the recipient's mailbox as well: a refusal that still
// delivered to the recipients it could carry would satisfy the code assertion
// alone.
describe("mail_send over a transport that assembles the message", () => {
  const SENDER = "alpha@test.interchange";
  const RECIPIENT = "beta@test.interchange";

  async function boundSend() {
    const transport = createInMemoryTransport();
    const senderKeys = await generateKeyPair();
    const recipientKeys = await generateKeyPair();
    transport.register(SENDER, createEd25519Crypto(senderKeys));
    transport.register(RECIPIENT, createEd25519Crypto(recipientKeys));

    const senderTransport = transport.getTransportFor(SENDER);
    const recipientTransport = transport.getTransportFor(RECIPIENT);
    return {
      handler: makeMailSendHandler(senderTransport),
      senderTransport,
      recipientTransport,
      delivered: async () =>
        await recipientTransport.search("INBOX", {}, signal),
    };
  }

  test("refuses a recipient no header can carry and delivers nothing", async () => {
    const { handler, delivered } = await boundSend();

    const result = await handler(
      {
        id: "a1",
        name: "mail_send",
        arguments: {
          to: [RECIPIENT, "x@y\r\nBcc: victim@test"],
          content: "hi",
        },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    // Not `send_failed`: that code says the transport rejected the submission
    // and leaves the outcome unknown, inviting a retry of a call no transport
    // can carry.
    expect(result.content["code"]).toBe("invalid_arguments");
    expect(String(result.content["error"])).toContain("to");
    expect(await delivered()).toHaveLength(0);
  });

  test("refuses a blank recipient, bare and inside a list", async () => {
    // A blank value names nobody, which is the reason the sibling inReplyTo
    // constraint requires a non-whitespace character as well.
    for (const to of ["", "   ", ["  "], [RECIPIENT, ""]]) {
      const { handler, delivered } = await boundSend();

      const result = await handler(
        { id: "a2", name: "mail_send", arguments: { to, content: "hi" } },
        signal,
      );

      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["code"]).toBe("invalid_arguments");
      expect(String(result.content["error"])).toContain("to");
      expect(await delivered()).toHaveLength(0);
    }
  });

  test("delivers to a recipient the header layer can carry", async () => {
    // The control for the two cases above: the constraint refuses the values a
    // field body cannot hold, and leaves an ordinary address alone.
    const { handler, delivered } = await boundSend();

    const result = await handler(
      {
        id: "a3",
        name: "mail_send",
        arguments: { to: RECIPIENT, content: "hi" },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    expect(await delivered()).toHaveLength(1);
  });

  test("an inReplyTo names that one parent in References, where a reply names the ancestry", async () => {
    // `mail_send` takes `inReplyTo` as an argument and never fetches the
    // message it names, so the chain it emits holds that one identifier. Only
    // `mail_reply` has the parent in hand and can extend the chain from the
    // parent's own References. Nothing distinguished the two paths, which is
    // how a claim that `mail_send` extends the chain by fetching the parent's
    // References survived in the message documentation.
    const { senderTransport, recipientTransport } = await boundSend();
    const betaSend = makeMailSendHandler(recipientTransport);
    const betaReply = makeMailReplyHandler(recipientTransport);
    const alphaReply = makeMailReplyHandler(senderTransport);

    function refAt(refs: MessageRef[], index: number): MessageRef {
      const ref = refs[index];
      if (ref === undefined) throw new Error(`no message at index ${index}`);
      return ref;
    }
    function messageIdOf(headers: MessageHeaders): string {
      const { messageId } = headers;
      if (messageId === undefined) {
        throw new Error("the assembled message carries no Message-Id");
      }
      return messageId;
    }

    // Generation 1: beta opens the thread to alpha, so it carries no ancestry.
    await betaSend(
      {
        id: "t1",
        name: "mail_send",
        arguments: { to: SENDER, content: "g1", subject: "T" },
      },
      signal,
    );
    const alphaInbox = await senderTransport.search("INBOX", {}, signal);
    expect(alphaInbox).toHaveLength(1);
    const g1 = await senderTransport.fetchHeaders(refAt(alphaInbox, 0), signal);
    expect(g1.references).toBeUndefined();

    // Generation 2: alpha replies, so g2 carries References = [g1].
    await alphaReply(
      {
        id: "t2",
        name: "mail_reply",
        arguments: { ref: refAt(alphaInbox, 0), content: "g2" },
      },
      signal,
    );
    const betaInbox = await recipientTransport.search("INBOX", {}, signal);
    expect(betaInbox).toHaveLength(1);
    const g2 = await recipientTransport.fetchHeaders(
      refAt(betaInbox, 0),
      signal,
    );
    // The parent has ancestry to lose, and it is a different message from g1.
    expect(messageIdOf(g2)).not.toBe(messageIdOf(g1));
    expect(g2.references).toEqual([messageIdOf(g1)]);

    // Generation 3a: beta answers g2 through mail_send, naming it by id alone.
    await betaSend(
      {
        id: "t3a",
        name: "mail_send",
        arguments: {
          to: SENDER,
          content: "g3 by send",
          inReplyTo: messageIdOf(g2),
        },
      },
      signal,
    );
    // Generation 3b: beta answers the same g2 through mail_reply.
    await betaReply(
      {
        id: "t3b",
        name: "mail_reply",
        arguments: { ref: refAt(betaInbox, 0), content: "g3 by reply" },
      },
      signal,
    );

    const afterBoth = await senderTransport.search("INBOX", {}, signal);
    expect(afterBoth).toHaveLength(3);
    const bySend = await senderTransport.fetchHeaders(
      refAt(afterBoth, 1),
      signal,
    );
    const byReply = await senderTransport.fetchHeaders(
      refAt(afterBoth, 2),
      signal,
    );

    // Both answer the SAME parent, so the chains below differ on the path
    // taken and on nothing else.
    expect(bySend.inReplyTo).toBe(messageIdOf(g2));
    expect(byReply.inReplyTo).toBe(messageIdOf(g2));

    expect(bySend.references).toEqual([messageIdOf(g2)]);
    expect(byReply.references).toEqual([messageIdOf(g1), messageIdOf(g2)]);
  });
});

// ---------------------------------------------------------------------------
// mail_reply handler
// ---------------------------------------------------------------------------

describe("mail_reply handler", () => {
  test("fetches parent headers and sends reply with inReplyTo", async () => {
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 10, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<parent@test>",
        subject: "Original subject",
      },
      flags: [],
      content: "original message",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r1",
        name: "mail_reply",
        arguments: { ref: parentRef, content: "This is the reply" },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    expect(transport.getSentMessages().length).toBe(1);

    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.to).toBe("user@test");
    expect(sent.inReplyTo).toBe("<parent@test>");
    expect(sent.subject).toBe("Original subject");
    expect(sent.content).toBe("This is the reply");
  });

  test("builds the full References chain from the parent's ancestry", async () => {
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 11, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<parent@test>",
        references: ["<root@test>", "<mid@test>"],
        subject: "Re: Original subject",
      },
      flags: [],
      content: "original message",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r3",
        name: "mail_reply",
        arguments: { ref: parentRef, content: "threaded reply" },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.inReplyTo).toBe("<parent@test>");
    // The full ancestry: the parent's own References plus the parent's own
    // Message-Id, in order.
    expect(sent.references).toEqual([
      "<root@test>",
      "<mid@test>",
      "<parent@test>",
    ]);
  });

  test("references falls back to just the parent when it has no ancestry", async () => {
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 12, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<lonely@test>",
      },
      flags: [],
      content: "thread opener",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    await handler(
      {
        id: "r4",
        name: "mail_reply",
        arguments: { ref: parentRef, content: "first reply" },
      },
      signal,
    );

    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.references).toEqual(["<lonely@test>"]);
  });

  test("carries the parent's correlation id onto the reply", async () => {
    // The reply half of a correlated exchange. The reactor's tryCorrelate
    // matches a response on Interchange-Correlation-ID alone and derives
    // nothing from inReplyTo or References, so a reply that drops the id
    // leaves the requester's suspended operation unresolved forever. Assert on
    // the message the transport received: the serializer reads
    // OutboundMessage.correlationId to write that header.
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 15, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<request@test>",
        subject: "Offering request",
        interchangeCorrelationId: "req-abc123",
      },
      flags: [],
      content: "please do the thing",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r7",
        name: "mail_reply",
        arguments: { ref: parentRef, content: "here is the result" },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();

    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.correlationId).toBe("req-abc123");
  });

  test("omits correlationId when the parent carries none", async () => {
    // Replying to ordinary uncorrelated mail is the common case. The reply
    // must still send, and must leave the key absent rather than carrying an
    // empty Interchange-Correlation-ID that no pending operation matches.
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 16, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<chatter@test>",
        subject: "Just talking",
      },
      flags: [],
      content: "hello there",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r8",
        name: "mail_reply",
        arguments: { ref: parentRef, content: "hello back" },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();

    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect("correlationId" in sent).toBe(false);
  });

  test("returns error when ref is missing", async () => {
    const handler = makeMailReplyHandler(makeMockTransport());

    const result = await handler(
      { id: "r2", name: "mail_reply", arguments: { content: "no ref" } },
      signal,
    );

    expect(result.isError).toBe(true);
  });

  test("rejects a reply naming neither content nor payload and sends nothing", async () => {
    // The advertised schema requires only 'ref' for the reason mail_send's
    // requires only 'to', so nothing but this guard keeps a bodiless reply from
    // being signed, delivered as an empty message, and reported as sent.
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 17, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<parent@test>",
      },
      flags: [],
      content: "original message",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      { id: "r9", name: "mail_reply", arguments: { ref: parentRef } },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_arguments");
    expect(transport.getSentMessages()).toHaveLength(0);
  });

  test("a bodiless reply naming an unfetchable parent reports the argument error, not not_found", async () => {
    // Two faults in one call, and only one of them is the caller's to fix. If
    // the parent lookup runs first the caller is told the message is missing,
    // which sends it hunting for a ref that was never the problem. Count the
    // fetch as well as reading the code: a handler that fetched, discarded the
    // failure, and then reported the argument error would satisfy the code
    // assertion while still paying for a round-trip it can never use.
    const transport = makeMockTransport();
    let fetches = 0;
    transport.fetchHeaders = async () => {
      fetches += 1;
      throw new Error("no such message");
    };

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r13",
        name: "mail_reply",
        arguments: { ref: { uid: 404, mailbox: "INBOX" } },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_arguments");
    expect(fetches).toBe(0);
    expect(transport.getSentMessages()).toHaveLength(0);
  });

  test("an unfetchable parent named by an otherwise valid reply is still not_found", async () => {
    // The other half of the pair above. Moving the body checks ahead of the
    // lookup must not cost the lookup its own code: when the arguments are
    // sound, a parent the transport cannot read is exactly what `not_found`
    // reports, and the fetch has to be attempted to learn that.
    const transport = makeMockTransport();
    let fetches = 0;
    transport.fetchHeaders = async () => {
      fetches += 1;
      throw new Error("no such message");
    };

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r14",
        name: "mail_reply",
        arguments: {
          ref: { uid: 404, mailbox: "INBOX" },
          content: "a reply to nothing",
        },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("not_found");
    expect(fetches).toBe(1);
    expect(transport.getSentMessages()).toHaveLength(0);
  });

  test("a reply body contradicting the declared type is invalid_arguments, not send_failed", async () => {
    // The reply carries the same body contract as the send, and the same
    // consequence for miscoding it: `send_failed` would blame the peer for a
    // contradiction in the reply the agent composed.
    const contradictions: [
      label: string,
      id: string,
      body: Record<string, unknown>,
    ][] = [
      [
        "a structured reply type carrying content",
        "r10",
        { type: "offering.response", content: "text" },
      ],
      [
        "a conversation reply type carrying a payload",
        "r11",
        { type: "conversation.message", payload: { a: 1 } },
      ],
    ];

    for (const [label, id, body] of contradictions) {
      const transport = makeMockTransport();

      const parentRef: MessageRef = { uid: 18, mailbox: "INBOX" };
      transport.enqueueMessage(parentRef, {
        ref: parentRef,
        headers: {
          from: "user@test",
          to: ["agent@local"],
          date: new Date().toISOString(),
          messageId: "<parent@test>",
        },
        flags: [],
        content: "original message",
        signatureStatus: "missing",
      });

      const handler = makeMailReplyHandler(transport);
      const result = await handler(
        { id, name: "mail_reply", arguments: { ref: parentRef, ...body } },
        signal,
      );

      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error(`${label}: expected object content`);
      expect(result.content["code"]).toBe("invalid_arguments");
      expect(transport.getSentMessages()).toHaveLength(0);
    }
  });

  test("sends a structured reply carrying only a payload", async () => {
    // The positive case the body rule must leave reachable. Making 'content'
    // mandatory would satisfy every refusal above while refusing this reply
    // too, so the two are asserted together.
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 19, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<request@test>",
      },
      flags: [],
      content: "please do the thing",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r12",
        name: "mail_reply",
        arguments: {
          ref: parentRef,
          type: "offering.response",
          payload: { ok: true },
        },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    const sent = transport.getSentMessages()[0];
    if (sent === undefined) throw new Error("no sent message");
    expect(sent.type).toBe("offering.response");
    expect(sent.payload).toEqual({ ok: true });
    expect("content" in sent).toBe(false);
  });

  test("refuses a reply to a parent carrying no From, and sends nothing", async () => {
    // A reply is addressed to the parent's originator. When the parent names
    // none there is nobody to reply to, and the alternative -- choosing an
    // address here -- puts the agent's words in front of a peer it never
    // named. Both halves matter: the caller must see the refusal AND no
    // message may leave, because a guard that reported an error but still
    // sent would satisfy the first half alone.
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 13, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<orphan@test>",
        subject: "Original subject",
      },
      flags: [],
      content: "original message",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r5",
        name: "mail_reply",
        arguments: { ref: parentRef, content: "This is the reply" },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string") {
      throw new Error("expected a structured error result");
    }
    expect(String(result.content["error"])).toContain("no From header");
    expect(transport.getSentMessages()).toHaveLength(0);
  });

  test("rejects an argument the shape does not declare and sends nothing", async () => {
    // 'attachments' is documented as a parameter of mail.reply and is not
    // declared by the shape, so an accepted key would send the reply without
    // the attachment and report success.
    const transport = makeMockTransport();

    const parentRef: MessageRef = { uid: 14, mailbox: "INBOX" };
    transport.enqueueMessage(parentRef, {
      ref: parentRef,
      headers: {
        from: "user@test",
        to: ["agent@local"],
        date: new Date().toISOString(),
        messageId: "<parent@test>",
      },
      flags: [],
      content: "original message",
      signatureStatus: "missing",
    });

    const handler = makeMailReplyHandler(transport);
    const result = await handler(
      {
        id: "r6",
        name: "mail_reply",
        arguments: {
          ref: parentRef,
          content: "This is the reply",
          attachments: [{ name: "report.pdf", contentType: "application/pdf" }],
        },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("attachments");
    expect(transport.getSentMessages()).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// mail_search handler
// ---------------------------------------------------------------------------

describe("mail_search handler", () => {
  test("calls transport.search and returns summaries", async () => {
    const handler = makeMailSearchHandler(makeMockTransport());

    const result = await handler(
      {
        id: "q1",
        name: "mail_search",
        arguments: {
          mailbox: "INBOX",
          query: { from: "user@test" },
          limit: 5,
        },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(Array.isArray(result.content["results"])).toBe(true);
  });

  test("defaults mailbox to INBOX when not specified", async () => {
    const handler = makeMailSearchHandler(makeMockTransport());

    const result = await handler(
      { id: "q2", name: "mail_search", arguments: { query: {} } },
      signal,
    );

    expect(result.isError).toBeUndefined();
  });

  test("reports a failure of the transport itself under the code its condition earns", async () => {
    // The transport says which case it is by naming its condition, and the
    // three cases are not one. A mailbox that is not there is the caller's to
    // fix. An operation the transport refused outright is nobody's to fix by
    // reissuing it, so it does not get a code that invites that. Only a
    // condition that leaves the outcome unknown, and a rejection naming none,
    // is a failure the caller can do nothing about but retry the same call.
    const failures: [label: string, cause: Error, code: string][] = [
      [
        "a transport that violated its own invariant",
        new MessageTransportError("SERVERBUG", "no reader is wired"),
        "search_failed",
      ],
      [
        "an operation the transport refuses outright",
        new MessageTransportError("CANNOT", "search is not supported here"),
        "not_available",
      ],
      [
        "a rejection naming no condition",
        new Error("the socket went away"),
        "search_failed",
      ],
    ];

    for (const [label, cause, code] of failures) {
      const transport = makeMockTransport();
      transport.search = async () => {
        throw cause;
      };

      const result = await makeMailSearchHandler(transport)(
        { id: "q2a", name: "mail_search", arguments: {} },
        signal,
      );

      if (typeof result.content === "string")
        throw new Error(`${label}: expected object content`);
      expect(`${label}: ${String(result.content["code"])}`).toBe(
        `${label}: ${code}`,
      );
      expect(result.content["error"]).toBe(cause.message);
    }
  });

  test("a date window narrows the results instead of returning the whole mailbox", async () => {
    const transport = makeMockTransport();
    const stored = [
      { uid: 1, date: new Date("2026-01-01T00:00:00Z") },
      { uid: 2, date: new Date("2026-06-01T00:00:00Z") },
      { uid: 3, date: new Date("2026-09-01T00:00:00Z") },
    ];
    // Shaped like the mailbox matcher, which excludes a message that falls
    // outside the window. A query date that is not a Date turns the comparison
    // into a not-a-number test, so nothing is excluded and the caller gets the
    // whole mailbox back.
    const withinWindow = (date: Date, query: SearchQuery) => {
      if (query.before !== undefined && date >= query.before) return false;
      return true;
    };
    transport.search = async (mailbox, query) =>
      stored
        .filter((m) => withinWindow(m.date, query))
        .map((m) => ({ uid: m.uid, mailbox }));

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      {
        id: "q3",
        name: "mail_search",
        arguments: { query: { before: "2026-03-01T00:00:00Z" } },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    const results = result.content["results"];
    if (!Array.isArray(results)) throw new Error("expected results array");
    expect(results).toHaveLength(1);
  });

  test("coerces a date filter carried as a JSON string into a Date", async () => {
    const transport = makeMockTransport();
    let received: SearchQuery | undefined;
    transport.search = async (_mailbox, query) => {
      received = query;
      return [];
    };

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      {
        id: "q4",
        name: "mail_search",
        arguments: { query: { on: "2026-03-01", from: "user@test" } },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    expect(received?.on).toBeInstanceOf(Date);
    expect(received?.on?.toISOString()).toBe(
      new Date("2026-03-01").toISOString(),
    );
    expect(received?.from).toBe("user@test");
  });

  test("rejects a date filter that is not a date without searching", async () => {
    const transport = makeMockTransport();
    let searched = false;
    transport.search = async () => {
      searched = true;
      return [];
    };

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      {
        id: "q5",
        name: "mail_search",
        arguments: { query: { before: "yesterday" } },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_query");
    expect(searched).toBe(false);
  });

  test("rejects a misspelled filter and names it", async () => {
    const transport = makeMockTransport();
    let searched = false;
    transport.search = async () => {
      searched = true;
      return [];
    };

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      {
        id: "q6",
        name: "mail_search",
        arguments: { query: { sender: "alice@test" } },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_query");
    expect(String(result.content["error"])).toContain("sender");
    expect(searched).toBe(false);
  });

  test("rejects an undeclared filter nested in a boolean composition", async () => {
    const transport = makeMockTransport();
    let searched = false;
    transport.search = async () => {
      searched = true;
      return [];
    };

    const handler = makeMailSearchHandler(transport);
    const cases = [
      {
        query: { and: [{ from: "alice@test" }, { form: "bob@test" }] },
        key: "form",
      },
      { query: { or: [{ recipient: "bob@test" }] }, key: "recipient" },
      { query: { not: { bodyText: "hello" } }, key: "bodyText" },
    ];

    for (const [index, { query, key }] of cases.entries()) {
      const result = await handler(
        {
          id: `q7-${String(index)}`,
          name: "mail_search",
          arguments: { query },
        },
        signal,
      );

      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["code"]).toBe("invalid_query");
      expect(String(result.content["error"])).toContain(key);
    }

    expect(searched).toBe(false);
  });

  test("rejects an undeclared field of the header filter", async () => {
    const transport = makeMockTransport();
    let searched = false;
    transport.search = async () => {
      searched = true;
      return [];
    };

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      {
        id: "q8",
        name: "mail_search",
        arguments: {
          query: {
            header: {
              field: "Interchange-Type",
              contains: "conversation.message",
              matches: "anything",
            },
          },
        },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_query");
    expect(String(result.content["error"])).toContain("matches");
    expect(searched).toBe(false);
  });

  test("accepts a query built only from declared filters", async () => {
    const transport = makeMockTransport();
    let received: SearchQuery | undefined;
    transport.search = async (_mailbox, query) => {
      received = query;
      return [];
    };

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      {
        id: "q9",
        name: "mail_search",
        arguments: {
          query: {
            from: "alice@test",
            to: "bob@test",
            cc: "carol@test",
            bcc: "dave@test",
            header: {
              field: "Interchange-Type",
              contains: "conversation.message",
            },
            before: "2026-03-01T00:00:00Z",
            after: "2026-01-01T00:00:00Z",
            on: "2026-02-01T00:00:00Z",
            sentBefore: "2026-03-01T00:00:00Z",
            sentAfter: "2026-01-01T00:00:00Z",
            sentOn: "2026-02-01T00:00:00Z",
            hasFlags: ["\\Seen"],
            missingFlags: ["\\Deleted"],
            body: "hello",
            text: "world",
            largerThan: 10,
            smallerThan: 1000,
            and: [{ from: "alice@test" }],
            or: [{ to: "bob@test" }],
            not: { body: "spam" },
          },
        },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    expect(received?.from).toBe("alice@test");
    expect(received?.to).toBe("bob@test");
    expect(received?.cc).toBe("carol@test");
    expect(received?.bcc).toBe("dave@test");
    expect(received?.header?.field).toBe("Interchange-Type");
    expect(received?.before).toBeInstanceOf(Date);
    expect(received?.after).toBeInstanceOf(Date);
    expect(received?.on).toBeInstanceOf(Date);
    expect(received?.sentBefore).toBeInstanceOf(Date);
    expect(received?.sentAfter).toBeInstanceOf(Date);
    expect(received?.sentOn).toBeInstanceOf(Date);
    expect(received?.hasFlags).toEqual(["\\Seen"]);
    expect(received?.missingFlags).toEqual(["\\Deleted"]);
    expect(received?.body).toBe("hello");
    expect(received?.text).toBe("world");
    expect(received?.largerThan).toBe(10);
    expect(received?.smallerThan).toBe(1000);
    expect(received?.and?.[0]?.from).toBe("alice@test");
    expect(received?.or?.[0]?.to).toBe("bob@test");
    expect(received?.not?.body).toBe("spam");
  });

  test("rejects a filter written as an argument instead of inside the query", async () => {
    const transport = makeMockTransport();
    let searched = false;
    transport.search = async () => {
      searched = true;
      return [];
    };

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      { id: "q10", name: "mail_search", arguments: { from: "alice@test" } },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("from");
    expect(searched).toBe(false);
  });

  test("rejects a misspelled limit instead of searching with the default", async () => {
    const transport = makeMockTransport();
    let searched = false;
    transport.search = async () => {
      searched = true;
      return [];
    };

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      {
        id: "q11",
        name: "mail_search",
        arguments: { query: { from: "alice@test" }, limlt: 5 },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("limlt");
    expect(searched).toBe(false);
  });

  test("accepts every declared argument and applies the limit", async () => {
    const transport = makeMockTransport();
    transport.search = async (mailbox) =>
      [1, 2, 3].map((uid) => ({ uid, mailbox }));

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      {
        id: "q12",
        name: "mail_search",
        arguments: {
          mailbox: "Archive",
          query: { from: "alice@test" },
          limit: 2,
        },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["results"]).toHaveLength(2);
  });

  test("refuses a limit that is not a positive whole number", async () => {
    const transport = makeMockTransport();
    transport.search = async (mailbox) =>
      [1, 2, 3, 4, 5].map((uid) => ({ uid, mailbox }));

    // `refs.slice(0, limit)` accepts every one of these and answers with a set
    // the caller is told is the whole match: -1 drops the last result, 0 and
    // -100 drop them all, and a fraction truncates. None of those is
    // distinguishable from a mailbox that holds no more, which is what makes
    // the refusal the only answer that tells the truth.
    const handler = makeMailSearchHandler(transport);
    for (const limit of [-1, -4, 0, -100, 0.5, 1.5]) {
      const result = await handler(
        {
          id: `q13-${String(limit)}`,
          name: "mail_search",
          arguments: { query: { from: "alice@test" }, limit },
        },
        signal,
      );
      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["code"]).toBe("invalid_arguments");
      // The refusal names the argument, so the caller learns which one to fix.
      expect(String(result.content["error"])).toContain("limit");
    }
  });

  test("accepts any positive limit, including one past the match count", async () => {
    const transport = makeMockTransport();
    transport.search = async (mailbox) =>
      [1, 2, 3, 4, 5].map((uid) => ({ uid, mailbox }));

    // The mailbox bounds the result set, not the argument, which is why the
    // argument carries no ceiling: asking for more than the mailbox holds is
    // how a caller asks for all of it, and it is answered rather than refused.
    const handler = makeMailSearchHandler(transport);
    for (const [limit, expected] of [
      [1, 1],
      [5, 5],
      [1_000_000, 5],
    ] as const) {
      const result = await handler(
        {
          id: `q14-${String(limit)}`,
          name: "mail_search",
          arguments: { limit },
        },
        signal,
      );
      expect(result.isError).toBeUndefined();
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["results"]).toHaveLength(expected);
    }
  });

  test("reports a result set the limit cut short", async () => {
    // A slice the caller cannot measure is the whole problem: 20 results out of
    // 20 matches and 20 out of 50 are the same array, and the caller is told
    // both are the whole match.
    const transport = makeMockTransport();
    transport.search = async (mailbox) =>
      Array.from({ length: 50 }, (_unused, index) => ({
        uid: index + 1,
        mailbox,
      }));

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      { id: "q15", name: "mail_search", arguments: { limit: 20 } },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["results"]).toHaveLength(20);
    expect(result.content["matched"]).toBe(50);
    expect(result.content["truncated"]).toBe(true);
  });

  test("reports a complete result set as complete, at the limit and under it", async () => {
    // A mailbox holding exactly the requested count is the case the caller
    // cannot otherwise tell from a truncated one.
    for (const [matches, limit] of [
      [20, 20],
      [3, 20],
      [0, 20],
    ] as const) {
      const transport = makeMockTransport();
      transport.search = async (mailbox) =>
        Array.from({ length: matches }, (_unused, index) => ({
          uid: index + 1,
          mailbox,
        }));

      const handler = makeMailSearchHandler(transport);
      const result = await handler(
        {
          id: `q16-${String(matches)}`,
          name: "mail_search",
          arguments: { limit },
        },
        signal,
      );

      expect(result.isError).toBeUndefined();
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["results"]).toHaveLength(matches);
      expect(result.content["matched"]).toBe(matches);
      expect(result.content["truncated"]).toBe(false);
    }
  });

  test("names the summary whose headers could not be read", async () => {
    // A discarded read leaves the summary with its header fields absent, which
    // is what a message carrying no headers looks like, so a corrupt index
    // reads as ordinary mail. The failure belongs on the summary it happened
    // to, because the other results are still answers.
    const transport = makeMockTransport();
    transport.search = async (mailbox) =>
      [1, 2].map((uid) => ({ uid, mailbox }));
    const readable = transport.fetchHeaders.bind(transport);
    transport.fetchHeaders = async (ref, headerSignal) => {
      if (ref.uid === 2) throw new Error("the index entry is corrupt");
      return await readable(ref, headerSignal);
    };

    const handler = makeMailSearchHandler(transport);
    const result = await handler(
      { id: "q17", name: "mail_search", arguments: {} },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    const results = result.content["results"];
    if (!Array.isArray(results)) throw new Error("expected results array");
    expect(results).toHaveLength(2);
    expect(results[0]["from"]).toBe("sender@test");
    expect(results[0]["headersError"]).toBeUndefined();
    expect(results[1]["from"]).toBeUndefined();
    expect(String(results[1]["headersError"])).toContain(
      "the index entry is corrupt",
    );
  });
});

// ---------------------------------------------------------------------------
// mail_read handler
// ---------------------------------------------------------------------------

describe("mail_read handler", () => {
  test("fetches full message when parts='full'", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 5, mailbox: "INBOX" };
    transport.enqueueMessage(ref, { ...makeInboundMessage(), ref });

    const handler = makeMailReadHandler(transport);
    const result = await handler(
      { id: "rd1", name: "mail_read", arguments: { ref, parts: "full" } },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["headers"]).toBeDefined();
    expect(result.content["signatureStatus"]).toBe("missing");
  });

  test("fetches only headers when parts='headers'", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 6, mailbox: "INBOX" };
    transport.enqueueMessage(ref, { ...makeInboundMessage(), ref });

    const handler = makeMailReadHandler(transport);
    const result = await handler(
      { id: "rd2", name: "mail_read", arguments: { ref, parts: "headers" } },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["headers"]).toBeDefined();
  });

  test("discloses the correlation id in the header modes, never in payload", async () => {
    // The handler hands back whatever the transport decoded, so which 'parts'
    // mode the caller picks is the whole of what keeps Interchange-Correlation-ID
    // out of the model's context. With no validator wired a correlation resolves
    // on that id alone, so whoever reads one can answer it (MESSAGE.md,
    // Correlation Security), and mail_search projects a fixed summary where this
    // handler projects nothing. The payload half is the load-bearing assertion:
    // the default mode returns no header set at all, and must keep returning
    // none whatever the correlation design becomes.
    const correlationId = "b2e5f1a4-7c3d-4e8b-9a06-5d1f2c3b4a97";
    const transport = makeMockTransport();
    const base = makeInboundMessage();
    const headers = {
      ...base.headers,
      interchangeCorrelationId: correlationId,
    };
    const conversationRef: MessageRef = { uid: 70, mailbox: "INBOX" };
    const structuredRef: MessageRef = { uid: 71, mailbox: "INBOX" };
    transport.enqueueMessage(conversationRef, {
      ...base,
      ref: conversationRef,
      headers,
    });
    transport.enqueueMessage(structuredRef, {
      ...base,
      ref: structuredRef,
      headers,
      payload: { type: "offering.response", version: "1", body: {} },
    });

    const handler = makeMailReadHandler(transport);
    const read = async (id: string, ref: MessageRef, parts: string) => {
      const result = await handler(
        { id, name: "mail_read", arguments: { ref, parts } },
        signal,
      );
      expect(result.isError).toBeUndefined();
      if (typeof result.content === "string")
        throw new Error("expected object content");
      return result.content;
    };

    const conversationPayload = await read("rd5", conversationRef, "payload");
    expect(conversationPayload["content"]).toBe(base.content);
    expect(conversationPayload["headers"]).toBeUndefined();
    expect(JSON.stringify(conversationPayload)).not.toContain(correlationId);

    const structuredPayload = await read("rd6", structuredRef, "payload");
    expect(structuredPayload["payload"]).toBeDefined();
    expect(structuredPayload["headers"]).toBeUndefined();
    expect(JSON.stringify(structuredPayload)).not.toContain(correlationId);

    const headerMode = await read("rd7", conversationRef, "headers");
    expect(headerMode["headers"]).toMatchObject({
      interchangeCorrelationId: correlationId,
    });

    const fullMode = await read("rd8", conversationRef, "full");
    expect(fullMode["headers"]).toMatchObject({
      interchangeCorrelationId: correlationId,
    });
  });

  test("returns error when ref is missing", async () => {
    const handler = makeMailReadHandler(makeMockTransport());

    const result = await handler(
      { id: "rd3", name: "mail_read", arguments: { parts: "full" } },
      signal,
    );

    expect(result.isError).toBe(true);
  });

  test("rejects a misnamed 'parts' instead of reading the default", async () => {
    // A dropped 'partPath' leaves 'parts' absent, which defaults to the payload
    // and returns a message body the caller never asked for -- reported as the
    // read of a MIME part it did ask for. Count every retrieval the handler
    // could make, so the test fails if the refusal still reaches the transport.
    const transport = makeMockTransport();
    let fetches = 0;
    const baseFetchHeaders = transport.fetchHeaders.bind(transport);
    transport.fetchHeaders = async (ref, s) => {
      fetches += 1;
      return baseFetchHeaders(ref, s);
    };
    const baseFetchFull = transport.fetchFull.bind(transport);
    transport.fetchFull = async (ref, s) => {
      fetches += 1;
      return baseFetchFull(ref, s);
    };
    const baseFetchPart = transport.fetchPart.bind(transport);
    transport.fetchPart = async (ref, partPath, s) => {
      fetches += 1;
      return baseFetchPart(ref, partPath, s);
    };

    const handler = makeMailReadHandler(transport);
    const result = await handler(
      {
        id: "rd4",
        name: "mail_read",
        arguments: { ref: { uid: 6, mailbox: "INBOX" }, partPath: "1.1" },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("partPath");
    expect(fetches).toBe(0);
  });

  test("a message that cannot be read back is fetch_failed, not not_found", async () => {
    // A structured payload the transport cannot assemble leaves the message
    // present and readable as headers. Reporting `not_found` sends the model
    // back to search, which finds it again, and round it goes.
    const transport = makeMockTransport();
    transport.fetchFull = async () => {
      throw new Error("invalid message payload");
    };

    const handler = makeMailReadHandler(transport);
    for (const parts of ["full", "payload"]) {
      const result = await handler(
        {
          id: `rd-decode-${parts}`,
          name: "mail_read",
          arguments: { ref: { uid: 9, mailbox: "INBOX" }, parts },
        },
        signal,
      );

      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["code"]).toBe("fetch_failed");
    }
  });

  test("a uid that names no message is not_found in every read mode", async () => {
    // Including the part path: a caller whose uid is wrong has nothing to fix
    // in a part path the tool never parsed.
    const transport = makeMockTransport();
    const absent = (): never => {
      throw new Error('Message UID 9 not found in mailbox "INBOX"');
    };
    transport.fetchHeaders = async () => absent();
    transport.fetchFull = async () => absent();
    transport.fetchPart = async () => absent();

    const handler = makeMailReadHandler(transport);
    for (const parts of ["headers", "full", "payload", "1.3"]) {
      const result = await handler(
        {
          id: `rd-absent-${parts}`,
          name: "mail_read",
          arguments: { ref: { uid: 9, mailbox: "INBOX" }, parts },
        },
        signal,
      );

      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["code"]).toBe("not_found");
    }
  });

  test("a rejection naming a condition keeps its code without a re-read", async () => {
    // The re-read is only for a rejection that left it open whether the
    // message is there. A transport that named `SERVERBUG` has already said
    // the fault is its own, and probing it would turn a broken transport into
    // a report that the caller's reference names nothing.
    const transport = makeMockTransport();
    transport.fetchFull = async () => {
      throw new MessageTransportError(
        "SERVERBUG",
        "the inbound surface is not wired",
      );
    };
    let probes = 0;
    transport.fetchHeaders = async () => {
      probes += 1;
      throw new MessageTransportError(
        "SERVERBUG",
        "the inbound surface is not wired",
      );
    };

    const handler = makeMailReadHandler(transport);
    const result = await handler(
      {
        id: "rd-serverbug",
        name: "mail_read",
        arguments: { ref: { uid: 9, mailbox: "INBOX" }, parts: "payload" },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("fetch_failed");
    expect(probes).toBe(0);
  });

  test("a part path a present message cannot answer is invalid_part", async () => {
    const transport = makeMockTransport();
    transport.fetchPart = async () => {
      throw new Error('no part "9.9" in this message');
    };

    const handler = makeMailReadHandler(transport);
    const result = await handler(
      {
        id: "rd-part",
        name: "mail_read",
        arguments: { ref: { uid: 9, mailbox: "INBOX" }, parts: "9.9" },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_part");
  });
});

// ---------------------------------------------------------------------------
// mail_wait handler
// ---------------------------------------------------------------------------

describe("mail_wait handler", () => {
  test("returns immediately when initial search yields a match", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 42, mailbox: "INBOX" };
    transport.enqueueMessage(ref, {
      ...makeInboundMessage("alice@test"),
      ref,
    });
    transport.setSearchResult([ref]);

    const handler = makeMailWaitHandler(transport);
    const result = await handler(
      {
        id: "w1",
        name: "mail_wait",
        arguments: { query: { from: "alice@test" } },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["from"]).toBe("alice@test");
    expect(result.content["ref"]).toEqual(ref);
  });

  test("a failed read of a pre-existing match is fetch_failed", async () => {
    const transport = makeMockTransport();
    transport.setSearchResult([{ uid: 42, mailbox: "INBOX" }]);
    transport.fetchFull = async () => {
      throw new Error("synthetic fetch failure");
    };

    // Driven through the runner rather than the bare handler, so an unguarded
    // throw would surface as the catch-all's `internal_error` -- the wrong
    // code this guards -- instead of rejecting out of the assertion.
    const tools = createMailTools({
      capabilities: makeCapabilities(transport),
    });

    const result = await tools.run(
      {
        id: "w10",
        name: "mail_wait",
        arguments: { query: { from: "alice@test" } },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("fetch_failed");
    expect(String(result.content["error"])).toContain(
      "synthetic fetch failure",
    );
  });

  test("a read-back the transport refuses as NONEXISTENT is invalid_mailbox", async () => {
    // The search answered for the mailbox, so a NONEXISTENT from the read-back
    // says the mailbox went while the wait held it. `fetch_failed` sends the
    // caller to re-read a message in a mailbox that is not there.
    const transport = makeMockTransport();
    transport.setSearchResult([{ uid: 42, mailbox: "INBOX" }]);
    transport.fetchFull = async () => {
      throw new MessageTransportError(
        "NONEXISTENT",
        `mailbox "INBOX" is not one this address holds`,
      );
    };

    const tools = createMailTools({
      capabilities: makeCapabilities(transport),
    });

    const result = await tools.run(
      {
        id: "w13",
        name: "mail_wait",
        arguments: { query: { from: "alice@test" } },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_mailbox");
  });

  test("a mailbox the initial search rejects is invalid_mailbox, as in mail_search", async () => {
    const transport = makeMockTransport();
    // The transport names the condition; the tools map it. Wording the message
    // to say "does not exist" would test this file's prose rather than the
    // mapping, and every transport words it differently.
    transport.search = async () => {
      throw new MessageTransportError(
        "NONEXISTENT",
        `mailbox "Nope" is not one this address holds`,
      );
    };

    const tools = createMailTools({
      capabilities: makeCapabilities(transport),
    });

    const waited = await tools.run(
      { id: "w11", name: "mail_wait", arguments: { mailbox: "Nope" } },
      signal,
    );
    const searched = await tools.run(
      { id: "w12", name: "mail_search", arguments: { mailbox: "Nope" } },
      signal,
    );

    if (typeof waited.content === "string")
      throw new Error("expected object content");
    if (typeof searched.content === "string")
      throw new Error("expected object content");
    // One transport failure, one code. mail_wait reporting `internal_error`
    // here would blame this package for a mailbox the caller named.
    expect(waited.content["code"]).toBe("invalid_mailbox");
    expect(searched.content["code"]).toBe("invalid_mailbox");
  });

  test("settles with an error when the fetch for a matched arrival rejects", async () => {
    const transport = makeMockTransport();
    transport.setSearchResult([]);
    transport.fetchFull = async () => {
      throw new Error("synthetic fetch failure");
    };

    const watching = Promise.withResolvers<boolean>();
    const baseWatch = transport.watch.bind(transport);
    transport.watch = (mailbox, callback) => {
      const unsubscribe = baseWatch(mailbox, callback);
      watching.resolve(true);
      return unsubscribe;
    };

    const handler = makeMailWaitHandler(transport);
    const pending = handler(
      {
        id: "w2",
        name: "mail_wait",
        arguments: { query: { from: "alice@test" } },
      },
      new AbortController().signal,
    );

    await watching.promise;
    // The transport appends before it notifies, so the message is searchable
    // by the time the arrival callback runs.
    transport.setSearchResult([{ uid: 7, mailbox: "INBOX" }]);
    transport.fireWatch({
      type: "exists",
      uid: 7,
      headers: {
        from: "alice@test",
        to: ["agent@test"],
        date: new Date().toISOString(),
        messageId: "<w2@test>",
      },
    });

    const result = await pending;

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("fetch_failed");
    expect(String(result.content["error"])).toContain(
      "synthetic fetch failure",
    );
  });

  test("settles as aborted when the signal is already aborted", async () => {
    const transport = makeMockTransport();
    transport.setSearchResult([]);

    const controller = new AbortController();
    controller.abort();

    const handler = makeMailWaitHandler(transport);
    const result = await handler(
      {
        id: "w3",
        name: "mail_wait",
        arguments: { query: { from: "alice@test" } },
      },
      controller.signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("aborted");
  });

  test("coerces a date filter carried as a JSON string before the initial search", async () => {
    const transport = makeMockTransport();
    let received: SearchQuery | undefined;
    transport.search = async (_mailbox, query) => {
      received = query;
      return [];
    };

    const controller = new AbortController();
    controller.abort();

    const handler = makeMailWaitHandler(transport);
    const result = await handler(
      {
        id: "w4",
        name: "mail_wait",
        arguments: { query: { after: "2026-01-01T00:00:00Z" } },
      },
      controller.signal,
    );

    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("aborted");
    expect(received?.after).toBeInstanceOf(Date);
  });

  test("a day filter does not raise a type error out of the initial search", async () => {
    const transport = makeMockTransport();
    // Shaped like the mailbox matcher, which reads the calendar day off the
    // query date. A string in that field raises a type error, which the
    // initial search's catch reports as `invalid_query`, so a failure to
    // coerce surfaces there instead of as the abort asserted below.
    transport.search = async (_mailbox, query) => {
      if (query.on !== undefined) query.on.getUTCFullYear();
      return [];
    };

    const controller = new AbortController();
    controller.abort();

    const handler = makeMailWaitHandler(transport);
    const result = await handler(
      {
        id: "w5",
        name: "mail_wait",
        arguments: { query: { on: "2026-03-01" } },
      },
      controller.signal,
    );

    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("aborted");
  });

  test("rejects a misspelled filter and names it", async () => {
    const transport = makeMockTransport();
    let searched = false;
    transport.search = async () => {
      searched = true;
      return [];
    };

    // Aborted up front so an unrejected query settles the wait promptly
    // instead of blocking on the watch until the timeout.
    const controller = new AbortController();
    controller.abort();

    const handler = makeMailWaitHandler(transport);
    const result = await handler(
      {
        id: "w6",
        name: "mail_wait",
        arguments: { query: { sender: "alice@test" } },
      },
      controller.signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("sender");
    expect(searched).toBe(false);
  });

  test("rejects an undeclared filter nested in a boolean composition", async () => {
    const transport = makeMockTransport();
    let searched = false;
    transport.search = async () => {
      searched = true;
      return [];
    };

    const controller = new AbortController();
    controller.abort();

    const handler = makeMailWaitHandler(transport);
    const result = await handler(
      {
        id: "w7",
        name: "mail_wait",
        arguments: {
          query: { or: [{ from: "alice@test" }, { form: "bob@test" }] },
        },
      },
      controller.signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("form");
    expect(searched).toBe(false);
  });

  test("reports a malformed query under the invalid_query code", async () => {
    const controller = new AbortController();
    controller.abort();

    const handler = makeMailWaitHandler(makeMockTransport());
    const result = await handler(
      {
        id: "w8",
        name: "mail_wait",
        arguments: { query: { sender: "alice@test" } },
      },
      controller.signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("invalid_query");
  });

  test("rejects a filter written as an argument instead of inside the query", async () => {
    const transport = makeMockTransport();
    let searched = false;
    transport.search = async () => {
      searched = true;
      return [];
    };

    // Aborted up front so an unrejected argument settles the wait promptly
    // instead of blocking on the watch until the timeout.
    const controller = new AbortController();
    controller.abort();

    const handler = makeMailWaitHandler(transport);
    const result = await handler(
      { id: "w9", name: "mail_wait", arguments: { from: "alice@test" } },
      controller.signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("from");
    expect(searched).toBe(false);
  });

  test("rejects a misspelled timeout instead of waiting for the default", async () => {
    const transport = makeMockTransport();
    let searched = false;
    transport.search = async () => {
      searched = true;
      return [];
    };

    const controller = new AbortController();
    controller.abort();

    const handler = makeMailWaitHandler(transport);
    const result = await handler(
      {
        id: "w10",
        name: "mail_wait",
        arguments: { query: { from: "alice@test" }, timeuot: 5 },
      },
      controller.signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("timeuot");
    expect(searched).toBe(false);
  });

  test("accepts every declared argument", async () => {
    const transport = makeMockTransport();
    const ref: MessageRef = { uid: 77, mailbox: "Archive" };
    transport.enqueueMessage(ref, { ...makeInboundMessage("alice@test"), ref });
    transport.setSearchResult([ref]);

    const handler = makeMailWaitHandler(transport);
    const result = await handler(
      {
        id: "w11",
        name: "mail_wait",
        arguments: {
          query: { from: "alice@test" },
          timeout: 5,
          mailbox: "Archive",
        },
      },
      signal,
    );

    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["ref"]).toEqual(ref);
  });
});

describe("mail_flag handler", () => {
  const ref = { uid: 3, mailbox: "INBOX" };

  test("set adds flags via setFlags", async () => {
    const transport = makeMockTransport();
    const handler = makeMailFlagHandler(transport);
    const result = await handler(
      { id: "f1", name: "mail_flag", arguments: { ref, set: ["\\Deleted"] } },
      signal,
    );
    expect(result.isError).toBeUndefined();
    expect(transport.getFlagCalls()).toEqual([
      { op: "set", ref, flags: ["\\Deleted"] },
    ]);
  });

  test("clear removes flags via clearFlags", async () => {
    const transport = makeMockTransport();
    const handler = makeMailFlagHandler(transport);
    await handler(
      { id: "f2", name: "mail_flag", arguments: { ref, clear: ["\\Seen"] } },
      signal,
    );
    expect(transport.getFlagCalls()).toEqual([
      { op: "clear", ref, flags: ["\\Seen"] },
    ]);
  });

  test("rejects a call that mixes set and clear and touches no transport", async () => {
    const transport = makeMockTransport();
    const handler = makeMailFlagHandler(transport);
    const result = await handler(
      {
        id: "f3",
        name: "mail_flag",
        arguments: { ref, set: ["\\Flagged"], clear: ["\\Seen"] },
      },
      signal,
    );
    expect(result.isError).toBe(true);
    // One direction per call keeps a failure from being half-applied; the
    // rejection lands at the boundary, so neither direction reaches the
    // transport.
    expect(transport.getFlagCalls()).toHaveLength(0);
  });

  test("rejects a call with neither set nor clear and touches no transport", async () => {
    const transport = makeMockTransport();
    const handler = makeMailFlagHandler(transport);
    const result = await handler(
      { id: "f4", name: "mail_flag", arguments: { ref } },
      signal,
    );
    expect(result.isError).toBe(true);
    expect(transport.getFlagCalls()).toHaveLength(0);
  });

  test("surfaces a rejection as a failure with flag_failed", async () => {
    const transport = makeMockTransport();
    transport.failMutations(new Error("supervisor dropped it"));
    const handler = makeMailFlagHandler(transport);
    const result = await handler(
      { id: "f5", name: "mail_flag", arguments: { ref, set: ["\\Deleted"] } },
      signal,
    );
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("flag_failed");
    expect(String(result.content["error"])).toMatch(
      /flag_failed: supervisor dropped it/,
    );
  });

  test("rejects an argument the shape does not declare and touches no transport", async () => {
    // The mailbox of a flag write is the one named in 'ref'. A caller that
    // names a second one has asked for a mutation somewhere other than where
    // the write would land, so an accepted key would flag a message in INBOX
    // and report it as the Archive write the caller asked for.
    const transport = makeMockTransport();
    const handler = makeMailFlagHandler(transport);
    const result = await handler(
      {
        id: "f6",
        name: "mail_flag",
        arguments: { ref, set: ["\\Deleted"], mailbox: "Archive" },
      },
      signal,
    );
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("mailbox");
    expect(transport.getFlagCalls()).toHaveLength(0);
  });
});

describe("mail_expunge handler", () => {
  test("sweeps INBOX and returns the expunged uids", async () => {
    const transport = makeMockTransport();
    transport.setExpungeResult([4, 7]);
    const handler = makeMailExpungeHandler(transport);
    const result = await handler(
      { id: "e1", name: "mail_expunge", arguments: {} },
      signal,
    );
    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["ok"]).toBe(true);
    expect(result.content["expungedUids"]).toEqual([4, 7]);
    expect(transport.getExpungeCalls()).toEqual(["INBOX"]);
  });

  test("surfaces a rejection as a failure with expunge_failed", async () => {
    const transport = makeMockTransport();
    transport.failMutations(new Error("supervisor dropped it"));
    const handler = makeMailExpungeHandler(transport);
    const result = await handler(
      { id: "e2", name: "mail_expunge", arguments: {} },
      signal,
    );
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("expunge_failed");
    expect(String(result.content["error"])).toMatch(
      /expunge_failed: supervisor dropped it/,
    );
  });

  test("a transport that failed for its own reason keeps expunge_failed", async () => {
    // Two conditions are mapped away from the operation's own code -- a mailbox
    // that is not there, and an operation the transport refused outright. This
    // is neither: a transport that violated its own invariant may have swept
    // the INBOX and lost the reply, so the outcome is genuinely unknown.
    const transport = makeMockTransport();
    transport.expunge = async () => {
      throw new MessageTransportError(
        "SERVERBUG",
        "the mutation bridge is not wired",
      );
    };
    const handler = makeMailExpungeHandler(transport);
    const result = await handler(
      { id: "e4", name: "mail_expunge", arguments: {} },
      signal,
    );
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("expunge_failed");
  });

  test("rejects an argument the shape does not declare and sweeps nothing", async () => {
    // The tool takes no parameters and always sweeps INBOX. An accepted
    // 'mailbox' would therefore remove messages from INBOX and report the sweep
    // as the Archive one the caller named.
    const transport = makeMockTransport();
    transport.setExpungeResult([4, 7]);
    const handler = makeMailExpungeHandler(transport);
    const result = await handler(
      { id: "e3", name: "mail_expunge", arguments: { mailbox: "Archive" } },
      signal,
    );
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(String(result.content["error"])).toContain("mailbox");
    expect(transport.getExpungeCalls()).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// A mailbox the transport says is not there
// ---------------------------------------------------------------------------

// RFC 5530's `NONEXISTENT` is raised before the operation is attempted, so
// nothing was read and nothing was written, and the mailbox the call named is
// the only thing that can change. Each tool's own failure code says something
// else, and each wrong thing sends the caller somewhere: `not_found` after a
// message that was never looked for, `invalid_part` to rewrite a part path
// that was never parsed, and `flag_failed` to re-read a message to learn
// whether the flag stuck -- a re-read that meets the same refusal and never
// names the mailbox either.
describe("a mailbox the transport says is not there", () => {
  const ref = { uid: 7, mailbox: "Nope" };

  function makeRefusingTransport(): MockTransport {
    const transport = makeMockTransport();
    // The transport names the condition and the tools map it. Matching on the
    // wording instead would test this file's prose, and every transport words
    // it differently.
    const refuse = (): never => {
      throw new MessageTransportError(
        "NONEXISTENT",
        `mailbox "Nope" is not one this address holds`,
      );
    };
    transport.fetchHeaders = async () => refuse();
    transport.fetchFull = async () => refuse();
    transport.fetchPart = async () => refuse();
    transport.setFlags = async () => refuse();
    transport.clearFlags = async () => refuse();
    transport.expunge = async () => refuse();
    return transport;
  }

  const calls: [label: string, call: ToolCall][] = [
    [
      "mail_reply",
      { id: "n1", name: "mail_reply", arguments: { ref, content: "hi" } },
    ],
    [
      "mail_read parts=headers",
      { id: "n2", name: "mail_read", arguments: { ref, parts: "headers" } },
    ],
    [
      "mail_read parts=full",
      { id: "n3", name: "mail_read", arguments: { ref, parts: "full" } },
    ],
    [
      "mail_read parts=payload",
      { id: "n4", name: "mail_read", arguments: { ref, parts: "payload" } },
    ],
    [
      "mail_read parts=1.3",
      { id: "n5", name: "mail_read", arguments: { ref, parts: "1.3" } },
    ],
    [
      "mail_flag set",
      { id: "n6", name: "mail_flag", arguments: { ref, set: ["\\Deleted"] } },
    ],
    [
      "mail_flag clear",
      { id: "n7", name: "mail_flag", arguments: { ref, clear: ["\\Seen"] } },
    ],
    [
      "mail_search",
      { id: "n8", name: "mail_search", arguments: { mailbox: "Nope" } },
    ],
    // The tool names no mailbox, so nothing in the call can change. The code
    // still earns its place: it says the sweep did not happen, where
    // `expunge_failed` says messages may be gone and a retry may help.
    ["mail_expunge", { id: "n10", name: "mail_expunge", arguments: {} }],
  ];

  for (const [label, call] of calls) {
    test(`${label} reports invalid_mailbox`, async () => {
      const transport = makeRefusingTransport();
      transport.search = async () => {
        throw new MessageTransportError(
          "NONEXISTENT",
          `mailbox "Nope" is not one this address holds`,
        );
      };
      const tools = createMailTools({
        capabilities: makeCapabilities(transport),
      });

      const result = await tools.run(call, signal);
      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["code"]).toBe("invalid_mailbox");
    });
  }

  test("a transport that failed for its own reason keeps the operation's own code", async () => {
    // The carve-out covers a mailbox that is not there and an operation the
    // transport refused outright. A transport that violated its own invariant
    // has told the caller neither, and a flag write that failed that way still
    // leaves the outcome unknown.
    const transport = makeMockTransport();
    transport.setFlags = async () => {
      throw new MessageTransportError(
        "SERVERBUG",
        "the mutation bridge is not wired",
      );
    };
    const handler = makeMailFlagHandler(transport);
    const result = await handler(
      {
        id: "n9",
        name: "mail_flag",
        arguments: { ref: { uid: 7, mailbox: "INBOX" }, set: ["\\Deleted"] },
      },
      signal,
    );
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("flag_failed");
  });
});

// ---------------------------------------------------------------------------
// Names Object.prototype carries
// ---------------------------------------------------------------------------

// Each shape's `"+": "reject"` refuses an undeclared key by asking whether the
// key is one the shape declares, and a shape declaring at least one key asks
// that of an ordinary object, so every name Object.prototype carries answers
// "declared" and is let through. The handlers refuse those names ahead of the
// shape. These tests hold that second path to the same contract as the first:
// the same refusal, with the same wording, and before the transport is touched.
//
// Every block runs an ordinary key alongside the inherited names, as the
// control on the block itself: where the block asserts a refusal the ordinary
// key is refused the same way, and where it asserts an acceptance the ordinary
// key is accepted the same way.

describe("inherited argument names", () => {
  // `{ __proto__: value }` in source sets the object's prototype instead of
  // adding a key, so the name is installed rather than written. The other five
  // names need no such care, but they are installed the same way to keep one
  // construction for the whole table.
  function withKey(
    base: Record<string, unknown>,
    key: string,
    value: unknown,
  ): Record<string, unknown> {
    return Object.defineProperty(base, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  }

  const INHERITED_NAMES = [
    "__proto__",
    "constructor",
    "toString",
    "hasOwnProperty",
    "valueOf",
    "__defineGetter__",
  ];

  // The key the shapes refuse on their own. It rides in every block as the
  // control: the handlers report it with the same wording as a reserved name,
  // so a block whose reserved cases stopped proving anything would still have
  // to show this one being refused.
  const ORDINARY_KEY = "zzz";

  type TracedTransport = { transport: MockTransport; touched: string[] };

  // Records the transport methods the handlers reach, so a refusal can be
  // shown to land before the transport rather than after it.
  function makeTracedTransport(): TracedTransport {
    const inner = makeMockTransport();
    const touched: string[] = [];

    // A seeded match means a handler that got past the refusal answers from
    // its first search. mail_wait would otherwise subscribe and hold the call
    // open for its whole deadline, so a handler that stopped refusing would
    // read as a hung test rather than as a failed assertion.
    inner.setSearchResult([{ uid: 1, mailbox: "INBOX" }]);

    const transport: MockTransport = {
      ...inner,
      send(message, signal) {
        touched.push("send");
        return inner.send(message, signal);
      },
      search(mailbox, query, signal) {
        touched.push("search");
        return inner.search(mailbox, query, signal);
      },
      fetchHeaders(ref, signal) {
        touched.push("fetchHeaders");
        return inner.fetchHeaders(ref, signal);
      },
      fetchFull(ref, signal) {
        touched.push("fetchFull");
        return inner.fetchFull(ref, signal);
      },
      fetchPart(ref, partPath, signal) {
        touched.push("fetchPart");
        return inner.fetchPart(ref, partPath, signal);
      },
      setFlags(ref, flags, signal) {
        touched.push("setFlags");
        return inner.setFlags(ref, flags, signal);
      },
      clearFlags(ref, flags, signal) {
        touched.push("clearFlags");
        return inner.clearFlags(ref, flags, signal);
      },
      expunge(mailbox, signal) {
        touched.push("expunge");
        return inner.expunge(mailbox, signal);
      },
      watch(mailbox, callback) {
        touched.push("watch");
        return inner.watch(mailbox, callback);
      },
    };

    return { transport, touched };
  }

  function expectRefusal(
    result: ToolResult,
    error: string,
    code: MailToolErrorCode,
  ): void {
    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe(code);
    // The dotted path is asserted whole: it names the key and where it sat, so
    // a refusal that reached the caller for some other reason cannot satisfy
    // it.
    expect(result.content["error"]).toBe(error);
  }

  type ShapeCase = {
    tool: string;
    makeHandler: (transport: MessageTransport) => ToolHandler;
    otherwiseValid: () => Record<string, unknown>;
  };

  // Every argument shape in the package, with arguments that are otherwise
  // complete, so the added key is the only thing wrong with the call.
  const ARGUMENT_SHAPES: ShapeCase[] = [
    {
      tool: "mail_send",
      makeHandler: makeMailSendHandler,
      otherwiseValid: () => ({ to: "peer@test", content: "hello" }),
    },
    {
      tool: "mail_reply",
      makeHandler: makeMailReplyHandler,
      otherwiseValid: () => ({
        ref: { uid: 1, mailbox: "INBOX" },
        content: "hello",
      }),
    },
    {
      tool: "mail_search",
      makeHandler: makeMailSearchHandler,
      otherwiseValid: () => ({ mailbox: "INBOX" }),
    },
    {
      tool: "mail_read",
      makeHandler: makeMailReadHandler,
      otherwiseValid: () => ({ ref: { uid: 1, mailbox: "INBOX" } }),
    },
    {
      tool: "mail_wait",
      makeHandler: makeMailWaitHandler,
      otherwiseValid: () => ({ mailbox: "INBOX" }),
    },
    {
      tool: "mail_flag",
      makeHandler: makeMailFlagHandler,
      otherwiseValid: () => ({
        ref: { uid: 1, mailbox: "INBOX" },
        set: ["\\Seen"],
      }),
    },
    // The tool takes no parameters, so every key a caller sends is undeclared
    // and an accepted one reads as a scope the sweep does not have. This is
    // the one shape whose refusal does not depend on the handler's own check:
    // a shape declaring no keys compiles to a check with no declared-key table
    // to consult, so `"+": "reject"` refuses an inherited name here as readily
    // as an ordinary one, and refuses it with the same wording. The case below
    // therefore pins the contract rather than the path that enforces it.
    {
      tool: "mail_expunge",
      makeHandler: makeMailExpungeHandler,
      otherwiseValid: () => ({}),
    },
  ];

  test("every covered name is one Object.prototype carries", () => {
    const carried = new Set(Object.getOwnPropertyNames(Object.prototype));
    for (const name of INHERITED_NAMES) {
      expect(carried.has(name)).toBe(true);
    }
    expect(carried.has(ORDINARY_KEY)).toBe(false);
  });

  describe("at the top level of an argument shape", () => {
    for (const shape of ARGUMENT_SHAPES) {
      for (const name of [...INHERITED_NAMES, ORDINARY_KEY]) {
        test(`${shape.tool} refuses '${name}'`, async () => {
          const { transport, touched } = makeTracedTransport();
          const handler = shape.makeHandler(transport);

          const result = await handler(
            {
              id: `${shape.tool}-top-${name}`,
              name: shape.tool,
              arguments: withKey(shape.otherwiseValid(), name, "anything"),
            },
            signal,
          );

          expectRefusal(result, `${name} must be removed`, "invalid_arguments");
          expect(touched).toEqual([]);
        });
      }
    }
  });

  describe("inside a message reference", () => {
    const refShapes: ShapeCase[] = [
      {
        tool: "mail_reply",
        makeHandler: makeMailReplyHandler,
        otherwiseValid: () => ({ content: "hello" }),
      },
      {
        tool: "mail_read",
        makeHandler: makeMailReadHandler,
        otherwiseValid: () => ({}),
      },
      {
        tool: "mail_flag",
        makeHandler: makeMailFlagHandler,
        otherwiseValid: () => ({ set: ["\\Seen"] }),
      },
    ];

    for (const shape of refShapes) {
      for (const name of [...INHERITED_NAMES, ORDINARY_KEY]) {
        test(`${shape.tool} refuses '${name}' in 'ref'`, async () => {
          const { transport, touched } = makeTracedTransport();
          const handler = shape.makeHandler(transport);

          const result = await handler(
            {
              id: `${shape.tool}-ref-${name}`,
              name: shape.tool,
              arguments: {
                ...shape.otherwiseValid(),
                ref: withKey(
                  { uid: 1, mailbox: "INBOX" },
                  name,
                  "another mailbox",
                ),
              },
            },
            signal,
          );

          expectRefusal(
            result,
            `ref.${name} must be removed`,
            "invalid_arguments",
          );
          expect(touched).toEqual([]);
        });
      }
    }
  });

  describe("inside a search query", () => {
    // A query is closed by SearchQueryArgs rather than by the argument shape,
    // and a malformed filter is an `invalid_query`, so a reserved name in one
    // carries that code and not `invalid_arguments`.
    const queryTools: ShapeCase[] = [
      {
        tool: "mail_search",
        makeHandler: makeMailSearchHandler,
        otherwiseValid: () => ({ mailbox: "INBOX" }),
      },
      {
        tool: "mail_wait",
        makeHandler: makeMailWaitHandler,
        otherwiseValid: () => ({ mailbox: "INBOX" }),
      },
    ];

    // Every position the walk has to reach. The 'and', 'or', and 'not'
    // branches resolve through the same recursive alias as the query root, and
    // two of them hold their branches in an array, so the walk reaches them
    // only by indexing one.
    const positions = [
      {
        label: "the query root",
        build: (key: string) => withKey({}, key, "anything"),
        path: (key: string) => key,
      },
      {
        label: "an 'and' branch",
        build: (key: string) => ({
          and: [{ from: "peer@test" }, withKey({}, key, "anything")],
        }),
        path: (key: string) => `and[1].${key}`,
      },
      {
        label: "an 'or' branch",
        build: (key: string) => ({ or: [withKey({}, key, "anything")] }),
        path: (key: string) => `or[0].${key}`,
      },
      {
        label: "a 'not' branch",
        build: (key: string) => ({ not: withKey({}, key, "anything") }),
        path: (key: string) => `not.${key}`,
      },
      {
        label: "a branch of a branch",
        build: (key: string) => ({
          and: [{ not: withKey({}, key, "anything") }],
        }),
        path: (key: string) => `and[0].not.${key}`,
      },
      {
        label: "the header filter",
        build: (key: string) => ({
          header: withKey(
            { field: "Interchange-Type", contains: "offering.request" },
            key,
            "anything",
          ),
        }),
        path: (key: string) => `header.${key}`,
      },
    ];

    for (const shape of queryTools) {
      for (const position of positions) {
        for (const name of [...INHERITED_NAMES, ORDINARY_KEY]) {
          test(`${shape.tool} refuses '${name}' in ${position.label}`, async () => {
            const { transport, touched } = makeTracedTransport();
            const handler = shape.makeHandler(transport);

            const result = await handler(
              {
                id: `${shape.tool}-query-${name}`,
                name: shape.tool,
                arguments: {
                  ...shape.otherwiseValid(),
                  query: position.build(name),
                },
              },
              signal,
            );

            expectRefusal(
              result,
              `${position.path(name)} must be removed`,
              "invalid_query",
            );
            expect(touched).toEqual([]);
          });
        }
      }
    }
  });

  describe("inside an array element of an argument", () => {
    // 'to' takes strings, so an object there is malformed whatever its keys
    // are. What is asserted is which failure the caller is told about: the
    // reserved name, named at its index, rather than the element's type.
    for (const name of INHERITED_NAMES) {
      test(`mail_send refuses '${name}' in a 'to' element`, async () => {
        const { transport, touched } = makeTracedTransport();
        const handler = makeMailSendHandler(transport);

        const result = await handler(
          {
            id: `mail_send-array-${name}`,
            name: "mail_send",
            arguments: {
              to: [withKey({}, name, "anything")],
              content: "hello",
            },
          },
          signal,
        );

        expectRefusal(
          result,
          `to[0].${name} must be removed`,
          "invalid_arguments",
        );
        expect(touched).toEqual([]);
      });
    }

    test("mail_send refuses an ordinary element the shape cannot take", async () => {
      // The control for the block above. An object holding no inherited name
      // is refused for what it is rather than for what it holds, so the two
      // refusals are told apart by their wording and not only by the fact that
      // both failed.
      const { transport, touched } = makeTracedTransport();
      const handler = makeMailSendHandler(transport);

      const result = await handler(
        {
          id: "mail_send-array-ordinary",
          name: "mail_send",
          arguments: {
            to: [withKey({}, ORDINARY_KEY, "anything")],
            content: "hello",
          },
        },
        signal,
      );

      expect(result.isError).toBe(true);
      if (typeof result.content === "string")
        throw new Error("expected object content");
      expect(result.content["code"]).toBe("invalid_arguments");
      expect(String(result.content["error"])).toContain("to[0] must be");
      expect(String(result.content["error"])).not.toContain("must be removed");
      expect(touched).toEqual([]);
    });
  });

  describe("inside a payload", () => {
    // A payload is arbitrary JSON bound for another agent, so a name inside it
    // is the caller's to choose and none of this applies to it. These are the
    // cases that hold the refusal to the keys the shapes own.
    const payloadTools: ShapeCase[] = [
      {
        tool: "mail_send",
        makeHandler: makeMailSendHandler,
        otherwiseValid: () => ({ to: "peer@test" }),
      },
      {
        tool: "mail_reply",
        makeHandler: makeMailReplyHandler,
        otherwiseValid: () => ({ ref: { uid: 1, mailbox: "INBOX" } }),
      },
    ];

    for (const shape of payloadTools) {
      for (const name of [...INHERITED_NAMES, ORDINARY_KEY]) {
        test(`${shape.tool} carries '${name}' in a payload`, async () => {
          const transport = makeMockTransport();
          const handler = shape.makeHandler(transport);

          const result = await handler(
            {
              id: `${shape.tool}-payload-${name}`,
              name: shape.tool,
              arguments: {
                ...shape.otherwiseValid(),
                type: "offering.request",
                payload: withKey({ kind: "quote" }, name, "the caller's"),
              },
            },
            signal,
          );

          expect(result.isError).toBeUndefined();
          const sent = transport.getSentMessages();
          expect(sent).toHaveLength(1);
          const payload = sent[0]?.payload;
          if (payload === undefined) throw new Error("expected a payload");
          expect(Object.keys(payload)).toContain(name);
        });
      }
    }
  });
});
