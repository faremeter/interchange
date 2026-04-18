import type {
  MessageTransport,
  OutboundMessage,
  SendReceipt,
  InboundMessage,
  MessageRef,
  Mailbox,
  MailboxStatus,
  SearchQuery,
  Thread,
  MessageHeaders,
  BodyStructure,
  MessagePart,
  SyncState,
  SyncResult,
  ListInfo,
  MailboxEvent,
  Unsubscribe,
  CryptoProvider,
} from "@interchange/types/runtime";
import {
  createAgentEntry,
  createMailboxStore,
  requireMessage,
  appendToMailbox,
  type AgentMailboxEntry,
} from "./mailbox";
import { parseHeaderSection } from "./mime";
import { buildMessageHeaders } from "./headers";
import { executeSend, type RemoteSendHandler } from "./send";
import { executeSearch } from "./search";
import { executeThread } from "./thread";
import {
  fetchHeaders as doFetchHeaders,
  fetchStructure as doFetchStructure,
  fetchPart as doFetchPart,
  fetchFull as doFetchFull,
} from "./fetch";

/**
 * In-memory MessageTransport implementing full IMAP semantics within a
 * single process. Messages are stored as real RFC 2822 MIME byte buffers.
 *
 * Every outbound message is PGP/MIME signed with the sender's CryptoProvider.
 * Signature verification runs on fetchFull().
 *
 * Agents must be registered before sending or receiving messages.
 */
export class InMemoryTransport implements MessageTransport {
  readonly #agentMailboxes = new Map<string, AgentMailboxEntry>();
  readonly #cryptoProviders = new Map<string, CryptoProvider>();
  #remoteSendHandler: RemoteSendHandler | undefined;

  /**
   * Set a handler for delivering messages to recipients not registered on
   * this transport. The federation layer calls this to wire up the websocket
   * connection to the hub. When set, send() forwards unregistered recipients
   * to this handler instead of throwing.
   */
  setRemoteSendHandler(handler: RemoteSendHandler): void {
    this.#remoteSendHandler = handler;
  }

  /**
   * Register an agent with its address and CryptoProvider. Creates the
   * default set of mailboxes (INBOX, Sent, Drafts, Archive, Trash).
   *
   * Throws if the address is already registered.
   */
  registerAgent(address: string, crypto: CryptoProvider): void {
    if (this.#agentMailboxes.has(address)) {
      throw new Error(`Agent "${address}" is already registered`);
    }
    this.#agentMailboxes.set(address, createAgentEntry());
    this.#cryptoProviders.set(address, crypto);
  }

  // ---------------------------------------------------------------------------
  // Outbound
  // ---------------------------------------------------------------------------

  async send(
    _message: OutboundMessage,
    _signal?: AbortSignal,
  ): Promise<SendReceipt> {
    throw new Error(
      "Use createInMemoryTransport().getTransportForAgent(address) to send messages",
    );
  }

  async append(
    _mailbox: string,
    _message: InboundMessage,
    _flags?: string[],
    _signal?: AbortSignal,
  ): Promise<MessageRef> {
    throw new Error(
      "Use createInMemoryTransport().getTransportForAgent(address) to append messages",
    );
  }

  // ---------------------------------------------------------------------------
  // Mailbox management (agent-scoped — use getTransportForAgent)
  // ---------------------------------------------------------------------------

  async listMailboxes(_signal?: AbortSignal): Promise<Mailbox[]> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async createMailbox(_name: string, _signal?: AbortSignal): Promise<Mailbox> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async deleteMailbox(_name: string, _signal?: AbortSignal): Promise<void> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async getMailboxStatus(
    _name: string,
    _signal?: AbortSignal,
  ): Promise<MailboxStatus> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async search(
    _mailbox: string,
    _query: SearchQuery,
    _signal?: AbortSignal,
  ): Promise<MessageRef[]> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async thread(
    _mailbox: string,
    _algorithm: "references" | "orderedsubject",
    _query?: SearchQuery,
    _signal?: AbortSignal,
  ): Promise<Thread[]> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async fetchHeaders(
    _ref: MessageRef,
    _signal?: AbortSignal,
  ): Promise<MessageHeaders> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async fetchStructure(
    _ref: MessageRef,
    _signal?: AbortSignal,
  ): Promise<BodyStructure> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async fetchPart(
    _ref: MessageRef,
    _partPath: string,
    _signal?: AbortSignal,
  ): Promise<MessagePart> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async fetchFull(
    _ref: MessageRef,
    _signal?: AbortSignal,
  ): Promise<InboundMessage> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async setFlags(
    _ref: MessageRef,
    _flags: string[],
    _signal?: AbortSignal,
  ): Promise<void> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async clearFlags(
    _ref: MessageRef,
    _flags: string[],
    _signal?: AbortSignal,
  ): Promise<void> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async move(
    _ref: MessageRef,
    _toMailbox: string,
    _signal?: AbortSignal,
  ): Promise<void> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async copy(
    _ref: MessageRef,
    _toMailbox: string,
    _signal?: AbortSignal,
  ): Promise<void> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async expunge(_mailbox: string, _signal?: AbortSignal): Promise<void> {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  watch(
    _mailbox: string,
    _callback: (event: MailboxEvent) => void,
  ): Unsubscribe {
    throw new Error("Use getTransportForAgent(address) for agent operations");
  }

  async sync(
    _mailbox: string,
    _knownState: SyncState,
    _signal?: AbortSignal,
  ): Promise<SyncResult> {
    throw new Error("sync() (QRESYNC) is not implemented");
  }

  async createList(
    _address: string,
    _name: string,
    _signal?: AbortSignal,
  ): Promise<ListInfo> {
    throw new Error("Distribution list management is not implemented");
  }

  async listMembers(
    _address: string,
    _signal?: AbortSignal,
  ): Promise<string[]> {
    throw new Error("Distribution list management is not implemented");
  }

  async subscribe(
    _listAddress: string,
    _agentAddress: string,
    _signal?: AbortSignal,
  ): Promise<void> {
    throw new Error("Distribution list management is not implemented");
  }

  async unsubscribe(
    _listAddress: string,
    _agentAddress: string,
    _signal?: AbortSignal,
  ): Promise<void> {
    throw new Error("Distribution list management is not implemented");
  }

  // ---------------------------------------------------------------------------
  // Inbound delivery from federation
  // ---------------------------------------------------------------------------

  /**
   * Deliver a signed MIME message to an agent's INBOX. Used by the
   * federation layer when a message arrives from the hub over the
   * websocket — the message is already assembled and signed by the
   * originating agent, so no further processing is needed beyond
   * envelope parsing and storage.
   *
   * Throws if the agent is not registered.
   */
  deliver(agentAddress: string, message: Uint8Array): void {
    const entry = this.#agentMailboxes.get(agentAddress);
    if (entry === undefined) {
      throw new Error(
        `Agent "${agentAddress}" is not registered — cannot deliver mail`,
      );
    }
    const inbox = entry.mailboxes.get("INBOX");
    if (inbox === undefined) {
      throw new Error(`Agent "${agentAddress}" has no INBOX`);
    }

    const { headers } = parseHeaderSection(message);

    const messageId = headers.get("message-id");
    const from = headers.get("from");
    const dateRaw = headers.get("date");
    if (messageId === undefined) {
      throw new Error("Cannot deliver message: missing Message-ID header");
    }
    if (from === undefined) {
      throw new Error("Cannot deliver message: missing From header");
    }
    if (dateRaw === undefined) {
      throw new Error("Cannot deliver message: missing Date header");
    }

    const msgHeaders = buildMessageHeaders(headers);

    const toRaw = headers.get("to") ?? "";
    const to = toRaw
      ? toRaw
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [];

    const refsRaw = headers.get("references");
    const references = refsRaw ? refsRaw.split(/\s+/).filter(Boolean) : [];

    const envelope: import("./mailbox").StoredEnvelope = {
      messageId,
      from,
      to,
      subject: headers.get("subject") ?? "",
      date: new Date(dateRaw),
      inReplyTo: headers.get("in-reply-to"),
      references,
      interchangeType: headers.get("interchange-type"),
      interchangeCorrelationId: headers.get("interchange-correlation-id"),
    };

    const uid = appendToMailbox(inbox, message, envelope, []);

    const callbacks = entry.watchCallbacks.get("INBOX");
    if (callbacks !== undefined && callbacks.size > 0) {
      const event: import("@interchange/types/runtime").MailboxEvent = {
        type: "exists",
        uid,
        headers: msgHeaders,
      };
      for (const cb of callbacks) {
        queueMicrotask(() => cb(event));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Internal: agent-scoped view
  // ---------------------------------------------------------------------------

  /**
   * Returns an agent-scoped MessageTransport bound to the given address.
   * The harness calls this to obtain a transport that acts as a specific agent.
   */
  getTransportForAgent(address: string): MessageTransport {
    if (!this.#agentMailboxes.has(address)) {
      throw new Error(
        `Agent "${address}" is not registered — call registerAgent() first`,
      );
    }
    return new AgentMessageTransport(
      address,
      this.#agentMailboxes,
      this.#cryptoProviders,
      () => this.#remoteSendHandler,
    );
  }
}

/**
 * Agent-scoped MessageTransport. All operations are scoped to one agent's
 * mailboxes. Constructed via InMemoryTransport.getTransportForAgent().
 */
class AgentMessageTransport implements MessageTransport {
  readonly #address: string;
  readonly #agentMailboxes: Map<string, AgentMailboxEntry>;
  readonly #cryptoProviders: Map<string, CryptoProvider>;
  readonly #getRemoteSendHandler: () => RemoteSendHandler | undefined;

  constructor(
    address: string,
    agentMailboxes: Map<string, AgentMailboxEntry>,
    cryptoProviders: Map<string, CryptoProvider>,
    getRemoteSendHandler: () => RemoteSendHandler | undefined,
  ) {
    this.#address = address;
    this.#agentMailboxes = agentMailboxes;
    this.#cryptoProviders = cryptoProviders;
    this.#getRemoteSendHandler = getRemoteSendHandler;
  }

  get #entry(): AgentMailboxEntry {
    const e = this.#agentMailboxes.get(this.#address);
    if (e === undefined) {
      throw new Error(`Agent "${this.#address}" has been deregistered`);
    }
    return e;
  }

  #requireMailbox(name: string) {
    const store = this.#entry.mailboxes.get(name);
    if (store === undefined) {
      throw new Error(
        `Mailbox "${name}" does not exist for agent "${this.#address}"`,
      );
    }
    return store;
  }

  async send(
    message: OutboundMessage,
    _signal?: AbortSignal,
  ): Promise<SendReceipt> {
    return executeSend(
      this.#address,
      message,
      this.#agentMailboxes,
      this.#cryptoProviders,
      this.#getRemoteSendHandler(),
    );
  }

  async append(
    mailbox: string,
    message: InboundMessage,
    flags?: string[],
    _signal?: AbortSignal,
  ): Promise<MessageRef> {
    const store = this.#requireMailbox(mailbox);
    // For append, we need to convert InboundMessage back to raw bytes.
    // Since InboundMessage may come from a prior fetchFull, we need the raw
    // bytes. This is a design gap — append() takes InboundMessage but we
    // need Uint8Array. We store a minimal representation.
    //
    // For now, serialize the InboundMessage as a minimal RFC 2822 message.
    const raw = inboundMessageToRaw(message);
    const envelope = {
      messageId: message.headers.messageId,
      from: message.headers.from,
      to: message.headers.to,
      subject: message.headers.subject ?? "",
      date: new Date(message.headers.date),
      inReplyTo: message.headers.inReplyTo,
      references: message.headers.references ?? [],
      interchangeType: message.headers.interchangeType,
      interchangeCorrelationId: message.headers.interchangeCorrelationId,
    };
    const uid = appendToMailbox(store, raw, envelope, flags ?? []);
    return { uid, mailbox };
  }

  async listMailboxes(_signal?: AbortSignal): Promise<Mailbox[]> {
    return Array.from(this.#entry.mailboxes.keys()).map((name) => ({
      name,
    }));
  }

  async createMailbox(name: string, _signal?: AbortSignal): Promise<Mailbox> {
    if (this.#entry.mailboxes.has(name)) {
      throw new Error(
        `Mailbox "${name}" already exists for agent "${this.#address}"`,
      );
    }
    this.#entry.mailboxes.set(name, createMailboxStore());
    return { name };
  }

  async deleteMailbox(name: string, _signal?: AbortSignal): Promise<void> {
    if (!this.#entry.mailboxes.has(name)) {
      throw new Error(
        `Mailbox "${name}" does not exist for agent "${this.#address}"`,
      );
    }
    this.#entry.mailboxes.delete(name);
  }

  async getMailboxStatus(
    name: string,
    _signal?: AbortSignal,
  ): Promise<MailboxStatus> {
    const store = this.#requireMailbox(name);
    const unseen = store.messages.filter((m) => !m.flags.has("\\Seen")).length;
    return {
      total: store.messages.length,
      unseen,
      recent: 0,
      uidNext: store.uidCounter,
      uidValidity: store.uidValidity,
      highestModSeq: store.modseqCounter - 1,
    };
  }

  async search(
    mailbox: string,
    query: SearchQuery,
    _signal?: AbortSignal,
  ): Promise<MessageRef[]> {
    const store = this.#requireMailbox(mailbox);
    return executeSearch(mailbox, store, query);
  }

  async thread(
    mailbox: string,
    algorithm: "references" | "orderedsubject",
    query?: SearchQuery,
    _signal?: AbortSignal,
  ): Promise<Thread[]> {
    const store = this.#requireMailbox(mailbox);
    return executeThread(mailbox, store, algorithm, query);
  }

  async fetchHeaders(
    ref: MessageRef,
    _signal?: AbortSignal,
  ): Promise<MessageHeaders> {
    const store = this.#requireMailbox(ref.mailbox);
    return doFetchHeaders(ref, store);
  }

  async fetchStructure(
    ref: MessageRef,
    _signal?: AbortSignal,
  ): Promise<BodyStructure> {
    const store = this.#requireMailbox(ref.mailbox);
    return doFetchStructure(ref, store);
  }

  async fetchPart(
    ref: MessageRef,
    partPath: string,
    _signal?: AbortSignal,
  ): Promise<MessagePart> {
    const store = this.#requireMailbox(ref.mailbox);
    return doFetchPart(ref, partPath, store);
  }

  async fetchFull(
    ref: MessageRef,
    _signal?: AbortSignal,
  ): Promise<InboundMessage> {
    const store = this.#requireMailbox(ref.mailbox);
    return doFetchFull(ref, store, this.#cryptoProviders);
  }

  async setFlags(
    ref: MessageRef,
    flags: string[],
    _signal?: AbortSignal,
  ): Promise<void> {
    const store = this.#requireMailbox(ref.mailbox);
    const msg = requireMessage(store, ref.uid, ref.mailbox);
    for (const flag of flags) {
      msg.flags.add(flag);
    }
    msg.modseq = store.modseqCounter++;
    this.#fireWatchCallbacks(ref.mailbox, {
      type: "flagsChanged",
      uid: ref.uid,
      flags: Array.from(msg.flags),
    });
  }

  async clearFlags(
    ref: MessageRef,
    flags: string[],
    _signal?: AbortSignal,
  ): Promise<void> {
    const store = this.#requireMailbox(ref.mailbox);
    const msg = requireMessage(store, ref.uid, ref.mailbox);
    for (const flag of flags) {
      msg.flags.delete(flag);
    }
    msg.modseq = store.modseqCounter++;
    this.#fireWatchCallbacks(ref.mailbox, {
      type: "flagsChanged",
      uid: ref.uid,
      flags: Array.from(msg.flags),
    });
  }

  async move(
    ref: MessageRef,
    toMailbox: string,
    _signal?: AbortSignal,
  ): Promise<void> {
    const fromStore = this.#requireMailbox(ref.mailbox);
    const toStore = this.#requireMailbox(toMailbox);
    const msgIdx = fromStore.messages.findIndex((m) => m.uid === ref.uid);
    if (msgIdx === -1) {
      throw new Error(
        `Message UID ${ref.uid} not found in mailbox "${ref.mailbox}"`,
      );
    }
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion -- guarded by findIndex !== -1 above
    const msg = fromStore.messages[msgIdx]!;
    fromStore.messages.splice(msgIdx, 1);

    const newUid = appendToMailbox(
      toStore,
      msg.raw,
      msg.envelope,
      Array.from(msg.flags),
    );

    this.#fireWatchCallbacks(ref.mailbox, {
      type: "expunged",
      uid: ref.uid,
    });

    // Notify watchers of the new message in the destination mailbox.
    const { headers: parsedHeaders } = parseHeaderSection(msg.raw);
    const msgHeaders = this.#buildMessageHeaders(parsedHeaders);
    this.#fireWatchCallbacks(toMailbox, {
      type: "exists",
      uid: newUid,
      headers: msgHeaders,
    });
  }

  async copy(
    ref: MessageRef,
    toMailbox: string,
    _signal?: AbortSignal,
  ): Promise<void> {
    const fromStore = this.#requireMailbox(ref.mailbox);
    const toStore = this.#requireMailbox(toMailbox);
    const msg = requireMessage(fromStore, ref.uid, ref.mailbox);

    const newUid = appendToMailbox(
      toStore,
      msg.raw,
      msg.envelope,
      Array.from(msg.flags),
    );

    const { headers: parsedHeaders } = parseHeaderSection(msg.raw);
    const msgHeaders = this.#buildMessageHeaders(parsedHeaders);
    this.#fireWatchCallbacks(toMailbox, {
      type: "exists",
      uid: newUid,
      headers: msgHeaders,
    });
  }

  async expunge(mailbox: string, _signal?: AbortSignal): Promise<void> {
    const store = this.#requireMailbox(mailbox);
    const toExpunge = store.messages.filter((m) => m.flags.has("\\Deleted"));

    store.messages = store.messages.filter((m) => !m.flags.has("\\Deleted"));

    for (const msg of toExpunge) {
      this.#fireWatchCallbacks(mailbox, {
        type: "expunged",
        uid: msg.uid,
      });
    }
  }

  watch(mailbox: string, callback: (event: MailboxEvent) => void): Unsubscribe {
    this.#requireMailbox(mailbox);
    let callbacks = this.#entry.watchCallbacks.get(mailbox);
    if (callbacks === undefined) {
      callbacks = new Set();
      this.#entry.watchCallbacks.set(mailbox, callbacks);
    }
    callbacks.add(callback);

    return () => {
      const cbs = this.#entry.watchCallbacks.get(mailbox);
      cbs?.delete(callback);
    };
  }

  async sync(
    _mailbox: string,
    _knownState: SyncState,
    _signal?: AbortSignal,
  ): Promise<SyncResult> {
    throw new Error("sync() (QRESYNC) is not implemented");
  }

  async createList(
    _address: string,
    _name: string,
    _signal?: AbortSignal,
  ): Promise<ListInfo> {
    throw new Error("Distribution list management is not implemented");
  }

  async listMembers(
    _address: string,
    _signal?: AbortSignal,
  ): Promise<string[]> {
    throw new Error("Distribution list management is not implemented");
  }

  async subscribe(
    _listAddress: string,
    _agentAddress: string,
    _signal?: AbortSignal,
  ): Promise<void> {
    throw new Error("Distribution list management is not implemented");
  }

  async unsubscribe(
    _listAddress: string,
    _agentAddress: string,
    _signal?: AbortSignal,
  ): Promise<void> {
    throw new Error("Distribution list management is not implemented");
  }

  #fireWatchCallbacks(mailbox: string, event: MailboxEvent): void {
    const callbacks = this.#entry.watchCallbacks.get(mailbox);
    if (callbacks === undefined || callbacks.size === 0) return;
    for (const cb of callbacks) {
      queueMicrotask(() => cb(event));
    }
  }

  #buildMessageHeaders(
    headers: Map<string, string>,
  ): import("@interchange/types/runtime").MessageHeaders {
    return buildMessageHeaders(headers);
  }
}

function inboundMessageToRaw(message: InboundMessage): Uint8Array {
  const enc = new TextEncoder();
  const CRLF = "\r\n";
  let headers = "";
  headers += `From: ${message.headers.from}${CRLF}`;
  headers += `To: ${message.headers.to.join(", ")}${CRLF}`;
  if (message.headers.cc && message.headers.cc.length > 0) {
    headers += `Cc: ${message.headers.cc.join(", ")}${CRLF}`;
  }
  headers += `Date: ${message.headers.date}${CRLF}`;
  headers += `Message-ID: ${message.headers.messageId}${CRLF}`;
  if (message.headers.subject !== undefined) {
    headers += `Subject: ${message.headers.subject}${CRLF}`;
  }
  if (message.headers.inReplyTo !== undefined) {
    headers += `In-Reply-To: ${message.headers.inReplyTo}${CRLF}`;
  }
  if (message.headers.references && message.headers.references.length > 0) {
    headers += `References: ${message.headers.references.join(" ")}${CRLF}`;
  }
  if (message.headers.interchangeType !== undefined) {
    headers += `Interchange-Type: ${message.headers.interchangeType}${CRLF}`;
  }

  const body =
    message.content ??
    (message.payload !== undefined ? JSON.stringify(message.payload) : "");
  headers += `Content-Type: text/plain${CRLF}`;
  headers += `${CRLF}`;
  return enc.encode(headers + body);
}
