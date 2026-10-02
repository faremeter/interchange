import type {
  MessageTransport,
  OutboundMessage,
  SendReceipt,
  InboundMessage,
  MessageRef,
  SearchQuery,
  MessageHeaders,
  MessagePart,
  MailboxEvent,
  Unsubscribe,
  CryptoProvider,
} from "@intx/types/runtime";
import { MessageTransportError } from "@intx/types/runtime";
import type {
  HubTransport,
  MessageSentHandler,
  RemoteSendHandler,
} from "@intx/types/runtime";
import { buildMessageHeaders, parseHeaderSection } from "@intx/mime";
import {
  executeSearch,
  fetchHeaders as doFetchHeaders,
  fetchPart as doFetchPart,
  fetchFull as doFetchFull,
  requireMessage,
  type StoredEnvelope,
} from "@intx/mailbox";
import { createAddressEntry, type AddressEntry } from "./mailbox";
import { executeSend } from "./send";

/**
 * In-memory MessageTransport implementing full IMAP semantics within a
 * single process. Messages are stored as real RFC 2822 MIME byte buffers.
 *
 * Every outbound message is PGP/MIME signed with the sender's CryptoProvider.
 * Signature verification runs on fetchFull().
 *
 * Addresses must be registered before sending or receiving messages.
 */
export class InMemoryTransport implements MessageTransport, HubTransport {
  readonly #entries = new Map<string, AddressEntry>();
  #remoteSendHandler: RemoteSendHandler | undefined;
  readonly #messageSentHandlers = new Set<MessageSentHandler>();

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
   * Register a handler that fires after every successful send(). Multiple
   * handlers may be registered. The message is already delivered when
   * handlers fire — a handler rejection does not mean the message was not
   * delivered.
   */
  addMessageSentHandler(handler: MessageSentHandler): void {
    this.#messageSentHandlers.add(handler);
  }

  /**
   * Register an address with its CryptoProvider. Creates the default set
   * of mailboxes (INBOX, Sent, Drafts, Archive, Trash).
   *
   * Throws if the address is already registered.
   */
  register(address: string, crypto: CryptoProvider): void {
    if (this.#entries.has(address)) {
      throw new Error(`Address "${address}" is already registered`);
    }
    this.#entries.set(address, createAddressEntry(crypto));
  }

  /**
   * Remove an address's mailboxes and crypto provider. Called when a
   * session is destroyed so the address can be re-registered later.
   */
  unregister(address: string): void {
    this.#entries.delete(address);
  }

  // ---------------------------------------------------------------------------
  // Outbound
  // ---------------------------------------------------------------------------

  async send(
    _message: OutboundMessage,
    _signal?: AbortSignal,
  ): Promise<SendReceipt> {
    throw new Error(
      "Use createInMemoryTransport().getTransportFor(address) to send messages",
    );
  }

  // ---------------------------------------------------------------------------
  // Mailbox management (per-address — use getTransportFor)
  // ---------------------------------------------------------------------------

  async search(
    _mailbox: string,
    _query: SearchQuery,
    _signal?: AbortSignal,
  ): Promise<MessageRef[]> {
    throw new Error("Use getTransportFor(address) for per-address operations");
  }

  async fetchHeaders(
    _ref: MessageRef,
    _signal?: AbortSignal,
  ): Promise<MessageHeaders> {
    throw new Error("Use getTransportFor(address) for per-address operations");
  }

  async fetchPart(
    _ref: MessageRef,
    _partPath: string,
    _signal?: AbortSignal,
  ): Promise<MessagePart> {
    throw new Error("Use getTransportFor(address) for per-address operations");
  }

  async fetchFull(
    _ref: MessageRef,
    _signal?: AbortSignal,
  ): Promise<InboundMessage> {
    throw new Error("Use getTransportFor(address) for per-address operations");
  }

  async readRaw(_ref: MessageRef, _signal?: AbortSignal): Promise<Uint8Array> {
    throw new Error("Use getTransportFor(address) for per-address operations");
  }

  async setFlags(
    _ref: MessageRef,
    _flags: string[],
    _signal?: AbortSignal,
  ): Promise<void> {
    throw new Error("Use getTransportFor(address) for per-address operations");
  }

  async clearFlags(
    _ref: MessageRef,
    _flags: string[],
    _signal?: AbortSignal,
  ): Promise<void> {
    throw new Error("Use getTransportFor(address) for per-address operations");
  }

  async expunge(
    _mailbox: string,
    _signal?: AbortSignal,
  ): Promise<{ expungedUids: number[] }> {
    throw new Error("Use getTransportFor(address) for per-address operations");
  }

  watch(
    _mailbox: string,
    _callback: (event: MailboxEvent) => void,
  ): Unsubscribe {
    throw new Error("Use getTransportFor(address) for per-address operations");
  }

  // ---------------------------------------------------------------------------
  // Inbound delivery from federation
  // ---------------------------------------------------------------------------

  /**
   * Deliver a signed MIME message to an address's INBOX. Used by the
   * federation layer when a message arrives from the hub over the
   * websocket — the message is already assembled and signed by the
   * originating sender, so no further processing is needed beyond
   * envelope parsing and storage.
   *
   * Throws if the address is not registered.
   */
  deliver(address: string, message: Uint8Array): void {
    const entry = this.#entries.get(address);
    if (entry === undefined) {
      throw new Error(
        `Address "${address}" is not registered — cannot deliver mail`,
      );
    }
    const inbox = entry.mailboxes.get("INBOX");
    if (inbox === undefined) {
      throw new Error(`Address "${address}" has no INBOX`);
    }

    const { headers } = parseHeaderSection(message);
    // The stored envelope and the `exists` event below are built from one
    // reading of these bytes, so the two cannot disagree about which headers
    // the message carried. `buildMessageHeaders` is that reading: it applies
    // the RFC rule that a present-but-blank `Date`, `Message-ID`, `From` or
    // `In-Reply-To` names nothing, so a blank one arrives here as an absence.
    const msgHeaders = buildMessageHeaders(headers);

    const dateRaw = msgHeaders.date;
    if (msgHeaders.messageId === undefined) {
      throw new Error("Cannot deliver message: missing Message-ID header");
    }
    if (msgHeaders.from === undefined) {
      throw new Error("Cannot deliver message: missing From header");
    }
    if (dateRaw === undefined) {
      throw new Error("Cannot deliver message: missing Date header");
    }

    const envelope: StoredEnvelope = {
      messageId: msgHeaders.messageId,
      from: msgHeaders.from,
      to: msgHeaders.to,
      subject: msgHeaders.subject ?? "",
      date: new Date(dateRaw),
      inReplyTo: msgHeaders.inReplyTo,
      references: msgHeaders.references ?? [],
      // Read off the raw map rather than `msgHeaders`, which keeps this field
      // only when it names a declared `InterchangeType`. The envelope mirrors
      // what the message carried, so an unrecognized type is stored verbatim
      // instead of being erased from the index.
      interchangeType: headers.get("interchange-type"),
      interchangeCorrelationId: msgHeaders.interchangeCorrelationId,
    };

    const uid = inbox.append(message, envelope, []);

    const callbacks = entry.watchCallbacks.get("INBOX");
    if (callbacks !== undefined && callbacks.size > 0) {
      const event: import("@intx/types/runtime").MailboxEvent = {
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
  // Internal: per-address view
  // ---------------------------------------------------------------------------

  /**
   * Returns a MessageTransport scoped to the given address. Callers use
   * this to send and read mail as that address.
   */
  getTransportFor(address: string): MessageTransport {
    if (!this.#entries.has(address)) {
      throw new Error(
        `Address "${address}" is not registered — call register() first`,
      );
    }
    return new ScopedMessageTransport(
      address,
      this.#entries,
      () => this.#remoteSendHandler,
      () => this.#messageSentHandlers,
    );
  }
}

/**
 * MessageTransport scoped to a single address. All operations target that
 * address's mailboxes. Constructed via InMemoryTransport.getTransportFor().
 */
class ScopedMessageTransport implements MessageTransport {
  readonly #address: string;
  readonly #entries: Map<string, AddressEntry>;
  readonly #getRemoteSendHandler: () => RemoteSendHandler | undefined;
  readonly #getMessageSentHandlers: () => Set<MessageSentHandler>;

  constructor(
    address: string,
    entries: Map<string, AddressEntry>,
    getRemoteSendHandler: () => RemoteSendHandler | undefined,
    getMessageSentHandlers: () => Set<MessageSentHandler>,
  ) {
    this.#address = address;
    this.#entries = entries;
    this.#getRemoteSendHandler = getRemoteSendHandler;
    this.#getMessageSentHandlers = getMessageSentHandlers;
  }

  get #entry(): AddressEntry {
    const e = this.#entries.get(this.#address);
    if (e === undefined) {
      // `CANNOT` rather than a bare rejection: a rejection naming no condition
      // leaves the outcome unknown, and an unknown outcome reads as worth
      // retrying, which no retry through this handle clears. The lookup is
      // against the live map, so registering the address again does revive the
      // handle against the new entry -- but registration belongs to whoever
      // owns the transport, not to the holder of a scoped handle.
      throw new MessageTransportError(
        "CANNOT",
        `Address "${this.#address}" has been deregistered`,
      );
    }
    return e;
  }

  #requireMailbox(name: string) {
    const store = this.#entry.mailboxes.get(name);
    if (store === undefined) {
      throw new MessageTransportError(
        "NONEXISTENT",
        `Mailbox "${name}" does not exist for address "${this.#address}"`,
      );
    }
    return store;
  }

  async send(
    message: OutboundMessage,
    _signal?: AbortSignal,
  ): Promise<SendReceipt> {
    // Trip the deregistered guard so callers using a stale scoped handle
    // see a precise error rather than the generic "sender is not
    // registered" thrown by executeSend.
    void this.#entry;

    const handlers = this.#getMessageSentHandlers();
    const aggregatedHandler: MessageSentHandler | undefined =
      handlers.size > 0
        ? async (ctx) => {
            await Promise.allSettled([...handlers].map((h) => h(ctx)));
          }
        : undefined;
    return executeSend(
      this.#address,
      message,
      this.#entries,
      this.#getRemoteSendHandler(),
      aggregatedHandler,
    );
  }

  async search(
    mailbox: string,
    query: SearchQuery,
    _signal?: AbortSignal,
  ): Promise<MessageRef[]> {
    const store = this.#requireMailbox(mailbox);
    return await executeSearch(mailbox, store, query);
  }

  async fetchHeaders(
    ref: MessageRef,
    _signal?: AbortSignal,
  ): Promise<MessageHeaders> {
    const store = this.#requireMailbox(ref.mailbox);
    return await doFetchHeaders(ref, store);
  }

  async fetchPart(
    ref: MessageRef,
    partPath: string,
    _signal?: AbortSignal,
  ): Promise<MessagePart> {
    const store = this.#requireMailbox(ref.mailbox);
    return await doFetchPart(ref, partPath, store);
  }

  async fetchFull(
    ref: MessageRef,
    _signal?: AbortSignal,
  ): Promise<InboundMessage> {
    const store = this.#requireMailbox(ref.mailbox);
    return await doFetchFull(
      ref,
      store,
      (addr) => this.#entries.get(addr)?.crypto,
    );
  }

  /**
   * The stored bytes, exactly as they were appended. `MailboxStore.readRaw` is
   * already the verbatim read every projection resolves through, so this is that
   * read surfaced -- no re-serialization, which is the point.
   */
  async readRaw(ref: MessageRef, _signal?: AbortSignal): Promise<Uint8Array> {
    const store = this.#requireMailbox(ref.mailbox);
    requireMessage(store, ref.uid, ref.mailbox);
    return await store.readRaw(ref.uid);
  }

  async setFlags(
    ref: MessageRef,
    flags: string[],
    _signal?: AbortSignal,
  ): Promise<void> {
    const store = this.#requireMailbox(ref.mailbox);
    const msg = store.addFlags(ref.uid, flags);
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
    const msg = store.removeFlags(ref.uid, flags);
    this.#fireWatchCallbacks(ref.mailbox, {
      type: "flagsChanged",
      uid: ref.uid,
      flags: Array.from(msg.flags),
    });
  }

  async expunge(
    mailbox: string,
    _signal?: AbortSignal,
  ): Promise<{ expungedUids: number[] }> {
    const store = this.#requireMailbox(mailbox);
    const toExpunge = store.messages.filter((m) => m.flags.has("\\Deleted"));

    for (const msg of toExpunge) {
      store.remove(msg.uid);
    }

    for (const msg of toExpunge) {
      this.#fireWatchCallbacks(mailbox, {
        type: "expunged",
        uid: msg.uid,
      });
    }

    return { expungedUids: toExpunge.map((m) => m.uid) };
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

  #fireWatchCallbacks(mailbox: string, event: MailboxEvent): void {
    const callbacks = this.#entry.watchCallbacks.get(mailbox);
    if (callbacks === undefined || callbacks.size === 0) return;
    for (const cb of callbacks) {
      queueMicrotask(() => cb(event));
    }
  }
}
