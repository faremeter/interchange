// Supervisor-backed `MessageTransport` for a unified-host step agent
// (OUTBOUND half of mailbox ownership, §3a).
//
// Under the unified host the supervisor is the sole mail owner: it holds
// the durable inbox and the host transport against which the agent's
// address is registered with its signing key. The step agent therefore
// does NOT subscribe its own transport for inbound mail (the supervisor
// delivers inputs via the step path) and does NOT hold a signing key to
// send outbound mail. Its mail tools are backed by this transport:
//
//   - INBOUND is a no-op. `watch` returns a no-op unsubscribe and never
//     fires; the supervisor delivers the agent's input as the step
//     input, not through the agent's own mailbox. The IMAP read surface
//     (`search`, `fetchFull`, `fetchHeaders`, ...) throws: the agent
//     owns no mailbox in the unified host, so a read against one is a
//     programming error, surfaced loudly rather than returning a
//     silently-empty result that would hide the missing inbound surface.
//   - OUTBOUND (`send` / `append`) routes through the supervisor over
//     the control IPC via the outbound-mail bridge. The supervisor
//     performs the actual signed send through the host transport, so the
//     outbound mail carries the agent's signature with full parity to
//     the in-process path. The agent never holds the key.

import type {
  BodyStructure,
  InboundMessage,
  ListInfo,
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
  Unsubscribe,
} from "@intx/types/runtime";

import type { ChildOutboundMailBridge } from "./outbound-mail-bridge";

/**
 * Construct a `MessageTransport` whose outbound side routes through the
 * supervisor (via `bridge`) and whose inbound side is inert. `address`
 * is the agent's mail address; the supervisor signs the outbound mail as
 * this address through the host transport, so it must be the address the
 * host registered the agent's `CryptoProvider` against.
 */
export function createSupervisorBackedTransport(
  bridge: ChildOutboundMailBridge,
  address: string,
): MessageTransport {
  function inboundUnsupported(method: string): never {
    throw new Error(
      `supervisor-backed transport: ${method} is not supported for unified-host step agent ${address}; the supervisor owns the mailbox and delivers inbound mail as the step input`,
    );
  }

  return {
    async send(
      message: OutboundMessage,
      _signal?: AbortSignal,
    ): Promise<SendReceipt> {
      return bridge.submit(address, message);
    },

    async append(
      _mailbox: string,
      _message: InboundMessage,
      _flags?: string[],
      _signal?: AbortSignal,
    ): Promise<MessageRef> {
      // `append` writes into a mailbox the agent owns; in the unified
      // host the agent owns none. The mail tools do not append (they
      // `send`), so a reachable `append` is a programming error.
      return inboundUnsupported("append");
    },

    async listMailboxes(_signal?: AbortSignal): Promise<Mailbox[]> {
      return inboundUnsupported("listMailboxes");
    },
    async createMailbox(
      _name: string,
      _signal?: AbortSignal,
    ): Promise<Mailbox> {
      return inboundUnsupported("createMailbox");
    },
    async deleteMailbox(_name: string, _signal?: AbortSignal): Promise<void> {
      return inboundUnsupported("deleteMailbox");
    },
    async getMailboxStatus(
      _name: string,
      _signal?: AbortSignal,
    ): Promise<MailboxStatus> {
      return inboundUnsupported("getMailboxStatus");
    },

    async search(
      _mailbox: string,
      _query: SearchQuery,
      _signal?: AbortSignal,
    ): Promise<MessageRef[]> {
      return inboundUnsupported("search");
    },
    async thread(
      _mailbox: string,
      _algorithm: "references" | "orderedsubject",
      _query?: SearchQuery,
      _signal?: AbortSignal,
    ): Promise<Thread[]> {
      return inboundUnsupported("thread");
    },
    async fetchHeaders(
      _ref: MessageRef,
      _signal?: AbortSignal,
    ): Promise<MessageHeaders> {
      return inboundUnsupported("fetchHeaders");
    },
    async fetchStructure(
      _ref: MessageRef,
      _signal?: AbortSignal,
    ): Promise<BodyStructure> {
      return inboundUnsupported("fetchStructure");
    },
    async fetchPart(
      _ref: MessageRef,
      _partPath: string,
      _signal?: AbortSignal,
    ): Promise<MessagePart> {
      return inboundUnsupported("fetchPart");
    },
    async fetchFull(
      _ref: MessageRef,
      _signal?: AbortSignal,
    ): Promise<InboundMessage> {
      return inboundUnsupported("fetchFull");
    },

    async setFlags(
      _ref: MessageRef,
      _flags: string[],
      _signal?: AbortSignal,
    ): Promise<void> {
      return inboundUnsupported("setFlags");
    },
    async clearFlags(
      _ref: MessageRef,
      _flags: string[],
      _signal?: AbortSignal,
    ): Promise<void> {
      return inboundUnsupported("clearFlags");
    },

    async move(
      _ref: MessageRef,
      _toMailbox: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      return inboundUnsupported("move");
    },
    async copy(
      _ref: MessageRef,
      _toMailbox: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      return inboundUnsupported("copy");
    },
    async expunge(_mailbox: string, _signal?: AbortSignal): Promise<void> {
      return inboundUnsupported("expunge");
    },

    watch(
      _mailbox: string,
      _callback: (event: MailboxEvent) => void,
    ): Unsubscribe {
      // Inbound delivery is a no-op: the supervisor delivers the agent's
      // input as the step input, not through the agent's mailbox. The
      // watch never fires; return a no-op unsubscribe so a mail tool that
      // installs a watch (mail_wait) does not throw at install time but
      // also never observes a spurious event.
      return () => undefined;
    },

    async sync(
      _mailbox: string,
      _knownState: SyncState,
      _signal?: AbortSignal,
    ): Promise<SyncResult> {
      return inboundUnsupported("sync");
    },

    async createList(
      _address: string,
      _name: string,
      _signal?: AbortSignal,
    ): Promise<ListInfo> {
      return inboundUnsupported("createList");
    },
    async listMembers(
      _address: string,
      _signal?: AbortSignal,
    ): Promise<string[]> {
      return inboundUnsupported("listMembers");
    },
    async subscribe(
      _listAddress: string,
      _subscriberAddress: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      return inboundUnsupported("subscribe");
    },
    async unsubscribe(
      _listAddress: string,
      _subscriberAddress: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      return inboundUnsupported("unsubscribe");
    },
  };
}
