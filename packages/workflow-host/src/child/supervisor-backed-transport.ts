// Supervisor-backed `MessageTransport` for a unified-host step agent
// (both halves of mailbox ownership, §3a OUTBOUND and §3b INBOUND).
//
// Under the unified host the supervisor is the sole mail owner: it holds
// the durable inbox and the host transport against which the agent's
// address is registered with its signing key. The step agent therefore
// does NOT hold a signing key to send outbound mail, and it does NOT own
// the host-side inbox directly. Its mail tools are backed by this
// transport:
//
//   - INBOUND is a functional local IMAP read surface once the sidecar
//     wires it (the `inbound` constructor argument). The supervisor
//     commits an arrived message to the deployment's workflow-run
//     substrate mailbox (`mailbox/INBOX/`) and fires a `mailbox.notify`
//     control frame. The read surface (`search`, `thread`,
//     `fetchHeaders`, `fetchStructure`, `fetchPart`, `fetchFull`, `sync`,
//     `getMailboxStatus`) answers by opening a fresh committed snapshot of
//     that mailbox through the child mailbox reader and running the
//     `@intx/mailbox` pure query functions over it -- local, no hub or
//     IPC round-trip. `watch` registers into the child watch registry, so
//     `mail_wait` unblocks when the routed `mailbox.notify` fires. The
//     WRITE methods (`setFlags` / `clearFlags` / `expunge`) do NOT touch
//     the local read surface: they route up to the supervisor through the
//     mailbox-mutation bridge (a `mailbox.mutate.request` frame), which
//     applies the mutation to the supervisor's owned store and replies.
//     The child never flushes the run ref, so it never races the
//     supervisor's mirror. The agent owns only the `INBOX`, so every
//     inbound method rejects a request for any other mailbox rather than
//     silently serving `INBOX` under the wrong name. When the sidecar
//     constructs the transport without the `inbound` argument, the inbound
//     methods throw a clear "not wired" error rather than answer against a
//     missing surface.
//   - OUTBOUND (`send`) routes through the supervisor over the control
//     IPC via the outbound-mail bridge. The supervisor performs the
//     actual signed send through the host transport, so the outbound mail
//     carries the agent's signature with full parity to the in-process
//     path. The agent never holds the key.
//
// A handful of methods stay unsupported and throw: they act on a resource
// the unified-host agent does not own. `append` and the mailbox-management
// methods (`listMailboxes` / `createMailbox` / `deleteMailbox`) target a
// mailbox the agent does not own; `move` / `copy` need a second mailbox it
// does not own; and the distribution-list methods are unimplemented across
// every transport.

import type {
  BodyStructure,
  CryptoProvider,
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

import {
  executeSearch,
  executeThread,
  fetchFull as doFetchFull,
  fetchHeaders as doFetchHeaders,
  fetchPart as doFetchPart,
  fetchStructure as doFetchStructure,
} from "@intx/mailbox";

import { deriveWorkflowRunId } from "@intx/types";

import { MAILBOX_INBOX_DIR } from "../adapters/substrate-mailbox-store";
import type { ChildMailboxReader } from "./child-mailbox-reader";
import type { ChildMailboxMutationBridge } from "./mailbox-mutation-bridge";
import type { MailboxWatchRegistry } from "./mailbox-watch-registry";
import type { ChildOutboundMailBridge } from "./outbound-mail-bridge";

/**
 * The dependencies backing the transport's whole inbox capability: the local
 * IMAP READ surface (`reader` / `watchRegistry` / `getCrypto`) that resolves
 * against the deployment's substrate mailbox, plus the routed-WRITE channel
 * (`mutationBridge`) that carries flag writes and expunge up to the supervisor.
 * The sidecar wires the bundle only for a build that owns an inbound mailbox
 * (the warm agent); a build without it has no inbox and every inbound method
 * throws a clear "not wired" error. Reads are local; writes route upstream --
 * the child never flushes the run ref, so it never races the supervisor.
 */
export interface SupervisorBackedTransportInbound {
  /**
   * Opens a fresh committed snapshot of the deployment's substrate `INBOX`.
   * Every inbound read opens a new snapshot, so a read taken after a
   * `mailbox.notify` -- or after a routed write the supervisor flushed before
   * replying -- observes the committed state.
   */
  reader: ChildMailboxReader;
  /**
   * The registry the child's control loop fires `mailbox.notify` into. It must
   * be the same instance `runWorkflowChild` routes the frame to, so a `watch`
   * installed here observes the supervisor's notification.
   */
  watchRegistry: MailboxWatchRegistry;
  /**
   * Resolve a sender address to its `CryptoProvider` so `fetchFull` can verify
   * the message signature. Returns `undefined` when no key is known for the
   * sender, in which case the signature status is reported as `unknown`.
   */
  getCrypto: (fromAddress: string) => CryptoProvider | undefined;
  /**
   * The upstream channel the write methods route through. `setFlags` /
   * `clearFlags` / `expunge` call `mutationBridge.submit`, which emits a
   * `mailbox.mutate.request` and resolves once the supervisor applies the
   * mutation to its owned store and replies. Bundled with the read surface
   * because the write methods and the reads share one presence condition:
   * this agent owns an inbox, or it owns none.
   */
  mutationBridge: ChildMailboxMutationBridge;
}

/**
 * Construct a `MessageTransport` whose outbound side routes through the
 * supervisor (via `bridge`) and whose inbound side is a local IMAP read
 * surface over `inbound`. `address` is the agent's mail address; the
 * supervisor signs the outbound mail as this address through the host
 * transport, so it must be the address the host registered the agent's
 * `CryptoProvider` against. When `inbound` is omitted, the inbound methods
 * throw a clear "not wired" error; the sidecar supplies it once the child's
 * mailbox reader and watch registry are threaded through.
 */
export function createSupervisorBackedTransport(
  bridge: ChildOutboundMailBridge,
  address: string,
  inbound?: SupervisorBackedTransportInbound,
): MessageTransport {
  function unsupported(method: string): never {
    throw new Error(
      `supervisor-backed transport: ${method} is not supported for unified-host step agent ${address}; the supervisor owns the mailbox and the agent owns only its own ${MAILBOX_INBOX_DIR}`,
    );
  }

  // Return the wired inbound surface, or fail loud when the sidecar
  // constructed the transport without it -- an inbound read against a missing
  // surface is a wiring error, not a silently-empty result.
  function requireInbound(method: string): SupervisorBackedTransportInbound {
    if (inbound === undefined) {
      throw new Error(
        `supervisor-backed transport: ${method} needs the inbound surface, but it is not wired for unified-host step agent ${address}; the sidecar must construct the transport with its mailbox reader, watch registry, and crypto`,
      );
    }
    return inbound;
  }

  // The unified-host agent owns exactly one mailbox, the substrate `INBOX`
  // the reader opens. Reject any other name rather than serve `INBOX` under
  // it, which would return the wrong mailbox's messages mislabeled.
  function requireInbox(mailbox: string): void {
    if (mailbox !== MAILBOX_INBOX_DIR) {
      throw new Error(
        `supervisor-backed transport: unified-host step agent ${address} owns only the "${MAILBOX_INBOX_DIR}" mailbox; "${mailbox}" is not available`,
      );
    }
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
      return unsupported("append");
    },

    async listMailboxes(_signal?: AbortSignal): Promise<Mailbox[]> {
      return unsupported("listMailboxes");
    },
    async createMailbox(
      _name: string,
      _signal?: AbortSignal,
    ): Promise<Mailbox> {
      return unsupported("createMailbox");
    },
    async deleteMailbox(_name: string, _signal?: AbortSignal): Promise<void> {
      return unsupported("deleteMailbox");
    },
    async getMailboxStatus(
      name: string,
      _signal?: AbortSignal,
    ): Promise<MailboxStatus> {
      const { reader } = requireInbound("getMailboxStatus");
      requireInbox(name);
      const store = await reader.open();
      const unseen = store.messages.filter(
        (m) => !m.flags.has("\\Seen"),
      ).length;
      return {
        total: store.messages.length,
        unseen,
        recent: 0,
        uidNext: store.uidNext,
        uidValidity: store.uidValidity,
        highestModSeq: store.highestModSeq,
      };
    },

    async search(
      mailbox: string,
      query: SearchQuery,
      _signal?: AbortSignal,
    ): Promise<MessageRef[]> {
      const { reader } = requireInbound("search");
      requireInbox(mailbox);
      const store = await reader.open();
      return await executeSearch(mailbox, store, query);
    },
    async thread(
      mailbox: string,
      algorithm: "references" | "orderedsubject",
      query?: SearchQuery,
      _signal?: AbortSignal,
    ): Promise<Thread[]> {
      const { reader } = requireInbound("thread");
      requireInbox(mailbox);
      const store = await reader.open();
      return await executeThread(mailbox, store, algorithm, query);
    },
    async fetchHeaders(
      ref: MessageRef,
      _signal?: AbortSignal,
    ): Promise<MessageHeaders> {
      const { reader } = requireInbound("fetchHeaders");
      requireInbox(ref.mailbox);
      const store = await reader.open();
      return await doFetchHeaders(ref, store);
    },
    async fetchStructure(
      ref: MessageRef,
      _signal?: AbortSignal,
    ): Promise<BodyStructure> {
      const { reader } = requireInbound("fetchStructure");
      requireInbox(ref.mailbox);
      const store = await reader.open();
      return await doFetchStructure(ref, store);
    },
    async fetchPart(
      ref: MessageRef,
      partPath: string,
      _signal?: AbortSignal,
    ): Promise<MessagePart> {
      const { reader } = requireInbound("fetchPart");
      requireInbox(ref.mailbox);
      const store = await reader.open();
      return await doFetchPart(ref, partPath, store);
    },
    async fetchFull(
      ref: MessageRef,
      _signal?: AbortSignal,
    ): Promise<InboundMessage> {
      const { reader, getCrypto } = requireInbound("fetchFull");
      requireInbox(ref.mailbox);
      const store = await reader.open();
      return await doFetchFull(ref, store, getCrypto);
    },

    async setFlags(
      ref: MessageRef,
      flags: string[],
      _signal?: AbortSignal,
    ): Promise<void> {
      const { mutationBridge } = requireInbound("setFlags");
      requireInbox(ref.mailbox);
      // Route the flag write to the supervisor -- the sole mailbox writer --
      // rather than flushing a second store against the run ref. `submit`
      // resolves only after the supervisor flushes, so a subsequent read
      // observes the flag.
      await mutationBridge.submit({
        runId: deriveWorkflowRunId(address),
        mailbox: ref.mailbox,
        op: "addFlags",
        uid: ref.uid,
        flags,
      });
    },
    async clearFlags(
      ref: MessageRef,
      flags: string[],
      _signal?: AbortSignal,
    ): Promise<void> {
      const { mutationBridge } = requireInbound("clearFlags");
      requireInbox(ref.mailbox);
      await mutationBridge.submit({
        runId: deriveWorkflowRunId(address),
        mailbox: ref.mailbox,
        op: "removeFlags",
        uid: ref.uid,
        flags,
      });
    },

    async move(
      _ref: MessageRef,
      _toMailbox: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      return unsupported("move");
    },
    async copy(
      _ref: MessageRef,
      _toMailbox: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      return unsupported("copy");
    },
    async expunge(mailbox: string, _signal?: AbortSignal): Promise<void> {
      const { mutationBridge } = requireInbound("expunge");
      requireInbox(mailbox);
      // Route to the supervisor, which sweeps every `\Deleted` message out of
      // its owned INBOX. The expunged bytes survive in git history (a
      // workflow-run repo's objects are never GC'd), so the replication check
      // permits the deletion. The supervisor reports the swept uids; this
      // void-returning method discards them. A caller expunging after a
      // `setFlags(\Deleted)` MUST await the two in sequence -- the supervisor
      // applies mutations in arrival order, so an unawaited (concurrent) pair
      // could let the sweep run before the flag is set and miss the message.
      await mutationBridge.submit({
        runId: deriveWorkflowRunId(address),
        mailbox,
        op: "expunge",
      });
    },

    watch(
      mailbox: string,
      callback: (event: MailboxEvent) => void,
    ): Unsubscribe {
      const { watchRegistry } = requireInbound("watch");
      requireInbox(mailbox);
      // The supervisor -- the sole mail owner -- fires `mailbox.notify` into
      // the registry when new mail lands; the registry delivers the typed
      // event to this callback. `mail_wait` installs the watch and unblocks on
      // the first delivery.
      return watchRegistry.watch(mailbox, callback);
    },

    async sync(
      mailbox: string,
      knownState: SyncState,
      _signal?: AbortSignal,
    ): Promise<SyncResult> {
      const { reader } = requireInbound("sync");
      requireInbox(mailbox);
      const store = await reader.open();
      const result = store.sync({
        uidValidity: knownState.uidValidity,
        highestModSeq: knownState.highestModSeq,
      });
      if (result.resync) {
        return {
          vanished: [],
          changed: [],
          newMessages: result.messages.map((m) => ({ uid: m.uid, mailbox })),
          fullResyncRequired: true,
        };
      }
      // The backing reports every message whose modseq advanced past the
      // client's known state as `changed`. Split it against the client's known
      // `uidNext`: a uid at or beyond it is a new arrival, one below it is a
      // flag change on a message the client already held.
      const newMessages: MessageRef[] = [];
      const changed: { uid: number; flags: string[] }[] = [];
      for (const m of result.changed) {
        if (m.uid >= knownState.uidNext) {
          newMessages.push({ uid: m.uid, mailbox });
        } else {
          changed.push({ uid: m.uid, flags: Array.from(m.flags) });
        }
      }
      return {
        vanished: [...result.vanished],
        changed,
        newMessages,
        fullResyncRequired: false,
      };
    },

    async createList(
      _address: string,
      _name: string,
      _signal?: AbortSignal,
    ): Promise<ListInfo> {
      return unsupported("createList");
    },
    async listMembers(
      _address: string,
      _signal?: AbortSignal,
    ): Promise<string[]> {
      return unsupported("listMembers");
    },
    async subscribe(
      _listAddress: string,
      _subscriberAddress: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      return unsupported("subscribe");
    },
    async unsubscribe(
      _listAddress: string,
      _subscriberAddress: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      return unsupported("unsubscribe");
    },
  };
}
