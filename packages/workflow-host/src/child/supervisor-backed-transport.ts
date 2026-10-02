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
  CryptoProvider,
  InboundMessage,
  MailboxEvent,
  MessageHeaders,
  MessagePart,
  MessageRef,
  MessageTransport,
  OutboundMessage,
  SearchQuery,
  SendReceipt,
  Unsubscribe,
} from "@intx/types/runtime";
import { MessageTransportError } from "@intx/types/runtime";

import {
  executeSearch,
  fetchFull as doFetchFull,
  fetchHeaders as doFetchHeaders,
  fetchPart as doFetchPart,
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
  // Return the wired inbound surface, or fail loud when the sidecar
  // constructed the transport without it -- an inbound read against a missing
  // surface is a wiring error, not a silently-empty result.
  function requireInbound(method: string): SupervisorBackedTransportInbound {
    if (inbound === undefined) {
      throw new MessageTransportError(
        "SERVERBUG",
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
      throw new MessageTransportError(
        "NONEXISTENT",
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
    async fetchHeaders(
      ref: MessageRef,
      _signal?: AbortSignal,
    ): Promise<MessageHeaders> {
      const { reader } = requireInbound("fetchHeaders");
      requireInbox(ref.mailbox);
      const store = await reader.open();
      return await doFetchHeaders(ref, store);
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

    /**
     * The committed bytes of one message, verbatim. The substrate keeps each
     * message as a write-once `<uid>.eml`, so this is a read of that blob --
     * byte-identical to what arrived, which is what a signature check over it
     * requires.
     */
    async readRaw(ref: MessageRef, _signal?: AbortSignal): Promise<Uint8Array> {
      const { reader } = requireInbound("readRaw");
      requireInbox(ref.mailbox);
      const store = await reader.open();
      return await store.readRaw(ref.uid);
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

    async expunge(
      mailbox: string,
      _signal?: AbortSignal,
    ): Promise<{ expungedUids: number[] }> {
      const { mutationBridge } = requireInbound("expunge");
      requireInbox(mailbox);
      // Route to the supervisor, which sweeps every `\Deleted` message out of
      // its owned INBOX and returns the swept uids. The expunged bytes survive
      // in git history (a workflow-run repo's objects are never GC'd), so the
      // replication check permits the deletion. A caller expunging after a
      // `setFlags(\Deleted)` MUST await the two in sequence -- the supervisor
      // applies mutations in arrival order, so an unawaited (concurrent) pair
      // could let the sweep run before the flag is set and miss the message.
      const result = await mutationBridge.submit({
        runId: deriveWorkflowRunId(address),
        mailbox,
        op: "expunge",
      });
      return { expungedUids: result.expungedUids ?? [] };
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
  };
}
