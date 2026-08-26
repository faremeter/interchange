// Child-side substrate mailbox reader (INBOUND half of mailbox ownership,
// design §3b).
//
// The supervisor commits an arrived message to the deployment's workflow-run
// substrate mailbox (`mailbox/INBOX/`), then fires a `mailbox.notify` control
// frame. A step agent's supervisor-backed transport answers the IMAP read
// surface (`search`, `fetchHeaders`, `fetchFull`, ...) by opening a fresh
// committed snapshot of that mailbox through this reader. `open` re-reads the
// committed subtree on every call, so a snapshot taken after a `mailbox.notify`
// observes the message the supervisor just committed.
//
// Modeled on the sibling `createMailPartReader` wiring: the same per-deployment
// substrate handles (substrate, repoId, principal, workflow-run ref), bound
// once so the transport reads without re-threading them. `createSubstrateMailboxStore`
// loads committed state on open and is async, so `open` returns a promise.

import {
  createSubstrateMailboxStore,
  type SubstrateMailboxStore,
  type SubstrateMailboxStoreOpts,
} from "../adapters/substrate-mailbox-store";

export interface ChildMailboxReader {
  /**
   * Open a fresh committed snapshot of the deployment's substrate INBOX. Each
   * call re-reads the committed `mailbox/INBOX/` subtree, so a snapshot opened
   * after a `mailbox.notify` observes the newly committed message.
   */
  open(): Promise<SubstrateMailboxStore>;
}

export function createChildMailboxReader(
  opts: SubstrateMailboxStoreOpts,
): ChildMailboxReader {
  return {
    open() {
      return createSubstrateMailboxStore(opts);
    },
  };
}
