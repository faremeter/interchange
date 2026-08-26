import type { CryptoProvider, MailboxEvent } from "@intx/types/runtime";
import {
  DEFAULT_MAILBOXES,
  createInMemoryMailboxStore,
  type MailboxStore,
} from "@intx/mailbox";

/**
 * Per-address state for the in-memory transport: one `MailboxStore` per
 * mailbox, the watch callbacks registered against each mailbox, and the
 * address's `CryptoProvider`.
 */
export type AddressEntry = {
  mailboxes: Map<string, MailboxStore>;
  watchCallbacks: Map<string, Set<(event: MailboxEvent) => void>>;
  crypto: CryptoProvider;
};

export function createAddressEntry(crypto: CryptoProvider): AddressEntry {
  const mailboxes = new Map<string, MailboxStore>();
  for (const name of DEFAULT_MAILBOXES) {
    mailboxes.set(name, createInMemoryMailboxStore());
  }
  return {
    mailboxes,
    watchCallbacks: new Map(),
    crypto,
  };
}
