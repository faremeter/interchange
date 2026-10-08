// Child-side mailbox watch registry (INBOUND half of mailbox ownership, §3b).
// The supervisor is the sole mail owner: it commits an arrived message and
// fires a `mailbox.notify` control frame; the child's control loop routes that
// frame to `fire`, which delivers an `exists` `MailboxEvent` to every callback
// registered through `watch`. Delivery is asynchronous per the IMAP IDLE
// contract: each callback fires on its own microtask, re-checking registration
// at delivery so an unsubscribed watcher observes no event.

import type { MailboxEvent, Unsubscribe } from "@intx/types/runtime";

export interface MailboxWatchRegistry {
  /**
   * Register a callback for a mailbox. The returned `Unsubscribe` removes it;
   * a callback observes no event after unsubscribe, including one whose `fire`
   * preceded the unsubscribe but whose delivery had not yet run.
   */
  watch(mailbox: string, callback: (event: MailboxEvent) => void): Unsubscribe;
  /**
   * Deliver a `MailboxEvent` to every callback registered for the mailbox,
   * each on its own microtask. No-op when none is registered.
   */
  fire(mailbox: string, event: MailboxEvent): void;
}

export function createMailboxWatchRegistry(): MailboxWatchRegistry {
  const watchers = new Map<string, Set<(event: MailboxEvent) => void>>();

  return {
    watch(mailbox, callback) {
      let set = watchers.get(mailbox);
      if (set === undefined) {
        set = new Set();
        watchers.set(mailbox, set);
      }
      set.add(callback);
      let active = true;
      return () => {
        // Idempotent: a second unsubscribe must not remove a same-identity
        // callback a later `watch` re-registered.
        if (!active) return;
        active = false;
        const current = watchers.get(mailbox);
        if (current === undefined) return;
        current.delete(callback);
        if (current.size === 0) watchers.delete(mailbox);
      };
    },
    fire(mailbox, event) {
      const set = watchers.get(mailbox);
      if (set === undefined) return;
      // Deliver each registered callback on its own microtask; re-check
      // membership at delivery so an unsubscribed callback sees no event.
      for (const callback of [...set]) {
        queueMicrotask(() => {
          const current = watchers.get(mailbox);
          if (current === undefined || !current.has(callback)) return;
          callback(event);
        });
      }
    },
  };
}
