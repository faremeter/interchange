// Child-side mailbox watch registry (INBOUND half of mailbox ownership,
// design §3b).
//
// The supervisor is the sole mail owner: it commits an arrived message to the
// workflow-run substrate mailbox and fires a `mailbox.notify` control frame.
// The child's control loop routes that frame to this registry's `fire`, which
// delivers a typed `exists` `MailboxEvent` to every callback registered for the
// mailbox through `watch`. The step agent's supervisor-backed transport
// implements `MessageTransport.watch` over this registry, so `mail_wait`
// unblocks when new mail lands -- decoupled from the FIFO trigger dispatch that
// resolves a run's first input.
//
// Delivery is ASYNCHRONOUS. A `fire` never invokes a callback synchronously on
// the delivering call stack: it schedules each callback on a microtask, per the
// IMAP IDLE contract the `MailboxEvent` watcher models (MESSAGE.md § Real-Time
// Notification). Delivery re-checks registration at the microtask, so a watcher
// that unsubscribes between `fire` and delivery observes no event.

import type { MailboxEvent, Unsubscribe } from "@intx/types/runtime";

export interface MailboxWatchRegistry {
  /**
   * Register a callback for a mailbox. Returns an `Unsubscribe` that removes
   * it; after unsubscribe the callback observes no further events, including
   * one whose `fire` preceded the unsubscribe but whose asynchronous delivery
   * had not yet run.
   */
  watch(mailbox: string, callback: (event: MailboxEvent) => void): Unsubscribe;
  /**
   * Deliver a `MailboxEvent` to every callback currently registered for the
   * mailbox, each on its own microtask. A no-op when no callback is registered
   * for the mailbox.
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
        // Idempotent: a double-unsubscribe must not remove a same-identity
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
      // Snapshot the callbacks registered at fire time, then deliver each on
      // its own microtask so no callback runs synchronously on this call
      // stack. Re-check membership at delivery so a callback unsubscribed
      // between now and its microtask does not receive the event.
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
