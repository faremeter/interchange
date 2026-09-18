// In-memory MailBusBindings, for supervisor tests that need mail delivered to
// a deployment address without a real bus.
//
// Seven copies of this had accumulated in five variants. Beyond cosmetics
// they differed in what they let a test observe: one kept a register/
// unregister history, one kept only the current set. Both observables are
// here, because a test asking "is it registered now" and one asking "was it
// ever unregistered" are asking different questions and the second cannot be
// answered from the first.

import type { MailBusBindings } from "../supervisor/types";

export type MockMailBus = MailBusBindings & {
  /** Addresses currently registered, in registration order. */
  registered(): readonly string[];
  /**
   * Every registration and unregistration in order, as `register:<address>`
   * and `unregister:<address>`. Distinguishes "never registered" from
   * "registered and then released", which `registered()` cannot.
   */
  registrationHistory(): readonly string[];
  /** Deliver a raw message to every handler subscribed for `address`. */
  deliver(address: string, message: Uint8Array): void;
  /**
   * Resolve once `address` is registered, whether it already is or is
   * registered later.
   *
   * The registration is the event a test waiting for a deployment to come up
   * actually wants; polling `registered()` on a timer was standing in for it.
   */
  awaitRegistered(address: string): Promise<void>;
};

export function createMockMailBus(): MockMailBus {
  const registered: string[] = [];
  const history: string[] = [];
  const subscribers = new Map<
    string,
    Set<(rawMessage: Uint8Array) => Promise<void>>
  >();
  let waiters: (() => void)[] = [];

  function announce(): void {
    const waiting = waiters;
    waiters = [];
    for (const waiter of waiting) waiter();
  }

  return {
    registerAddress(address: string) {
      registered.push(address);
      history.push(`register:${address}`);
      announce();
    },
    unregisterAddress(address: string) {
      const idx = registered.lastIndexOf(address);
      if (idx >= 0) registered.splice(idx, 1);
      subscribers.delete(address);
      history.push(`unregister:${address}`);
      // Every mutation of the registration set is announced, which is what
      // makes an arbitrary predicate over that set awaitable: a waiter re-reads
      // on any change rather than on the subset of changes someone remembered
      // to report. `awaitRegistered` is the only waiter today, and an
      // unregister can only falsify its predicate, so this wake settles nothing
      // for it -- a fact about that one predicate, not a gap in the reporting.
      announce();
    },
    subscribeMailForAddress(
      address: string,
      handler: (rawMessage: Uint8Array) => Promise<void>,
    ) {
      let set = subscribers.get(address);
      if (set === undefined) {
        set = new Set();
        subscribers.set(address, set);
      }
      set.add(handler);
      return () => {
        const current = subscribers.get(address);
        current?.delete(handler);
      };
    },
    sendOutbound() {
      throw new Error("sendOutbound not exercised in this test");
    },
    registered: () => registered.slice(),
    registrationHistory: () => history.slice(),
    deliver(address: string, message: Uint8Array) {
      const set = subscribers.get(address);
      if (set === undefined) return;
      for (const handler of set) void handler(message).catch(() => undefined);
    },
    async awaitRegistered(address: string) {
      for (;;) {
        // Re-read on every pass, so an address registered before this call
        // resolves it rather than leaving it waiting for a re-registration.
        const changed = new Promise<void>((resolve) => {
          waiters.push(resolve);
        });
        if (registered.includes(address)) return;
        await changed;
      }
    },
  };
}
