// Adapter that surfaces an existing `HubTransport` instance as the
// supervisor-facing `MailBusBindings` shape. Hosts that already own
// a hub-mail transport (the sidecar, the integration test harness,
// any future alternative-sidecar implementation) wire the supervisor
// through this adapter instead of standing up a second bus.
//
// Per-address subscriber maps live inside the adapter: the
// supervisor's `subscribeMailForAddress` registers a handler here;
// the host's transport delivers messages via `routeInbound` and the
// adapter fans the bytes out to every subscribed handler. The
// transport itself is treated as a sink the host already owns -- the
// adapter does not register addresses on the transport on the
// supervisor's behalf (production sidecars already register via
// their own `provisionAgent` flow) and does not double-deliver
// messages the transport itself routes.

import type { HubTransport } from "@intx/mail-memory";

import type { MailBusBindings } from "../supervisor/types";

/**
 * Returned shape: the `MailBusBindings` surface the supervisor
 * consumes, plus a `routeInbound` method the host calls to deliver
 * an inbound message to every handler subscribed at the named
 * address. The split keeps the supervisor's subscription contract
 * narrow while letting the host drive delivery through its existing
 * transport plumbing.
 */
export interface HubTransportMailBusAdapter extends MailBusBindings {
  /**
   * Fan a delivered message out to every handler subscribed at
   * `address`. Returns immediately if no handler is registered --
   * the supervisor's lifecycle (`subscribeMailForAddress` returns a
   * disposer the supervisor calls on teardown) is the authoritative
   * source of which addresses are live; addresses without an active
   * subscriber drop the message silently.
   */
  routeInbound(address: string, message: Uint8Array): void;
}

/**
 * Wrap an existing `HubTransport` instance as the supervisor-facing
 * `MailBusBindings` shape. The `transport` argument is held only as
 * a sink-side reference the adapter does not actively reach into
 * today -- production wiring uses the existing `provisionAgent`
 * flow to register agent addresses on the transport, and the
 * adapter delivers inbound bytes through `routeInbound` directly
 * into the per-address subscriber map below.
 */
export function wrapHubTransportAsMailBus(
  transport: HubTransport,
): HubTransportMailBusAdapter {
  const subscribers = new Map<string, Set<(rawMessage: Uint8Array) => void>>();
  return {
    registerAddress(address: string) {
      // The supervisor's address registration is the seam for the
      // multi-step branch where the workflow-process child owns
      // its own mailbox; production sidecars register the trivial
      // address through `SessionManager.provisionAgent` before any
      // workflow-host hook runs, so this method is intentionally
      // inert. The `transport` reference is retained so a future
      // routing change that wants the bus to own registration has
      // the handle in scope.
      void address;
      void transport;
    },
    unregisterAddress(address: string) {
      subscribers.delete(address);
    },
    subscribeMailForAddress(
      address: string,
      handler: (rawMessage: Uint8Array) => void,
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
    routeInbound(address: string, message: Uint8Array) {
      const set = subscribers.get(address);
      if (set === undefined) return;
      for (const handler of set) handler(message);
    },
  };
}
