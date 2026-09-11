// Per-recipient-address registry of RESOLVED inbound-mail admission policies.
//
// The sidecar host resolves each hydrated deployment's authored
// `inboundMailPolicy` into a total `ResolvedInboundMailPolicy` ONCE, at the
// post-spawn registration window, and stores it here keyed by the deployment's
// mail address. The hub-link's `mail.inbound` seam reads it back to decide, per
// message, whether an inbound frame's admission outcome is admitted or rejected.
//
// The registry itself holds only what a live deployment registered. The
// fail-closed default for an address the registry does not hold lives in the
// lookup this module builds (`createInboundMailPolicyLookup`), so the seam sees
// a total policy for every address and never re-derives a default of its own.

import type { ResolvedInboundMailPolicy } from "./inbound-signature";

/**
 * The fully-closed resolved policy: EVERY inbound-mail outcome -- including
 * `clean` -- maps to `reject`. It is the admission decision for an inbound frame
 * whose recipient address has no registered policy: an address the sidecar
 * never hydrated a deployment for, or already tore one down, has no author
 * intent to honor, so it admits nothing.
 *
 * This is DISTINCT from `resolveInboundMailPolicy(undefined)`, which admits a
 * `clean` message: that path has a live deployment whose author simply declared
 * no policy, whereas this one has no deployment behind the address at all.
 */
export const FULLY_CLOSED_INBOUND_MAIL_POLICY: ResolvedInboundMailPolicy = {
  clean: "reject",
  error: "reject",
  untrustedFrom: "reject",
  invalid: "reject",
  missing: "reject",
  unknown: "reject",
};

/**
 * Address-keyed store of resolved inbound-mail policies. The sidecar host
 * `register`s a deployment's policy beside its mail-router registration and
 * `unregister`s it in the same teardown, so a reused address never inherits a
 * stale policy. `get` returns `undefined` for an unregistered address; the
 * lookup built over this store maps that miss onto the fully-closed default.
 */
export type InboundMailPolicyRegistry = {
  register(address: string, policy: ResolvedInboundMailPolicy): void;
  unregister(address: string): void;
  get(address: string): ResolvedInboundMailPolicy | undefined;
};

export function createInboundMailPolicyRegistry(): InboundMailPolicyRegistry {
  const policies = new Map<string, ResolvedInboundMailPolicy>();
  return {
    register(address, policy) {
      policies.set(address, policy);
    },
    unregister(address) {
      policies.delete(address);
    },
    get(address) {
      return policies.get(address);
    },
  };
}

/**
 * Build the per-address lookup the hub-link seam consumes. It returns the
 * registered policy for an address, or {@link FULLY_CLOSED_INBOUND_MAIL_POLICY}
 * when the registry holds none. This is the single edge that owns the
 * unknown-address default: the seam calls the lookup and indexes the returned
 * total map directly, with no fallback of its own.
 */
export function createInboundMailPolicyLookup(
  registry: InboundMailPolicyRegistry,
): (address: string) => ResolvedInboundMailPolicy {
  return (address) => registry.get(address) ?? FULLY_CLOSED_INBOUND_MAIL_POLICY;
}
