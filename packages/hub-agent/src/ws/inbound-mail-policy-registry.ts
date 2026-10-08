// Per-recipient-address registry of RESOLVED inbound-mail admission policies.
//
// The sidecar host resolves each hydrated deployment's authored
// `inboundMailPolicy` into a total `ResolvedInboundMailPolicy` once, at the
// post-spawn registration window, keyed by the deployment's mail address.
// The hub-link's `mail.inbound` seam reads it back to decide admission per
// message. The fail-closed default for an unknown address lives in the
// lookup built here (`createInboundMailPolicyLookup`), so the seam never
// re-derives a default of its own.

import type { ResolvedInboundMailPolicy } from "./inbound-signature";

/**
 * The fully-closed resolved policy: EVERY inbound-mail outcome -- including
 * `clean` -- maps to `reject`. The admission decision for an address with no
 * registered policy: an address the sidecar never hydrated a deployment for
 * (or already tore down) has no author intent to honor, so it admits nothing.
 *
 * DISTINCT from `resolveInboundMailPolicy(undefined)`, which admits a `clean`
 * message: that path has a live deployment whose author declared no policy;
 * this one has no deployment behind the address at all.
 */
export const FULLY_CLOSED_INBOUND_MAIL_POLICY: ResolvedInboundMailPolicy = {
  clean: "reject",
  error: "reject",
  untrustedFrom: "reject",
  mismatchedFrom: "reject",
  absentFrom: "reject",
  invalid: "reject",
  missing: "reject",
  unknown: "reject",
};

/**
 * Address-keyed store of resolved inbound-mail policies. The host `register`s
 * a deployment's policy beside its mail-router registration and `unregister`s
 * it in the same teardown, so a reused address never inherits a stale policy.
 * `get` returns `undefined` for an unregistered address; the lookup built over
 * this store maps that miss onto the fully-closed default.
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
 * Build the per-address lookup the hub-link seam consumes: the registered
 * policy for an address, or {@link FULLY_CLOSED_INBOUND_MAIL_POLICY} when the
 * registry holds none. The single edge that owns the unknown-address default;
 * the seam adds no fallback of its own.
 */
export function createInboundMailPolicyLookup(
  registry: InboundMailPolicyRegistry,
): (address: string) => ResolvedInboundMailPolicy {
  return (address) => registry.get(address) ?? FULLY_CLOSED_INBOUND_MAIL_POLICY;
}
