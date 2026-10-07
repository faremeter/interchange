// Shared authorization gate for the `user` principal variant, used by
// every kind handler that accepts user-token-authenticated requests
// (workflow, skill, agent-state, workflow-run).
//
// The route layer pre-resolved the grant verdict and attached it as
// `authz`; this gate checks the bearer-token's claims bound the requested
// (ref, action) and have not expired, and sanity-checks the verdict
// targets this exact resource and grant verb, before honouring `effect`.
// Funnelling every kind through one gate keeps the security-critical
// cross-check from drifting; the only per-kind input is the
// `resourcePrefix` the verdict's `resource` must carry.

import { type } from "arktype";
import { glob, repoActionToGrantVerb } from "@intx/hub-common";
import type { RepoAction, RepoId, Principal } from "./types";
import { UserPrincipal } from "./types";

export type AuthorizeUserPrincipalArgs = {
  principal: Principal;
  repoId: RepoId;
  ref: string;
  action: RepoAction;
  /**
   * Resource-kind prefix the pre-resolved authz verdict's `resource`
   * must carry: compared against `<resourcePrefix>:<repoId.id>`.
   */
  resourcePrefix: string;
};

/**
 * Verdict for a `user` principal performing `action` on `ref` of
 * `repoId`, in the shape the substrate's `AuthorizeFn` expects. The
 * caller dispatches on `principal.kind === "user"` first; this function
 * narrows with `UserPrincipal` and applies the full claim/verdict
 * cross-check.
 */
export function authorizeUserPrincipal({
  principal,
  repoId,
  ref,
  action,
  resourcePrefix,
}: AuthorizeUserPrincipalArgs):
  | { allowed: true }
  | { allowed: false; reason: string } {
  const parsed = UserPrincipal(principal);
  if (parsed instanceof type.errors) {
    return {
      allowed: false,
      reason: `user principal is malformed: ${parsed.summary}`,
    };
  }
  if (!parsed.tokenClaims.actions.includes(action)) {
    return {
      allowed: false,
      reason: `token does not grant action ${action}`,
    };
  }
  // `ref === "*"` is the substrate's sentinel for the bulk read
  // performed by `listRefs`; per-ref filtering is the advertise-refs
  // layer's job, so the bulk read is gated on action and expiry alone.
  if (ref !== "*" && !glob.match(parsed.tokenClaims.refPattern, ref)) {
    return {
      allowed: false,
      reason: `token refPattern ${parsed.tokenClaims.refPattern} does not match ${ref}`,
    };
  }
  if (Date.now() >= parsed.tokenClaims.expiresAt) {
    return {
      allowed: false,
      reason: `token expired at ${parsed.tokenClaims.expiresAt}`,
    };
  }
  const expectedResource = `${resourcePrefix}:${repoId.id}`;
  if (parsed.authz.resource !== expectedResource) {
    return {
      allowed: false,
      reason: `authz verdict resource ${parsed.authz.resource} does not match ${expectedResource}`,
    };
  }
  const expectedGrantVerb = repoActionToGrantVerb(action);
  if (parsed.authz.grantVerb !== expectedGrantVerb) {
    return {
      allowed: false,
      reason: `authz verdict grantVerb ${parsed.authz.grantVerb} does not match ${expectedGrantVerb}`,
    };
  }
  if (parsed.authz.effect === "allow") {
    return { allowed: true };
  }
  return {
    allowed: false,
    reason: `authz verdict denied for ${expectedResource} ${expectedGrantVerb}`,
  };
}
