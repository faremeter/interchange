// Shared authorization gate for the `user` principal variant, used by
// every kind handler that accepts user-token-authenticated requests
// (workflow, skill, agent-state, workflow-run).
//
// The route layer has already pre-resolved the grant verdict and
// attached it as `authz`; the kind handlers do NOT re-query the grant
// store here. This gate (a) checks the bearer-token's claims bound the
// requested (ref, action) and have not expired, and (b) sanity-checks
// that the pre-resolved verdict targets this exact resource and grant
// verb. Both gates must pass before the verdict's `effect` is honoured.
//
// Funnelling every kind through this one gate keeps the security-
// critical claim/verdict cross-check from drifting between kinds. The
// only per-kind input is the `resourcePrefix` the verdict's `resource`
// must carry (`asset:<id>` for the codebase kinds, `agent-state:<id>`,
// `workflow-run:<id>`).

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
   * The resource-kind prefix the pre-resolved authz verdict must carry
   * for this kind: the verdict's `resource` is compared against
   * `<resourcePrefix>:<repoId.id>`.
   */
  resourcePrefix: string;
};

/**
 * Verdict for a `user` principal performing `action` on `ref` of
 * `repoId`, in the shape the substrate's `AuthorizeFn` contract
 * expects. The caller dispatches on `principal.kind === "user"` first;
 * this function narrows with `UserPrincipal` and applies the full
 * claim/verdict cross-check.
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
  // performed by `listRefs`. Per-ref filtering is the advertise-refs
  // layer's responsibility, so the bulk read is gated on action and
  // expiry alone.
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
