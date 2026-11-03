/**
 * Smart-HTTP glue for the bearer-token git routes in `assets.ts`.
 *
 * The routes resolve a URL to a `RepoId`, pre-resolve the authz verdict
 * against the grant store, and construct a `UserPrincipal` carrying that
 * verdict so the substrate's authorize gate only sanity-checks rather than
 * re-querying.
 *
 * Bearer-claim `expiresAt` is a `Date` on the wire; the substrate's
 * `UserPrincipal.tokenClaims.expiresAt` is a `number`. The Date →
 * number conversion happens exactly once, at the route handler
 * boundary, in `buildUserPrincipal`.
 */

import { authorize } from "@intx/authz";
import { repoActionToGrantVerb } from "@intx/hub-common";
import type { RefEntry, RepoStore, UserPrincipal } from "@intx/hub-sessions";
import type { RepoAction } from "@intx/types/repo";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";

import type { GitTokenClaims } from "../middleware/git-token-auth";
import type { RefSource } from "../git-http/advertise-refs";
import type { UploadPackRepoStore } from "../git-http/upload-pack";

function dateToNumber(d: Date): number {
  return d.getTime();
}

/**
 * Pre-resolve the grant verdict for a user-token-authenticated
 * smart-HTTP request. `resource` is the full verdict resource string
 * for this kind (`asset:<id>`); the grant verb is
 * derived from `action`. The route layer attaches the returned
 * verdict to the constructed `UserPrincipal`; the substrate does NOT
 * re-query the grant store.
 */
export async function resolveAuthzVerdict(args: {
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  principalId: string;
  tenantId: string;
  resource: string;
  action: RepoAction;
  /**
   * The run-token consumer, or `undefined` for a caller that is not one.
   * Required so that no request-driven check can forget it: a check without
   * the consumer would skip a `{tool}`-scoped deny grant.
   */
  consumer: string | undefined;
}): Promise<UserPrincipal["authz"]> {
  const grantVerb = repoActionToGrantVerb(args.action);
  const verdict = await authorize(
    args.grantStore,
    args.principalId,
    args.tenantId,
    args.resource,
    grantVerb,
    {
      conditions: args.conditionRegistry,
      ...(args.consumer !== undefined ? { consumer: args.consumer } : {}),
    },
  );
  return {
    effect: verdict.effect === "allow" ? "allow" : "deny",
    resource: args.resource,
    grantVerb,
  };
}

export function buildUserPrincipal(args: {
  principalId: string;
  tenantId: string;
  authz: UserPrincipal["authz"];
  claims: GitTokenClaims;
}): UserPrincipal {
  return {
    kind: "user",
    principalId: args.principalId,
    tenantId: args.tenantId,
    authz: args.authz,
    tokenClaims: {
      refPattern: args.claims.refPattern,
      actions: args.claims.actions,
      expiresAt: dateToNumber(args.claims.expiresAt),
    },
  };
}

// Substrate adapters: bridge the substrate's RepoStore to the narrow
// per-handler contracts that advertise-refs and upload-pack expose.

export function makeRefSource(
  repoStore: RepoStore,
  principal: UserPrincipal,
): RefSource {
  return {
    async listRefs(_p, repoId): Promise<RefEntry[]> {
      return repoStore.listRefs(principal, repoId);
    },
    async resolveHead(_p, repoId) {
      return repoStore.resolveHead(principal, repoId);
    },
  };
}

export function makeUploadPackStore(
  repoStore: RepoStore,
  principal: UserPrincipal,
): UploadPackRepoStore {
  return {
    async listRefs(_p, repoId): Promise<RefEntry[]> {
      return repoStore.listRefs(principal, repoId);
    },
    async getRepoDir(_p, repoId): Promise<string> {
      return repoStore.getRepoDir(repoId);
    },
  };
}
