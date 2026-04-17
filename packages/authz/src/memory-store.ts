// In-memory grant store for testing and local demos.
//
// The caller provides a pre-scoped array of grants. The store filters by
// principalId on each collectGrants call, matching the DB store's behavior.
//
// Limitations vs. the DB store:
//   - tenantId is accepted by the interface but is a no-op here. The caller
//     scopes grants to the correct tenant when constructing the store.
//   - Role-based grants (principalId: null, roleId set) are not resolved.
//     The DB store performs a principalRole join to find role memberships;
//     this store has no role data. Grants with principalId: null are never
//     returned. If you need role-based grants in tests, set principalId
//     directly on each grant.

import type { GrantRule, GrantStore } from "./types";

export function createInMemoryGrantStore(grants: GrantRule[]): GrantStore {
  return {
    async collectGrants(principalId: string): Promise<GrantRule[]> {
      const now = new Date();
      return grants.filter((g) => {
        if (g.principalId !== principalId) return false;
        if (g.expiresAt !== null && g.expiresAt <= now) return false;
        return true;
      });
    },
  };
}
