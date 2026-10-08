// In-memory grant store for tests and local demos. Filters by principalId on
// each collectGrants call, like the DB store.
//
// Limitations vs. the DB store:
//   - tenantId is a no-op here: the caller pre-scopes grants to the tenant.
//   - Role grants (principalId: null) are not resolved; there is no role data.
//     Set principalId directly on each grant if you need role-based grants.

import type { GrantRule, GrantStore } from "./types";

export function createInMemoryGrantStore(grants: GrantRule[]): GrantStore {
  function collect(principalId: string): GrantRule[] {
    const now = new Date();
    return grants.filter((g) => {
      if (g.principalId !== principalId) return false;
      if (g.expiresAt !== null && g.expiresAt <= now) return false;
      return true;
    });
  }

  // tenantId (and the ancestor chain) is a no-op: the caller pre-scopes
  // the grant array, so chain collection returns the same set as
  // single-tenant collection.
  return {
    async collectGrants(principalId: string): Promise<GrantRule[]> {
      return collect(principalId);
    },
    async collectGrantsInChain(principalId: string): Promise<GrantRule[]> {
      return collect(principalId);
    },
  };
}
