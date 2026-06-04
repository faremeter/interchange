// Permissive AuthorizeFn for tests and examples.
//
// Returns { effect: "allow" } for every call. Useful when the agent is
// exercised without grants -- the test cares about the agent surface
// rather than the authz decision. Production callers must supply a real
// authorize function tied to actual policy.

import type { AuthorizeFn } from "../env";

/**
 * Construct a permissive AuthorizeFn that allows every call. The
 * returned function ignores its arguments and returns the same shape
 * the production authz extension expects.
 */
export function permissiveAuthorize(): AuthorizeFn {
  return async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });
}
