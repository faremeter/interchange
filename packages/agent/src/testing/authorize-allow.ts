// Permissive AuthorizeFn for tests and examples: allows every call.
// Production callers must supply a real authorize function tied to
// actual policy.

import type { AuthorizeFn } from "../env";

/**
 * Construct a permissive AuthorizeFn that allows every call, ignoring
 * its arguments and returning the shape the production authz
 * extension expects.
 */
export function permissiveAuthorize(): AuthorizeFn {
  return async (_resource, _action, _context) => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });
}
