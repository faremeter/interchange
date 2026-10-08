// Package-namespaced id validation for tool and director factories.
// Ids are scoped ("@scope/pkg/name") or unscoped ("pkg/name"); bare
// ids are rejected at definition time so independently-authored
// bundles cannot collide on plausible names like "default".

// Per-segment character set: alphanumerics, dot, hyphen, underscore.
// Whitespace and other punctuation are excluded so ids never carry
// characters that break log-line parsing or downstream tooling.
const SEGMENT = "[A-Za-z0-9._-]+";

// Scoped: "@scope/pkg/name". Three slash-separated segments; the first
// starts with "@" followed by the segment character set.
const SCOPED = new RegExp(`^@${SEGMENT}\\/${SEGMENT}\\/${SEGMENT}$`);

// Unscoped: "pkg/name". Two slash-separated segments. The package
// segment cannot start with "@" -- that route is the scoped form.
const UNSCOPED = new RegExp(`^${SEGMENT}\\/${SEGMENT}$`);

/**
 * Validate a package-namespaced id. Throws with a precise diagnostic
 * when the id is not in one of the two supported shapes.
 *
 *   "@intx/agent/default"             -> ok (scoped)
 *   "@my-org/my-workflow/special"     -> ok (scoped)
 *   "lodash-style/director-name"      -> ok (unscoped)
 *   "default"                         -> rejected (no package portion)
 *   "@intx/agent"                     -> rejected (missing name segment)
 *   "@intx/agent/"                    -> rejected (empty name segment)
 */
export function validateNamespacedId(id: string): void {
  if (!SCOPED.test(id) && !UNSCOPED.test(id)) {
    throw new Error(
      `id must be package-namespaced ` +
        `(e.g. "@vendor/pkg/name" or "pkg/name"); got ${JSON.stringify(id)}`,
    );
  }
}
