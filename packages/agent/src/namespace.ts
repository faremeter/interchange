// Package-namespaced id validation for tool and director factories.
//
// Ids must be either scoped ("@scope/pkg/name") or unscoped
// ("pkg/name"). The package portion is the identity anchor; the trailing
// segment names the tool or director within that package. Bare ids
// without a package portion are rejected at definition time so two
// independently-authored bundles cannot accidentally collide on an
// otherwise plausible name like "default".

// Per-segment character set. Mirrors what npm and most package-manager
// ecosystems accept inside an id segment: alphanumerics, dot, hyphen,
// underscore. Whitespace (space, tab, newline) and other punctuation
// are excluded so an id never carries characters that would render
// strangely in error messages, break log-line parsing, or trip
// downstream tooling that splits on whitespace.
const SEGMENT = "[A-Za-z0-9._-]+";

// Scoped: "@scope/pkg/name". Three slash-separated segments; the first
// starts with "@" followed by the segment character set. Each segment
// must be non-empty.
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
