// Content-addressed hash of a wire-projected workflow definition.
//
// The deploy gate, the install-time probe, and re-verify must all agree
// on the deployment's content handle, so they hash the exact same
// canonical form. This module is the single source of truth those call
// sites import; the hash is a hex SHA-256 of the definition's canonical
// JSON.

import { hexEncode } from "./hex";

/**
 * Project a value into a canonical JSON string with deterministically
 * sorted object keys. Object key order and surrounding whitespace do not
 * affect the output, so two structurally equal values serialize to
 * byte-identical strings and therefore hash equal.
 *
 * Values follow `JSON.stringify`'s semantics for what JSON can represent:
 * an object key whose value is `undefined`, a function, or a symbol is
 * dropped, and such an array element renders as `null`. The only intended
 * difference from `JSON.stringify` is the deterministic key order. So for
 * JSON-representable values the output is invariant across a JSON
 * round-trip; a value with a custom `toJSON` (e.g. a `Date`) is NOT
 * round-trip invariant here, because this canonicalizer does not invoke
 * `toJSON` -- a caller needing round-trip invariance must pass it
 * already-JSON values.
 */
export function canonicalJsonStringify(value: unknown): string {
  if (value === null) return "null";
  if (typeof value !== "object") {
    // Primitives serialize as JSON does. A value JSON cannot represent
    // (`undefined`, a function, a symbol) has no JSON text, so
    // `JSON.stringify` returns `undefined`; canonicalize it to `null`, matching
    // how JSON renders such a value as an array element (see below). A
    // top-level such value never reaches a definition hash.
    return JSON.stringify(value) ?? "null";
  }
  if (Array.isArray(value)) {
    // JSON renders an `undefined`/function/symbol array element as `null`; the
    // scalar branch above produces exactly that, so a plain recursive map keeps
    // parity.
    return `[${value.map((v) => canonicalJsonStringify(v)).join(",")}]`;
  }
  // Drop keys whose value JSON.stringify would omit (`undefined`, functions,
  // symbols), mirroring how JSON serializes an object.
  const entries = Object.entries(value)
    .filter(
      ([, v]) =>
        v !== undefined && typeof v !== "function" && typeof v !== "symbol",
    )
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries
    .map(([k, v]) => `${JSON.stringify(k)}:${canonicalJsonStringify(v)}`)
    .join(",")}}`;
}

/**
 * Compute the content hash for a wire-projected workflow definition:
 * SHA-256 of the canonical JSON of the `WorkflowDefinition` projection,
 * hex-encoded. This is the deployment's content-addressed handle; every
 * party that binds identity, approval, or re-verify to a deployment
 * derives it from the same canonical form so their values compare by
 * byte equality.
 *
 * Invariance across the boundary is load-bearing: the hub hashes a
 * projection parsed off the JSON wire (where `undefined`-valued keys are
 * already gone) while a child hashes an in-memory projection, and the two
 * must produce byte-identical strings or re-verify would fail on a
 * legitimately-approved definition. The canonicalizer's Date/`toJSON`
 * caveat is moot here: both sides hash a projection parsed from the JSON
 * wire, so no custom-`toJSON` value ever reaches the canonicalizer and the
 * two sides agree.
 */
export async function computeWireDefinitionHash(
  definition: unknown,
): Promise<string> {
  const canonical = canonicalJsonStringify(definition);
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(canonical),
  );
  return hexEncode(new Uint8Array(digest));
}
