// Content-addressed hash of a wire-projected workflow definition.
//
// The deploy gate, the install-time probe, and re-verify must all agree
// on the deployment's content handle, so they hash the exact same
// canonical form. This module is the single source of truth those call
// sites import; the hash is a hex SHA-256 of the definition's canonical
// JSON.
//
// The canonicalizer mirrors -- but is deliberately not the same as --
// the runlocal repo-store's equality canonicalizer
// (`packages/workflow/src/runlocal/repo-store.ts`). That one drops
// `undefined`-valued fields for structural event comparison; this one
// preserves them so the content hash is a faithful projection of the
// wire bytes. The two are kept separate on purpose.

import { hexEncode } from "./hex";

/**
 * Project a value into a canonical JSON string with deterministically
 * sorted object keys. Object key order and surrounding whitespace do not
 * affect the output, so two structurally equal values serialize to
 * byte-identical strings and therefore hash equal.
 */
export function canonicalJsonStringify(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJsonStringify).join(",")}]`;
  }
  const entries = Object.entries(value).sort(([a], [b]) =>
    a < b ? -1 : a > b ? 1 : 0,
  );
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
