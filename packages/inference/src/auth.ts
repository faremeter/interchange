import type { InferenceSource } from "@intx/types/runtime";
import type { CredentialMaterialResolver } from "@intx/types";

// Sentinel placeholder strings adapters use in their built request
// headers to declare which credential the harness should fill at send
// time. The harness scans every header value and replaces exact-match
// sentinels with the secret resolved from the source's `credentialId`
// against the run's credential cell. Adapters never see the API key.
//
// Each new provider adds a new header name + sentinel choice in its
// `buildRequest`; the harness needs no per-provider knowledge. The
// alternative pattern -- a switch in the harness keyed on header name
// -- was abandoned because the constraint ("how does this provider
// want its credential delivered") lives with the adapter, not with the
// harness, and growing a hardcoded branch per provider violates the
// constraint-ownership rule.
//
// The sentinel strings deliberately contain angle brackets and a
// keyword prefix that would never appear in a legitimate header value:
// matching is exact, but defense-in-depth ensures a literal echo from
// an upstream system can't accidentally trigger replacement.

/**
 * Sentinel for headers that carry the API key verbatim (no prefix).
 * Used by providers like Anthropic (`x-api-key`) and Google
 * (`x-goog-api-key`) that accept the raw credential.
 */
export const CREDENTIAL_SENTINEL = "<inject:credential>";

/**
 * Sentinel for headers that carry a Bearer-prefixed API key. Used by
 * providers that follow the `Authorization: Bearer <token>` convention
 * (OpenAI, OpenAI-compatible).
 */
export const BEARER_CREDENTIAL_SENTINEL = "<inject:bearer-credential>";

/**
 * Replace credential sentinels in a header map with material derived
 * from the inference source. Returns a new object; the input is not
 * mutated. Non-sentinel header values pass through unchanged.
 *
 * A header value that contains a sentinel as a substring but is not
 * exactly equal to it is left alone -- partial replacement would be
 * surprising, and no legitimate adapter constructs sentinel-bearing
 * composite values.
 */
export function injectCredentials(
  headers: Record<string, string>,
  source: InferenceSource,
  readMaterial: CredentialMaterialResolver,
): Record<string, string> {
  // Resolve the source's secret lazily and once: only when a header actually
  // carries a sentinel, so a request with no credential sentinel never touches
  // the cell, and the fail-closed read (revoked/absent credential) surfaces only
  // when the secret is genuinely needed.
  let cachedSecret: string | undefined;
  const secret = (): string => {
    cachedSecret ??= readMaterial(source.credentialId).secret;
    return cachedSecret;
  };
  const result: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers)) {
    if (value === CREDENTIAL_SENTINEL) {
      result[name] = secret();
    } else if (value === BEARER_CREDENTIAL_SENTINEL) {
      result[name] = `Bearer ${secret()}`;
    } else {
      result[name] = value;
    }
  }
  return result;
}
