// Credential provider plugins: shape a resolved provider-backed
// credential into a mediated handle. A provider owns HOW the handle
// authenticates; it never acquires material or decides authorization --
// both happen upstream at the delivery boundary.
//
// The registry is a Map-backed, throw-on-missing lookup keyed by
// provider identifier. A Map never consults Object.prototype, so an
// untrusted key like "toString" resolves to the loud unknown-provider
// error rather than an inherited member.

import type {
  CredentialProvider,
  CredentialShapeContext,
  HttpMediatedCredential,
} from "@intx/types";

/** Resolves a provider identifier to the plugin that shapes its handles. */
export interface CredentialProviderRegistry {
  has(key: string): boolean;
  resolve(key: string): CredentialProvider;
}

/**
 * Build a registry from a list of providers; the list is copied into a
 * private `Map`, so callers cannot mutate the set and lookups never
 * reach `Object.prototype`. A duplicate key throws at construction.
 */
export function createCredentialProviderRegistry(
  providers: readonly CredentialProvider[],
): CredentialProviderRegistry {
  const byKey = new Map<string, CredentialProvider>();
  for (const provider of providers) {
    if (byKey.has(provider.key)) {
      throw new Error(`Duplicate credential provider key: ${provider.key}`);
    }
    byKey.set(provider.key, provider);
  }

  return {
    has(key: string): boolean {
      return byKey.has(key);
    },
    resolve(key: string): CredentialProvider {
      const provider = byKey.get(key);
      if (provider === undefined) {
        throw new Error(`Unknown credential provider: ${key}`);
      }
      return provider;
    },
  };
}

/** The `fetch` subset a shaped handle needs; the global `fetch` and a test stub both satisfy it. */
export type FetchLike = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;

/** Options for the built-in HTTP provider. */
export interface HttpCredentialProviderOptions {
  /** `fetch` for the shaped handle; defaults to the global one, injectable for tests. */
  fetch?: FetchLike;
}

/**
 * The built-in HTTP provider: shapes an `HttpMediatedCredential` -- an
 * authed `fetch` pinned to the credential's provider origin, injecting
 * the current secret as a bearer token per request. Material is read
 * fresh on every call, so a rotation reaches the handle without a
 * rebuild.
 *
 * Origin pinning is load-bearing security: only the initial,
 * origin-checked request authenticates, and redirects are never
 * followed (`redirect: "manual"`), so the bearer is never sent to any
 * origin but the pinned one.
 *
 * Bearer is the only auth scheme today; other schemes are separate
 * plugins.
 */
export function createHttpCredentialProvider(
  opts?: HttpCredentialProviderOptions,
): CredentialProvider {
  const fetchImpl: FetchLike = opts?.fetch ?? globalThis.fetch;

  return {
    key: "http",
    shape(context: CredentialShapeContext): HttpMediatedCredential {
      const pinnedOrigin = new URL(context.origin).origin;

      return {
        kind: "http",
        async fetch(
          input: string | URL | Request,
          init?: RequestInit,
        ): Promise<Response> {
          const target = resolveTargetUrl(input, pinnedOrigin);
          if (target.origin !== pinnedOrigin) {
            throw new Error(
              `http credential is pinned to ${pinnedOrigin}; refusing cross-origin request to ${target.origin}`,
            );
          }

          // Read the secret fresh on every call so a rotation reaches this handle.
          const { secret } = context.readCurrentMaterial();

          // redirect:"manual" is dictated by the handle, never inherited
          // from caller input: the origin check guards only the initial
          // url, so following a 3xx to a foreign origin would carry the
          // bearer off the pinned host. The 3xx is returned unfollowed;
          // a same-origin retry re-pins and re-auths, a cross-origin one
          // is refused above.
          if (input instanceof Request) {
            // Re-issue the caller's request (method, body preserved)
            // with the auth header and forced redirect mode; its url was
            // origin-checked above.
            const headers = new Headers(input.headers);
            headers.set("authorization", `Bearer ${secret}`);
            return fetchImpl(
              new Request(input, { headers, redirect: "manual" }),
            );
          }

          const headers = new Headers(init?.headers);
          headers.set("authorization", `Bearer ${secret}`);
          return fetchImpl(target, { ...init, headers, redirect: "manual" });
        },
        dispose(): void {
          // An http handle allocates no resources; nothing to release.
        },
      };
    },
  };
}

/** The built-in providers every host registers; hosts extend the list passed to `createCredentialProviderRegistry`. */
export function builtinCredentialProviders(): CredentialProvider[] {
  return [createHttpCredentialProvider()];
}

/**
 * Resolve a request's target URL: a relative string resolves against
 * the pinned origin; an absolute string, URL, or `Request` keeps its
 * own origin (refused by the caller if it differs).
 */
function resolveTargetUrl(
  input: string | URL | Request,
  pinnedOrigin: string,
): URL {
  if (typeof input === "string") {
    return new URL(input, pinnedOrigin);
  }
  if (input instanceof URL) {
    return input;
  }
  return new URL(input.url);
}
