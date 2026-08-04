// Credential provider plugins: the seam that shapes a resolved provider-backed
// credential into a mediated handle a consumer can use. A provider owns HOW the
// handle authenticates (an authed `fetch`, a future key-file + socket); it is
// given a material source and never acquires material or decides authorization
// -- both happen upstream, at the delivery boundary, before a provider is
// consulted.
//
// The registry mirrors @intx/inference's AdapterRegistry: a Map-backed lookup
// keyed by provider identifier, prototype-pollution-safe (a Map never consults
// Object.prototype, so an untrusted key like "toString" resolves to the loud
// unknown-provider error rather than an inherited member), throw-on-missing.

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
 * Build a registry from a list of providers. The list is copied into a private
 * `Map`, so callers cannot mutate the set after construction and lookups never
 * reach `Object.prototype`. A duplicate key is a wiring error and throws at
 * construction rather than silently shadowing.
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

/**
 * The minimal call signature the shaped handle needs from `fetch`. The global
 * `fetch` satisfies it; a test stub can too, without implementing the extra
 * members (`preconnect`) the full `fetch` type carries.
 */
export type FetchLike = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;

/** Options for the built-in HTTP provider. */
export interface HttpCredentialProviderOptions {
  /**
   * The `fetch` the shaped handle delegates to once the request is
   * origin-checked and the auth header is injected. Defaults to the global
   * `fetch`; injectable so origin-pinning can be exercised without a network.
   */
  fetch?: FetchLike;
}

/**
 * The built-in HTTP credential provider. It shapes an `HttpMediatedCredential`:
 * an authed `fetch` pinned to the credential's provider origin, injecting the
 * current secret as a bearer token per request. The material is read fresh on
 * every call, so a rotation that updates the underlying cell is picked up
 * without rebuilding the handle.
 *
 * Origin pinning is load-bearing security: the handle authenticates only the
 * initial, origin-checked request and never follows redirects. A request whose
 * resolved origin is not the pinned one is refused, and a server 3xx is
 * returned to the caller unfollowed (`redirect: "manual"`), so the bearer is
 * never sent to any origin but the pinned one. Transparent redirect-following
 * is intentionally not provided: a tool re-issues a same-origin redirect target
 * through the handle (a cross-origin one is refused). This keeps token safety
 * in the handle rather than resting on the injected `fetch`'s redirect
 * behavior.
 *
 * Bearer is the only auth scheme today; providers that authenticate differently
 * (a `token` scheme, an `x-api-key` header) are separate plugins, not a branch
 * here.
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

          // Read the secret fresh on every call so a rotation of the underlying
          // material cell reaches this handle without a rebuild.
          const { secret } = context.readCurrentMaterial();

          // redirect:"manual" is dictated by the handle, never inherited from
          // caller input. The origin check guards only the INITIAL url, so
          // following a server 3xx to a foreign origin would carry the bearer
          // off the pinned host. Instead the 3xx is returned to the caller
          // unfollowed: a same-origin target is re-issued through the handle
          // (which re-pins and re-auths); a cross-origin one is refused above.
          if (input instanceof Request) {
            // Re-issue the caller's request (method, body preserved) with the
            // auth header added and the redirect mode forced; its url was
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

/**
 * The built-in credential providers every host registers. A single `http`
 * provider today; a host composes additional providers by extending the list
 * passed to `createCredentialProviderRegistry`.
 */
export function builtinCredentialProviders(): CredentialProvider[] {
  return [createHttpCredentialProvider()];
}

/**
 * Resolve the URL a request targets. A relative string resolves against the
 * pinned origin (so a tool can call `/repos`); an absolute string or URL keeps
 * its own origin (and is refused by the caller if it differs); a `Request`
 * carries an absolute URL already.
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
