// The runtime mediated-credential surface: how a resolved provider-backed
// credential reaches the consumer that uses it (a tool, or the built-in
// reactor) WITHOUT handing over the raw secret.
//
// A consumer declares a credential handle (see `ToolCredentialHandle`) and, at
// handler-init, resolves a *mediated credential* -- a handle that lets it
// authenticate against the provider without holding the secret on its own API.
// An HTTP credential mediates by exposing an authed `fetch` pinned to the
// credential's provider origin; the bearer token is injected per request and
// never surfaced.
//
// Honest scope of the mediation: it is NOT containment against hostile tool
// code. A tool that legitimately receives an http mediated credential can read
// the Authorization header the fetch sends. What mediation buys is (a) the
// secret is off the tool's declared API surface, (b) a single rotation point --
// material is read fresh per use, so a rotation reaches every holder without
// re-shaping the handle -- and (c) consumer-scoped resolution. Confidentiality
// from the receiving tool needs process/VM isolation, a different boundary.
//
// The provider plugin owns how a handle is shaped; the acquisition of material
// (resolve a credential row, authorize, decrypt) lives on the delivery side and
// is never the plugin's decision.

/** The current secret material behind a credential, read fresh at each use. */
export interface CredentialMaterial {
  readonly secret: string;
}

/**
 * Reads the current material for one credential. A provider handle calls this
 * per use rather than capturing a snapshot, so a rotation that updates the
 * underlying cell is picked up without rebuilding the handle.
 */
export type CredentialMaterialSource = () => CredentialMaterial;

/** What a provider plugin is given to shape a mediated credential. */
export interface CredentialShapeContext {
  /**
   * The provider origin the credential authenticates to (e.g.
   * `https://api.github.com`). An http handle pins its requests to this origin.
   */
  readonly origin: string;
  /** Reads the current secret material at each use (rotation indirection). */
  readCurrentMaterial: CredentialMaterialSource;
}

/** Fields shared by every mediated-credential variant. */
export interface MediatedCredentialBase {
  /** Discriminates the variant a consumer narrows on. */
  readonly kind: string;
  /**
   * Release resources the handle allocated. An http handle allocates none; a
   * future key-file/socket handle would. Idempotent; run on teardown.
   */
  dispose(): void | Promise<void>;
}

/**
 * An HTTP-authenticated mediated credential: an authed `fetch` pinned to the
 * credential's provider origin. A request whose resolved origin is not the
 * pinned one is refused, and redirects are not followed (a 3xx is returned to
 * the caller), so the bearer token is only ever sent to the pinned origin and a
 * holder cannot redirect it to an attacker-chosen host.
 */
export interface HttpMediatedCredential extends MediatedCredentialBase {
  readonly kind: "http";
  fetch(input: string | URL | Request, init?: RequestInit): Promise<Response>;
}

/**
 * A mediated credential handed to a consumer at resolve time. A discriminated
 * union on `kind`; `http` is the only variant today. Future provider kinds
 * (e.g. an ssh key-file + agent socket) extend the union with their own `kind`.
 */
export type MediatedCredential = HttpMediatedCredential;

/**
 * A provider plugin: the seam that owns how a mediated credential is shaped for
 * its provider. Registered under `key`, matched against a resolved provider's
 * plugin identifier. The plugin shapes a handle from a material source; it does
 * not acquire material and never decides authorization -- both happen upstream,
 * at the delivery boundary, before a plugin is ever consulted.
 */
export interface CredentialProvider {
  readonly key: string;
  shape(context: CredentialShapeContext): MediatedCredential;
}

/**
 * The runtime `credentials` capability: a sub-registry a consumer queries by
 * the credential handle it declared, receiving a mediated credential. It is the
 * dynamic (per-binding) axis that lives under the fixed, statically-typed
 * capability map.
 *
 * Resolution is consumer-scoped and fail-closed: it yields a handle only for a
 * credential the calling consumer is authorized to use. An unbound handle, or
 * one the consumer lacks a `credential:{id}` / `use` grant for, throws. `resolve`
 * is async because the authorization check is.
 */
export interface CredentialCapability {
  resolve(handle: string): Promise<MediatedCredential>;
}
