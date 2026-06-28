import type {
  ConversationTurn,
  InferenceEvent,
  InferenceOptions,
  LastCycleSource,
} from "@intx/types/runtime";

// The request shape the harness passes to fetch.
export type BuiltRequest = {
  url: string;
  headers: Record<string, string>;
  body: string;
};

// A request builder takes the internal message format and produces a
// provider-specific HTTP request. Pure function — no state, no side effects.
export type RequestBuilder = (
  messages: ConversationTurn[],
  model: string,
  options: InferenceOptions,
) => BuiltRequest;

// A response parser converts one SSE data payload string into zero or more
// internal inference events. May close over per-request state (e.g., for
// correlating content block indices with tool call IDs). Each adapter
// instance is created per inference call, so state does not leak across
// requests.
//
// The parser may return an empty array for events it doesn't care about
// (e.g., Anthropic's ping events). It MAY throw `ProtocolMismatchError`
// when the upstream chunk violates the provider's protocol (malformed
// JSON, schema validation failure, out-of-order events); the harness's
// stream-error catch converts that into an `inference.error` with
// category `"protocol_mismatch"` via `classifyStreamError`. No other
// throw type is permitted, and adapter-returned `inference.error` or
// `inference.done` events are silently dropped — the harness owns
// emission of those terminal types and the only path through which
// adapter-detected failures can surface is `ProtocolMismatchError`.
export type ResponseParser = (sseData: string) => InferenceEvent[];

// An adapter pairs a request builder with a response parser. Registration
// is a map keyed by provider identifier — no class hierarchy required.

// Extracts a retry delay from provider-specific response headers on a 429.
// Returns milliseconds to wait, or undefined if no retry info is available.
export type RetryAfterExtractor = (headers: Headers) => number | undefined;

// Extracts a pacing delay from response headers on ANY response (including
// success). Checks remaining rate limit capacity and returns how long to
// wait before the next request, or undefined if no pacing is needed.
export type PacingExtractor = (headers: Headers) => number | undefined;

export type ProviderAdapter = {
  buildRequest: RequestBuilder;
  parseResponse: ResponseParser;
  extractRetryAfterMs?: RetryAfterExtractor;
  extractPacingDelayMs?: PacingExtractor;
};

// Builds a fresh adapter for one inference call. Invoked per call so the
// returned adapter's per-request parser state never leaks across calls.
export type AdapterFactory = (source: LastCycleSource) => ProviderAdapter;

// Resolves an inference source to a provider adapter. Membership is keyed by
// the source's `provider` identifier; resolution mints a fresh adapter so the
// per-instance stateful parser is isolated per call and per failover attempt.
export type AdapterRegistry = {
  has(provider: string): boolean;
  resolve(source: LastCycleSource): ProviderAdapter;
};

/**
 * Builds an adapter registry from a map of provider identifier to adapter
 * factory. The registry closes over a private copy of the map, so callers
 * cannot mutate the set after construction. The copy is a `Map`, whose
 * lookups never consult `Object.prototype`, so an untrusted provider string
 * (e.g. `"toString"`) cannot reach an inherited member and be invoked as a
 * factory — it resolves to the loud `Unknown inference provider` error.
 *
 * `resolve` invokes the matching factory fresh on every call and never
 * memoizes the adapter instance. That per-call freshness is load-bearing:
 * the response parser holds per-request state, so a cached adapter would
 * leak that state across inference calls and across failover attempts.
 *
 * @param factories - Map of provider identifier to adapter factory
 * @returns A registry exposing membership and per-call resolution
 */
export function createAdapterRegistry(
  factories: Readonly<Record<string, AdapterFactory>>,
): AdapterRegistry {
  const byProvider = new Map<string, AdapterFactory>(Object.entries(factories));

  return {
    has(provider: string): boolean {
      return byProvider.has(provider);
    },
    resolve(source: LastCycleSource): ProviderAdapter {
      const factory = byProvider.get(source.provider);
      if (factory === undefined) {
        throw new Error(`Unknown inference provider: ${source.provider}`);
      }
      return factory(source);
    },
  };
}
