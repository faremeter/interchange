import type {
  ConversationTurn,
  InferenceEvent,
  InferenceOptions,
} from "@interchange/types/runtime";

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
// (e.g., Anthropic's ping events). It must not throw; errors should be
// returned as inference.error events.
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
