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
// internal inference events. Pure function — all state lives in the harness.
//
// The parser may return an empty array for events it doesn't care about
// (e.g., Anthropic's ping events). It must not throw; errors should be
// returned as inference.error events.
export type ResponseParser = (sseData: string) => InferenceEvent[];

// An adapter pairs a request builder with a response parser. Registration
// is a map keyed by provider identifier — no class hierarchy required.
export type ProviderAdapter = {
  buildRequest: RequestBuilder;
  parseResponse: ResponseParser;
};
