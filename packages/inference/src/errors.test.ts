import { describe, test, expect } from "bun:test";
import {
  classifyHTTPError,
  classifyNetworkError,
  classifyAbortError,
  classifyStreamError,
  classifyProtocolMismatch,
  ProtocolMismatchError,
} from "./errors";
import overflowFixture from "./fixtures/context-overflow-anthropic.json";

describe("classifyHTTPError", () => {
  test("401 → credential_failure", () => {
    const err = classifyHTTPError(401, "Unauthorized");
    expect(err.category).toBe("credential_failure");
    expect(err.statusCode).toBe(401);
  });

  test("403 → credential_failure", () => {
    const err = classifyHTTPError(403, "Forbidden");
    expect(err.category).toBe("credential_failure");
    expect(err.statusCode).toBe(403);
  });

  test("429 → quota_exhausted", () => {
    const err = classifyHTTPError(429, "Too Many Requests");
    expect(err.category).toBe("quota_exhausted");
    expect(err.statusCode).toBe(429);
  });

  test("500 → retryable", () => {
    const err = classifyHTTPError(500, "Internal Server Error");
    expect(err.category).toBe("retryable");
    expect(err.statusCode).toBe(500);
  });

  test("503 → retryable", () => {
    const err = classifyHTTPError(503, "Service Unavailable");
    expect(err.category).toBe("retryable");
  });

  test("400 with context_length_exceeded → context_overflow", () => {
    const err = classifyHTTPError(
      400,
      "context_length_exceeded: too many tokens",
    );
    expect(err.category).toBe("context_overflow");
  });

  test("400 with 'input is too long' → context_overflow", () => {
    const err = classifyHTTPError(400, "input is too long for this model");
    expect(err.category).toBe("context_overflow");
  });

  test("400 with generic message → fatal", () => {
    const err = classifyHTTPError(400, "Bad request");
    expect(err.category).toBe("fatal");
  });

  test("404 → fatal", () => {
    const err = classifyHTTPError(404, "Not Found");
    expect(err.category).toBe("fatal");
  });

  test("carries raw body", () => {
    const raw = { error: { message: "oops" } };
    const err = classifyHTTPError(500, "Server Error", raw);
    expect(err.raw).toBe(raw);
  });

  test("real Anthropic overflow body → context_overflow", () => {
    const err = classifyHTTPError(
      400,
      overflowFixture.error.message,
      overflowFixture,
    );
    expect(err.category).toBe("context_overflow");
  });

  test("429 with context-overflow wording → context_overflow, not quota_exhausted", () => {
    const err = classifyHTTPError(
      429,
      "Request too large: the context is too long for this model",
    );
    expect(err.category).toBe("context_overflow");
  });

  test("413 with context-overflow wording → context_overflow", () => {
    const err = classifyHTTPError(
      413,
      "413 Payload Too Large: the request exceeds the maximum context length allowed",
    );
    expect(err.category).toBe("context_overflow");
  });

  test("413 with no overflow signal → fatal", () => {
    const err = classifyHTTPError(413, "Payload Too Large");
    expect(err.category).toBe("fatal");
  });

  test("400 with 'maximum' alone → fatal", () => {
    const err = classifyHTTPError(400, "maximum retries exceeded");
    expect(err.category).toBe("fatal");
  });

  test("400 with 'context' followed by a limit word → context_overflow", () => {
    const err = classifyHTTPError(
      400,
      "context length exceeded: 210000 tokens requested, 200000 allowed",
    );
    expect(err.category).toBe("context_overflow");
  });

  test("400 with 'too many tokens' → fatal", () => {
    const err = classifyHTTPError(400, "too many tokens");
    expect(err.category).toBe("fatal");
  });

  test("429 with 'too many tokens in flight' → quota_exhausted", () => {
    const err = classifyHTTPError(429, "too many tokens in flight");
    expect(err.category).toBe("quota_exhausted");
  });

  test("429 rate limit that mentions 'context' → quota_exhausted", () => {
    const err = classifyHTTPError(
      429,
      "Rate limit exceeded for this deployment context. Retry after 20 seconds.",
    );
    expect(err.category).toBe("quota_exhausted");
  });

  test("429 OpenAI-style TPM rate limit → quota_exhausted", () => {
    const err = classifyHTTPError(
      429,
      "Rate limit reached for requests. You've used 29000 of 30000 tokens per min (TPM): Limit 30000, Used 29000, Requested 2000",
    );
    expect(err.category).toBe("quota_exhausted");
  });

  test("429 Anthropic rate_limit_error wording → quota_exhausted", () => {
    const err = classifyHTTPError(
      429,
      "Number of request tokens has exceeded your per-minute rate limit. Please try again later.",
    );
    expect(err.category).toBe("quota_exhausted");
  });

  test("400 max_tokens output-cap validation error → fatal", () => {
    const err = classifyHTTPError(
      400,
      "max_tokens: 300000 > 64000, which is the maximum allowed",
    );
    expect(err.category).toBe("fatal");
  });
});

describe("classifyNetworkError", () => {
  test("Error instance → retryable with message", () => {
    const err = classifyNetworkError(new Error("ECONNRESET"));
    expect(err.category).toBe("retryable");
    expect(err.message).toBe("ECONNRESET");
  });

  test("string → retryable", () => {
    const err = classifyNetworkError("network timeout");
    expect(err.category).toBe("retryable");
    expect(err.message).toBe("network timeout");
  });
});

describe("classifyAbortError", () => {
  test("always returns aborted category", () => {
    const err = classifyAbortError();
    expect(err.category).toBe("aborted");
  });
});

describe("classifyStreamError", () => {
  test("AbortError → aborted", () => {
    const abort = new DOMException("Aborted", "AbortError");
    const err = classifyStreamError(abort);
    expect(err.category).toBe("aborted");
  });

  test("generic Error → retryable", () => {
    const err = classifyStreamError(new Error("stream corrupted"));
    expect(err.category).toBe("retryable");
    expect(err.message).toBe("stream corrupted");
  });

  test("ProtocolMismatchError → protocol_mismatch with raw passed through", () => {
    const raw = { choices: [{ delta: { role: 42 } }] };
    const cause = new ProtocolMismatchError(
      "delta.role must be a string (was number)",
      raw,
    );
    const err = classifyStreamError(cause);
    expect(err.category).toBe("protocol_mismatch");
    expect(err.message).toBe("delta.role must be a string (was number)");
    expect(err.raw).toBe(raw);
  });
});

describe("classifyProtocolMismatch", () => {
  test("constructs a protocol_mismatch error with the given detail and raw", () => {
    const raw = { malformed: true };
    const err = classifyProtocolMismatch("bad chunk", raw);
    expect(err.category).toBe("protocol_mismatch");
    expect(err.message).toBe("bad chunk");
    expect(err.raw).toBe(raw);
  });

  test("omits raw when none is supplied", () => {
    const err = classifyProtocolMismatch("bad chunk");
    expect(err.category).toBe("protocol_mismatch");
    expect(err.message).toBe("bad chunk");
    expect("raw" in err).toBe(false);
  });
});
