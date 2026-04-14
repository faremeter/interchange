import { describe, test, expect } from "bun:test";
import {
  classifyHTTPError,
  classifyNetworkError,
  classifyAbortError,
  classifyStreamError,
} from "./errors";

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
});
