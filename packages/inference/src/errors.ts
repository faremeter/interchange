import type { InferenceError } from "@intx/types/runtime";

export type { InferenceError };

export function classifyHTTPError(
  statusCode: number,
  message: string,
  raw?: unknown,
  retryAfterMs?: number,
): InferenceError {
  if (statusCode === 401 || statusCode === 403) {
    return { category: "credential_failure", message, statusCode, raw };
  }

  if (
    (statusCode === 400 || statusCode === 413 || statusCode === 429) &&
    isContextOverflowMessage(message)
  ) {
    return { category: "context_overflow", message, statusCode, raw };
  }

  if (statusCode === 429) {
    return {
      category: "quota_exhausted",
      message,
      statusCode,
      ...(retryAfterMs !== undefined ? { retryAfterMs } : {}),
      raw,
    };
  }

  if (statusCode >= 500 && statusCode < 600) {
    return { category: "retryable", message, statusCode, raw };
  }

  return { category: "fatal", message, statusCode, raw };
}

export function classifyNetworkError(cause: unknown): InferenceError {
  const message = cause instanceof Error ? cause.message : String(cause);
  return { category: "retryable", message, raw: cause };
}

export function classifyAbortError(): InferenceError {
  return { category: "aborted", message: "inference aborted" };
}

export function classifyTimeoutError(
  kind: "inactivity" | "total",
  thresholdMs: number,
): InferenceError {
  const message =
    kind === "inactivity"
      ? `inference call exceeded inactivity timeout (${String(thresholdMs)} ms with no events from the provider)`
      : `inference call exceeded total timeout (${String(thresholdMs)} ms wall-clock)`;
  return { category: "timeout", message };
}

/**
 * The one throw type a response parser is permitted to raise. See the
 * `ResponseParser` contract on `adapter.ts` for full semantics. `raw`
 * carries the offending bytes or parsed object so operators can
 * inspect what came over the wire.
 */
export class ProtocolMismatchError extends Error {
  readonly raw: unknown;
  constructor(detail: string, raw?: unknown) {
    super(detail);
    this.name = "ProtocolMismatchError";
    this.raw = raw;
  }
}

export function classifyProtocolMismatch(
  detail: string,
  raw?: unknown,
): InferenceError {
  return {
    category: "protocol_mismatch",
    message: detail,
    ...(raw !== undefined ? { raw } : {}),
  };
}

export function classifyStreamError(cause: unknown): InferenceError {
  if (isAbortError(cause)) {
    return classifyAbortError();
  }
  if (cause instanceof ProtocolMismatchError) {
    return classifyProtocolMismatch(cause.message, cause.raw);
  }
  const message = cause instanceof Error ? cause.message : String(cause);
  return { category: "retryable", message, raw: cause };
}

// Vendor overflow wording is free text; no cross-provider error-code taxonomy
// exists to switch on instead. Each pattern is anchored to context/prompt-size
// vocabulary, not to a loose word like "maximum" or "tokens" alone, so a
// rate-limit or concurrency message that happens to mention tokens does not
// match.
const CONTEXT_OVERFLOW_PATTERNS: readonly RegExp[] = [
  /context_length_exceeded/i,
  /\bprompt is too long\b/i,
  /\binput is too long\b/i,
  /\bmaximum context\b/i,
  // "context" plus a size/limit word within the same message, e.g.
  // "the context is too long for this model" or "maximum context length".
  /\bcontext\b[^.]{0,40}\b(too long|exceeds?|exceeded|maximum|length)\b/i,
];

function isContextOverflowMessage(message: string): boolean {
  return CONTEXT_OVERFLOW_PATTERNS.some((pattern) => pattern.test(message));
}

function isAbortError(value: unknown): boolean {
  return (
    value instanceof Error &&
    (value.name === "AbortError" ||
      value.message === "The user aborted a request.")
  );
}
