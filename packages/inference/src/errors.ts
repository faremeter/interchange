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

  if (statusCode === 429) {
    return {
      category: "quota_exhausted",
      message,
      statusCode,
      ...(retryAfterMs !== undefined ? { retryAfterMs } : {}),
      raw,
    };
  }

  if (statusCode === 400) {
    // Context-overflow manifests as a 400 with a provider-specific message.
    // Check for known patterns before falling through to fatal.
    if (isContextOverflowMessage(message)) {
      return { category: "context_overflow", message, statusCode, raw };
    }
    return { category: "fatal", message, statusCode, raw };
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

function isContextOverflowMessage(message: string): boolean {
  const lower = message.toLowerCase();
  return (
    lower.includes("context_length_exceeded") ||
    lower.includes("context length") ||
    lower.includes("too many tokens") ||
    lower.includes("maximum context") ||
    lower.includes("input is too long")
  );
}

function isAbortError(value: unknown): boolean {
  return (
    value instanceof Error &&
    (value.name === "AbortError" ||
      value.message === "The user aborted a request.")
  );
}
