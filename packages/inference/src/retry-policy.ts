// The default per-call mechanical retry policy. See `RetryPolicy` /
// `RetrySituation` / `RetryDecision` in `@intx/types/runtime` for the
// public contract. This module ships the opinionated defaults the
// harness substitutes when `InferenceOptions.retryPolicy` is omitted.
//
// The defaults are deliberately conservative: enough to absorb the
// transient-flake surface (TCP resets, 5xx, rate-limit jitter, half-
// streamed connection drops) without masking a genuinely persistent
// failure under a retry loop a human would never notice.

import type {
  RetryPolicy,
  RetrySituation,
  RetryDecision,
} from "@intx/types/runtime";

const MAX_ATTEMPTS = 3;
// Indexed by the failed attempt number (1-indexed): the delay BEFORE
// the attempt-after-this-one starts. Length must be `MAX_ATTEMPTS - 1`
// because after the final attempt fails the policy aborts. Drives the
// `retryable` and `timeout` schedules.
const RETRYABLE_BACKOFF_BY_FAILED_ATTEMPT_MS: readonly number[] = [500, 1000];
const QUOTA_DEFAULT_DELAY_MS = 1000;

/**
 * The default retry policy bundled with `@intx/inference`. Behaviour by
 * `InferenceError.category`:
 *
 * - `credential_failure`, `context_overflow`, `fatal`, `aborted`,
 *   `protocol_mismatch` — never retry. These categories describe a
 *   deterministic per-call failure that re-issuing the identical
 *   request cannot resolve: bad credentials stay bad, a too-large
 *   context stays too large, a caller-driven abort is intentional,
 *   and a wire-shape mismatch will repeat on the next response.
 * - `retryable`, `timeout` — up to 3 attempts total. 500ms before
 *   attempt 2, then 1000ms before attempt 3. Exponential rather than
 *   constant so a server taking longer than usual to recover gets a
 *   slightly larger window each time without compounding into a long
 *   tail.
 * - `quota_exhausted` — up to 3 attempts total. The delay is taken
 *   from `error.retryAfterMs` when the provider returned one (the
 *   server told us when it would be ready); otherwise a flat
 *   `1000`ms baseline. The baseline does NOT grow across attempts —
 *   if 1s isn't long enough for a rate limit to clear, exponential
 *   backoff on top of the provider's own pacing instructions is more
 *   likely to mask a config problem than help. Operators who need
 *   exponential pacing for rate limits should supply a custom policy.
 *
 * The 3-attempt cap is the same across every retryable category: a
 * single transient flake is plausible, two is rare, and a third
 * failure across the backoff schedule is a real signal that the call
 * is not going to succeed on its own.
 */
export function createDefaultRetryPolicy(): RetryPolicy {
  return (situation: RetrySituation): RetryDecision => {
    const { error, attempt } = situation;

    switch (error.category) {
      case "credential_failure":
      case "context_overflow":
      case "fatal":
      case "aborted":
      case "protocol_mismatch":
        return { kind: "abort" };

      case "retryable":
      case "timeout": {
        if (attempt >= MAX_ATTEMPTS) return { kind: "abort" };
        const delayMs = RETRYABLE_BACKOFF_BY_FAILED_ATTEMPT_MS[attempt - 1];
        if (delayMs === undefined) {
          // Unreachable in practice given the `attempt >= MAX_ATTEMPTS`
          // guard above, but the explicit narrowing keeps the schedule
          // table and the cap from drifting silently if anyone bumps
          // `MAX_ATTEMPTS` without extending the table.
          return { kind: "abort" };
        }
        return { kind: "retry", delayMs };
      }

      case "quota_exhausted":
        if (attempt >= MAX_ATTEMPTS) return { kind: "abort" };
        return {
          kind: "retry",
          delayMs: error.retryAfterMs ?? QUOTA_DEFAULT_DELAY_MS,
        };

      default: {
        // Exhaustiveness: if a new InferenceError.category lands
        // without a clause here, the never-assignment fails at
        // compile time rather than silently returning undefined
        // from the policy callback.
        const exhaustive: never = error.category;
        throw new Error(
          `createDefaultRetryPolicy: unhandled error category ${String(exhaustive)}`,
        );
      }
    }
  };
}
