// Reissuing an operation that a mail server refused for a reason that clears.
//
// Two layers need this and they classify on different error vocabularies: SMTP
// submission reads nodemailer's codes and reply codes, IMAP login reads
// imapflow's response codes. The loop, the backoff and the distinction between
// "refused permanently" and "retried until the attempts ran out" are the same
// for both, so they live here and each caller supplies its own classifier.
//
// Both callers retry an authentication failure, which the protocols label
// permanent. The justification is specific to this deployment and is written
// out at each classifier.

import { getLogger } from "@intx/log";

const logger = getLogger(["interchange", "mail-imap", "retry"]);

export type RetryPolicy = {
  /** Attempts in total, not retries after the first. */
  attempts: number;
  baseMs: number;
  sleep: (ms: number) => Promise<void>;
};

export const DEFAULT_RETRY_BASE_MS = 250;

export const realSleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

export type RetryOptions = {
  /**
   * The operation, named for the messages and logs -- "SMTP submission from
   * hub@example.com". It is read by someone holding only the log line, so it
   * names the subject and not just the verb.
   */
  what: string;
  isTransient: (cause: unknown) => boolean;
  /** The server's own classification of a refusal, for the reported message. */
  describe: (cause: unknown) => string;
  policy: RetryPolicy;
};

/**
 * Run `run` until it resolves, it is refused permanently, or the attempts run
 * out. Each attempt calls `run` afresh, so a caller whose client cannot be
 * reused -- imapflow's cannot -- builds a new one inside it.
 */
export async function retrying<T>(
  run: () => Promise<T>,
  options: RetryOptions,
): Promise<T> {
  const { what, isTransient, describe, policy } = options;

  if (policy.attempts < 1) {
    throw new Error(
      `attempts must be at least 1, got ${String(policy.attempts)}`,
    );
  }

  for (let attempt = 1; ; attempt += 1) {
    try {
      const value = await run();
      if (attempt > 1) {
        logger.info`${what} succeeded on attempt ${String(attempt)}`;
      }
      return value;
    } catch (cause) {
      const transient = isTransient(cause);
      if (!transient || attempt >= policy.attempts) {
        // Why it stopped belongs on the error: a caller that reports this
        // upward cannot otherwise tell a permanent refusal from an exhausted
        // retry sequence, and those warrant different responses.
        throw new Error(
          transient
            ? `${what} still failing after ${String(attempt)} attempts (${describe(cause)})`
            : `${what} refused permanently (${describe(cause)})`,
          { cause },
        );
      }
      const delayMs = policy.baseMs * 2 ** (attempt - 1);
      logger.warn`${what} failed transiently (${describe(cause)}); retrying in ${String(delayMs)}ms`;
      await policy.sleep(delayMs);
    }
  }
}
