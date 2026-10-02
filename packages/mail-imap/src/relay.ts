// SMTP submission for a host that already holds assembled message bytes.
//
// The hub composes and signs its own mail -- a workflow trigger, a session
// conversation message -- and today hands the bytes to the sidecar over the
// control socket. This relay submits those same bytes to an SMTP server
// instead, so the recipient receives them through its own mailbox.
//
// It takes assembled bytes and an envelope, and never composes: the hub's
// signature is computed over the bytes it built, so anything that re-serialized
// them would void it.

import { getLogger } from "@intx/log";
import nodemailer from "nodemailer";

import {
  DEFAULT_RETRY_BASE_MS,
  realSleep,
  retrying,
  type RetryPolicy,
} from "./retry";

const logger = getLogger(["interchange", "mail-imap", "relay"]);

/** Attempts in total, not retries after the first. */
export const DEFAULT_SUBMIT_ATTEMPTS = 4;

export type SmtpRelayConfig = {
  host: string;
  port: number;
  secure: boolean;
  auth?: { user: string; pass: string };
  /** Permit an untrusted certificate. For a local test server only. */
  ignoreTLS?: boolean;
  /**
   * How many times to submit before giving up. A caller that holds an HTTP
   * request open across the retries wants this small: the delays are bounded by
   * `retryBaseMs` doubling, so the whole sequence is a second or two.
   */
  submitAttempts?: number;
  retryBaseMs?: number;
  /** Replaced in tests, so a retry sequence costs no wall-clock. */
  sleep?: (ms: number) => Promise<void>;
};

export type SmtpRelay = {
  /**
   * Submit `raw` verbatim, with `from` as the envelope sender and `recipients`
   * as the envelope recipients. Rejects when the server refused the submission.
   *
   * Resolution means the relay accepted responsibility for delivery, not that
   * delivery happened. A caller that needs to know the message reached a mailbox
   * has to observe the mailbox.
   *
   * A transient refusal is resubmitted -- see `isTransientSubmitFailure` for
   * what counts and why. Every attempt submits the same bytes, so a resubmission
   * of a message the server did accept arrives carrying its original Message-ID,
   * which the recipient's arrival dedup absorbs.
   */
  submit(raw: Uint8Array, from: string, recipients: string[]): Promise<void>;
  close(): void;
};

/** The subset of a nodemailer failure this module classifies on. */
type SubmitFailure = {
  code?: unknown;
  responseCode?: unknown;
  command?: unknown;
};

function failureFields(cause: unknown): SubmitFailure {
  if (typeof cause !== "object" || cause === null) return {};
  const { code, responseCode, command } = cause as SubmitFailure;
  return { code, responseCode, command };
}

// Connection-establishment failures. The server never saw the message on any of
// these, so resubmitting cannot duplicate it.
const TRANSIENT_CODES = new Set([
  "ECONNECTION",
  "ESOCKET",
  "ETIMEDOUT",
  "EDNS",
]);

/**
 * Whether a refused submission is worth reissuing.
 *
 * Two of the three cases are uncontroversial: a connection that never came up,
 * and a 4xx, which RFC 5321 section 4.2.1 defines as a transient negative reply
 * whose command may be retried.
 *
 * The third is `EAUTH`, and it is a deliberate departure. SMTP gives
 * authentication failure a 5xx code (535), which the same section defines as
 * permanent and tells the client not to repeat verbatim. Taking that at face
 * value is wrong for this deployment: the hub provisions a mailbox inside the
 * deploy it is serving, provisioning reloads the mail server, and the reload
 * drops the hub's own authenticated submission session. The 535 that follows
 * describes a server mid-reload, not a bad password -- and it clears in under a
 * second.
 *
 * The cost of being wrong in each direction decides it. Retrying a genuinely
 * bad password wastes a few submissions and then reports the same failure. Not
 * retrying a reload turns every deploy into a coin flip, because the refusal
 * surfaces to the caller as `409 deployment_unreachable` on a deployment that
 * is in fact fine.
 */
export function isTransientSubmitFailure(cause: unknown): boolean {
  const { code, responseCode } = failureFields(cause);

  if (code === "EAUTH") return true;
  if (typeof code === "string" && TRANSIENT_CODES.has(code)) return true;
  if (
    typeof responseCode === "number" &&
    responseCode >= 400 &&
    responseCode < 500
  ) {
    return true;
  }
  return false;
}

export function describeSubmitFailure(cause: unknown): string {
  const { code, responseCode, command } = failureFields(cause);
  const parts = [
    typeof code === "string" ? code : undefined,
    typeof responseCode === "number" ? String(responseCode) : undefined,
    typeof command === "string" ? `on ${command}` : undefined,
  ].filter((part): part is string => part !== undefined);
  return parts.length > 0 ? parts.join(" ") : "unclassified";
}

/**
 * Submit through `send` until it resolves, it is refused permanently, or the
 * attempts run out. Split from the relay so the policy can be exercised against
 * an injected sequence of failures rather than a server coaxed into producing
 * them.
 *
 * `from` names the submission in the messages and logs, and is the envelope
 * sender at the only call site.
 */
export async function submitWithRetry(
  send: () => Promise<void>,
  from: string,
  policy: RetryPolicy,
): Promise<void> {
  await retrying(send, {
    what: `SMTP submission from ${from}`,
    isTransient: isTransientSubmitFailure,
    describe: describeSubmitFailure,
    policy,
  });
}

export function createSmtpRelay(config: SmtpRelayConfig): SmtpRelay {
  const transport = nodemailer.createTransport({
    host: config.host,
    port: config.port,
    secure: config.secure,
    ...(config.auth !== undefined ? { auth: config.auth } : {}),
    ...(config.ignoreTLS === true
      ? { ignoreTLS: true, tls: { rejectUnauthorized: false } }
      : {}),
  });

  const policy: RetryPolicy = {
    attempts: config.submitAttempts ?? DEFAULT_SUBMIT_ATTEMPTS,
    baseMs: config.retryBaseMs ?? DEFAULT_RETRY_BASE_MS,
    sleep: config.sleep ?? realSleep,
  };

  return {
    async submit(raw, from, recipients) {
      if (recipients.length === 0) {
        throw new Error("SMTP submission needs at least one recipient");
      }

      await submitWithRetry(
        async () => {
          await transport.sendMail({
            envelope: { from, to: recipients },
            raw: Buffer.from(raw),
          });
        },
        from,
        policy,
      );
      logger.debug`relayed ${String(raw.byteLength)} bytes from ${from} to ${recipients.join(", ")}`;
    },
    close() {
      transport.close();
    },
  };
}
