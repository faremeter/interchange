// `MessageTransport` over a real SMTP submission relay and a real IMAP mailbox.
//
// Outbound composes and signs through `composeOutbound` -- the same function the
// in-memory transport uses -- then submits the resulting bytes verbatim over
// SMTP. Inbound issues real IMAP commands and runs the `@intx/mailbox`
// projections over what comes back, so a message reads the same here as it does
// over the in-memory and substrate backings.
//
// Two connections, not one. IMAP IDLE monitors only the selected mailbox and
// occupies the connection for as long as it runs, so a transport that both
// serves commands and watches needs a second connection for the watch. The
// command connection is opened on `start()`; the watch connection is opened
// there too, because `MessageTransport.watch` returns synchronously and so
// cannot wait for a connect.
//
// Every method of `MessageTransport` is implemented here. The interface asks
// only for what a server has to be able to do -- base IMAP4rev1 SEARCH and
// FETCH, IDLE, and arbitrary keywords -- so there is nothing it declares that
// this backing has to refuse.

import { getLogger } from "@intx/log";
import {
  composeOutbound,
  executeSearch,
  fetchFull as projectFull,
  fetchHeaders as projectHeaders,
  fetchPart as projectPart,
  type StoredEnvelope,
} from "@intx/mailbox";
import { buildMessageHeaders, parseHeaderSection } from "@intx/mime";
import { DEFAULT_SUBMIT_ATTEMPTS, submitWithRetry } from "./relay";
import { DEFAULT_RETRY_BASE_MS, realSleep, retrying } from "./retry";
import type {
  CryptoProvider,
  InboundMessage,
  MailboxEvent,
  MessageHeaders,
  MessagePart,
  MessageRef,
  MessageSentHandler,
  MessageTransport,
  OutboundMessage,
  SearchQuery,
  SendReceipt,
  Unsubscribe,
} from "@intx/types/runtime";
import { MessageTransportError } from "@intx/types/runtime";
import { ImapFlow, type MailboxLockObject } from "imapflow";
import nodemailer from "nodemailer";

import {
  createFetchedStore,
  type FetchedMessage,
  type MailboxCounters,
} from "./fetched-store";
import { translate } from "./search-criteria";

const logger = getLogger(["interchange", "mail-imap"]);

export type ImapEndpoint = {
  host: string;
  port: number;
  secure: boolean;
  auth: { user: string; pass: string };
};

export type SmtpEndpoint = {
  host: string;
  port: number;
  secure: boolean;
  auth?: { user: string; pass: string };
  /** Permit an untrusted certificate. For a local test server only. */
  ignoreTLS?: boolean;
};

export type ImapTransportConfig = {
  /** The address this transport sends as and whose mailbox it reads. */
  address: string;
  /** Signs outbound mail as `address`. */
  crypto: CryptoProvider;
  /**
   * Resolves a sender address to the key that verifies its mail, or `undefined`
   * when no key is held -- in which case `fetchFull` reports the signature
   * status as `unknown`.
   */
  getCrypto: (fromAddress: string) => CryptoProvider | undefined;
  imap: ImapEndpoint;
  smtp: SmtpEndpoint;
  /**
   * The mailbox the watch connection selects at `start()`, so a `watch` on it
   * is armed before any caller can install one. Defaults to `INBOX`, the only
   * mailbox an agent watches.
   *
   * IMAP IDLE monitors the SELECTED mailbox, so one connection arms exactly one
   * mailbox. A `watch` on any other name is still accepted, but it selects
   * after the call has already returned, and an arrival inside that window is
   * missed -- see the note on `watch`.
   */
  watchMailbox?: string;
  /**
   * How long the WATCH connection stays inactive before it enters IDLE.
   *
   * This is the dominant term in notification latency, and the client's default
   * is 15 seconds -- so a `mail_wait` on an otherwise idle mailbox learns about
   * a message up to 15 seconds after it arrived. That default is sized for a
   * connection that also serves commands, where entering IDLE costs a
   * break-and-resume round trip on the next one. This connection serves no
   * commands but the ingress fetches, so it can idle almost at once.
   */
  watchIdleDelayMs?: number;
  /**
   * Observes each successful submission with the bytes that went out.
   *
   * `send` returns only a `SendReceipt`, which carries no bytes, so a host that
   * needs the wire message for an audit trail cannot reconstruct it from the
   * return value -- re-composing would produce a different Message-ID and a
   * different signature. This hands over the exact bytes submitted.
   *
   * Awaited before `send` resolves, so a caller that observes the receipt knows
   * the hook has run. It must not throw: the message is already submitted, and
   * a throw here would be reported to the sender as a send failure.
   */
  onSent?: MessageSentHandler;
  /**
   * Fired once when either connection closes for a reason this transport did
   * not ask for.
   *
   * `ImapFlow` holds a single connection and cannot reconnect -- a closed
   * instance stays closed -- so a transport whose socket drops is finished, and
   * a deployment behind it stops receiving mail with nothing to say about it.
   * That is not a rare condition here: creating a mailbox on the server reloads
   * it and terminates every live session, so it happens on every deployment.
   *
   * The owner of this hook recovers by discarding the transport and building a
   * new one. It never fires for a close this transport initiated through
   * `close()`.
   */
  onClose?: (reason: string) => void;
  /**
   * How many times `start()` logs in before giving up.
   *
   * More than one because a login can be refused for a reason that clears --
   * see `isTransientLoginFailure`. A caller that holds a deploy open across the
   * retries wants this small: the delays double from `loginRetryBaseMs`, so the
   * whole sequence is a second or two.
   */
  loginAttempts?: number;
  loginRetryBaseMs?: number;
  /**
   * How many times `send` submits before giving up. More than one for the same
   * reason -- see `isTransientSubmitFailure`. A refused submission reaches the
   * agent as a failed step and fails the whole run, so the retry is what keeps
   * a server reload from costing a deployment its run.
   */
  submitAttempts?: number;
  submitRetryBaseMs?: number;
  /** Replaced in tests, so a retry sequence costs no wall-clock. */
  sleep?: (ms: number) => Promise<void>;
};

/** Attempts in total, not retries after the first. */
export const DEFAULT_LOGIN_ATTEMPTS = 4;

/**
 * Default delay before the watch connection enters IDLE. Short, because the
 * connection has nothing else to do and every millisecond of it is added to the
 * time an agent waits for mail. Not zero: a fetch the ingress issues would
 * otherwise race an IDLE on every arrival.
 */
const DEFAULT_WATCH_IDLE_DELAY_MS = 150;

export type ImapTransport = MessageTransport & {
  /** Open the command and watch connections. Call before any other method. */
  start(): Promise<void>;
  /** Close both connections. */
  close(): Promise<void>;
};

/**
 * Map a client or server failure onto the transport's condition vocabulary.
 *
 * The vocabulary is RFC 5530's, and it has no name for the condition a network
 * client meets most often: a transient failure whose outcome is unknown (the
 * connection dropped, the server answered 4xx, a command timed out). `CANNOT`
 * is wrong because it tells the caller not to retry, so such a failure is
 * reported as `SERVERBUG`, which the mail tools map to a code that leaves the
 * outcome unknown. The name misdescribes it -- the fault is upstream, not ours.
 */
function transportError(cause: unknown, what: string): MessageTransportError {
  const message = cause instanceof Error ? cause.message : String(cause);
  const code =
    cause !== null && typeof cause === "object" && "serverResponseCode" in cause
      ? String(cause.serverResponseCode)
      : "";
  if (
    code === "NONEXISTENT" ||
    /does not exist|\[NONEXISTENT\]/i.test(message)
  ) {
    return new MessageTransportError("NONEXISTENT", `${what}: ${message}`);
  }
  return new MessageTransportError("SERVERBUG", `${what}: ${message}`);
}

/**
 * Narrow imapflow's SEARCH result to the uid list.
 *
 * imapflow reports a SEARCH it could not run by RETURNING `false` rather than
 * rejecting, and an empty match set as `[]`. Those are different outcomes and
 * must not collapse: treating `false` as "nothing matched" reports a failed
 * command as an empty mailbox, which a caller cannot tell from the truth.
 */
function requireUids(
  result: number[] | false | undefined,
  what: string,
): number[] {
  if (result === false || result === undefined) {
    throw new MessageTransportError(
      "SERVERBUG",
      `${what} did not run; the server returned no search result`,
    );
  }
  return result;
}

/** Build the envelope the mailbox model stores, from the message's own bytes. */
function envelopeFromRaw(raw: Uint8Array): StoredEnvelope {
  const { headers } = parseHeaderSection(raw);
  const parsed = buildMessageHeaders(headers);
  const date = parsed.date === undefined ? undefined : new Date(parsed.date);
  return {
    messageId: parsed.messageId ?? "",
    from: parsed.from,
    to: parsed.to,
    subject: parsed.subject ?? "",
    date:
      date !== undefined && !Number.isNaN(date.getTime()) ? date : undefined,
    inReplyTo: parsed.inReplyTo,
    references: parsed.references ?? [],
    interchangeType: parsed.interchangeType,
    interchangeCorrelationId: parsed.interchangeCorrelationId,
  };
}

/** The subset of an imapflow or socket failure `isTransientLoginFailure` reads. */
type LoginFailure = {
  code?: unknown;
  authenticationFailed?: unknown;
  serverResponseCode?: unknown;
  responseText?: unknown;
};

function loginFailureFields(cause: unknown): LoginFailure {
  if (typeof cause !== "object" || cause === null) return {};
  const { code, authenticationFailed, serverResponseCode, responseText } =
    cause as LoginFailure;
  return { code, authenticationFailed, serverResponseCode, responseText };
}

// Socket-level failures. The session never opened on any of these.
const TRANSIENT_SOCKET_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "ETIMEDOUT",
  "EPIPE",
  "EHOSTUNREACH",
  "ENOTFOUND",
]);

// RFC 5530 response codes that name a server which is temporarily unable to
// serve this session, as distinct from one refusing the credentials outright.
const TRANSIENT_RESPONSE_CODES = new Set(["UNAVAILABLE", "INUSE", "SERVERBUG"]);

/**
 * Whether a refused IMAP login is worth reissuing.
 *
 * `AUTHENTICATIONFAILED` is retried, and that is the whole point of this
 * predicate. RFC 3501 gives a rejected login a tagged `NO`, which is final for
 * that command, and the honest reading is that the password is wrong. It is the
 * wrong reading here for the same reason the relay retries a 535: creating a
 * mailbox reloads the mail server, a reload refuses logins for a moment, and
 * the hub creates a mailbox inside the deploy whose sidecar is about to log in.
 * The server answers `AUTHENTICATIONFAILED` to an account whose password is
 * correct and which works a moment later.
 *
 * Retrying a password that really is wrong costs a few logins and then reports
 * the same refusal. Not retrying leaves a deployment with no mailbox
 * connection, which is silent -- mail accumulates on the server and the run
 * never starts -- so the asymmetry is decisive.
 */
export function isTransientLoginFailure(cause: unknown): boolean {
  const { code, authenticationFailed, serverResponseCode } =
    loginFailureFields(cause);

  if (authenticationFailed === true) return true;
  if (
    typeof serverResponseCode === "string" &&
    (serverResponseCode === "AUTHENTICATIONFAILED" ||
      TRANSIENT_RESPONSE_CODES.has(serverResponseCode))
  ) {
    return true;
  }
  if (typeof code === "string" && TRANSIENT_SOCKET_CODES.has(code)) return true;
  return false;
}

export function describeLoginFailure(cause: unknown): string {
  const { code, serverResponseCode, responseText } = loginFailureFields(cause);
  const parts = [
    typeof serverResponseCode === "string" ? serverResponseCode : undefined,
    typeof code === "string" ? code : undefined,
    typeof responseText === "string" ? responseText : undefined,
  ].filter((part): part is string => part !== undefined);
  return parts.length > 0 ? parts.join(" ") : "unclassified";
}

export function createImapTransport(
  config: ImapTransportConfig,
): ImapTransport {
  const buildCommandClient = () =>
    new ImapFlow({
      host: config.imap.host,
      port: config.imap.port,
      secure: config.imap.secure,
      auth: config.imap.auth,
      logger: false,
      // Nothing watches this connection, so an IDLE on it delivers
      // notifications to no one while costing a break-and-resume round trip on
      // the next command. Turning it off does NOT keep this connection's view of
      // the mailbox current -- that is what the NOOP in `withMailbox` is for.
      disableAutoIdle: true,
    });
  const buildWatchClient = () =>
    new ImapFlow({
      host: config.imap.host,
      port: config.imap.port,
      secure: config.imap.secure,
      auth: config.imap.auth,
      logger: false,
      autoIdleDelay: config.watchIdleDelayMs ?? DEFAULT_WATCH_IDLE_DELAY_MS,
    });

  // Replaced, not reconnected: `ImapFlow` cannot reopen a closed instance, so a
  // login retry needs a new pair. Every method reads these bindings when it
  // runs rather than capturing them, so a replacement is visible to all of them.
  let commandClient = buildCommandClient();
  let watchClient = buildWatchClient();

  const smtp = nodemailer.createTransport({
    host: config.smtp.host,
    port: config.smtp.port,
    secure: config.smtp.secure,
    ...(config.smtp.auth !== undefined ? { auth: config.smtp.auth } : {}),
    ...(config.smtp.ignoreTLS === true
      ? { ignoreTLS: true, tls: { rejectUnauthorized: false } }
      : {}),
  });

  // mailbox name -> the callbacks watching it, and the highest uid already
  // reported. The watch connection selects one mailbox at a time, matching the
  // IMAP IDLE constraint; a second watched mailbox would need a third
  // connection.
  // Set by `close()` so the unexpected-close hook stays silent for a shutdown
  // this transport asked for. Either client closing is reported once and only
  // once: a dropped socket usually closes both, and the owner needs one signal
  // to rebuild on, not two.
  let closing = false;
  let closeReported = false;

  function reportUnexpectedClose(which: string): void {
    if (closing || closeReported) return;
    closeReported = true;
    logger.warn`${which} connection for ${config.address} closed unexpectedly; the transport cannot reconnect and must be rebuilt`;
    config.onClose?.(`${which} connection closed`);
  }

  const watchers = new Map<
    string,
    { callbacks: Set<(event: MailboxEvent) => void>; lastUid: number }
  >();
  const armedMailbox = config.watchMailbox ?? "INBOX";
  /** Highest uid present in `armedMailbox` when it was selected at `start()`. */
  let armedLastUid = 0;
  let selectedForWatch: string | undefined;

  // Set when the WATCH connection has seen an arrival the COMMAND connection has
  // not been told about yet.
  //
  // A message enters a client's view of a selected mailbox only once the server
  // has sent that client an untagged EXISTS. Dovecot answers the first command
  // issued after an arrival against the mailbox as the client still knows it, and
  // sends the EXISTS alongside that answer -- so a SEARCH run at that moment
  // returns nothing and the NEXT one returns the message. Splitting watch and
  // command across two connections makes this reachable by construction: the
  // watch connection learns about the arrival and the command connection does
  // not. Clearing the flag with a NOOP solicits the pending untagged responses on
  // the command connection, which is what NOOP is specified for, so the command
  // that follows is evaluated against a current view.
  let arrivalUnseenByCommandConnection = false;

  async function withMailbox<T>(
    mailbox: string,
    what: string,
    body: (lock: MailboxLockObject) => Promise<T>,
  ): Promise<T> {
    let lock: MailboxLockObject;
    try {
      lock = await commandClient.getMailboxLock(mailbox);
    } catch (cause) {
      throw transportError(cause, `${what} on mailbox "${mailbox}"`);
    }
    try {
      if (arrivalUnseenByCommandConnection) {
        // Clear it before the NOOP, not after: an arrival that lands DURING the
        // NOOP must leave the flag set for the next command rather than be
        // cleared by this one.
        arrivalUnseenByCommandConnection = false;
        await commandClient.noop();
      }
      return await body(lock);
    } catch (cause) {
      if (cause instanceof MessageTransportError) throw cause;
      throw transportError(cause, `${what} on mailbox "${mailbox}"`);
    } finally {
      lock.release();
    }
  }

  function countersFrom(lock: MailboxLockObject): MailboxCounters {
    const box = commandClient.mailbox;
    if (box === false) {
      throw new MessageTransportError(
        "SERVERBUG",
        `no mailbox is selected on the command connection while holding a lock on "${lock.path}"`,
      );
    }
    return {
      uidValidity: Number(box.uidValidity),
      uidNext: box.uidNext ?? 0,
      highestModSeq: Number(box.highestModseq ?? 0n),
    };
  }

  /** FETCH the given uids with their flags, modseq, and verbatim source. */
  async function fetchMessages(uids: number[]): Promise<FetchedMessage[]> {
    if (uids.length === 0) return [];
    const out: FetchedMessage[] = [];
    for await (const msg of commandClient.fetch(
      uids.join(","),
      { uid: true, flags: true, source: true },
      { uid: true },
    )) {
      if (msg.source === undefined) {
        throw new MessageTransportError(
          "SERVERBUG",
          `FETCH of uid ${String(msg.uid)} returned no message source`,
        );
      }
      const raw = new Uint8Array(msg.source);
      out.push({
        uid: msg.uid,
        modseq: Number(msg.modseq ?? 0n),
        flags: msg.flags ?? new Set<string>(),
        envelope: envelopeFromRaw(raw),
        raw,
      });
    }
    return out;
  }

  /** FETCH one uid, or reject when the mailbox does not hold it. */
  async function fetchOneOrThrow(
    ref: MessageRef,
    what: string,
  ): Promise<FetchedMessage> {
    const [found] = await fetchMessages([ref.uid]);
    if (found === undefined) {
      throw new MessageTransportError(
        "NONEXISTENT",
        `${what}: mailbox "${ref.mailbox}" holds no message with uid ${String(ref.uid)}`,
      );
    }
    return found;
  }

  /** Re-emit IDLE notifications as `MailboxEvent`s on the watch connection. */
  function wireWatchEvents(): void {
    watchClient.on("exists", (data: { path: string; count: number }) => {
      const entry = watchers.get(data.path);
      if (entry === undefined) return;
      void (async () => {
        try {
          // IMAP's EXISTS carries a COUNT, not a uid, so the arriving message's
          // uid and headers have to be fetched. MESSAGE.md requires the `exists`
          // event to carry both, and this is the round trip that cost buys.
          const fetched: { uid: number; headers: MessageHeaders }[] = [];
          for await (const msg of watchClient.fetch(
            `${String(entry.lastUid + 1)}:*`,
            { uid: true, headers: true },
            { uid: true },
          )) {
            if (msg.uid <= entry.lastUid) continue;
            const headerBytes =
              msg.headers === undefined
                ? new Uint8Array()
                : new Uint8Array(msg.headers);
            const { headers } = parseHeaderSection(
              // `parseHeaderSection` wants the blank line that ends the section.
              concatCRLF(headerBytes),
            );
            fetched.push({
              uid: msg.uid,
              headers: buildMessageHeaders(headers),
            });
          }
          logger.debug`watch dispatch on ${data.path}: ${String(fetched.length)} arrival(s) to ${String(entry.callbacks.size)} callback(s)`;
          // Mark BEFORE notifying: a callback that searches synchronously must
          // find the flag already set, or its search runs against a view that
          // does not hold the arrival it was just told about.
          if (fetched.length > 0) arrivalUnseenByCommandConnection = true;
          for (const item of fetched) {
            entry.lastUid = Math.max(entry.lastUid, item.uid);
            for (const callback of entry.callbacks) {
              callback({
                type: "exists",
                uid: item.uid,
                headers: item.headers,
              });
            }
          }
        } catch (cause) {
          logger.warn`watch on ${data.path} could not resolve an arrival: ${cause instanceof Error ? cause.message : String(cause)}`;
        }
      })();
    });

    watchClient.on(
      "flags",
      (data: { path: string; uid?: number; flags: Set<string> }) => {
        const entry = watchers.get(data.path);
        if (entry === undefined || data.uid === undefined) return;
        for (const callback of entry.callbacks) {
          callback({
            type: "flagsChanged",
            uid: data.uid,
            flags: [...data.flags],
          });
        }
      },
    );

    watchClient.on("expunge", (data: { path: string; seq: number }) => {
      const entry = watchers.get(data.path);
      if (entry === undefined) return;
      // An untagged EXPUNGE names a SEQUENCE NUMBER. Only QRESYNC's VANISHED
      // names uids, so without it the uid this event must carry cannot be
      // recovered after the message is already gone. Logged rather than guessed.
      logger.warn`watch on ${data.path} saw an expunge of sequence ${String(data.seq)}; plain IMAP does not report its uid, so no expunged event is emitted`;
    });
  }

  return {
    async start(): Promise<void> {
      await retrying(
        async () => {
          // A fresh pair per attempt. The previous attempt's instances are
          // closed or half-open and neither can be reused.
          commandClient = buildCommandClient();
          watchClient = buildWatchClient();
          try {
            await commandClient.connect();
            await watchClient.connect();
          } catch (cause) {
            // The command connection may have opened before the watch
            // connection was refused. Closing both leaves no session behind on
            // the server holding a mailbox this transport no longer reads.
            await Promise.allSettled([
              commandClient.logout(),
              watchClient.logout(),
            ]);
            throw cause;
          }
        },
        {
          what: `IMAP login for ${config.address}`,
          isTransient: isTransientLoginFailure,
          describe: describeLoginFailure,
          policy: {
            attempts: config.loginAttempts ?? DEFAULT_LOGIN_ATTEMPTS,
            baseMs: config.loginRetryBaseMs ?? DEFAULT_RETRY_BASE_MS,
            sleep: config.sleep ?? realSleep,
          },
        },
      );
      wireWatchEvents();
      commandClient.on("close", () => {
        reportUnexpectedClose("command");
      });
      watchClient.on("close", () => {
        reportUnexpectedClose("watch");
      });
      // Arm the watch mailbox here, not on the first `watch` call. `watch`
      // returns synchronously, so a SELECT issued from inside it completes
      // after the caller has already moved on, and a message that arrives in
      // that window fires no event the caller can still observe. Selecting at
      // start closes the window for the mailbox an agent actually watches.
      const box = await watchClient.mailboxOpen(armedMailbox);
      armedLastUid = (box.uidNext ?? 1) - 1;
      // No manual `idle()`: imapflow auto-idles the connection on inactivity and
      // re-enters IDLE after each command it runs, whereas a manual `idle()`
      // turns that off and idles exactly once. Letting it manage IDLE is what
      // keeps the watch live across the FETCHes the `exists` handler issues.
    },

    async close(): Promise<void> {
      closing = true;
      watchers.clear();
      await Promise.allSettled([watchClient.logout(), commandClient.logout()]);
      smtp.close();
    },

    async send(
      message: OutboundMessage,
      _signal?: AbortSignal,
    ): Promise<SendReceipt> {
      const composed = await composeOutbound(
        config.address,
        message,
        config.crypto,
      );
      try {
        // Retried on a transient refusal for the same reason the hub's relay
        // is: provisioning a mailbox reloads the mail server, and an agent
        // whose step sends mail during that window is refused by a server that
        // accepts the same submission a moment later. The refusal reaches the
        // agent as a failed step, which fails the whole run.
        await submitWithRetry(
          async () => {
            await smtp.sendMail({
              envelope: { from: config.address, to: composed.recipients },
              raw: Buffer.from(composed.rawBytes),
            });
          },
          config.address,
          {
            attempts: config.submitAttempts ?? DEFAULT_SUBMIT_ATTEMPTS,
            baseMs: config.submitRetryBaseMs ?? DEFAULT_RETRY_BASE_MS,
            sleep: config.sleep ?? realSleep,
          },
        );
      } catch (cause) {
        // "send", not "SMTP submission": `submitWithRetry` already names the
        // submission and its sender in the message it reports.
        throw transportError(cause, "send");
      }

      if (config.onSent !== undefined) {
        await config.onSent({
          senderAddress: config.address,
          rawMessage: composed.rawBytes,
          messageId: composed.messageId,
          recipients: composed.recipients,
          to: composed.to,
          cc: composed.cc,
          // Never true here. The relay routes every recipient, hosted or not,
          // so there is no sense in which a submission stayed local.
          localOnly: false,
        });
      }

      // A relay accepting a message means it took responsibility for delivering
      // it, not that it delivered it. `delivered` would be a claim this
      // transport cannot make; the in-memory transport can only make it because
      // its "relay" is the recipient's mailbox.
      return { messageId: composed.messageId, status: "queued" };
    },

    async search(
      mailbox: string,
      query: SearchQuery,
      _signal?: AbortSignal,
    ): Promise<MessageRef[]> {
      const { criteria, residue } = translate(query);
      return await withMailbox(mailbox, "SEARCH", async () => {
        const uids = requireUids(
          await commandClient.search(criteria, { uid: true }),
          `SEARCH on "${mailbox}"`,
        );
        if (residue.length === 0) {
          return uids.map((uid) => ({ uid, mailbox }));
        }
        // The server matched a superset. Narrow it with the predicates IMAP
        // SEARCH could not carry, reusing the same matcher the other backings
        // use so the two agree on what a predicate means.
        logger.debug`SEARCH on ${mailbox} left ${String(residue.length)} predicate(s) for a local re-check`;
        const fetched = await fetchMessages(uids);
        const store = createFetchedStore(fetched, {
          uidValidity: 0,
          uidNext: 0,
          highestModSeq: 0,
        });
        let surviving = fetched.map((m) => m.uid);
        for (const predicate of residue) {
          const hits = await executeSearch(mailbox, store, predicate);
          const allowed = new Set(hits.map((h) => h.uid));
          surviving = surviving.filter((uid) => allowed.has(uid));
        }
        return surviving.map((uid) => ({ uid, mailbox }));
      });
    },

    async fetchHeaders(ref: MessageRef): Promise<MessageHeaders> {
      return await withMailbox(ref.mailbox, "FETCH HEADER", async (lock) => {
        const message = await fetchOneOrThrow(ref, "fetchHeaders");
        const store = createFetchedStore([message], countersFrom(lock));
        return await projectHeaders(ref, store);
      });
    },

    async fetchPart(ref: MessageRef, partPath: string): Promise<MessagePart> {
      // This fetches the WHOLE message and then walks to the part, which is the
      // opposite of what IMAP partial fetch is for. `@intx/mailbox`'s part
      // projection resolves a path against whole-message bytes, so serving the
      // part from a `BODY.PEEK[<path>]` fetch would need that projection to
      // accept pre-fetched part bytes instead.
      return await withMailbox(ref.mailbox, "FETCH BODY", async (lock) => {
        const message = await fetchOneOrThrow(ref, "fetchPart");
        const store = createFetchedStore([message], countersFrom(lock));
        return await projectPart(ref, partPath, store);
      });
    },

    async readRaw(ref: MessageRef): Promise<Uint8Array> {
      return await withMailbox(ref.mailbox, "FETCH BODY[] (raw)", async () => {
        const message = await fetchOneOrThrow(ref, "readRaw");
        return message.raw;
      });
    },

    async fetchFull(ref: MessageRef): Promise<InboundMessage> {
      return await withMailbox(ref.mailbox, "FETCH BODY[]", async (lock) => {
        const message = await fetchOneOrThrow(ref, "fetchFull");
        const store = createFetchedStore([message], countersFrom(lock));
        return await projectFull(ref, store, config.getCrypto);
      });
    },

    async setFlags(ref: MessageRef, flags: string[]): Promise<void> {
      await withMailbox(ref.mailbox, "STORE +FLAGS", async () => {
        const ok = await commandClient.messageFlagsAdd(String(ref.uid), flags, {
          uid: true,
        });
        if (!ok) {
          throw new MessageTransportError(
            "SERVERBUG",
            `STORE +FLAGS on uid ${String(ref.uid)} in "${ref.mailbox}" was not applied`,
          );
        }
      });
    },

    async clearFlags(ref: MessageRef, flags: string[]): Promise<void> {
      await withMailbox(ref.mailbox, "STORE -FLAGS", async () => {
        const ok = await commandClient.messageFlagsRemove(
          String(ref.uid),
          flags,
          { uid: true },
        );
        if (!ok) {
          throw new MessageTransportError(
            "SERVERBUG",
            `STORE -FLAGS on uid ${String(ref.uid)} in "${ref.mailbox}" was not applied`,
          );
        }
      });
    },

    async expunge(mailbox: string): Promise<{ expungedUids: number[] }> {
      return await withMailbox(mailbox, "EXPUNGE", async () => {
        // Resolve which uids the sweep will take BEFORE expunging: afterwards
        // the server reports only sequence numbers, and the messages are gone.
        const doomed = requireUids(
          await commandClient.search({ deleted: true }, { uid: true }),
          `SEARCH DELETED on "${mailbox}"`,
        );
        if (doomed.length === 0) return { expungedUids: [] };
        await commandClient.messageDelete(doomed.join(","), { uid: true });
        return { expungedUids: doomed };
      });
    },

    watch(
      mailbox: string,
      callback: (event: MailboxEvent) => void,
    ): Unsubscribe {
      let entry = watchers.get(mailbox);
      if (entry === undefined) {
        // The armed mailbox was selected at `start()`, so its high-water mark is
        // already known and nothing already present is reported as an arrival.
        entry = {
          callbacks: new Set(),
          lastUid: mailbox === armedMailbox ? armedLastUid : 0,
        };
        watchers.set(mailbox, entry);
      }
      entry.callbacks.add(callback);

      if (mailbox !== armedMailbox && selectedForWatch !== mailbox) {
        // A second watched mailbox cannot be armed ahead of time: IDLE follows
        // the SELECTED mailbox, and this connection is already holding the armed
        // one. Selecting here moves the watch off the armed mailbox AND lands
        // after this synchronous call has returned, so an arrival in the window
        // is missed and the interface has no way to report either problem.
        // Watching two mailboxes properly needs one connection per mailbox.
        selectedForWatch = mailbox;
        logger.warn`watch on ${mailbox} is not the armed mailbox ${armedMailbox}; it is selected asynchronously and takes the watch connection off the armed mailbox`;
        void (async () => {
          try {
            const box = await watchClient.mailboxOpen(mailbox);
            const current = watchers.get(mailbox);
            if (current !== undefined) {
              current.lastUid = Math.max(
                current.lastUid,
                (box.uidNext ?? 1) - 1,
              );
            }
          } catch (cause) {
            logger.error`watch install on ${mailbox} failed: ${cause instanceof Error ? cause.message : String(cause)}`;
          }
        })();
      }

      return () => {
        const current = watchers.get(mailbox);
        if (current === undefined) return;
        current.callbacks.delete(callback);
        if (current.callbacks.size === 0) watchers.delete(mailbox);
      };
    },
  };
}

/** Append a CRLF pair so a bare header block ends with the blank line a parse wants. */
function concatCRLF(headerBytes: Uint8Array): Uint8Array {
  const suffix = new TextEncoder().encode("\r\n");
  const out = new Uint8Array(headerBytes.length + suffix.length);
  out.set(headerBytes, 0);
  out.set(suffix, headerBytes.length);
  return out;
}
