import { getLogger } from "@intx/log";
import type {
  CryptoProvider,
  InboundMailOutcome,
  InboundMailPolicy,
} from "@intx/types/runtime";
import { verifyMimeSignature } from "@intx/mailbox";
import { parseHeaderSection, extractAddrSpec } from "@intx/mime";

const logger = getLogger([
  "interchange",
  "hub-agent",
  "ws",
  "inbound-signature",
]);

/**
 * The two-axis verdict of verifying one inbound mail frame.
 *
 * `signature` reuses the `SignatureStatus` vocabulary --
 * `valid | invalid | missing | unknown` -- with an added `error` for a fault in
 * the verifier itself (verify throwing, an unparseable sender). It answers: does
 * the message's detached signature verify against the key the recipient's local
 * cache holds for `authenticatedSender`? `unknown` means the cache holds no key
 * for the sender, so there is nothing to verify against.
 *
 * `fromMatch` is an orthogonal axis over the message's visible `From`, evaluated
 * for EVERY non-error signature status (valid, invalid, missing, unknown):
 *   - `unchecked`: the message carries no `From`, OR it carries a parseable
 *     `From` but the signature is not `valid`, so a match is not meaningful.
 *   - `unparseable`: the message carries a `From` that cannot be reduced to one
 *     addr-spec. This is DISTINCT from `unchecked` (no `From` at all) -- a
 *     present but unparseable `From` is suspicious, not benign.
 *   - `match` / `mismatch`: only atop a VALID signature with a parseable `From`,
 *     whether that `From` binds to `authenticatedSender`. A valid signature over
 *     a `From` that names a different sender is an identity forgery -- a
 *     legitimate key signing under a borrowed display identity.
 *
 * The signature covers only the message's signed content part, NOT its
 * top-level `From` header (see `@intx/mime` `assembleMessage`). The binding
 * checked here is therefore the hub-stamped sender against the visible envelope
 * `From`, not a `From` inside the signed bytes.
 */
export type InboundSignatureVerdict = {
  signature: "valid" | "invalid" | "missing" | "unknown" | "error";
  fromMatch: "match" | "mismatch" | "unchecked" | "unparseable";
  authenticatedSender: string;
  messageFrom: string | null;
};

/**
 * Reduce a two-axis {@link InboundSignatureVerdict} to the single
 * {@link InboundMailOutcome} a delivery decision keys on.
 *
 * The precedence is reject-dominant: when more than one axis is unhappy, the
 * outcome is the one that most resists admission. Two outcomes dominate, for
 * distinct reasons:
 *   - `error` outranks everything -- a fault stopped the check from running, so
 *     we could not check the message and can make no trust claim about it.
 *   - `untrustedFrom` outranks the signature axis -- a `From` we cannot trust
 *     (present but unparseable, or a valid signature worn under a mismatched
 *     identity) is a forgery signal that must not be masked by a merely
 *     unverifiable signature underneath it.
 *
 * `match`/`mismatch` only ever occur atop a `valid` signature (a guarantee of
 * how the verdict is produced), while `unparseable` can accompany any non-error
 * signature status; the ordering below reflects both facts.
 */
export function outcomeForVerdict(
  verdict: InboundSignatureVerdict,
): InboundMailOutcome {
  const { signature, fromMatch } = verdict;
  if (signature === "error") return "error";
  if (fromMatch === "unparseable") return "untrustedFrom";
  if (signature === "valid" && fromMatch === "mismatch") return "untrustedFrom";
  if (signature === "invalid") return "invalid";
  if (signature === "missing") return "missing";
  if (signature === "unknown") return "unknown";
  return "clean";
}

/**
 * A TOTAL admission decision map: for EVERY {@link InboundMailOutcome}, whether a
 * message that resolved to that outcome is `reject`ed or `admit`ted. The per-mail
 * delivery decision looks this up directly by the message's outcome with no
 * fallback -- every key is present, so there is never an absent value for the
 * lookup to default.
 *
 * This is the resolved counterpart of the SPARSE authored `InboundMailPolicy`:
 * the sparse policy carries only what an author declared, and
 * {@link resolveInboundMailPolicy} expands it into this total map ONCE.
 */
export type ResolvedInboundMailPolicy = Record<
  InboundMailOutcome,
  "reject" | "admit"
>;

/**
 * Resolve the SPARSE authored {@link InboundMailPolicy} into a TOTAL
 * {@link ResolvedInboundMailPolicy}, applying every default HERE. This is the
 * single place inbound-admission defaults live: the resolution runs once, and
 * the per-mail delivery path looks up the resolved map directly -- it must never
 * re-derive a default with a `?? "reject"` of its own.
 *
 * The two non-author-controllable outcomes are pinned regardless of what the
 * author declared:
 *   - `clean` -> always `admit`: nothing about the message was suspect, so there
 *     is nothing to relax and no reason to reject.
 *   - `error` -> always `reject`: a fault stopped the check from running, so we
 *     could make no trust claim about the message. `error` is not even a key in
 *     {@link InboundMailPolicy}, so an authored policy cannot relax it -- a
 *     message we could not check through is never something an author waves past.
 *
 * The four author-controllable outcomes (`untrustedFrom`, `invalid`, `missing`,
 * `unknown`) take the authored value where the author set that key, and default
 * to `reject` otherwise. `reject` is the secure default: an outcome the author
 * did not explicitly choose to admit stays rejected, so an omitted policy (or a
 * policy that omits one outcome) fails closed rather than open.
 */
export function resolveInboundMailPolicy(
  authored: InboundMailPolicy | undefined,
): ResolvedInboundMailPolicy {
  return {
    clean: "admit",
    error: "reject",
    untrustedFrom: authored?.untrustedFrom ?? "reject",
    invalid: authored?.invalid ?? "reject",
    missing: authored?.missing ?? "reject",
    unknown: authored?.unknown ?? "reject",
  };
}

export type InboundSignatureInput = {
  raw: Uint8Array;
  authenticatedSender: string;
  messageId: string | undefined;
  agentAddress: string;
};

/**
 * Verify an inbound mail frame's signature against the key the recipient's
 * local cache holds for `authenticatedSender` and LOG the verdict. The key is
 * resolved through `resolveSenderCrypto` -- the cache-backed source populated by
 * the hub's co-delivery on the run's grants barrier -- so the recipient verifies
 * locally against the key the hub vouched for, never a key travelling on the
 * message itself.
 *
 * This NEVER throws. A fault degrades to an `error` verdict, logged at ERROR
 * and returned like any other verdict. The enforcement caller relies on this
 * contract: it awaits this inline on the delivery path with no per-call catch,
 * gates admission on the returned outcome, and drops the mail on a reject, so a
 * throw that escaped here would wedge the delivery chain. Returns the verdict so
 * the caller (or a test) can read it without scraping the log.
 *
 * A cache miss is a quiet `unknown` (an expected, benign state -- see below),
 * not a fault. A genuine fault (the resolver throwing, the cached key being
 * unreadable, or the verify throwing) degrades to an `error` verdict logged at
 * ERROR -- surfaced loudly and kept distinct from `unknown`.
 */
export async function verifyInboundSignature(
  input: InboundSignatureInput,
  resolveSenderCrypto: (address: string) => CryptoProvider | undefined,
): Promise<InboundSignatureVerdict> {
  const { raw, authenticatedSender } = input;

  let verdict: InboundSignatureVerdict;
  try {
    // Resolve and read the cached key inside the try so ANY fault -- the
    // resolver throwing, `getPublicKey` throwing, or the verify throwing --
    // is contained as a single `error` verdict rather than escaping. This is
    // what keeps the "never throws" contract true.
    const crypto = resolveSenderCrypto(authenticatedSender);
    if (crypto === undefined) {
      // Cache miss: the local keyring holds no key for this sender, so there
      // is nothing to verify against. Expected for a sender whose key was
      // never co-delivered (a run authorized before this shipped, relayed mail
      // with no preceding grant co-delivery) or was unresolvable, or a
      // rotation the cache has not yet refreshed. The mail is admitted as an
      // unverifiable sender -- a quiet `unknown`, keyed by `authenticatedSender`
      // in the log so the observation window stays legible.
      verdict = {
        signature: "unknown",
        fromMatch: "unchecked",
        authenticatedSender,
        messageFrom: null,
      };
    } else {
      // The cached key is raw bytes, already validated 32-byte Ed25519 at cache
      // write/load time, so it feeds `verifyMimeSignature` directly.
      const signature = await verifyMimeSignature(raw, crypto.getPublicKey());
      verdict = {
        signature,
        fromMatch: "unchecked",
        authenticatedSender,
        messageFrom: null,
      };
    }
    // Evaluate the visible From for EVERY non-error status. It records
    // `messageFrom` and, atop a VALID signature, resolves the `fromMatch`
    // binding; a present-but-unparseable From is marked `unparseable` even
    // under invalid/missing/unknown, so a later enforcement precedence can see
    // it. `evaluateVisibleFrom` never throws, so it does not reach the `error`
    // path below.
    evaluateVisibleFrom(verdict, raw);
  } catch (cause) {
    logger.error(
      "inbound mail signature verify FAULTED for {authenticatedSender} (messageId {messageId}, agentAddress {agentAddress}): {cause}",
      {
        // Carry the same `signature`/`fromMatch` keys the clean verdict logs, so
        // a consumer counting the verdict corpus by `signature` sees faults too.
        signature: "error",
        fromMatch: "unchecked",
        authenticatedSender,
        messageId: input.messageId ?? null,
        agentAddress: input.agentAddress,
        cause: describeCause(cause),
      },
    );
    return {
      signature: "error",
      fromMatch: "unchecked",
      authenticatedSender,
      messageFrom: null,
    };
  }

  return logVerdict(verdict, input);
}

/**
 * Evaluate the message's visible `From` onto `verdict`, for any non-error
 * signature status. Uses the tri-state of `readMessageFrom`:
 *   - no `From` (or empty): leaves the binding `unchecked`, `messageFrom` null.
 *   - `From` present but unparseable (`readMessageFrom` throws): marks the
 *     binding `unparseable` -- a present but malformed `From` is a distinct,
 *     suspicious state, kept separate from the benign no-`From` `unchecked`.
 *   - `From` present and parsed: records `messageFrom`, and ONLY atop a VALID
 *     signature compares it to `authenticatedSender` for `match`/`mismatch`.
 *
 * A stamped `authenticatedSender` that is not a bare addr-spec leaves the
 * binding `unchecked` rather than clobbering the standing signature verdict --
 * the signature is the primary signal, and the check must not turn an unparseable
 * header into a false verdict.
 */
function evaluateVisibleFrom(
  verdict: InboundSignatureVerdict,
  raw: Uint8Array,
): void {
  let messageFrom: string | null;
  try {
    messageFrom = readMessageFrom(raw);
  } catch (cause) {
    // The message carries a `From` that `extractAddrSpec` refuses -- present
    // but unparseable, distinct from no `From` at all.
    verdict.fromMatch = "unparseable";
    logger.debug(
      "inbound mail From-binding unparseable for {authenticatedSender}: {cause}",
      {
        authenticatedSender: verdict.authenticatedSender,
        cause: describeCause(cause),
      },
    );
    return;
  }
  if (messageFrom === null) return;
  verdict.messageFrom = messageFrom;
  // A From-binding is only meaningful atop a valid signature.
  if (verdict.signature !== "valid") return;
  try {
    verdict.fromMatch =
      messageFrom === extractAddrSpec(verdict.authenticatedSender)
        ? "match"
        : "mismatch";
  } catch (cause) {
    logger.debug(
      "inbound mail sender stamp {authenticatedSender} is not a parseable addr-spec; leaving From-binding unchecked: {cause}",
      {
        authenticatedSender: verdict.authenticatedSender,
        cause: describeCause(cause),
      },
    );
  }
}

function readMessageFrom(raw: Uint8Array): string | null {
  const { headers } = parseHeaderSection(raw);
  const from = headers.get("from");
  if (from === undefined || from.trim() === "") return null;
  return extractAddrSpec(from);
}

function logVerdict(
  verdict: InboundSignatureVerdict,
  input: InboundSignatureInput,
): InboundSignatureVerdict {
  logger.info(
    "inbound mail signature verdict {signature}/{fromMatch} for {authenticatedSender} (from {messageFrom}, messageId {messageId}, agentAddress {agentAddress})",
    {
      signature: verdict.signature,
      fromMatch: verdict.fromMatch,
      authenticatedSender: verdict.authenticatedSender,
      messageFrom: verdict.messageFrom,
      messageId: input.messageId ?? null,
      agentAddress: input.agentAddress,
    },
  );
  return verdict;
}

function describeCause(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}
