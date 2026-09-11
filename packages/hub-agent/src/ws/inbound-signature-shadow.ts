import { getLogger } from "@intx/log";
import type { CryptoProvider } from "@intx/types/runtime";
import { verifyMimeSignature } from "@intx/mailbox";
import { parseHeaderSection, extractAddrSpec } from "@intx/mime";

const logger = getLogger([
  "interchange",
  "hub-agent",
  "ws",
  "inbound-signature-shadow",
]);

/**
 * The two-axis verdict of shadow-verifying one inbound mail frame.
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

export type InboundSignatureShadowInput = {
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
 * Shadow only: this NEVER rejects delivery and NEVER throws -- the caller runs
 * it beside an unconditional admit. Returns the verdict so a caller (or a test)
 * can read it without scraping the log.
 *
 * A cache miss is a quiet `unknown` (an expected, benign state -- see below),
 * not a fault. A genuine fault (the resolver throwing, the cached key being
 * unreadable, or the verify throwing) degrades to an `error` verdict logged at
 * ERROR -- surfaced loudly and kept distinct from `unknown`.
 */
export async function shadowVerifyInboundSignature(
  input: InboundSignatureShadowInput,
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
      "inbound mail signature shadow-verify FAULTED for {authenticatedSender} (messageId {messageId}, agentAddress {agentAddress}): {cause}",
      {
        // Carry the same `signature`/`fromMatch` keys the clean verdict logs, so
        // a consumer counting the shadow corpus by `signature` sees faults too.
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
 * the signature is the primary signal, and a shadow must not turn an unparseable
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
  input: InboundSignatureShadowInput,
): InboundSignatureVerdict {
  logger.info(
    "inbound mail signature shadow verdict {signature}/{fromMatch} for {authenticatedSender} (from {messageFrom}, messageId {messageId}, agentAddress {agentAddress})",
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
