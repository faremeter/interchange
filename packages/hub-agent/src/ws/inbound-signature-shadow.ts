import { getLogger } from "@intx/log";
import { hexDecode } from "@intx/types";
import { verifyMimeSignature } from "@intx/mailbox";
import { parseHeaderSection, extractAddrSpec } from "@intx/mime";

const logger = getLogger([
  "interchange",
  "hub-agent",
  "ws",
  "inbound-signature-shadow",
]);

// A raw Ed25519 public key is 32 bytes; the hub stamps it hex-encoded on the
// frame.
const ED25519_PUBLIC_KEY_BYTES = 32;

/**
 * The two-axis verdict of shadow-verifying one inbound mail frame.
 *
 * `signature` reuses the `SignatureStatus` vocabulary --
 * `valid | invalid | missing | unknown` -- with an added `error` for a fault in
 * the verifier itself (a malformed key, an unparseable sender). It answers: does
 * the message's detached signature verify against the key the hub resolved for
 * `authenticatedSender`?
 *
 * `fromMatch` is an orthogonal axis: given a VALID signature, does the message's
 * visible `From` bind to `authenticatedSender`? A valid signature over a `From`
 * that names a different sender is an identity forgery -- a legitimate key
 * signing under a borrowed display identity -- and it can only arise atop a
 * valid signature, so `fromMatch` is `unchecked` whenever the signature is not
 * `valid` or the message carries no parseable `From`.
 *
 * The signature covers only the message's signed content part, NOT its
 * top-level `From` header (see `@intx/mime` `assembleMessage`). The binding
 * checked here is therefore the hub-stamped sender against the visible envelope
 * `From`, not a `From` inside the signed bytes.
 */
export type InboundSignatureVerdict = {
  signature: "valid" | "invalid" | "missing" | "unknown" | "error";
  fromMatch: "match" | "mismatch" | "unchecked";
  authenticatedSender: string;
  messageFrom: string | null;
};

export type InboundSignatureShadowInput = {
  raw: Uint8Array;
  authenticatedSender: string;
  authenticatedSenderPublicKey: string | null;
  messageId: string | undefined;
  agentAddress: string;
};

/**
 * Verify an inbound mail frame's signature against the hub-resolved sender key
 * and LOG the verdict. Shadow only: this NEVER rejects delivery and NEVER
 * throws -- the caller runs it beside an unconditional admit. Returns the
 * verdict so a caller (or a test) can read it without scraping the log.
 *
 * A fault in the verifier (malformed key, unparseable sender) degrades to an
 * `error` verdict logged at ERROR -- surfaced loudly and kept distinct from the
 * ordinary `unknown` of an unresolvable sender, which stays a quiet `info`.
 */
export async function shadowVerifyInboundSignature(
  input: InboundSignatureShadowInput,
): Promise<InboundSignatureVerdict> {
  const { raw, authenticatedSender, authenticatedSenderPublicKey } = input;

  if (authenticatedSenderPublicKey === null) {
    // No hub-resolved key: an unresolvable sender (a run whose deploy is not yet
    // acked, an address matching no principal). Nothing to verify against; the
    // mail is admitted as an unverifiable sender. Not a fault -- a quiet
    // `unknown`.
    return logVerdict(
      {
        signature: "unknown",
        fromMatch: "unchecked",
        authenticatedSender,
        messageFrom: null,
      },
      input,
    );
  }

  let verdict: InboundSignatureVerdict;
  try {
    const key = hexDecode(authenticatedSenderPublicKey);
    if (key.length !== ED25519_PUBLIC_KEY_BYTES) {
      throw new Error(
        `expected a ${ED25519_PUBLIC_KEY_BYTES}-byte Ed25519 key, got ${key.length}`,
      );
    }
    const signature = await verifyMimeSignature(raw, key);
    verdict = {
      signature,
      fromMatch: "unchecked",
      authenticatedSender,
      messageFrom: null,
    };
    if (signature === "valid") {
      bindVisibleFrom(verdict, raw);
    }
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
 * Bind the message's visible `From` to `authenticatedSender` on a VALID
 * signature. A parse failure (a malformed `From`, or an `authenticatedSender`
 * that is not a bare addr-spec) leaves the binding `unchecked` rather than
 * clobbering the standing signature verdict -- the signature is the primary
 * signal, and a shadow must not turn an unparseable header into a false verdict.
 */
function bindVisibleFrom(
  verdict: InboundSignatureVerdict,
  raw: Uint8Array,
): void {
  try {
    const messageFrom = readMessageFrom(raw);
    verdict.messageFrom = messageFrom;
    if (messageFrom !== null) {
      verdict.fromMatch =
        messageFrom === extractAddrSpec(verdict.authenticatedSender)
          ? "match"
          : "mismatch";
    }
  } catch (cause) {
    logger.debug(
      "inbound mail From-binding unparseable for {authenticatedSender}: {cause}",
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
