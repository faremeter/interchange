import { getLogger } from "@intx/log";
import type {
  AuthorControllableOutcome,
  CryptoProvider,
  InboundMailOutcome,
  InboundMailPolicy,
} from "@intx/types/runtime";
import { verifyMimeSignature } from "@intx/mailbox";
import { parseHeaderSection, extractAddrSpec } from "@intx/mime";

import { isUsableSenderKey } from "../sender-key-cache";

const logger = getLogger([
  "interchange",
  "hub-agent",
  "ws",
  "inbound-signature",
]);

/**
 * The two-axis verdict of verifying one inbound mail frame. `absent` is the
 * gate looking for a visible `From` and finding none; `notEvaluated` is the
 * placeholder an `error` verdict carries for an axis the gate never reached.
 *
 * The signature covers only the message's signed content part, NOT its
 * top-level `From` header, so `fromMatch` binds the hub-stamped sender to the
 * visible envelope `From` rather than to a `From` inside the signed bytes.
 */
export type InboundSignatureVerdict = {
  signature: "valid" | "invalid" | "missing" | "unknown" | "error";
  fromMatch: "match" | "mismatch" | "absent" | "notEvaluated" | "unparseable";
  authenticatedSender: string;
  messageFrom: string | null;
};

/**
 * Reduce a two-axis {@link InboundSignatureVerdict} to the single
 * {@link InboundMailOutcome} that best SUMMARIZES it, for the verdict log line.
 * This is a headline, NOT the admission decision -- that is
 * {@link decideInboundAdmission}, and the two can name different findings. The
 * ordering below is not a security precedence and must not be read as one.
 */
export function outcomeForVerdict(
  verdict: InboundSignatureVerdict,
): InboundMailOutcome {
  const { signature, fromMatch } = verdict;
  if (signature === "error") return "error";
  if (fromMatch === "unparseable") return "untrustedFrom";
  if (fromMatch === "mismatch") return "mismatchedFrom";
  if (signature === "valid") {
    if (fromMatch === "match") return "clean";
    if (fromMatch === "absent") return "absentFrom";
    if (fromMatch === "notEvaluated") return "untrustedFrom";
    const _exhaustiveBinding: never = fromMatch;
    return _exhaustiveBinding;
  }
  if (signature === "invalid") return "invalid";
  if (signature === "missing") return "missing";
  if (signature === "unknown") return "unknown";
  const _exhaustive: never = signature;
  return _exhaustive;
}

/** The total counterpart of the SPARSE authored `InboundMailPolicy`. */
export type ResolvedInboundMailPolicy = Record<
  InboundMailOutcome,
  "reject" | "admit"
>;

/**
 * Resolve the SPARSE authored {@link InboundMailPolicy} into a TOTAL
 * {@link ResolvedInboundMailPolicy}, applying every default HERE. The per-mail
 * delivery path looks up the resolved map directly -- it must never re-derive a
 * default with a `?? "reject"` of its own.
 *
 * `clean` and `error` are pinned whatever the author declared: nothing about a
 * clean message is suspect, and a fault stopped the check from running, so no
 * trust claim can be made about it. `error` is not a key in
 * {@link InboundMailPolicy} at all, so an authored policy cannot relax it.
 *
 * Every author-controllable outcome defaults to `reject`, so a policy that
 * omits one fails closed rather than open.
 */
export function resolveInboundMailPolicy(
  authored: InboundMailPolicy | undefined,
): ResolvedInboundMailPolicy {
  return {
    clean: "admit",
    error: "reject",
    untrustedFrom: authored?.untrustedFrom ?? "reject",
    mismatchedFrom: authored?.mismatchedFrom ?? "reject",
    absentFrom: authored?.absentFrom ?? "reject",
    invalid: authored?.invalid ?? "reject",
    missing: authored?.missing ?? "reject",
    unknown: authored?.unknown ?? "reject",
  };
}

export type InboundAdmission = {
  findings: AuthorControllableOutcome[];
  rejectedBy: InboundMailOutcome | null;
};

/**
 * Decide whether one inbound mail frame is admitted, over the SET of findings
 * its verdict raised rather than over a single reduced outcome. Each axis
 * carries a separate author judgement, so reducing the pair to one outcome and
 * keying admission on that discards one of the two judgements -- and it can
 * discard it in the ADMITTING direction.
 */
export function decideInboundAdmission(
  verdict: InboundSignatureVerdict,
  policy: ResolvedInboundMailPolicy,
): InboundAdmission {
  const { signature, fromMatch } = verdict;
  if (signature === "error") return { findings: [], rejectedBy: "error" };

  const findings: AuthorControllableOutcome[] = [];
  const fromSignature = signatureFinding(signature);
  if (fromSignature !== null) findings.push(fromSignature);
  const fromBinding = bindingFinding(fromMatch);
  if (fromBinding !== null) findings.push(fromBinding);

  for (const finding of findings) {
    if (policy[finding] !== "admit") return { findings, rejectedBy: finding };
  }
  // A binding that raised nothing without binding was never evaluated, whatever
  // the signature axis found. The gate has no trust claim to make for it, and no
  // policy relaxes that.
  if (fromBinding === null && fromMatch !== "match") {
    return { findings, rejectedBy: "error" };
  }
  if (policy.clean !== "admit") return { findings, rejectedBy: "clean" };
  return { findings, rejectedBy: null };
}

function signatureFinding(
  signature: Exclude<InboundSignatureVerdict["signature"], "error">,
): AuthorControllableOutcome | null {
  if (signature === "valid") return null;
  if (signature === "invalid") return "invalid";
  if (signature === "missing") return "missing";
  if (signature === "unknown") return "unknown";
  const _exhaustive: never = signature;
  return _exhaustive;
}

function bindingFinding(
  fromMatch: InboundSignatureVerdict["fromMatch"],
): AuthorControllableOutcome | null {
  if (fromMatch === "match") return null;
  if (fromMatch === "notEvaluated") return null;
  if (fromMatch === "mismatch") return "mismatchedFrom";
  if (fromMatch === "unparseable") return "untrustedFrom";
  if (fromMatch === "absent") return "absentFrom";
  const _exhaustive: never = fromMatch;
  return _exhaustive;
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
 * so a throw that escaped here would wedge the delivery chain.
 */
export async function verifyInboundSignature(
  input: InboundSignatureInput,
  resolveSenderCrypto: (address: string) => CryptoProvider | undefined,
): Promise<InboundSignatureVerdict> {
  const { raw, authenticatedSender } = input;

  let verdict: InboundSignatureVerdict;
  try {
    // Parsed above the cache lookup and above every early return below, so the
    // fault belongs on every path.
    const stampedAddrSpec = parseSenderStamp(authenticatedSender);
    // Keyed on the RAW stamp, not on `stampedAddrSpec`: the cache is populated
    // under the address the hub sends, and `extractAddrSpec` lowercases.
    const crypto = resolveSenderCrypto(authenticatedSender);
    let signature: InboundSignatureVerdict["signature"];
    if (crypto === undefined) {
      signature = "unknown";
    } else {
      const publicKey = crypto.getPublicKey();
      // The cache-backed resolver cannot reach this: that cache refuses a
      // wrong-length key at write and THROWS for an entry that failed to load,
      // so an address it answers for carries a usable key. The resolver is an
      // injected seam, and a composition supplying its own is not covered by
      // that, so the check stays.
      requireUsableSenderKey(authenticatedSender, publicKey);
      signature = await verifyMimeSignature(raw, publicKey);
    }
    const from = evaluateVisibleFrom({
      raw,
      stampedAddrSpec,
      authenticatedSender,
    });
    verdict = {
      signature,
      fromMatch: from.fromMatch,
      authenticatedSender,
      messageFrom: from.messageFrom,
    };
  } catch (cause) {
    logger.error(
      "inbound mail signature verify FAULTED for {authenticatedSender} (messageId {messageId}, agentAddress {agentAddress}): {cause}",
      {
        // Carry the same `signature`/`fromMatch` keys the clean verdict logs, so
        // a consumer counting the verdict corpus by `signature` sees faults too.
        signature: "error",
        fromMatch: "notEvaluated",
        authenticatedSender,
        messageId: input.messageId ?? null,
        agentAddress: input.agentAddress,
        cause: describeCause(cause),
      },
    );
    return {
      signature: "error",
      fromMatch: "notEvaluated",
      authenticatedSender,
      messageFrom: null,
    };
  }

  return logVerdict(verdict, input);
}

type VisibleFromArgs = {
  raw: Uint8Array;
  stampedAddrSpec: string;
  authenticatedSender: string;
};

/**
 * The comparison does NOT consult the signature axis, and must not. Nothing
 * downstream of the gate sees `authenticatedSender`, so a `From` contradicting
 * the stamp is a claim the gate can refuse whether or not a verified signature
 * stood behind it.
 */
function evaluateVisibleFrom(
  args: VisibleFromArgs,
): Pick<InboundSignatureVerdict, "fromMatch" | "messageFrom"> {
  const { raw, stampedAddrSpec, authenticatedSender } = args;
  let messageFrom: string | null;
  try {
    messageFrom = readMessageFrom(raw);
  } catch (cause) {
    logger.debug(
      "inbound mail From-binding unparseable for {authenticatedSender}: {cause}",
      {
        authenticatedSender,
        cause: describeCause(cause),
      },
    );
    return { fromMatch: "unparseable", messageFrom: null };
  }
  if (messageFrom === null) return { fromMatch: "absent", messageFrom: null };
  return {
    fromMatch: messageFrom === stampedAddrSpec ? "match" : "mismatch",
    messageFrom,
  };
}

/**
 * `verifyMimeSignature` refuses such a key too, so the throw is not what makes
 * the fault reach `error`. What this adds is a log line naming the keyring as
 * the thing at fault.
 */
function requireUsableSenderKey(
  authenticatedSender: string,
  publicKey: Uint8Array,
): void {
  if (isUsableSenderKey(publicKey)) return;
  logger.error(
    "inbound mail sender key for {authenticatedSender} is unusable: {keyBytes} bytes of cached material that is not a key, so no signature could be checked against it",
    { authenticatedSender, keyBytes: publicKey.length },
  );
  throw new Error(
    `cached sender key for ${JSON.stringify(authenticatedSender)} cannot verify a signature`,
  );
}

function parseSenderStamp(authenticatedSender: string): string {
  try {
    return extractAddrSpec(authenticatedSender);
  } catch (cause) {
    throw new Error(
      `authenticated sender stamp is not a bare addr-spec: ${JSON.stringify(authenticatedSender)}`,
      { cause },
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
