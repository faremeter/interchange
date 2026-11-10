// Admission of a REST mail message the caller signed itself.
//
// A caller whose key the hub does not hold -- a workflow run, whose key stays
// in its sidecar -- posts a complete signed message. The hub relays it as the
// caller's own mail, so before it does, it checks what a recipient trusts:
//
// - the routing headers. The signature covers only the signed body part, so
//   From, To, Message-ID and the type header are outside it. These checks
//   keep the request consistent with the authenticated caller and the route's
//   target; they prove nothing cryptographically. The recipient binds the
//   sender through the hub's stamp (the caller's address), the visible From,
//   and the key the hub co-delivers.
// - the signature, against the key the hub vouches for at the caller's
//   address, which is the key the recipient is given.
//
// The hub checks only the syntax of the caller's Message-ID. RFC 6409
// section 8.3 lets a submission server change a Message-ID only when it is
// missing or invalid, so the hub refuses an invalid one and does not change a
// valid one. The receiver de-duplicates by the authenticated sender and the
// Message-ID, so a caller cannot collide with another sender's Message-ID.
//
// The bytes the hub verified are the bytes it relays: the request's base64 is
// decoded once and re-encoded, so a lenient decode cannot let the relayed
// bytes differ from the verified ones.

import {
  extractAddrSpec,
  isDotAtomText,
  isMessageId,
  parseHeaderFields,
  verifyMimeSignature,
} from "@intx/mime";
import {
  resolveSenderKey,
  type DBExecutor,
  type PrincipalKeyStore,
} from "@intx/db";
import { base64Decode, hexDecode } from "@intx/types";

/**
 * The longest Message-ID a caller may choose. The hub stores it in the
 * durable dispatch row and logs it, so the limit is well under the RFC 5322
 * line limit.
 */
const MAX_CALLER_MESSAGE_ID_LENGTH = 256;

/** Seconds a caller waits before it retries a sender key that is absent. */
const SENDER_KEY_RETRY_AFTER_SECONDS = 5;

export type CallerSignedMailRejection = {
  status: 400 | 503;
  code: string;
  message: string;
  /** Set on a refusal the caller may retry, as `Retry-After`. */
  retryAfterSeconds?: number;
};

type CheckedCallerSignedMail = {
  /** The decoded bytes; the caller relays these, re-encoded. */
  rawMessage: Uint8Array;
  messageId: string;
};

export type AdmittedCallerSignedMail = CheckedCallerSignedMail & {
  /** The hex public key the signature verified against. */
  publicKey: string;
};

type CheckArgs = {
  rawMessageBase64: string;
  /** The authenticated caller's mail address; From must name it. */
  callerAddress: string;
  /** The route's target run address; To must name only it. */
  targetAddress: string;
  tenantId: string;
};

function invalidHeader(message: string): CallerSignedMailRejection {
  return { status: 400, code: "mail_header_invalid", message };
}

/**
 * The one value of a header that must appear exactly once. A repeated field
 * is refused: the hub would check one value and a reader could use another.
 */
function single(
  fields: Record<string, string[]>,
  name: string,
): string | CallerSignedMailRejection {
  const values = fields[name] ?? [];
  if (values.length !== 1) {
    return invalidHeader(
      `${name} must appear exactly once; the message carries ${String(values.length)}`,
    );
  }
  const [value] = values;
  if (value === undefined) return invalidHeader(`${name} is missing`);
  return value;
}

function sameAddress(headerValue: string, expected: string): boolean {
  try {
    return extractAddrSpec(headerValue) === expected.toLowerCase();
  } catch {
    return false;
  }
}

/**
 * Decode a caller-signed message and check its routing headers. The
 * signature is checked separately, once the caller's key is resolved.
 */
function checkCallerSignedMail(
  args: CheckArgs,
): CheckedCallerSignedMail | CallerSignedMailRejection {
  let rawMessage: Uint8Array;
  try {
    rawMessage = base64Decode(args.rawMessageBase64);
  } catch {
    return {
      status: 400,
      code: "malformed_raw_message",
      message: "rawMessage is not valid base64",
    };
  }

  let fields: Record<string, string[]>;
  try {
    fields = parseHeaderFields(rawMessage);
  } catch (cause) {
    return {
      status: 400,
      code: "malformed_raw_message",
      message: `rawMessage has a malformed header section: ${cause instanceof Error ? cause.message : String(cause)}`,
    };
  }

  const from = single(fields, "from");
  if (typeof from !== "string") return from;
  if (!sameAddress(from, args.callerAddress)) {
    return invalidHeader(
      `From must be the caller's address ${args.callerAddress}`,
    );
  }

  const to = single(fields, "to");
  if (typeof to !== "string") return to;
  if (!sameAddress(to, args.targetAddress)) {
    return invalidHeader(
      `To must be exactly the target run address ${args.targetAddress}`,
    );
  }
  for (const name of ["cc", "bcc"]) {
    if ((fields[name] ?? []).length > 0) {
      return invalidHeader(
        `${name} is not allowed; the run is the only recipient`,
      );
    }
  }

  const interchangeType = single(fields, "interchange-type");
  if (typeof interchangeType !== "string") return interchangeType;
  if (interchangeType !== "conversation.message") {
    return invalidHeader("interchange-type must be conversation.message");
  }

  // The caller's sidecar does not know the tenant id, so the header is
  // optional; when it is present it must name this tenant.
  const tenantValues = fields["interchange-tenant-id"] ?? [];
  if (
    tenantValues.length > 1 ||
    (tenantValues.length === 1 && tenantValues[0] !== args.tenantId)
  ) {
    return invalidHeader(
      `interchange-tenant-id, when present, must be once and name tenant ${args.tenantId}`,
    );
  }

  const messageId = single(fields, "message-id");
  if (typeof messageId !== "string") return messageId;
  // `isMessageId` checks the shape and refuses an `@` on either side, so the
  // first `@` splits the id. Both sides must also be dot-atom-text, so no
  // control character or non-ASCII text reaches the dispatch store or the
  // recipient. RFC 5322 also allows a domain literal on the right side; the
  // hub refuses one.
  const at = messageId.indexOf("@");
  if (
    !isMessageId(messageId) ||
    !isDotAtomText(messageId.slice(1, at)) ||
    !isDotAtomText(messageId.slice(at + 1, -1)) ||
    messageId.length > MAX_CALLER_MESSAGE_ID_LENGTH
  ) {
    return invalidHeader(
      `message-id must be an RFC 5322 message identifier of at most ${String(MAX_CALLER_MESSAGE_ID_LENGTH)} characters`,
    );
  }

  return { rawMessage, messageId };
}

/**
 * Check the message's signature against the caller's public key, as hex.
 * `invalid` and `missing` both refuse: the caller must sign its own mail.
 */
async function verifyCallerSignature(
  rawMessage: Uint8Array,
  publicKeyHex: string,
): Promise<CallerSignedMailRejection | null> {
  const status = await verifyMimeSignature(rawMessage, hexDecode(publicKeyHex));
  if (status === "valid") return null;
  return {
    status: 400,
    code: "mail_signature_invalid",
    message:
      status === "missing"
        ? "rawMessage is not a signed (multipart/signed) message"
        : "rawMessage's signature does not verify against the caller's key",
  };
}

/**
 * Admit a caller-signed message: check its routing headers, resolve the key
 * the hub vouches for at the caller's address, and verify the signature
 * against it. The key returned is the one the recipient must be given.
 *
 * The caller bounds the input: the REST mail routes' body limit holds it to
 * the relay frame cap, so REST mail is no larger than relayed mail.
 *
 * The strict resolver is used: a fault resolving the key throws rather than
 * reading as an absent key. An absent key is retryable: a run's key is absent
 * while its sidecar is replaced. The hub cannot tell that from a sender that
 * is gone, so both answer 503.
 */
export async function admitCallerSignedMail(
  db: DBExecutor,
  principalKeyStore: PrincipalKeyStore,
  args: CheckArgs,
): Promise<AdmittedCallerSignedMail | CallerSignedMailRejection> {
  const checked = checkCallerSignedMail(args);
  if ("status" in checked) return checked;
  const senderKey = await resolveSenderKey(
    db,
    principalKeyStore,
    args.callerAddress,
  );
  if (senderKey === null) {
    return {
      status: 503,
      code: "sender_key_unavailable",
      message: `No signing key is recorded for ${args.callerAddress}; a run's key is absent while its sidecar is replaced`,
      retryAfterSeconds: SENDER_KEY_RETRY_AFTER_SECONDS,
    };
  }
  const rejection = await verifyCallerSignature(
    checked.rawMessage,
    senderKey.publicKey,
  );
  if (rejection !== null) return rejection;
  return { ...checked, publicKey: senderKey.publicKey };
}
