import {
  isAllowedMimeType,
  mimeTypeAndSubtype,
  PER_ATTACHMENT_LIMIT_BYTES,
  PER_MESSAGE_TOTAL_LIMIT_BYTES,
} from "./attachments";
import { base64Decode } from "./base64";
import type { MessageAttachment } from "./runtime";
import type { AttachmentError } from "./sessions";

/**
 * A single attachment as it arrives at a boundary: a MIME type, the bytes,
 * and an optional filename. A string `data` is base64 (the request-body
 * form); a caller that already holds the bytes passes them as-is.
 */
export type AttachmentInput = {
  mimeType: string;
  data: string | Uint8Array;
  name?: string;
};

/**
 * The attachment policy the route validates against. Defaults to the
 * system-level allowlist and limits; a future per-agent or per-workflow
 * lookup substitutes a narrowed policy here (the injection seam — no
 * validation logic changes).
 */
export type AttachmentPolicy = {
  isAllowed: (mimeType: string) => boolean;
  perAttachmentLimitBytes: number;
  perMessageTotalLimitBytes: number;
};

export const DEFAULT_ATTACHMENT_POLICY: AttachmentPolicy = {
  isAllowed: isAllowedMimeType,
  perAttachmentLimitBytes: PER_ATTACHMENT_LIMIT_BYTES,
  perMessageTotalLimitBytes: PER_MESSAGE_TOTAL_LIMIT_BYTES,
};

// The error shape is the wire contract `AttachmentError` from @intx/types:
// a machine-actionable `code`, the fields needed to locate the rejection,
// and a human-readable `message`. Defining it once in @intx/types lets the
// route document the structured 400 in its OpenAPI surface.
export type AttachmentValidationError = AttachmentError;

export type AttachmentValidationResult =
  | { ok: true; attachments: MessageAttachment[] }
  | { ok: false; error: AttachmentError };

function decode(data: string | Uint8Array): Uint8Array | null {
  if (typeof data !== "string") return data;
  try {
    return base64Decode(data.replace(/\s+/g, ""));
  } catch {
    return null;
  }
}

/**
 * Validate and decode attachments against a policy at either boundary
 * (mail tools or the request body).
 *
 * Encoded base64 is size-checked from its string length before decode —
 * decoded bytes never exceed 3/4 of the encoded length — and the first
 * error returns without decoding later entries. Remaining checks, in
 * encounter order: per-attachment oversize, disallowed MIME type, invalid
 * name, malformed base64. After every attachment passes, the per-message
 * total is checked. On success the decoded `MessageAttachment[]` is
 * returned with names defaulted to `attachment-{index}` by input position.
 */
export function validateAttachments(
  inputs: readonly AttachmentInput[],
  policy: AttachmentPolicy = DEFAULT_ATTACHMENT_POLICY,
): AttachmentValidationResult {
  const decoded: MessageAttachment[] = [];

  for (const [index, input] of inputs.entries()) {
    if (typeof input.data === "string") {
      const encodedUpperBound = Math.floor((input.data.length * 3) / 4);
      if (encodedUpperBound > policy.perAttachmentLimitBytes) {
        return {
          ok: false,
          error: {
            code: "oversize_attachment",
            message: `attachment ${index} is ${encodedUpperBound} bytes, over the ${policy.perAttachmentLimitBytes}-byte limit`,
            attachmentIndex: index,
            byteLength: encodedUpperBound,
            limitBytes: policy.perAttachmentLimitBytes,
          },
        };
      }
    }

    const bytes = decode(input.data);
    if (bytes === null) {
      return {
        ok: false,
        error: {
          code: "malformed_base64",
          message: `attachment ${index} is not valid base64`,
          attachmentIndex: index,
        },
      };
    }
    if (bytes.length > policy.perAttachmentLimitBytes) {
      return {
        ok: false,
        error: {
          code: "oversize_attachment",
          message: `attachment ${index} is ${bytes.length} bytes, over the ${policy.perAttachmentLimitBytes}-byte limit`,
          attachmentIndex: index,
          byteLength: bytes.length,
          limitBytes: policy.perAttachmentLimitBytes,
        },
      };
    }
    const mimeType = mimeTypeAndSubtype(input.mimeType);
    if (!policy.isAllowed(mimeType)) {
      return {
        ok: false,
        error: {
          code: "disallowed_mime_type",
          message: `attachment ${index} has unsupported content type "${input.mimeType}"`,
          attachmentIndex: index,
          mimeType: input.mimeType,
        },
      };
    }
    // A user-supplied name becomes the MIME part's quoted filename, so it
    // must not contain characters that would break out of the header
    // (line breaks or a double quote). Empty and whitespace-only names
    // cannot round-trip through `filename=""`. The default name is always
    // safe.
    if (
      input.name !== undefined &&
      (input.name.trim() === "" || /[\r\n"]/.test(input.name))
    ) {
      return {
        ok: false,
        error: {
          code: "invalid_attachment_name",
          message: `attachment ${index} has a name with invalid characters (no quotes or line breaks)`,
          attachmentIndex: index,
        },
      };
    }
    decoded.push({
      name: input.name ?? `attachment-${index}`,
      contentType: mimeType,
      data: bytes,
    });
  }

  const totalBytes = decoded.reduce((sum, a) => sum + a.data.length, 0);
  if (totalBytes > policy.perMessageTotalLimitBytes) {
    return {
      ok: false,
      error: {
        code: "oversize_total",
        message: `attachments total ${totalBytes} bytes, over the ${policy.perMessageTotalLimitBytes}-byte limit`,
        totalBytes,
        limitBytes: policy.perMessageTotalLimitBytes,
      },
    };
  }

  return { ok: true, attachments: decoded };
}
