import {
  base64Decode,
  isAllowedMimeType,
  PER_ATTACHMENT_LIMIT_BYTES,
  PER_MESSAGE_TOTAL_LIMIT_BYTES,
  type AttachmentError,
} from "@intx/types";
import type { MessageAttachment } from "@intx/types/runtime";

/**
 * A single attachment as it arrives on the request body: a MIME type, the
 * base64-encoded bytes, and an optional filename.
 */
export type AttachmentInput = {
  mimeType: string;
  data: string;
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

function decode(data: string): Uint8Array | null {
  try {
    return base64Decode(data.replace(/\s+/g, ""));
  } catch {
    return null;
  }
}

/**
 * Validate and decode request-body attachments against a policy.
 *
 * The most specific error wins, in this priority order so the caller sees
 * the most actionable failure: per-attachment oversize, then disallowed
 * MIME type, then invalid name, then malformed base64, then per-message
 * total oversize. On success the decoded `MessageAttachment[]` is returned
 * with names defaulted to `attachment-{index}` by request-body position.
 */
export function validateAttachments(
  inputs: readonly AttachmentInput[],
  policy: AttachmentPolicy = DEFAULT_ATTACHMENT_POLICY,
): AttachmentValidationResult {
  let oversize: AttachmentValidationError | undefined;
  let disallowed: AttachmentValidationError | undefined;
  let invalidName: AttachmentValidationError | undefined;
  let malformed: AttachmentValidationError | undefined;
  const decoded: MessageAttachment[] = [];

  for (const [index, input] of inputs.entries()) {
    const bytes = decode(input.data);
    if (bytes === null) {
      malformed ??= {
        code: "malformed_base64",
        message: `attachment ${index} is not valid base64`,
        attachmentIndex: index,
      };
      continue;
    }
    if (bytes.length > policy.perAttachmentLimitBytes) {
      oversize ??= {
        code: "oversize_attachment",
        message: `attachment ${index} is ${bytes.length} bytes, over the ${policy.perAttachmentLimitBytes}-byte limit`,
        attachmentIndex: index,
        byteLength: bytes.length,
        limitBytes: policy.perAttachmentLimitBytes,
      };
      continue;
    }
    if (!policy.isAllowed(input.mimeType)) {
      disallowed ??= {
        code: "disallowed_mime_type",
        message: `attachment ${index} has unsupported content type "${input.mimeType}"`,
        attachmentIndex: index,
        mimeType: input.mimeType,
      };
      continue;
    }
    // A user-supplied name becomes the MIME part's quoted filename, so it
    // must not contain characters that would break out of the header
    // (line breaks or a double quote). The default name is always safe.
    if (input.name !== undefined && /[\r\n"]/.test(input.name)) {
      invalidName ??= {
        code: "invalid_attachment_name",
        message: `attachment ${index} has a name with invalid characters (no quotes or line breaks)`,
        attachmentIndex: index,
      };
      continue;
    }
    decoded.push({
      name: input.name ?? `attachment-${index}`,
      contentType: input.mimeType,
      data: bytes,
    });
  }

  if (oversize !== undefined) return { ok: false, error: oversize };
  if (disallowed !== undefined) return { ok: false, error: disallowed };
  if (invalidName !== undefined) return { ok: false, error: invalidName };
  if (malformed !== undefined) return { ok: false, error: malformed };

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
