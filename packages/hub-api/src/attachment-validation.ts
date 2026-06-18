import {
  base64Decode,
  isAllowedMimeType,
  PER_ATTACHMENT_LIMIT_BYTES,
  PER_MESSAGE_TOTAL_LIMIT_BYTES,
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

// Each error carries `code` plus structured fields so clients can act on the
// code without string-parsing, and a human-readable `message` matching the
// shape of every other error this route returns.
export type AttachmentValidationError =
  | {
      code: "oversize_attachment";
      message: string;
      attachmentIndex: number;
      byteLength: number;
      limitBytes: number;
    }
  | {
      code: "disallowed_mime_type";
      message: string;
      attachmentIndex: number;
      mimeType: string;
    }
  | { code: "malformed_base64"; message: string; attachmentIndex: number }
  | {
      code: "oversize_total";
      message: string;
      totalBytes: number;
      limitBytes: number;
    };

export type AttachmentValidationResult =
  | { ok: true; attachments: MessageAttachment[] }
  | { ok: false; error: AttachmentValidationError };

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
 * MIME type, then malformed base64, then per-message total oversize. On
 * success the decoded `MessageAttachment[]` is returned with names
 * defaulted to `attachment-{index}` by request-body position.
 */
export function validateAttachments(
  inputs: readonly AttachmentInput[],
  policy: AttachmentPolicy = DEFAULT_ATTACHMENT_POLICY,
): AttachmentValidationResult {
  let oversize: AttachmentValidationError | undefined;
  let disallowed: AttachmentValidationError | undefined;
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
    decoded.push({
      name: input.name ?? `attachment-${index}`,
      contentType: input.mimeType,
      data: bytes,
    });
  }

  if (oversize !== undefined) return { ok: false, error: oversize };
  if (disallowed !== undefined) return { ok: false, error: disallowed };
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
