// Attachment allowlist — the system-level source of truth for which MIME
// types the hub accepts as conversation attachments and which ContentBlock
// category each maps to. Adding a MIME type is a one-line change here.
//
// This is the hard capability ceiling: a type is only useful if the pipeline
// can produce the right ContentBlock and an adapter can marshal it. Per-agent
// or per-workflow narrowing rides on top of this ceiling — it narrows the
// accepted set, it never widens past what the adapters support.

export const ATTACHMENT_CATEGORIES = [
  "image",
  "video",
  "audio",
  "document",
] as const;
export type AttachmentCategory = (typeof ATTACHMENT_CATEGORIES)[number];

export const ATTACHMENT_ALLOWLIST = {
  "image/png": "image",
  "image/jpeg": "image",
  "image/gif": "image",
  "image/webp": "image",
  "image/heic": "image",
  "image/heif": "image",
  "video/mp4": "video",
  "video/webm": "video",
  "video/quicktime": "video",
  "audio/mpeg": "audio",
  "audio/wav": "audio",
  "audio/ogg": "audio",
  "audio/webm": "audio",
  "application/pdf": "document",
  "application/json": "document",
  "text/plain": "document",
  "text/csv": "document",
  "text/markdown": "document",
} as const satisfies Record<string, AttachmentCategory>;

export type AllowedMimeType = keyof typeof ATTACHMENT_ALLOWLIST;

export function isAllowedMimeType(
  mimeType: string,
): mimeType is AllowedMimeType {
  return mimeType in ATTACHMENT_ALLOWLIST;
}

/**
 * The ContentBlock category for an allowlisted MIME type, or `undefined`
 * when the type is not on the allowlist. Callers decide how to treat an
 * unknown type (the route rejects it at the boundary; turn construction
 * surfaces it as a text marker).
 */
export function attachmentCategory(
  mimeType: string,
): AttachmentCategory | undefined {
  if (isAllowedMimeType(mimeType)) {
    return ATTACHMENT_ALLOWLIST[mimeType];
  }
  return undefined;
}

// Default size limits, on decoded bytes. These are the system-level
// ceiling; a future per-agent/per-workflow policy resolves an effective
// limit that defaults to these.
export const PER_ATTACHMENT_LIMIT_BYTES = 10 * 1024 * 1024;
export const PER_MESSAGE_TOTAL_LIMIT_BYTES = 30 * 1024 * 1024;
