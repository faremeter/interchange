import type { MediaRef } from "@intx/inference-discovery/catalog";

// Single source of truth for the extension → MIME type mapping used by
// both the inline media path (vision-input, document-input) and the
// Files API upload path. Adding a new extension here is the only place
// it needs to land for both call sites to pick it up.
const EXTENSION_TO_MEDIA_TYPE: Readonly<Record<string, string>> = {
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  png: "image/png",
  gif: "image/gif",
  webp: "image/webp",
  pdf: "application/pdf",
};

export function extensionFor(path: string): string {
  const dot = path.lastIndexOf(".");
  if (dot < 0 || dot === path.length - 1) {
    throw new Error(
      `anthropic: cannot infer media type, no extension in path: ${path}`,
    );
  }
  return path.slice(dot + 1).toLowerCase();
}

export function mediaTypeFor(ref: MediaRef): string {
  const ext = extensionFor(ref.path);
  const mime = EXTENSION_TO_MEDIA_TYPE[ext];
  if (mime === undefined) {
    throw new Error(
      `anthropic: no media-type mapping for extension .${ext} (path ${ref.path})`,
    );
  }
  return mime;
}
