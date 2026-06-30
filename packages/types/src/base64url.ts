// Base64url codec for byte strings (RFC 4648 section 5).
//
// URL- and filename-safe base64: standard base64 with `+`/`/` replaced by
// `-`/`_` and trailing `=` padding stripped. Used for opaque pagination
// cursors and git PAT secrets that ride in URLs and HTTP basic-auth headers,
// where the standard `+`, `/`, and `=` characters are unsafe. Reuses the
// base64 core so the two encodings stay byte-compatible.

import { base64Decode, base64Encode } from "./base64";

export function base64urlEncode(bytes: Uint8Array): string {
  return base64Encode(bytes)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

export function base64urlDecode(s: string): Uint8Array {
  const translated = s.replace(/-/g, "+").replace(/_/g, "/");
  const padLength = (4 - (translated.length % 4)) % 4;
  return base64Decode(translated + "=".repeat(padLength));
}
