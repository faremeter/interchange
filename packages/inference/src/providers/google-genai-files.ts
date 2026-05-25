import { type } from "arktype";

// Google's Generative Language Files API endpoint for raw single-part
// uploads. The "raw" upload protocol (declared via
// `X-Goog-Upload-Protocol: raw`) accepts the bytes as the request
// body and returns the file resource (uri + metadata) on the
// response. The resumable and multipart protocols target larger
// files and a different endpoint vocabulary; the helper here covers
// only the raw shape because it maps cleanly onto a single fetch.
const FILES_API_UPLOAD_DEFAULT_URL =
  "https://generativelanguage.googleapis.com/upload/v1beta/files";

// Parseable-integer pattern. Accepts an optional minus sign followed
// by digits and nothing else. Excludes whitespace, trailing
// non-digits, scientific notation, decimals. `Number.parseInt`
// alone is permissive on all four ("42abc" parses to 42, "  42"
// parses to 42); this guard makes the wire schema honest.
const PARSEABLE_INTEGER = /^-?\d+$/;

const FilesApiUploadResponse = type({
  file: {
    // `> 0` rejects empty strings -- a "" uri is not dereferenceable
    // and would slip through a `"string"` validator. Same for the
    // mime type.
    uri: "string > 0",
    mimeType: "string > 0",
    // `sizeBytes` lands as a stringified number on the wire. The
    // schema accepts string or number; the parser normalizes both
    // to a runtime integer at extraction time and throws on any
    // value that is not a parseable integer.
    "sizeBytes?": "string | number",
    "name?": "string",
    "state?": "string",
    "source?": "string",
    "createTime?": "string",
    "updateTime?": "string",
    "expirationTime?": "string",
    "sha256Hash?": "string",
  },
});

/**
 * Callable shape the helper accepts for `fetch` injection. Mirrors
 * `Dependencies.fetch` on the inference harness, so test doubles
 * stay structurally compatible across both surfaces. Bun's global
 * `typeof fetch` carries non-callable members (e.g. `preconnect`)
 * that test doubles would otherwise have to satisfy gratuitously.
 */
export type UploadGoogleGenAIFileFetch = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;

export interface UploadGoogleGenAIFileOpts {
  apiKey: string;
  mimeType: string;
  // Display name surfaced on the file resource. The Files API
  // accepts arbitrary strings here; callers typically pass the
  // local file's basename.
  displayName: string;
  bytes: Uint8Array;
  // Optional override for the upload endpoint. Defaults to
  // Google's public Files API full URL (path included). A
  // self-hosted or regional endpoint can be substituted by
  // passing the full target URL -- the helper does not append
  // any path to this value.
  uploadURL?: string;
  // Optional fetch implementation. Defaults to the global `fetch`.
  fetch?: UploadGoogleGenAIFileFetch;
  // Optional AbortSignal for caller cancellation. Forwarded to
  // `fetch` verbatim.
  signal?: AbortSignal;
}

export interface UploadedGoogleGenAIFile {
  // The dereferenceable `fileUri` the caller threads back into a
  // `fileData.fileUri` part on a subsequent inference request, or
  // into a `MediaSource` with `kind: "file-reference"`.
  fileUri: string;
  // The MIME type the API recorded for the file (echoed from the
  // upload request).
  mimeType: string;
  // Size in bytes, normalized from the wire's string-encoded
  // integer. Absent on the wire becomes absent here -- the
  // helper does not synthesize a value.
  sizeBytes?: number;
  // Provider-side file id (`files/<id>`). Useful for management
  // operations against the Files API (delete, list).
  name?: string;
  // Lifecycle state (`ACTIVE`, `PROCESSING`, `FAILED`).
  state?: string;
}

// Header values must be free of control characters (CR/LF are
// the smuggling-relevant ones, but NUL and other CTL bytes are
// also illegal per RFC 9110 §5.5 visible-US-ASCII rule). The
// downstream `fetch` typically rejects CR/LF but not NUL, so
// catching the broader set here keeps the diagnostic specific
// to the offending input. This is the boundary that turns
// caller strings into HTTP request structure (per the style
// skill's Data Validation rule), so the check belongs here.
//
// `apiKey` flows through the same guard because the rule is
// "validate at the boundary" -- the boundary cannot know an
// input's provenance, and the caller may not have run their own
// sanity check.
// The guard's whole purpose is to reject CR/LF/NUL/other CTL
// bytes before they reach the downstream fetch, so the regex
// MUST match those code points.
// eslint-disable-next-line no-control-regex
const FORBIDDEN_HEADER_CHARS = /[\x00-\x1f\x7f]/;
function assertHeaderValueSafe(name: string, value: string): void {
  if (FORBIDDEN_HEADER_CHARS.test(value)) {
    throw new Error(
      `google-genai files-API upload: header ${JSON.stringify(name)} ` +
        `contains a control character (CR, LF, NUL, or other CTL byte), ` +
        `which would let the value smuggle additional headers or be ` +
        `rejected downstream with a less specific message.`,
    );
  }
}

/**
 * Upload bytes to the Gemini Files API and return the file URI.
 *
 * The Files API is the dereferencable handle path for Gemini media:
 * upload once, reference the returned `fileUri` on subsequent
 * inference requests via a `fileData.fileUri` part (or a
 * `MediaSource` of `kind: "file-reference"` whose `reference` is the
 * URI). The trade-off relative to inline `base64` is bytes-on-the-wire:
 * inline payloads ship with every request, file references ship
 * once and are then quoted.
 *
 * The helper exclusively uses the "raw" upload protocol (one
 * `POST` carries the full bytes). The resumable and multipart
 * protocols are out of scope; large-file workflows that need them
 * should compose against the Gemini SDK directly.
 *
 * @throws an Error when the upload returns non-2xx, when the
 *   response is not JSON, or when the response shape does not
 *   carry a non-empty `file.uri` + `file.mimeType`. The thrown
 *   error's message names the failure mode; HTTP errors include
 *   the status code and a snippet of the response body, and
 *   schema/JSON failures carry a snippet of the response body too.
 */
export async function uploadGoogleGenAIFile(
  opts: UploadGoogleGenAIFileOpts,
): Promise<UploadedGoogleGenAIFile> {
  const fetchImpl = opts.fetch ?? fetch;
  const url = opts.uploadURL ?? FILES_API_UPLOAD_DEFAULT_URL;

  assertHeaderValueSafe("Content-Type", opts.mimeType);
  assertHeaderValueSafe("X-Goog-Upload-File-Name", opts.displayName);
  assertHeaderValueSafe("x-goog-api-key", opts.apiKey);

  // The `x-goog-api-key` header is intentionally lowercase to
  // match the wire shape Google's Files API responds to (the
  // captured `request-headers.json` records the same casing).
  // HTTP header names are case-insensitive on the wire, so the
  // mixed-case neighbors are functionally identical -- the choice
  // is documentation, not behavior.
  const headers: Record<string, string> = {
    "Content-Type": opts.mimeType,
    "X-Goog-Upload-Protocol": "raw",
    "X-Goog-Upload-File-Name": opts.displayName,
    "x-goog-api-key": opts.apiKey,
  };

  const init: RequestInit = {
    method: "POST",
    headers,
    body: opts.bytes,
  };
  // `RequestInit.signal` is typed as `AbortSignal | null` under
  // `exactOptionalPropertyTypes`; only attach the property when
  // the caller actually supplied a signal so an absent `signal`
  // does not become `undefined` on the init object (which the
  // overload then rejects).
  if (opts.signal !== undefined) {
    init.signal = opts.signal;
  }
  const response = await fetchImpl(url, init);

  // Read the body as text first, then parse JSON ourselves. This
  // keeps a snippet of the actual server response available for
  // both the JSON-parse and schema-mismatch error paths; reading
  // through `response.json()` would consume the stream before the
  // error site can sample it.
  let body: string;
  try {
    body = await response.text();
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    throw new Error(
      `google-genai files-API upload: failed to read response body ` +
        `(status ${String(response.status)} ${response.statusText}): ${message}`,
      { cause },
    );
  }

  if (!response.ok) {
    throw new Error(
      `google-genai files-API upload failed: ${String(response.status)} ` +
        `${response.statusText}: ${body.slice(0, 500)}`,
    );
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    throw new Error(
      `google-genai files-API upload: response was not valid JSON ` +
        `(${message}): ${body.slice(0, 500)}`,
      { cause },
    );
  }

  const validated = FilesApiUploadResponse(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `google-genai files-API upload: response did not match the expected ` +
        `shape (missing file.uri/mimeType or a malformed file resource): ` +
        `${validated.summary}; body: ${body.slice(0, 500)}`,
    );
  }

  const { file } = validated;
  // Normalize `sizeBytes` from `string | number | undefined` to
  // `number | undefined`. The wire ships an integer as a string
  // ("4193"); the helper rejects strings that are not exactly a
  // parseable integer and numbers that are not integers (a size
  // in bytes by definition is not fractional). `Number.parseInt`
  // alone is permissive ("42abc" parses to 42) so the regex guard
  // is what makes the contract honest. The final integer must
  // also be non-negative (bytes count up from zero) and within
  // JavaScript's safe-integer range -- Files API docs call this
  // field int64, and a string like "9007199254740993" silently
  // rounds when coerced to a JS number, so a precision check at
  // the boundary keeps the returned value faithful to the wire.
  function assertSafeNonNegativeInteger(n: number, raw: string | number): void {
    if (n < 0) {
      throw new Error(
        `google-genai files-API upload: file.sizeBytes ` +
          `${JSON.stringify(raw)} is negative; a byte count cannot be ` +
          `less than zero.`,
      );
    }
    if (n > Number.MAX_SAFE_INTEGER) {
      throw new Error(
        `google-genai files-API upload: file.sizeBytes ` +
          `${JSON.stringify(raw)} exceeds Number.MAX_SAFE_INTEGER and ` +
          `cannot be represented as a JS number without precision loss.`,
      );
    }
  }

  let sizeBytes: number | undefined;
  if (typeof file.sizeBytes === "string") {
    if (!PARSEABLE_INTEGER.test(file.sizeBytes)) {
      throw new Error(
        `google-genai files-API upload: file.sizeBytes ` +
          `${JSON.stringify(file.sizeBytes)} is not a parseable integer.`,
      );
    }
    const parsedSize = Number.parseInt(file.sizeBytes, 10);
    assertSafeNonNegativeInteger(parsedSize, file.sizeBytes);
    sizeBytes = parsedSize;
  } else if (typeof file.sizeBytes === "number") {
    if (!Number.isInteger(file.sizeBytes)) {
      throw new Error(
        `google-genai files-API upload: file.sizeBytes ` +
          `${JSON.stringify(file.sizeBytes)} is not an integer.`,
      );
    }
    assertSafeNonNegativeInteger(file.sizeBytes, file.sizeBytes);
    sizeBytes = file.sizeBytes;
  }

  const result: UploadedGoogleGenAIFile = {
    fileUri: file.uri,
    mimeType: file.mimeType,
  };
  if (sizeBytes !== undefined) result.sizeBytes = sizeBytes;
  if (file.name !== undefined) result.name = file.name;
  if (file.state !== undefined) result.state = file.state;
  return result;
}
