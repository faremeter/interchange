import { type } from "arktype";

// Raw single-part upload endpoint: the bytes are the request body and the
// file resource (uri + metadata) comes back in the response. Resumable and
// multipart protocols are out of scope.
const FILES_API_UPLOAD_DEFAULT_URL =
  "https://generativelanguage.googleapis.com/upload/v1beta/files";

// `Number.parseInt` alone accepts trailing junk, whitespace, sci-notation,
// and decimals; this guard rejects all four.
const PARSEABLE_INTEGER = /^-?\d+$/;

const FilesApiUploadResponse = type({
  file: {
    // `> 0` rejects empty strings: a `""` uri is not dereferenceable.
    uri: "string > 0",
    mimeType: "string > 0",
    // Lands stringified; the parser normalizes to a runtime integer.
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
 * Fetch injection shape; mirrors the harness's `Dependencies.fetch` so test
 * doubles stay structurally compatible. Bun's global `typeof fetch` carries
 * non-callable members a double would otherwise have to satisfy.
 */
export type UploadGoogleGenAIFileFetch = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;

export interface UploadGoogleGenAIFileOpts {
  apiKey: string;
  mimeType: string;
  // Display name on the file resource; callers usually pass the local
  // basename.
  displayName: string;
  bytes: Uint8Array;
  // Full target URL override; defaults to the public Files API URL. The
  // helper appends no path to this value.
  uploadURL?: string;
  // Defaults to the global `fetch`.
  fetch?: UploadGoogleGenAIFileFetch;
  // Caller cancellation; forwarded to `fetch` verbatim.
  signal?: AbortSignal;
}

export interface UploadedGoogleGenAIFile {
  // Threaded back into `fileData.fileUri` on a later request, or into a
  // `MediaSource` with `kind: "file-reference"`.
  fileUri: string;
  // Echoed from the upload request.
  mimeType: string;
  // Normalized from the wire's string-encoded integer; absent stays absent.
  sizeBytes?: number;
  // Provider-side id (`files/<id>`) for management ops.
  name?: string;
  // Lifecycle state (`ACTIVE`, `PROCESSING`, `FAILED`).
  state?: string;
}

// Header values must be free of control characters (RFC 9110 §5.5). `fetch`
// rejects CR/LF but not NUL, so catch the broader set here at the boundary
// that turns caller strings into HTTP structure. `apiKey` gets the same
// guard for the same reason.
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
 * Upload once, then reference the returned `fileUri` on later requests via a
 * `fileData.fileUri` part (or a `MediaSource` of `kind: "file-reference"`).
 * Inline `base64` ships with every request; file references ship once.
 *
 * Uses only the "raw" upload protocol (one `POST` with the full bytes);
 * resumable and multipart are out of scope.
 *
 * @throws when the upload returns non-2xx, the body is not JSON, or the
 *   shape lacks a non-empty `file.uri` + `file.mimeType`. HTTP errors name
 *   the status and a body snippet.
 */
export async function uploadGoogleGenAIFile(
  opts: UploadGoogleGenAIFileOpts,
): Promise<UploadedGoogleGenAIFile> {
  const fetchImpl = opts.fetch ?? fetch;
  const url = opts.uploadURL ?? FILES_API_UPLOAD_DEFAULT_URL;

  assertHeaderValueSafe("Content-Type", opts.mimeType);
  assertHeaderValueSafe("X-Goog-Upload-File-Name", opts.displayName);
  assertHeaderValueSafe("x-goog-api-key", opts.apiKey);

  // Lowercase to match the captured wire shape; header names are
  // case-insensitive, so this is documentation, not behavior.
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
  // `signal` is typed `AbortSignal | null`; attach only when supplied so an
  // absent signal does not become `undefined` on the init object.
  if (opts.signal !== undefined) {
    init.signal = opts.signal;
  }
  const response = await fetchImpl(url, init);

  // Read as text first so both error paths can sample a body snippet;
  // `response.json()` would consume the stream first.
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
  // Normalize `sizeBytes` (wire string or number) to a non-negative safe
  // integer. The regex rejects what `Number.parseInt` alone accepts
  // ("42abc"); the safe-integer check keeps an int64 string from silently
  // rounding.
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
