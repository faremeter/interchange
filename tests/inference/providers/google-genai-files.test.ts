// Tests for the Gemini Files API upload helper. The bulk of the
// coverage uses a synthetic `fetch` that returns the captured
// `files-api-reference-streaming/upload/response.json` fixture --
// pinning the wire-shape parsing without a live API key. A final
// guarded test hits the real Files API when `GEMINI_API_KEY` is
// set in the environment, providing a smoke check that the
// captured wire shape is still current. Unguarded runs (CI, local
// without the env var) skip the live test cleanly.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { type } from "arktype";
import { describe, expect, test } from "bun:test";

import {
  uploadGoogleGenAIFile,
  type UploadGoogleGenAIFileFetch,
} from "@intx/inference";

// `RequestInit.headers` is typed as the union `HeadersInit`
// (Headers | Record<string, string> | [string, string][]). The
// helper always passes a Record, but the type system carries the
// wider union at the test site; an arktype-validated narrowing
// keeps the test honest if the helper ever changes to passing a
// `Headers` instance.
const HeadersRecord = type("Record<string, string>");

const FIXTURE_ROOT = join(
  import.meta.dir,
  "..",
  "..",
  "..",
  "packages",
  "inference-testing",
  "wire",
  "google-genai",
);

const UPLOAD_RESPONSE_FIXTURE = join(
  FIXTURE_ROOT,
  "gemini-2.5-flash",
  "files-api-reference-streaming",
  "upload",
  "response.json",
);

const UPLOAD_REQUEST_BIN = join(
  FIXTURE_ROOT,
  "gemini-2.5-flash",
  "files-api-reference-streaming",
  "upload",
  "request.bin",
);

function fixtureUploadResponse(): unknown {
  return JSON.parse(readFileSync(UPLOAD_RESPONSE_FIXTURE, "utf-8"));
}

describe("uploadGoogleGenAIFile", () => {
  test("posts to the Files API and returns the parsed file resource", async () => {
    // Captures the request the helper would issue against the
    // real Files API, then returns the captured upload response.
    // Asserts both directions: the request shape matches the
    // documented protocol (`X-Goog-Upload-Protocol: raw`, the API
    // key on `x-goog-api-key`, the bytes as the body), and the
    // helper's return value is the normalized shape from the
    // captured response.
    const recorded: { url?: string; init?: RequestInit } = {};
    const fakeFetch: UploadGoogleGenAIFileFetch = (input, init) => {
      recorded.url = typeof input === "string" ? input : input.toString();
      if (init !== undefined) recorded.init = init;
      return Promise.resolve(
        new Response(JSON.stringify(fixtureUploadResponse()), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      );
    };

    const bytes = new Uint8Array([1, 2, 3, 4]);
    const result = await uploadGoogleGenAIFile({
      apiKey: "test-key",
      mimeType: "application/pdf",
      displayName: "sample.pdf",
      bytes,
      fetch: fakeFetch,
    });

    expect(recorded.url).toBe(
      "https://generativelanguage.googleapis.com/upload/v1beta/files",
    );
    const init = recorded.init;
    if (init === undefined) {
      throw new Error("expected the helper to pass an init object to fetch");
    }
    expect(init.method).toBe("POST");
    expect(init.body).toBe(bytes);
    const headers = HeadersRecord.assert(init.headers);
    expect(headers["Content-Type"]).toBe("application/pdf");
    expect(headers["X-Goog-Upload-Protocol"]).toBe("raw");
    expect(headers["X-Goog-Upload-File-Name"]).toBe("sample.pdf");
    expect(headers["x-goog-api-key"]).toBe("test-key");

    // Captured fixture values: uri, mimeType, sizeBytes=4193,
    // name="files/ub8ska7qvvn2", state="ACTIVE".
    expect(result.fileUri).toBe(
      "https://generativelanguage.googleapis.com/v1beta/files/ub8ska7qvvn2",
    );
    expect(result.mimeType).toBe("application/pdf");
    expect(result.sizeBytes).toBe(4193);
    expect(result.name).toBe("files/ub8ska7qvvn2");
    expect(result.state).toBe("ACTIVE");
  });

  test("omits the signal property when none is supplied", async () => {
    // Under `exactOptionalPropertyTypes`, the helper must not
    // assign `signal: undefined` on the init object (the global
    // fetch's RequestInit types `signal` as `AbortSignal | null`
    // and rejects `undefined`). The omit-when-absent path is
    // exercised every call that does not supply a signal.
    const recorded: { init?: RequestInit } = {};
    const fakeFetch: UploadGoogleGenAIFileFetch = (_input, init) => {
      if (init !== undefined) recorded.init = init;
      return Promise.resolve(
        new Response(JSON.stringify(fixtureUploadResponse()), { status: 200 }),
      );
    };
    await uploadGoogleGenAIFile({
      apiKey: "k",
      mimeType: "application/pdf",
      displayName: "x.pdf",
      bytes: new Uint8Array([0]),
      fetch: fakeFetch,
    });
    if (recorded.init === undefined) {
      throw new Error("expected init to be captured");
    }
    expect("signal" in recorded.init).toBe(false);
  });

  test("forwards an explicit AbortSignal to fetch", async () => {
    const recorded: { init?: RequestInit } = {};
    const fakeFetch: UploadGoogleGenAIFileFetch = (_input, init) => {
      if (init !== undefined) recorded.init = init;
      return Promise.resolve(
        new Response(JSON.stringify(fixtureUploadResponse()), { status: 200 }),
      );
    };
    const controller = new AbortController();
    await uploadGoogleGenAIFile({
      apiKey: "k",
      mimeType: "application/pdf",
      displayName: "x.pdf",
      bytes: new Uint8Array([0]),
      fetch: fakeFetch,
      signal: controller.signal,
    });
    if (recorded.init === undefined) {
      throw new Error("expected init to be captured");
    }
    expect(recorded.init.signal).toBe(controller.signal);
  });

  test("respects a custom uploadURL when supplied", async () => {
    const recorded: { url?: string } = {};
    const fakeFetch: UploadGoogleGenAIFileFetch = (input) => {
      recorded.url = typeof input === "string" ? input : input.toString();
      return Promise.resolve(
        new Response(JSON.stringify(fixtureUploadResponse()), { status: 200 }),
      );
    };
    await uploadGoogleGenAIFile({
      apiKey: "k",
      mimeType: "application/pdf",
      displayName: "x.pdf",
      bytes: new Uint8Array([0]),
      uploadURL: "https://custom-endpoint.example/upload",
      fetch: fakeFetch,
    });
    expect(recorded.url).toBe("https://custom-endpoint.example/upload");
  });

  test("HTTP error response throws with the status and body snippet", async () => {
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(
        new Response("permission denied: bad key", {
          status: 403,
          statusText: "Forbidden",
        }),
      );

    let thrown: unknown;
    try {
      await uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      });
    } catch (e) {
      thrown = e;
    }
    if (!(thrown instanceof Error)) {
      throw new Error("expected the upload to throw on a 403");
    }
    expect(thrown.message).toMatch(/403/);
    expect(thrown.message).toMatch(/Forbidden/);
    expect(thrown.message).toMatch(/permission denied/);
  });

  test("non-JSON response body throws with the parse error chained as `cause` and a body snippet", async () => {
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(
        new Response("not json at all", {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      );

    let thrown: unknown;
    try {
      await uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      });
    } catch (e) {
      thrown = e;
    }
    if (!(thrown instanceof Error)) {
      throw new Error("expected the upload to throw on a non-JSON body");
    }
    expect(thrown.message).toMatch(/was not valid JSON/);
    // The body snippet survives in the thrown message now that the
    // helper reads-text-then-parses-JSON rather than calling
    // response.json() (which would consume the stream).
    expect(thrown.message).toMatch(/not json at all/);
    expect(thrown.cause).toBeInstanceOf(Error);
  });

  test("response with missing file.uri throws naming the validation failure and including a body snippet", async () => {
    const malformed = { file: { mimeType: "application/pdf" } };
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(
        new Response(JSON.stringify(malformed), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      );

    let thrown: unknown;
    try {
      await uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      });
    } catch (e) {
      thrown = e;
    }
    if (!(thrown instanceof Error)) {
      throw new Error("expected the upload to throw on a missing file.uri");
    }
    expect(thrown.message).toMatch(/did not match the expected shape/);
    expect(thrown.message).toMatch(/body:/);
  });

  test("response with missing file.mimeType throws", async () => {
    const malformed = { file: { uri: "https://example/u" } };
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(new Response(JSON.stringify(malformed), { status: 200 }));
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/did not match the expected shape/);
  });

  test("response with empty file.uri string is rejected (not dereferenceable)", async () => {
    const malformed = { file: { uri: "", mimeType: "application/pdf" } };
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(new Response(JSON.stringify(malformed), { status: 200 }));
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/did not match the expected shape/);
  });

  test("response with no file object at all throws", async () => {
    const malformed = { notAFile: {} };
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(new Response(JSON.stringify(malformed), { status: 200 }));
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/did not match the expected shape/);
  });

  test("non-parseable string sizeBytes throws naming the offending value", async () => {
    const malformed = {
      file: {
        uri: "https://example/uri",
        mimeType: "application/pdf",
        sizeBytes: "not-a-number",
      },
    };
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(new Response(JSON.stringify(malformed), { status: 200 }));
    let thrown: unknown;
    try {
      await uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      });
    } catch (e) {
      thrown = e;
    }
    if (!(thrown instanceof Error)) {
      throw new Error("expected the upload to throw on bad sizeBytes");
    }
    expect(thrown.message).toMatch(/not-a-number/);
    expect(thrown.message).toMatch(/parseable integer/);
  });

  test("string sizeBytes with trailing junk is rejected", async () => {
    // `Number.parseInt` would silently accept "42abc" as 42; the
    // helper's strict regex rejects anything that is not exactly
    // a signed integer.
    const malformed = {
      file: {
        uri: "https://example/uri",
        mimeType: "application/pdf",
        sizeBytes: "42abc",
      },
    };
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(new Response(JSON.stringify(malformed), { status: 200 }));
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/parseable integer/);
  });

  test("numeric non-integer sizeBytes is rejected", async () => {
    const malformed = {
      file: {
        uri: "https://example/uri",
        mimeType: "application/pdf",
        sizeBytes: 4.5,
      },
    };
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(new Response(JSON.stringify(malformed), { status: 200 }));
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/is not an integer/);
  });

  test("negative sizeBytes is rejected (a byte count cannot be < 0)", async () => {
    const malformed = {
      file: {
        uri: "https://example/uri",
        mimeType: "application/pdf",
        sizeBytes: "-1",
      },
    };
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(new Response(JSON.stringify(malformed), { status: 200 }));
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/is negative/);
  });

  test("sizeBytes string above MAX_SAFE_INTEGER is rejected (precision loss)", async () => {
    // The Files API documents sizeBytes as int64; a string like
    // "9007199254740993" (2^53 + 1) silently rounds when parsed
    // into a JS number. The helper rejects values past
    // Number.MAX_SAFE_INTEGER to keep the returned value
    // faithful to the wire.
    const malformed = {
      file: {
        uri: "https://example/uri",
        mimeType: "application/pdf",
        sizeBytes: "9007199254740993",
      },
    };
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(new Response(JSON.stringify(malformed), { status: 200 }));
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/exceeds Number\.MAX_SAFE_INTEGER/);
  });

  test("NUL byte in mimeType is rejected at the boundary", async () => {
    // The CTL-byte guard generalizes beyond CR/LF; NUL bytes
    // would otherwise reach the downstream fetch and produce a
    // less specific error message.
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(
        new Response(JSON.stringify(fixtureUploadResponse()), { status: 200 }),
      );
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf\x00X-Injected: evil",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/control character/);
  });

  test("CR/LF in apiKey is rejected at the boundary", async () => {
    // The apiKey lands on `x-goog-api-key`; the same header-value
    // safety rule applies. Validation runs at the boundary
    // regardless of input provenance -- the helper does not
    // assume the caller pre-sanitized the value.
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(
        new Response(JSON.stringify(fixtureUploadResponse()), { status: 200 }),
      );
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k\r\nX-Smuggled: yes",
        mimeType: "application/pdf",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/control character/);
  });

  test("CR/LF in mimeType is rejected at the boundary", async () => {
    // The mimeType lands directly in the `Content-Type` header.
    // An injected newline would smuggle additional headers onto
    // the request; the helper rejects the input rather than
    // forward it.
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(
        new Response(JSON.stringify(fixtureUploadResponse()), { status: 200 }),
      );
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf\r\nX-Injected: evil",
        displayName: "x.pdf",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/control character/);
  });

  test("CR/LF in displayName is rejected at the boundary", async () => {
    const fakeFetch: UploadGoogleGenAIFileFetch = () =>
      Promise.resolve(
        new Response(JSON.stringify(fixtureUploadResponse()), { status: 200 }),
      );
    expect(
      uploadGoogleGenAIFile({
        apiKey: "k",
        mimeType: "application/pdf",
        displayName: "x.pdf\nX-Injected: evil",
        bytes: new Uint8Array([0]),
        fetch: fakeFetch,
      }),
    ).rejects.toThrow(/control character/);
  });

  // ----- Live, env-gated -----------------------------------------
  // The block below hits the real Files API. It runs only when
  // `GEMINI_API_KEY` is set in the environment; CI and local
  // developer runs without the variable skip cleanly. The wire-
  // shape fixture pinned above provides offline coverage; the
  // gated test catches API drift (e.g. Google changes the
  // response shape under us) against a live endpoint.
  //
  // The test deletes the uploaded file in a `finally` block so a
  // green run does not leak resources against the project tied to
  // the API key. Files API resources persist for 48h and count
  // against quotas; an undeleted upload per test run would
  // accumulate quickly under CI.
  const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

  test.skipIf(GEMINI_API_KEY === undefined || GEMINI_API_KEY === "")(
    "uploads the captured request.bin against the live Files API and deletes the result",
    async () => {
      // `skipIf` evaluates the predicate at test-collection time,
      // so this branch only runs when the env var was set. An
      // explicit guard inside the branch narrows the type without
      // a non-null assertion.
      const apiKey = GEMINI_API_KEY;
      if (apiKey === undefined || apiKey === "") {
        throw new Error(
          "GEMINI_API_KEY guard inverted: the skipIf predicate should " +
            "have stopped this test from running.",
        );
      }
      const bytes = readFileSync(UPLOAD_REQUEST_BIN);
      const result = await uploadGoogleGenAIFile({
        apiKey,
        mimeType: "application/pdf",
        displayName: "sample.pdf",
        bytes: new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength),
      });
      try {
        expect(result.fileUri).toMatch(
          /^https:\/\/generativelanguage\.googleapis\.com\/v1beta\/files\//,
        );
        expect(result.mimeType).toBe("application/pdf");
        expect(result.sizeBytes).toBe(4193);
        expect(result.state).toMatch(/^(ACTIVE|PROCESSING)$/);
      } finally {
        // Delete the uploaded file. The helper does not expose a
        // delete surface (out of scope -- the inference path is
        // upload-and-reference); a one-off fetch from the test is
        // sufficient. Best-effort: a delete failure does not
        // re-throw because the assertions above are the test's
        // contract and a leaked file under a successful upload is
        // less urgent than a misleading assertion failure.
        if (result.name !== undefined) {
          try {
            await fetch(
              `https://generativelanguage.googleapis.com/v1beta/${result.name}`,
              {
                method: "DELETE",
                headers: { "x-goog-api-key": apiKey },
              },
            );
          } catch {
            // Ignored: cleanup failures are not the test's contract.
          }
        }
      }
    },
  );
});
