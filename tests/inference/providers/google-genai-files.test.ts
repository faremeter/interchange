// Tests for the Gemini Files API upload helper. A synthetic `fetch`
// returns the captured `files-api-reference-streaming/upload/response.json`
// fixture, pinning wire-shape parsing without a live API key; a final
// env-gated test hits the real API when `GEMINI_API_KEY` is set.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { type } from "arktype";
import { describe, expect, test } from "bun:test";

import {
  uploadGoogleGenAIFile,
  type UploadGoogleGenAIFileFetch,
} from "@intx/inference";

// `RequestInit.headers` is the union `HeadersInit`; arktype-validated
// narrowing keeps the test honest if the helper ever passes a
// `Headers` instance instead of a Record.
const HeadersRecord = type("Record<string, string>");

const FIXTURE_ROOT = join(
  import.meta.dir,
  "..",
  "..",
  "..",
  "packages",
  "inference-discovery-google-genai",
  "sessions",
  "google-genai",
);

const UPLOAD_RESPONSE_FIXTURE = join(
  FIXTURE_ROOT,
  "gemini-2.5-flash",
  "files-api-reference-streaming",
  "exchanges",
  "0",
  "response.json",
);

const UPLOAD_REQUEST_BIN = join(
  FIXTURE_ROOT,
  "gemini-2.5-flash",
  "files-api-reference-streaming",
  "exchanges",
  "0",
  "request.bin",
);

function fixtureUploadResponse(): unknown {
  return JSON.parse(readFileSync(UPLOAD_RESPONSE_FIXTURE, "utf-8"));
}

describe("uploadGoogleGenAIFile", () => {
  test("posts to the Files API and returns the parsed file resource", async () => {
    // Captures the request, then returns the captured upload response.
    // Asserts both directions: the request matches the documented
    // protocol (`X-Goog-Upload-Protocol: raw`, key on
    // `x-goog-api-key`, bytes as body) and the return value is the
    // normalized fixture shape.
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
    // Under `exactOptionalPropertyTypes`, the helper must omit
    // `signal` rather than assign `undefined` (RequestInit types it
    // as `AbortSignal | null`).
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
    // The helper reads text then parses JSON, so the body snippet
    // survives in the thrown message.
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
    // Strict regex rejects anything that is not exactly a signed
    // integer (parseInt would silently accept "42abc").
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
    // sizeBytes is int64 on the wire; values past
    // Number.MAX_SAFE_INTEGER would silently round in a JS number,
    // so the helper rejects them.
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
    // The guard covers all CTL bytes, not just CR/LF; a NUL would
    // otherwise reach the fetch and produce a vaguer error.
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
    // The key lands on `x-goog-api-key` under the same header-value
    // safety rule; validation runs at the boundary regardless of
    // input provenance.
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
    // mimeType lands in `Content-Type`; an injected newline would
    // smuggle extra headers, so the helper rejects the input.
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
  // Hits the real Files API only when `GEMINI_API_KEY` is set,
  // catching API drift that the pinned fixture above cannot.
  //
  // The test deletes the upload in a `finally` block: Files API
  // resources persist 48h and count against quotas.
  const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

  test.skipIf(GEMINI_API_KEY === undefined || GEMINI_API_KEY === "")(
    "uploads the captured request.bin against the live Files API and deletes the result",
    async () => {
      // `skipIf` evaluates at collection time; the in-branch guard
      // narrows the type without a non-null assertion.
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
        // Best-effort delete: the helper has no delete surface (the
        // inference path is upload-and-reference), and a failure must
        // not mask the assertions above.
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
