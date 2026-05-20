import { describe, test, expect, beforeEach } from "bun:test";
import { mkdtemp, readFile, readdir, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  FIXTURE_ROOT,
  buildMetadata,
  fixtureDirectoryFor,
  redactRequestHeaders,
  runStreamingCapture,
  teeStreamToBytes,
  writeFixture,
  writeMultiStepMetadata,
  writeStepFixture,
} from "./capture.ts";

describe("redactRequestHeaders", () => {
  test("replaces x-goog-api-key value with <redacted>", () => {
    const headers = {
      "content-type": "application/json",
      "x-goog-api-key": "AIzaSyABCDEF",
    };
    const out = redactRequestHeaders(headers);
    expect(out["x-goog-api-key"]).toBe("<redacted>");
    expect(out["content-type"]).toBe("application/json");
  });

  test("matches the API key header case-insensitively", () => {
    for (const variant of [
      "X-Goog-Api-Key",
      "x-goog-api-key",
      "X-GOOG-API-KEY",
    ]) {
      const out = redactRequestHeaders({
        [variant]: "AIzaSyABCDEF",
      });
      expect(out[variant]).toBe("<redacted>");
    }
  });

  test("preserves the header name verbatim while redacting the value", () => {
    const out = redactRequestHeaders({ "X-Goog-Api-Key": "secret" });
    expect(Object.keys(out)).toEqual(["X-Goog-Api-Key"]);
    expect(out["X-Goog-Api-Key"]).toBe("<redacted>");
  });

  test("does not mutate the input map", () => {
    const headers = { "x-goog-api-key": "secret" };
    redactRequestHeaders(headers);
    expect(headers["x-goog-api-key"]).toBe("secret");
  });
});

describe("fixtureDirectoryFor", () => {
  test("produces packages/inference-testing/wire/gemini/<capability>", () => {
    const dir = fixtureDirectoryFor("text-non-streaming");
    expect(
      dir.endsWith(
        "/packages/inference-testing/wire/gemini/text-non-streaming",
      ),
    ).toBe(true);
    expect(dir.startsWith(FIXTURE_ROOT)).toBe(true);
  });
});

describe("buildMetadata", () => {
  test("includes model, endpoint, capturedAt (ISO 8601), scriptVersion", () => {
    const now = new Date("2026-05-20T12:34:56.789Z");
    const meta = buildMetadata({
      capability: "text-non-streaming",
      model: "gemini-2.0-flash",
      endpoint: "generateContent",
      scriptVersion: "1",
      now,
    });
    expect(meta.model).toBe("gemini-2.0-flash");
    expect(meta.endpoint).toBe("generateContent");
    expect(meta.capturedAt).toBe("2026-05-20T12:34:56.789Z");
    expect(meta.scriptVersion).toBe("1");
    expect(meta.capability).toBe("text-non-streaming");
  });
});

describe("teeStreamToBytes", () => {
  test("captures raw bytes from a multi-chunk ReadableStream verbatim", async () => {
    const chunks = [
      new Uint8Array([0x64, 0x61, 0x74, 0x61, 0x3a, 0x20]),
      new Uint8Array([0x7b, 0x7d, 0x0a, 0x0a]),
    ];
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const c of chunks) controller.enqueue(c);
        controller.close();
      },
    });
    const seen: Uint8Array[] = [];
    const merged = await teeStreamToBytes(stream, (c) => {
      seen.push(c);
    });
    expect(merged.byteLength).toBe(10);
    expect(Array.from(merged)).toEqual([
      0x64, 0x61, 0x74, 0x61, 0x3a, 0x20, 0x7b, 0x7d, 0x0a, 0x0a,
    ]);
    expect(seen.length).toBe(2);
  });
});

describe("writeFixture", () => {
  const originalFixtureRoot = FIXTURE_ROOT;
  let workdir: string;

  beforeEach(async () => {
    workdir = await mkdtemp(join(tmpdir(), "gemini-fixture-test-"));
  });

  test("non-streaming writes request/response JSON, headers, and metadata", async () => {
    const capDir = join(workdir, "non-streaming-cap");
    await writeFixtureInto(capDir, {
      capability: "non-streaming-cap",
      model: "gemini-2.0-flash",
      endpoint: "generateContent",
      scriptVersion: "1",
      requestBody: { contents: [{ role: "user", parts: [{ text: "hi" }] }] },
      requestHeaders: {
        "content-type": "application/json",
        "x-goog-api-key": "AIzaSyTEST",
      },
      responseHeaders: { "content-type": "application/json" },
      responseJson: { candidates: [] },
      now: new Date("2026-05-20T00:00:00.000Z"),
    });

    const files = (await readdir(capDir)).sort();
    expect(files).toEqual([
      "metadata.json",
      "request-headers.json",
      "request.json",
      "response-headers.json",
      "response.json",
    ]);

    const reqHeaders = JSON.parse(
      await readFile(join(capDir, "request-headers.json"), "utf8"),
    );
    expect(reqHeaders["x-goog-api-key"]).toBe("<redacted>");
    expect(reqHeaders["content-type"]).toBe("application/json");

    const metadata = JSON.parse(
      await readFile(join(capDir, "metadata.json"), "utf8"),
    );
    expect(metadata.model).toBe("gemini-2.0-flash");
    expect(metadata.endpoint).toBe("generateContent");
    expect(metadata.scriptVersion).toBe("1");
    expect(metadata.capturedAt).toBe("2026-05-20T00:00:00.000Z");

    await rm(workdir, { recursive: true, force: true });
  });

  test("streaming writes response.sse bytes verbatim, not response.json", async () => {
    const capDir = join(workdir, "streaming-cap");
    const bytes = new Uint8Array([
      0x64, 0x61, 0x74, 0x61, 0x3a, 0x20, 0x7b, 0x7d, 0x0a, 0x0a,
    ]);
    await writeFixtureInto(capDir, {
      capability: "streaming-cap",
      model: "gemini-2.0-flash",
      endpoint: "streamGenerateContent",
      scriptVersion: "1",
      requestBody: { contents: [] },
      requestHeaders: { "x-goog-api-key": "AIzaSyTEST" },
      responseHeaders: { "content-type": "text/event-stream" },
      responseBytes: bytes,
    });

    const files = (await readdir(capDir)).sort();
    expect(files).toEqual([
      "metadata.json",
      "request-headers.json",
      "request.json",
      "response-headers.json",
      "response.sse",
    ]);
    const onDisk = await readFile(join(capDir, "response.sse"));
    expect(Array.from(onDisk)).toEqual(Array.from(bytes));

    const stats = await stat(join(capDir, "response.sse"));
    expect(stats.size).toBe(bytes.byteLength);

    await rm(workdir, { recursive: true, force: true });
  });

  test("requires exactly one of responseBytes / responseJson", async () => {
    await expect(
      writeFixture({
        capability: "neither",
        model: "m",
        endpoint: "e",
        scriptVersion: "1",
        requestBody: {},
        requestHeaders: {},
        responseHeaders: {},
      }),
    ).rejects.toThrow(/either responseBytes or responseJson/);

    await expect(
      writeFixture({
        capability: "both",
        model: "m",
        endpoint: "e",
        scriptVersion: "1",
        requestBody: {},
        requestHeaders: {},
        responseHeaders: {},
        responseBytes: new Uint8Array([1]),
        responseJson: {},
      }),
    ).rejects.toThrow(/both responseBytes and responseJson/);
  });

  // Reference originalFixtureRoot so a future change to FIXTURE_ROOT visibility
  // surfaces here; the tests above use writeFixtureInto with explicit dirs so
  // they do not pollute the real fixture tree under the repo.
  test("FIXTURE_ROOT export is stable", () => {
    expect(typeof originalFixtureRoot).toBe("string");
    expect(originalFixtureRoot.length).toBeGreaterThan(0);
  });
});

describe("writeStepFixture", () => {
  let workdir: string;

  beforeEach(async () => {
    workdir = await mkdtemp(join(tmpdir(), "gemini-step-fixture-test-"));
  });

  test("writes per-step files into a turn-N subdirectory and omits per-step metadata", async () => {
    const capDir = join(workdir, "multi-turn-cap");
    const stepDir = await writeStepFixture({
      capability: "multi-turn-cap",
      stepName: "turn-1",
      requestBody: { contents: [{ role: "user", parts: [{ text: "hi" }] }] },
      requestHeaders: {
        "content-type": "application/json",
        "x-goog-api-key": "AIzaSyTEST",
      },
      responseHeaders: { "content-type": "application/json" },
      responseJson: { candidates: [] },
      destinationOverride: capDir,
    });

    expect(stepDir.endsWith("/multi-turn-cap/turn-1")).toBe(true);

    const files = (await readdir(stepDir)).sort();
    expect(files).toEqual([
      "request-headers.json",
      "request.json",
      "response-headers.json",
      "response.json",
    ]);

    const reqHeaders = JSON.parse(
      await readFile(join(stepDir, "request-headers.json"), "utf8"),
    );
    expect(reqHeaders["x-goog-api-key"]).toBe("<redacted>");
    expect(reqHeaders["content-type"]).toBe("application/json");

    await rm(workdir, { recursive: true, force: true });
  });

  test("supports arbitrary, non-turn step names like 'upload' and 'generate'", async () => {
    const capDir = join(workdir, "two-phase-cap");
    const uploadDir = await writeStepFixture({
      capability: "two-phase-cap",
      stepName: "upload",
      requestBody: { file: "abc" },
      requestHeaders: { "x-goog-api-key": "AIzaSyTEST" },
      responseHeaders: { "content-type": "application/json" },
      responseJson: { fileId: "files/123" },
      destinationOverride: capDir,
    });
    const generateDir = await writeStepFixture({
      capability: "two-phase-cap",
      stepName: "generate",
      requestBody: { contents: [{ role: "user", parts: [] }] },
      requestHeaders: { "x-goog-api-key": "AIzaSyTEST" },
      responseHeaders: { "content-type": "application/json" },
      responseJson: { candidates: [] },
      destinationOverride: capDir,
    });

    expect(uploadDir.endsWith("/two-phase-cap/upload")).toBe(true);
    expect(generateDir.endsWith("/two-phase-cap/generate")).toBe(true);

    const top = (await readdir(capDir)).sort();
    expect(top).toEqual(["generate", "upload"]);

    const uploadFiles = (await readdir(uploadDir)).sort();
    expect(uploadFiles).toEqual([
      "request-headers.json",
      "request.json",
      "response-headers.json",
      "response.json",
    ]);

    await rm(workdir, { recursive: true, force: true });
  });

  test("rejects empty step names", async () => {
    await expect(
      writeStepFixture({
        capability: "cap",
        stepName: "",
        requestBody: {},
        requestHeaders: {},
        responseHeaders: {},
        responseJson: {},
        destinationOverride: workdir,
      }),
    ).rejects.toThrow(/non-empty stepName/);
  });

  test("rejects step names containing path separators", async () => {
    await expect(
      writeStepFixture({
        capability: "cap",
        stepName: "turn-1/sneaky",
        requestBody: {},
        requestHeaders: {},
        responseHeaders: {},
        responseJson: {},
        destinationOverride: workdir,
      }),
    ).rejects.toThrow(/path separators/);

    await rm(workdir, { recursive: true, force: true });
  });
});

describe("writeMultiStepMetadata", () => {
  let workdir: string;

  beforeEach(async () => {
    workdir = await mkdtemp(join(tmpdir(), "gemini-multi-meta-test-"));
  });

  test("writes top-level metadata.json including sequence", async () => {
    const capDir = join(workdir, "multi-cap");
    const path = await writeMultiStepMetadata({
      capability: "multi-cap",
      model: "gemini-2.5-flash",
      endpoint: "generateContent",
      scriptVersion: "1",
      sequence: ["turn-1", "turn-2"],
      now: new Date("2026-05-20T00:00:00.000Z"),
      destinationOverride: capDir,
    });
    expect(path.endsWith("/multi-cap/metadata.json")).toBe(true);

    const meta = JSON.parse(await readFile(path, "utf8"));
    expect(meta.capability).toBe("multi-cap");
    expect(meta.model).toBe("gemini-2.5-flash");
    expect(meta.endpoint).toBe("generateContent");
    expect(meta.scriptVersion).toBe("1");
    expect(meta.capturedAt).toBe("2026-05-20T00:00:00.000Z");
    expect(meta.sequence).toEqual(["turn-1", "turn-2"]);

    await rm(workdir, { recursive: true, force: true });
  });

  test("accepts arbitrary step names in the sequence", async () => {
    const capDir = join(workdir, "two-phase");
    await writeMultiStepMetadata({
      capability: "two-phase",
      model: "gemini-2.5-flash",
      endpoint: "generateContent",
      scriptVersion: "1",
      sequence: ["upload", "generate"],
      destinationOverride: capDir,
    });
    const meta = JSON.parse(
      await readFile(join(capDir, "metadata.json"), "utf8"),
    );
    expect(meta.sequence).toEqual(["upload", "generate"]);

    await rm(workdir, { recursive: true, force: true });
  });

  test("rejects empty sequence", async () => {
    await expect(
      writeMultiStepMetadata({
        capability: "cap",
        model: "m",
        endpoint: "e",
        scriptVersion: "1",
        sequence: [],
        destinationOverride: workdir,
      }),
    ).rejects.toThrow(/non-empty sequence/);
  });
});

describe("runStreamingCapture (in-memory source)", () => {
  test("tees a constructed stream to disk verbatim via writeFixture", async () => {
    const workdir = await mkdtemp(join(tmpdir(), "gemini-stream-test-"));
    const chunks = [
      new Uint8Array([0x64, 0x61, 0x74, 0x61, 0x3a, 0x20]),
      new Uint8Array([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x31, 0x7d, 0x0a, 0x0a]),
    ];
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const c of chunks) controller.enqueue(c);
        controller.close();
      },
    });

    const capDir = join(workdir, "in-mem-stream");
    const result = await callRunStreamingCaptureWithRedirectedRoot(
      capDir,
      stream,
    );
    expect(result.bytes.byteLength).toBe(15);
    const onDisk = await readFile(join(capDir, "response.sse"));
    expect(Array.from(onDisk)).toEqual(Array.from(result.bytes));

    await rm(workdir, { recursive: true, force: true });
  });
});

type WriteArgs = Parameters<typeof writeFixture>[0];

async function writeFixtureInto(
  capDir: string,
  args: Omit<WriteArgs, "destinationOverride">,
): Promise<void> {
  await writeFixture({
    ...args,
    destinationOverride: capDir,
  });
}

async function callRunStreamingCaptureWithRedirectedRoot(
  capDir: string,
  stream: ReadableStream<Uint8Array>,
): Promise<{ bytes: Uint8Array }> {
  const out = await runStreamingCapture({
    capability: "test-streaming",
    model: "gemini-2.0-flash",
    endpoint: "streamGenerateContent",
    body: { contents: [] },
    apiKey: "AIzaSyTEST",
    scriptVersion: "1",
    source: {
      stream,
      responseHeaders: { "content-type": "text/event-stream" },
    },
    destinationOverride: capDir,
  });
  return { bytes: out.bytes };
}
