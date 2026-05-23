import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { writeCapture } from "./write-capture";

async function makeTempDir(): Promise<string> {
  return await fs.mkdtemp(path.join(os.tmpdir(), "write-capture-test-"));
}

describe("writeCapture", () => {
  let dir: string;

  beforeEach(async () => {
    dir = await makeTempDir();
  });

  afterEach(async () => {
    await fs.rm(dir, { recursive: true, force: true });
  });

  test("writes JSON request body and response/metadata files", async () => {
    await writeCapture(dir, {
      request: {
        kind: "json",
        body: { messages: [{ role: "user", content: "hi" }] },
      },
      requestHeaders: {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": "secret",
      },
      redactRequestHeaders: ["x-goog-api-key"],
      response: { kind: "json", body: { text: "hello" } },
      responseHeaders: { "Content-Type": "application/json" },
      redactResponseHeaders: [],
    });

    const entries = (await fs.readdir(dir)).sort();
    expect(entries).toEqual([
      "request-headers.json",
      "request.json",
      "response-headers.json",
      "response.json",
    ]);

    const requestHeaders = JSON.parse(
      await fs.readFile(path.join(dir, "request-headers.json"), "utf8"),
    );
    expect(requestHeaders["X-Goog-Api-Key"]).toBe("<REDACTED>");
    expect(requestHeaders["Content-Type"]).toBe("application/json");

    const body = JSON.parse(
      await fs.readFile(path.join(dir, "response.json"), "utf8"),
    );
    expect(body).toEqual({ text: "hello" });
  });

  test("writes a raw request body to request.bin verbatim", async () => {
    const bytes = new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e]);
    await writeCapture(dir, {
      request: {
        kind: "raw",
        bytes,
        contentType: "application/pdf",
      },
      requestHeaders: {
        "Content-Type": "application/pdf",
        "X-Goog-Api-Key": "secret",
      },
      redactRequestHeaders: ["x-goog-api-key"],
      response: { kind: "json", body: { fileId: "abc" } },
      responseHeaders: { "Content-Type": "application/json" },
      redactResponseHeaders: [],
    });

    const entries = (await fs.readdir(dir)).sort();
    expect(entries).toEqual([
      "request-headers.json",
      "request.bin",
      "response-headers.json",
      "response.json",
    ]);

    const written = await fs.readFile(path.join(dir, "request.bin"));
    expect(Array.from(written)).toEqual(Array.from(bytes));

    const requestHeaders = JSON.parse(
      await fs.readFile(path.join(dir, "request-headers.json"), "utf8"),
    );
    expect(requestHeaders["X-Goog-Api-Key"]).toBe("<REDACTED>");
    expect(requestHeaders["Content-Type"]).toBe("application/pdf");
  });

  test("writes SSE response as response.sse and not response.json", async () => {
    const bytes = new TextEncoder().encode("data: hello\n\n");
    await writeCapture(dir, {
      request: { kind: "json", body: {} },
      requestHeaders: {},
      redactRequestHeaders: [],
      response: { kind: "sse", bytes },
      responseHeaders: { "Content-Type": "text/event-stream" },
      redactResponseHeaders: [],
    });
    const entries = (await fs.readdir(dir)).sort();
    expect(entries).toContain("response.sse");
    expect(entries).not.toContain("response.json");
    const written = await fs.readFile(path.join(dir, "response.sse"));
    expect(new TextDecoder().decode(written)).toBe("data: hello\n\n");
  });

  test("redaction is case-insensitive on header names", async () => {
    await writeCapture(dir, {
      request: { kind: "json", body: {} },
      requestHeaders: { Authorization: "Bearer xyz" },
      redactRequestHeaders: ["AUTHORIZATION"],
      response: { kind: "json", body: {} },
      responseHeaders: { "Set-Cookie": "abc=1" },
      redactResponseHeaders: ["set-cookie"],
    });

    const requestHeaders = JSON.parse(
      await fs.readFile(path.join(dir, "request-headers.json"), "utf8"),
    );
    expect(requestHeaders.Authorization).toBe("<REDACTED>");

    const responseHeaders = JSON.parse(
      await fs.readFile(path.join(dir, "response-headers.json"), "utf8"),
    );
    expect(responseHeaders["Set-Cookie"]).toBe("<REDACTED>");
  });

  test("creates target directory if it does not exist", async () => {
    const nested = path.join(dir, "a", "b", "c");
    await writeCapture(nested, {
      request: { kind: "json", body: {} },
      requestHeaders: {},
      redactRequestHeaders: [],
      response: { kind: "json", body: {} },
      responseHeaders: {},
      redactResponseHeaders: [],
    });
    const entries = await fs.readdir(nested);
    expect(entries).toContain("request.json");
  });
});
