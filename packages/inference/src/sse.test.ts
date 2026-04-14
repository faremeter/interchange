import { describe, test, expect } from "bun:test";
import { parseSSE } from "./sse";

function makeStream(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
}

async function collectSSE(
  stream: ReadableStream<Uint8Array>,
): Promise<string[]> {
  const results: string[] = [];
  for await (const data of parseSSE(stream)) {
    results.push(data);
  }
  return results;
}

describe("parseSSE", () => {
  test("parses a single complete data line", async () => {
    const stream = makeStream(["data: hello\n\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["hello"]);
  });

  test("parses multiple data events", async () => {
    const stream = makeStream(["data: first\n\ndata: second\n\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["first", "second"]);
  });

  test("strips the optional space after data:", async () => {
    const stream = makeStream(["data:nospace\n\ndata: withspace\n\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["nospace", "withspace"]);
  });

  test("skips comment lines", async () => {
    const stream = makeStream([": this is a comment\ndata: payload\n\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["payload"]);
  });

  test("skips blank lines between events", async () => {
    const stream = makeStream(["data: a\n\n\ndata: b\n\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["a", "b"]);
  });

  test("stops at [DONE] sentinel", async () => {
    const stream = makeStream(["data: a\n\ndata: [DONE]\n\ndata: b\n\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["a"]);
  });

  test("handles CRLF line endings", async () => {
    const stream = makeStream(["data: hello\r\n\r\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["hello"]);
  });

  test("handles data split across chunks", async () => {
    // The data line is split mid-word across two chunks.
    const stream = makeStream(["data: hel", "lo\n\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["hello"]);
  });

  test("handles newline split across chunks", async () => {
    const stream = makeStream(["data: hello\n", "\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["hello"]);
  });

  test("handles many small chunks", async () => {
    const stream = makeStream([
      "d",
      "a",
      "t",
      "a",
      ":",
      " ",
      "t",
      "o",
      "k",
      "e",
      "n",
      "\n",
      "\n",
    ]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["token"]);
  });

  test("ignores non-data field lines", async () => {
    const stream = makeStream([
      "event: content_block_delta\ndata: payload\n\n",
    ]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["payload"]);
  });

  test("handles empty stream", async () => {
    const stream = makeStream([]);
    const results = await collectSSE(stream);
    expect(results).toEqual([]);
  });

  test("handles stream with only comments", async () => {
    const stream = makeStream([": ping\n: ping\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual([]);
  });

  test("parses JSON data payloads", async () => {
    const stream = makeStream(['data: {"type":"text","text":"hello"}\n\n']);
    const results = await collectSSE(stream);
    expect(results).toEqual(['{"type":"text","text":"hello"}']);
  });

  test("handles multiple events in one chunk", async () => {
    const stream = makeStream(["data: one\n\ndata: two\n\ndata: three\n\n"]);
    const results = await collectSSE(stream);
    expect(results).toEqual(["one", "two", "three"]);
  });
});
