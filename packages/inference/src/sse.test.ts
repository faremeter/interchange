import { describe, test, expect } from "bun:test";
import { parseSSE } from "./sse";

const encoder = new TextEncoder();

function makeStream(chunks: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
}

// Answers each read only when the test sends a chunk, so a test can order
// reads across streams. A pending read proves the parser has finished with
// every chunk sent before it.
function createScriptedStream() {
  const reads: ((chunk: Uint8Array, end: boolean) => void)[] = [];
  let onRead: (() => void) | undefined;
  const stream = new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        return new Promise<void>((resolve) => {
          reads.push((chunk, end) => {
            controller.enqueue(chunk);
            if (end) controller.close();
            resolve();
          });
          onRead?.();
        });
      },
    },
    { highWaterMark: 0 },
  );
  async function readPending(): Promise<void> {
    while (reads.length === 0) {
      await new Promise<void>((resolve) => {
        onRead = resolve;
      });
    }
  }
  async function send(chunk: Uint8Array, end = false): Promise<void> {
    await readPending();
    const answer = reads.shift();
    if (answer === undefined) throw new Error("no pending read");
    answer(chunk, end);
  }
  return { stream, readPending, send };
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

  test("keeps decoder state separate across concurrent streams", async () => {
    const bytesA = encoder.encode("data: \u00e9\n");
    const cut = bytesA.indexOf(0xc3) + 1;
    const a = createScriptedStream();
    const b = createScriptedStream();
    const iterA = parseSSE(a.stream)[Symbol.asyncIterator]();
    const iterB = parseSSE(b.stream)[Symbol.asyncIterator]();

    const firstA = iterA.next();
    await a.send(bytesA.subarray(0, cut));
    await a.readPending();

    const firstB = iterB.next();
    await b.send(encoder.encode("data: other\n"), true);
    expect(await firstB).toEqual({ done: false, value: "other" });
    expect(await iterB.next()).toEqual({ done: true, value: undefined });

    await a.send(bytesA.subarray(cut), true);
    expect(await firstA).toEqual({ done: false, value: "\u00e9" });
    expect(await iterA.next()).toEqual({ done: true, value: undefined });
  });
});
