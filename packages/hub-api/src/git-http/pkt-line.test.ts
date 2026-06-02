import { describe, test, expect } from "bun:test";
import {
  readPktLine,
  writePktLine,
  writeFlush,
  writeDelim,
  writeErr,
  PKT_LINE_MAX_PAYLOAD,
} from "./pkt-line";
import type { PktLine } from "./pkt-line";

function head(lines: PktLine[]): PktLine {
  const f = lines[0];
  if (!f) throw new Error("expected at least one pkt-line");
  return f;
}

function dataPayload(line: PktLine): Uint8Array {
  if (line.kind !== "data") {
    throw new Error(`expected data pkt-line, got ${line.kind}`);
  }
  return line.payload;
}

function bytes(...parts: (string | Uint8Array)[]): Uint8Array {
  const enc = new TextEncoder();
  const chunks = parts.map((p) => (typeof p === "string" ? enc.encode(p) : p));
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}

function streamOf(...frames: Uint8Array[]): ReadableStream<Uint8Array> {
  let i = 0;
  return new ReadableStream({
    pull(controller) {
      if (i < frames.length) {
        controller.enqueue(frames[i++]);
      } else {
        controller.close();
      }
    },
  });
}

async function readAll(stream: ReadableStream<Uint8Array>): Promise<PktLine[]> {
  const reader = stream.getReader();
  const out: PktLine[] = [];
  for (;;) {
    const line = await readPktLine(reader);
    if (line === null) break;
    out.push(line);
  }
  return out;
}

describe("writePktLine round-trip", () => {
  test("writes a standard data pkt-line with 4-byte hex length prefix", async () => {
    const buf = new Uint8Array(
      await collectWrites((w) => writePktLine(w, "hello\n")),
    );
    const expected = bytes("000ahello\n");
    expect(Array.from(buf)).toEqual(Array.from(expected));
  });

  test("readPktLine parses what writePktLine produces", async () => {
    const payload = bytes(
      await collectWrites((w) => writePktLine(w, "want abc123\n")),
    );
    const lines = await readAll(streamOf(payload));
    expect(lines.length).toBe(1);
    const line = head(lines);
    expect(line.kind).toBe("data");
    expect(new TextDecoder().decode(dataPayload(line))).toBe("want abc123\n");
  });

  test("writeFlush emits 0000", async () => {
    const buf = bytes(await collectWrites((w) => writeFlush(w)));
    expect(new TextDecoder().decode(buf)).toBe("0000");
  });

  test("writeDelim emits 0001", async () => {
    const buf = bytes(await collectWrites((w) => writeDelim(w)));
    expect(new TextDecoder().decode(buf)).toBe("0001");
  });

  test("readPktLine recognises flush packet", async () => {
    const lines = await readAll(streamOf(bytes("0000")));
    expect(lines.length).toBe(1);
    expect(head(lines).kind).toBe("flush");
  });

  test("readPktLine recognises delim packet", async () => {
    const lines = await readAll(streamOf(bytes("0001")));
    expect(lines.length).toBe(1);
    expect(head(lines).kind).toBe("delim");
  });

  test("empty data line (length 0004) is allowed and yields zero-length payload", async () => {
    const lines = await readAll(streamOf(bytes("0004")));
    expect(lines.length).toBe(1);
    const line = head(lines);
    expect(line.kind).toBe("data");
    expect(dataPayload(line).length).toBe(0);
  });

  test("writeErr emits an ERR pkt-line in the canonical shape", async () => {
    const buf = bytes(await collectWrites((w) => writeErr(w, "forbidden ref")));
    expect(new TextDecoder().decode(buf)).toBe("0016ERR forbidden ref\n");
    const lines = await readAll(streamOf(buf));
    expect(lines.length).toBe(1);
    const line = head(lines);
    expect(line.kind).toBe("data");
    expect(new TextDecoder().decode(dataPayload(line))).toBe(
      "ERR forbidden ref\n",
    );
  });
});

describe("readPktLine error handling", () => {
  test("malformed length (non-hex characters) throws", async () => {
    await expect(readAll(streamOf(bytes("zzzzfoo")))).rejects.toThrow(
      /malformed pkt-line length/i,
    );
  });

  test("length below 4 (and not 0000 or 0001) throws", async () => {
    await expect(readAll(streamOf(bytes("0003")))).rejects.toThrow(
      /reserved pkt-line length/i,
    );
  });

  test("truncated body throws", async () => {
    await expect(readAll(streamOf(bytes("0010abc")))).rejects.toThrow(
      /truncated pkt-line/i,
    );
  });

  test("truncated length header throws", async () => {
    await expect(readAll(streamOf(bytes("00")))).rejects.toThrow(
      /truncated pkt-line/i,
    );
  });

  test("oversize payload (length > 65520) throws", async () => {
    // 65521 = 0xFFF1 — one more byte than max pkt-line length
    await expect(readAll(streamOf(bytes("fff1")))).rejects.toThrow(
      /oversize pkt-line/i,
    );
  });
});

describe("readPktLine across stream chunk boundaries", () => {
  test("length header split across chunks", async () => {
    const lines = await readAll(streamOf(bytes("00"), bytes("0aHELLO\n")));
    expect(lines.length).toBe(1);
    const line = head(lines);
    expect(line.kind).toBe("data");
    expect(new TextDecoder().decode(dataPayload(line))).toBe("HELLO\n");
  });

  test("body split across chunks", async () => {
    const lines = await readAll(streamOf(bytes("000aHEL"), bytes("LO\n")));
    expect(lines.length).toBe(1);
    expect(new TextDecoder().decode(dataPayload(head(lines)))).toBe("HELLO\n");
  });

  test("single byte at a time", async () => {
    const frame = bytes("000aHELLO\n");
    const chunks = Array.from(frame).map((b) => new Uint8Array([b]));
    const lines = await readAll(streamOf(...chunks));
    expect(lines.length).toBe(1);
    expect(new TextDecoder().decode(dataPayload(head(lines)))).toBe("HELLO\n");
  });

  test("multiple frames mixed across chunk boundaries", async () => {
    // "want abc" = 8 bytes -> frame length 0x0c ; "have def" same.
    const lines = await readAll(
      streamOf(bytes("000cwant"), bytes(" abc000chave def0000")),
    );
    expect(lines.map((l) => l.kind)).toEqual(["data", "data", "flush"]);
  });
});

describe("writePktLine size constraints", () => {
  test("rejects payload larger than max pkt-line payload", async () => {
    const oversized = "x".repeat(PKT_LINE_MAX_PAYLOAD + 1);
    await expect(
      collectWrites((w) => writePktLine(w, oversized)),
    ).rejects.toThrow(/oversize/i);
  });

  test("accepts payload of exactly max pkt-line payload size", async () => {
    const max = "x".repeat(PKT_LINE_MAX_PAYLOAD);
    const buf = bytes(await collectWrites((w) => writePktLine(w, max)));
    // 0xFFF0 = 65520
    expect(new TextDecoder().decode(buf.slice(0, 4))).toBe("fff0");
    expect(buf.length).toBe(4 + PKT_LINE_MAX_PAYLOAD);
  });
});

async function collectWrites(
  fn: (w: WritableStreamDefaultWriter<Uint8Array>) => Promise<void>,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  const sink = new WritableStream<Uint8Array>({
    write(chunk) {
      chunks.push(chunk);
    },
  });
  const writer = sink.getWriter();
  await fn(writer);
  await writer.close();
  return bytes(...chunks);
}
