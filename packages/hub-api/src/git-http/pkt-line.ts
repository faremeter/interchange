/**
 * pkt-line framing as defined by Git's
 * `Documentation/technical/protocol-common.txt`. A pkt-line is a
 * 4-byte ASCII hex length header followed by `length - 4` bytes of
 * payload. Two reserved special headers carry no payload:
 *
 * - `0000` flush packet (end of a logical message)
 * - `0001` delim packet (separator inside a message)
 *
 * Maximum total frame length is 0xFFF0; thus the maximum payload
 * size is 0xFFF0 - 4 = 65520.
 */

const HEADER_BYTES = 4;
const MAX_FRAME_LENGTH = 0xfff0;

export const PKT_LINE_MAX_PAYLOAD = MAX_FRAME_LENGTH - HEADER_BYTES; // 65520

export type PktLine =
  | { kind: "flush" }
  | { kind: "delim" }
  | { kind: "data"; payload: Uint8Array };

function hexDigit(v: number): string {
  if (v < 10) return String.fromCharCode(0x30 + v);
  return String.fromCharCode(0x61 + (v - 10));
}

function hex4(n: number): string {
  return (
    hexDigit((n >> 12) & 0xf) +
    hexDigit((n >> 8) & 0xf) +
    hexDigit((n >> 4) & 0xf) +
    hexDigit(n & 0xf)
  );
}

function parseHex4(s: string): number {
  let v = 0;
  for (let i = 0; i < 4; i++) {
    const c = s.charCodeAt(i);
    let d: number;
    if (c >= 0x30 && c <= 0x39) {
      d = c - 0x30;
    } else if (c >= 0x61 && c <= 0x66) {
      d = c - 0x61 + 10;
    } else if (c >= 0x41 && c <= 0x46) {
      d = c - 0x41 + 10;
    } else {
      throw new Error(`malformed pkt-line length: ${JSON.stringify(s)}`);
    }
    v = (v << 4) | d;
  }
  return v;
}

function concatBytes(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

function sliceBytes(a: Uint8Array, start: number, end?: number): Uint8Array {
  const e = end ?? a.length;
  const out = new Uint8Array(e - start);
  out.set(a.subarray(start, e), 0);
  return out;
}

/**
 * Reader interface accepted by `readPktLine`: any object that yields
 * `{ value, done }` results when `.read()` is called. The DOM
 * `ReadableStreamDefaultReader<Uint8Array>` and the Node
 * `node:stream/web` reader both satisfy this shape, so callers can
 * pass `stream.getReader()` directly without type-juggling between
 * the two reader flavours.
 */
export interface PktLineByteReader {
  read(): Promise<{ done: boolean; value?: Uint8Array | undefined }>;
}

/**
 * Buffered reader that exposes byte-exact reads and tolerates
 * underlying chunks of any size, including chunks that split a
 * pkt-line header or body.
 */
class BufferedByteReader {
  private buf: Uint8Array = new Uint8Array(0);
  private done = false;

  constructor(private readonly reader: PktLineByteReader) {}

  private async pull(): Promise<void> {
    if (this.done) return;
    const r = await this.reader.read();
    if (r.done) {
      this.done = true;
      return;
    }
    const value = r.value;
    if (value && value.length > 0) {
      this.buf =
        this.buf.length === 0
          ? new Uint8Array(value)
          : concatBytes(this.buf, value);
    }
  }

  async readExact(n: number): Promise<Uint8Array | null> {
    while (this.buf.length < n) {
      if (this.done) {
        if (this.buf.length === 0) return null;
        throw new Error(
          `truncated pkt-line: needed ${n} bytes, got ${this.buf.length}`,
        );
      }
      await this.pull();
    }
    const out = sliceBytes(this.buf, 0, n);
    this.buf = sliceBytes(this.buf, n);
    return out;
  }

  async readExactOrThrow(n: number, what: string): Promise<Uint8Array> {
    while (this.buf.length < n) {
      if (this.done) {
        throw new Error(
          `truncated pkt-line ${what}: needed ${n} bytes, got ${this.buf.length}`,
        );
      }
      await this.pull();
    }
    const out = sliceBytes(this.buf, 0, n);
    this.buf = sliceBytes(this.buf, n);
    return out;
  }
}

const READER_BUFFER = new WeakMap<PktLineByteReader, BufferedByteReader>();

function bufferFor(r: PktLineByteReader): BufferedByteReader {
  let b = READER_BUFFER.get(r);
  if (!b) {
    b = new BufferedByteReader(r);
    READER_BUFFER.set(r, b);
  }
  return b;
}

export async function readPktLine(
  r: PktLineByteReader,
): Promise<PktLine | null> {
  const buf = bufferFor(r);
  const header = await buf.readExact(HEADER_BYTES);
  if (header === null) return null;
  const h0 = header[0];
  const h1 = header[1];
  const h2 = header[2];
  const h3 = header[3];
  if (
    h0 === undefined ||
    h1 === undefined ||
    h2 === undefined ||
    h3 === undefined
  ) {
    throw new Error("truncated pkt-line: short header");
  }
  const headerStr = String.fromCharCode(h0, h1, h2, h3);
  const length = parseHex4(headerStr);
  if (length === 0) return { kind: "flush" };
  if (length === 1) return { kind: "delim" };
  if (length === 2 || length === 3) {
    throw new Error(`reserved pkt-line length: ${headerStr}`);
  }
  if (length > MAX_FRAME_LENGTH) {
    throw new Error(`oversize pkt-line: length ${length}`);
  }
  const bodyLen = length - HEADER_BYTES;
  const payload =
    bodyLen === 0
      ? new Uint8Array(0)
      : await buf.readExactOrThrow(bodyLen, "body");
  return { kind: "data", payload };
}

function encodePayload(payload: string | Uint8Array): Uint8Array {
  if (typeof payload === "string") return new TextEncoder().encode(payload);
  return payload;
}

export async function writePktLine(
  w: WritableStreamDefaultWriter<Uint8Array>,
  payload: string | Uint8Array,
): Promise<void> {
  const body = encodePayload(payload);
  if (body.length > PKT_LINE_MAX_PAYLOAD) {
    throw new Error(
      `oversize pkt-line payload: ${body.length} > ${PKT_LINE_MAX_PAYLOAD}`,
    );
  }
  const length = body.length + HEADER_BYTES;
  const header = new TextEncoder().encode(hex4(length));
  const frame = new Uint8Array(header.length + body.length);
  frame.set(header, 0);
  frame.set(body, header.length);
  await w.write(frame);
}

export async function writeFlush(
  w: WritableStreamDefaultWriter<Uint8Array>,
): Promise<void> {
  await w.write(new TextEncoder().encode("0000"));
}

export async function writeDelim(
  w: WritableStreamDefaultWriter<Uint8Array>,
): Promise<void> {
  await w.write(new TextEncoder().encode("0001"));
}

/**
 * Emit an ERR pkt-line as used during upload-pack negotiation. Stock
 * git surfaces this as `remote: <msg>; fatal: protocol error`. The
 * frame body is `ERR <msg>\n`, encoded as a normal data pkt-line.
 *
 * Receive-pack reports failures with `ng <ref> <msg>` during the
 * report-status phase instead; do not use writeErr there.
 */
export async function writeErr(
  w: WritableStreamDefaultWriter<Uint8Array>,
  msg: string,
): Promise<void> {
  await writePktLine(w, `ERR ${msg}\n`);
}
