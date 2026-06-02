import { describe, test, expect } from "bun:test";
import {
  chunkPackToSideBand,
  SIDE_BAND_CHANNEL_MAX_PAYLOAD,
} from "./side-band-64k";
import type { SideBandChannel } from "./side-band-64k";

async function collectStream(
  stream: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  const reader = stream.getReader();
  const parts: Uint8Array[] = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) parts.push(value);
  }
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

type Frame = { channel: SideBandChannel; payload: Uint8Array };

function parseFrames(buf: Uint8Array): Frame[] {
  const dec = new TextDecoder();
  const frames: Frame[] = [];
  let off = 0;
  while (off < buf.length) {
    if (off + 4 > buf.length) {
      throw new Error(`truncated pkt-line length at ${off}`);
    }
    const lenHex = dec.decode(buf.slice(off, off + 4));
    const len = parseInt(lenHex, 16);
    if (Number.isNaN(len)) {
      throw new Error(`malformed pkt-line length: ${lenHex}`);
    }
    if (len === 0) {
      off += 4;
      // flush — side-band stream shouldn't normally end with a bare
      // flush mid-pack, but allow it for completeness.
      continue;
    }
    if (len < 5) {
      throw new Error(`side-band frame too short: ${len}`);
    }
    if (off + len > buf.length) {
      throw new Error(`truncated pkt-line body at ${off}`);
    }
    const channelByte = buf[off + 4];
    if (channelByte !== 1 && channelByte !== 2 && channelByte !== 3) {
      throw new Error(`unknown side-band channel: ${channelByte}`);
    }
    const channel: SideBandChannel = channelByte;
    const payload = buf.slice(off + 5, off + len);
    frames.push({ channel, payload });
    off += len;
  }
  return frames;
}

function packStream(...chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  let i = 0;
  return new ReadableStream({
    pull(controller) {
      if (i < chunks.length) {
        controller.enqueue(chunks[i++]);
      } else {
        controller.close();
      }
    },
  });
}

describe("chunkPackToSideBand channel-1 chunking", () => {
  test("small pack passes through as a single channel-1 frame", async () => {
    const pack = new Uint8Array([1, 2, 3, 4, 5]);
    const stream = chunkPackToSideBand(packStream(pack));
    const buf = await collectStream(stream);
    const frames = parseFrames(buf);
    expect(frames.length).toBe(1);
    const first = frames[0];
    if (!first) throw new Error("expected one frame");
    expect(first.channel).toBe(1);
    expect(Array.from(first.payload)).toEqual([1, 2, 3, 4, 5]);
  });

  test("every channel-1 frame is at most SIDE_BAND_CHANNEL_MAX_PAYLOAD bytes", async () => {
    const size = SIDE_BAND_CHANNEL_MAX_PAYLOAD * 3 + 17;
    const pack = new Uint8Array(size);
    for (let i = 0; i < size; i++) pack[i] = i & 0xff;
    const stream = chunkPackToSideBand(packStream(pack));
    const buf = await collectStream(stream);
    const frames = parseFrames(buf);
    expect(frames.length).toBeGreaterThan(1);
    for (const f of frames) {
      expect(f.channel).toBe(1);
      expect(f.payload.length).toBeLessThanOrEqual(
        SIDE_BAND_CHANNEL_MAX_PAYLOAD,
      );
    }
  });

  test("multi-chunk channel-1 reassembly is byte-exact", async () => {
    const size = SIDE_BAND_CHANNEL_MAX_PAYLOAD * 2 + 511;
    const pack = new Uint8Array(size);
    for (let i = 0; i < size; i++) pack[i] = (i * 7) & 0xff;
    // Feed in randomly sized input chunks; the chunker output must
    // still reassemble byte-equal to the source.
    const inputChunks: Uint8Array[] = [];
    let off = 0;
    while (off < size) {
      const take = Math.min(size - off, 1000);
      inputChunks.push(pack.slice(off, off + take));
      off += take;
    }
    const stream = chunkPackToSideBand(packStream(...inputChunks));
    const buf = await collectStream(stream);
    const frames = parseFrames(buf);
    const reassembled = new Uint8Array(size);
    let r = 0;
    for (const f of frames) {
      expect(f.channel).toBe(1);
      reassembled.set(f.payload, r);
      r += f.payload.length;
    }
    expect(r).toBe(size);
    expect(Array.from(reassembled)).toEqual(Array.from(pack));
  });
});

describe("chunkPackToSideBand channel-2 progress", () => {
  test("progress messages emit on channel 2 interleaved with channel-1 data", async () => {
    const pack = new Uint8Array([10, 20, 30, 40]);
    const stream = chunkPackToSideBand(packStream(pack), {
      progress: ["counting objects", "compressing"],
    });
    const buf = await collectStream(stream);
    const frames = parseFrames(buf);
    const progress = frames
      .filter((f) => f.channel === 2)
      .map((f) => new TextDecoder().decode(f.payload));
    expect(progress).toEqual(["counting objects", "compressing"]);
    const data = frames.filter((f) => f.channel === 1);
    expect(data.length).toBe(1);
    const first = data[0];
    if (!first) throw new Error("expected channel-1 frame");
    expect(Array.from(first.payload)).toEqual([10, 20, 30, 40]);
  });
});

describe("chunkPackToSideBand channel-3 fatal", () => {
  test("fatal error emitted as channel-3 frame and stream ends", async () => {
    const pack = new Uint8Array([1, 2, 3]);
    const stream = chunkPackToSideBand(packStream(pack), {
      fatal: "pack stream aborted",
    });
    const buf = await collectStream(stream);
    const frames = parseFrames(buf);
    const fatal = frames.filter((f) => f.channel === 3);
    expect(fatal.length).toBe(1);
    const fatalFrame = fatal[0];
    if (!fatalFrame) throw new Error("expected channel-3 frame");
    expect(new TextDecoder().decode(fatalFrame.payload)).toBe(
      "pack stream aborted",
    );
    // Fatal terminates: no channel-1 frames must appear after the
    // channel-3 frame in the output.
    const fatalIdx = frames.findIndex((f) => f.channel === 3);
    for (let i = fatalIdx + 1; i < frames.length; i++) {
      const f = frames[i];
      if (!f) continue;
      expect(f.channel).not.toBe(1);
    }
  });
});
