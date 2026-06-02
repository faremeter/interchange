/**
 * Side-band-64k framing for git smart-HTTP. Each outbound frame is a
 * pkt-line whose first body byte is a channel marker:
 *
 * - channel 1: pack data
 * - channel 2: progress messages (shown by stock git as `remote: ...`)
 * - channel 3: fatal error; the remote terminates the transfer
 *
 * The maximum pkt-line payload is 0xFFF0 (65520) bytes including the
 * channel marker, so each channel-1 frame can carry at most
 * `SIDE_BAND_CHANNEL_MAX_PAYLOAD` bytes of pack data.
 */

const PKT_LINE_HEADER_BYTES = 4;
const PKT_LINE_MAX_FRAME = 0xfff0;

export const SIDE_BAND_CHANNEL_MAX_PAYLOAD =
  PKT_LINE_MAX_FRAME - PKT_LINE_HEADER_BYTES - 1; // 65515

export type SideBandChannel = 1 | 2 | 3;

export type ChunkPackToSideBandOpts = {
  /** Progress messages emitted on channel 2 before pack data. */
  progress?: string[];
  /**
   * If set, the source stream is abandoned and a single channel-3
   * fatal frame is emitted after any channel-2 progress messages.
   */
  fatal?: string;
};

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

function frame(channel: SideBandChannel, body: Uint8Array): Uint8Array {
  const length = body.length + 1 + PKT_LINE_HEADER_BYTES;
  if (length > PKT_LINE_MAX_FRAME) {
    throw new Error(
      `side-band frame too large: ${length} > ${PKT_LINE_MAX_FRAME}`,
    );
  }
  const out = new Uint8Array(length);
  const header = new TextEncoder().encode(hex4(length));
  out.set(header, 0);
  out[PKT_LINE_HEADER_BYTES] = channel;
  out.set(body, PKT_LINE_HEADER_BYTES + 1);
  return out;
}

function textFrame(channel: SideBandChannel, msg: string): Uint8Array {
  return frame(channel, new TextEncoder().encode(msg));
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
 * Wrap a packfile byte stream in side-band-64k frames. Channel-1
 * frames are emitted in order, each at most
 * `SIDE_BAND_CHANNEL_MAX_PAYLOAD` bytes. Optional progress strings
 * are emitted on channel 2 before the first channel-1 frame. If
 * `fatal` is set, the source stream is not read; a single channel-3
 * frame carrying the error message is emitted after any progress
 * messages and the output ends.
 */
export function chunkPackToSideBand(
  pack: ReadableStream<Uint8Array>,
  opts: ChunkPackToSideBandOpts = {},
): ReadableStream<Uint8Array> {
  const progress = opts.progress ?? [];
  const fatal = opts.fatal;

  return new ReadableStream<Uint8Array>({
    async start(controller) {
      try {
        for (const msg of progress) {
          controller.enqueue(textFrame(2, msg));
        }
        if (fatal !== undefined) {
          controller.enqueue(textFrame(3, fatal));
          await pack.cancel().catch(() => undefined);
          controller.close();
          return;
        }
        const reader = pack.getReader();
        let pending: Uint8Array = new Uint8Array(0);
        for (;;) {
          const r = await reader.read();
          if (r.done) break;
          const value = r.value;
          if (!value || value.length === 0) continue;
          pending =
            pending.length === 0
              ? new Uint8Array(value)
              : concatBytes(pending, value);
          while (pending.length >= SIDE_BAND_CHANNEL_MAX_PAYLOAD) {
            controller.enqueue(
              frame(1, sliceBytes(pending, 0, SIDE_BAND_CHANNEL_MAX_PAYLOAD)),
            );
            pending = sliceBytes(pending, SIDE_BAND_CHANNEL_MAX_PAYLOAD);
          }
        }
        if (pending.length > 0) {
          controller.enqueue(frame(1, pending));
        }
        controller.close();
      } catch (cause) {
        controller.error(cause);
      }
    },
  });
}
