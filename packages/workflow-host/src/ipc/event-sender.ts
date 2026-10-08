import { hexEncode } from "@intx/types/hex";
import type { EventPayload } from "./event-channel";

import {
  encodeEnvelope,
  type FrameEnvelope,
  type MacedEnvelope,
} from "./envelope";
import { signHmac } from "./crypto";

export interface FrameWriter {
  write(bytes: Uint8Array): Promise<void> | void;
}

export interface EventChannelSenderOpts {
  hmacKey: Uint8Array;
  channelId: string;
  writer: FrameWriter;
}

export interface EventChannelSender {
  send(payload: EventPayload): Promise<void>;
  readonly seq: number;
}

/**
 * Construct the child-side event-channel sender. The shared HMAC
 * key lives in closure. The child mints monotonic seq values per
 * channelId; the receiver enforces strict monotonicity.
 */
export function createEventChannelSender(
  opts: EventChannelSenderOpts,
): EventChannelSender {
  let seq = 0;
  // Serialize sends. `signHmac` is async, so without a lock two
  // concurrent callers could each assign seq, suspend on the signer, and
  // resume in HMAC-resolution order — writing frames out of seq order,
  // which the receiver rejects as a gap and crashes the channel. The
  // production caller fires events without awaiting (`void send(event)`),
  // so this is the live case. The promise chain makes each send await the
  // previous send's completion before it assigns seq, signs, and writes,
  // keeping that critical section atomic. Mirrors the control channel's
  // sender, whose Ed25519 signing has the same shape.
  let tail: Promise<void> = Promise.resolve();
  return {
    get seq() {
      return seq;
    },
    send(payload: EventPayload): Promise<void> {
      const previous = tail;
      let release: () => void = () => undefined;
      tail = new Promise<void>((resolve) => {
        release = resolve;
      });
      return (async () => {
        await previous;
        try {
          seq += 1;
          const envelope: FrameEnvelope = {
            seq,
            channelId: opts.channelId,
            payload,
          };
          const envelopeBytes = encodeEnvelope(envelope);
          const tag = await signHmac(envelopeBytes, opts.hmacKey);
          const maced: MacedEnvelope = {
            envelope,
            mac: hexEncode(tag),
          };
          // Newline-delimit each frame. The channel rides a byte-stream
          // pipe (fd3) where the kernel may coalesce successive writes into
          // one read or split one write across reads -- the one-write-equals-
          // one-frame assumption does not hold under the burst of events a
          // real step emits. `JSON.stringify` never emits a literal newline
          // (newlines inside strings are escaped as `\n`), so `\n` is an
          // unambiguous frame terminator the receiver splits on, mirroring
          // the control channel's NDJSON discipline.
          await opts.writer.write(
            new TextEncoder().encode(`${JSON.stringify(maced)}\n`),
          );
        } finally {
          release();
        }
      })();
    },
  };
}
