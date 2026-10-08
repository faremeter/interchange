// Control-channel sender: Ed25519-signed NDJSON over stdio. Each line carries
// { envelope: { seq, channelId, payload }, sig: <hex signature> }, with the
// signature covering the envelope's canonical bytes. Each direction uses its
// sender's private key; control-receiver.ts verifies against the peer's key.

import { hexEncode } from "@intx/types/hex";
import {
  encodeEnvelope,
  type FrameEnvelope,
  type SignedEnvelope,
} from "./envelope";
import { signEd25519 } from "./crypto";
import type { ControlPayload } from "./control-payloads";

export interface NdjsonWriter {
  write(line: string): Promise<void> | void;
}

export interface NdjsonReader {
  read(): AsyncIterableIterator<string>;
}

export interface ControlChannelSenderOpts {
  privateKeySeed: Uint8Array;
  channelId: string;
  writer: NdjsonWriter;
}

export interface ControlChannelSender {
  send(payload: ControlPayload): Promise<void>;
  readonly seq: number;
}

/**
 * JSON has no NaN or Infinity: `JSON.stringify` emits `null` for both.
 * The receiver then rejects the frame and crashes the child. Refuse
 * before `seq` advances. A skipped sequence is itself a crash on the
 * next frame.
 */
function rejectNonFiniteNumbers(value: unknown, path: string): void {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(
        `control channel: cannot encode non-finite number at ${path}`,
      );
    }
    return;
  }
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      rejectNonFiniteNumbers(value[index], `${path}[${String(index)}]`);
    }
    return;
  }
  if (typeof value === "object" && value !== null) {
    for (const [key, child] of Object.entries(value)) {
      rejectNonFiniteNumbers(child, `${path}.${key}`);
    }
  }
}

/**
 * Construct the supervisor-side control-channel sender. The
 * supervisor's Ed25519 seed lives in closure. The matching public
 * key flows to the child through spawn-time env -- never the seed.
 */
export function createControlChannelSender(
  opts: ControlChannelSenderOpts,
): ControlChannelSender {
  let seq = 0;
  // Serialize sends. Signing is async, so without a lock two concurrent
  // callers could each assign seq, suspend on `signEd25519`, and resume in
  // signature-resolution order — writing frames out of seq order, which the
  // receiver rejects as a gap and crashes the channel. The promise chain
  // makes each send await the previous send's completion before it assigns
  // seq, signs, and writes, keeping that critical section atomic.
  let tail: Promise<void> = Promise.resolve();
  return {
    get seq() {
      return seq;
    },
    send(payload: ControlPayload): Promise<void> {
      const previous = tail;
      let release: () => void = () => undefined;
      tail = new Promise<void>((resolve) => {
        release = resolve;
      });
      return (async () => {
        await previous;
        try {
          rejectNonFiniteNumbers(payload, "payload");
          seq += 1;
          const envelope: FrameEnvelope = {
            seq,
            channelId: opts.channelId,
            payload,
          };
          const envelopeBytes = encodeEnvelope(envelope);
          const sig = await signEd25519(envelopeBytes, opts.privateKeySeed);
          const signed: SignedEnvelope = {
            envelope,
            sig: hexEncode(sig),
          };
          await opts.writer.write(JSON.stringify(signed) + "\n");
        } finally {
          release();
        }
      })();
    },
  };
}
