// Event channel: UNIX socketpair, HMAC-SHA256 authenticated.
//
// The workflow-process child sends InferenceEvents (high rate, plus
// the per-message `message.run.started` / `message.run.ended`
// brackets) to the supervisor. The supervisor verifies each frame's
// MAC and forwards into the hub via the existing `agent.event`
// session-channel path. The shared 32-byte HMAC key is minted by the
// supervisor at spawn time and passed to the child in spawn-time env.
// Both sides authenticate every frame: a malformed or tampered frame
// is a crash signal, not a recoverable error.
//
// Backpressure: the supervisor keeps a bounded userspace ring (default
// 1024 frames). On overrun the supervisor logs the saturation, fires
// the caller-supplied crash callback, and the workflow-process kills
// itself on the next signal cycle. Observability lost equals
// invariant violated equals crash; the audit chain cannot tolerate a
// silent drop, and a blocking writer would deadlock against the
// reactor's emit path.
//
// Mixing failure mode: the payload union here covers InferenceEvent
// shapes only. A "control message" structurally shaped as `drain` or
// `recycle` will not satisfy this union and the receiver will crash.
// The discriminated arktype validators in `control-channel.ts` and
// here are disjoint by construction.

import { type } from "arktype";

import { hexDecode, hexEncode } from "@intx/types";
import { InferenceEvent } from "@intx/types/runtime";

import {
  decodeEnvelope,
  encodeEnvelope,
  FrameEnvelope,
  MacedEnvelope,
} from "./envelope";
import { signHmac, verifyHmac } from "./crypto";

/**
 * The event channel additionally carries the two bracket events the
 * reactor emits per message run. They are part of the InferenceEvent
 * union upstream; re-exporting the union directly keeps this channel
 * structurally identical to what the hub's `agent.event` frame
 * carries today.
 */
export const EventPayload = InferenceEvent;
export type EventPayload = typeof EventPayload.infer;

export interface FrameWriter {
  write(bytes: Uint8Array): Promise<void> | void;
}

export interface FrameReader {
  read(): AsyncIterableIterator<Uint8Array>;
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

export interface EventChannelReceiverOpts {
  hmacKey: Uint8Array;
  channelId: string;
  reader: FrameReader;
  /**
   * Userspace bound on the in-flight buffer between substrate read
   * and caller consume. Overrun calls `onCrash` and stops the
   * iterator. Default 1024 matches the discipline documented in
   * the IPC threat model.
   */
  bufferLimit?: number;
  onCrash: (reason: string) => void;
}

/**
 * Construct the supervisor-side event-channel receiver. Yields one
 * verified, in-order `EventPayload` per call. Any frame that fails
 * HMAC verification, carries a non-current channelId, arrives out
 * of order, or arrives faster than the consumer drains -- triggers
 * `onCrash` and ends the iterator.
 */
export async function* receiveEventChannel(
  opts: EventChannelReceiverOpts,
): AsyncGenerator<EventPayload, void, void> {
  const limit = opts.bufferLimit ?? DEFAULT_EVENT_BUFFER_LIMIT;
  let highestSeq = 0;

  const buffer: EventPayload[] = [];
  let crashed = false;
  let producerDone = false;
  let waiter: (() => void) | null = null;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }

  const pump = (async () => {
    // The channel rides a byte-stream pipe: the kernel can coalesce
    // several sender writes into one read chunk or split one write
    // across chunks, so a chunk boundary is not a frame boundary. The
    // sender newline-delimits every frame; this decoder accumulates raw
    // bytes and splits on `\n` so each complete line is exactly one
    // envelope, mirroring the control channel's NDJSON reader. A partial
    // trailing line stays buffered until its terminator arrives.
    const decoder = new TextDecoder("utf-8");
    let pending = "";
    try {
      for await (const chunk of opts.reader.read()) {
        if (crashed) return;
        pending += decoder.decode(chunk, { stream: true });
        let nl = pending.indexOf("\n");
        while (nl >= 0) {
          const line = pending.slice(0, nl).replace(/\r$/, "");
          pending = pending.slice(nl + 1);
          nl = pending.indexOf("\n");
          if (line.length === 0) continue;
          if (crashed) return;

          let raw: unknown;
          try {
            raw = JSON.parse(line);
          } catch (cause) {
            crashed = true;
            opts.onCrash(
              `event channel received non-JSON frame: ${errorMessage(cause)}`,
            );
            wake();
            return;
          }

          const crashedOnLine = await processLine(raw);
          if (crashedOnLine) return;
        }
      }
      // A non-empty trailing buffer at EOF is a truncated final frame:
      // the sender always terminates a frame with `\n`, so unterminated
      // bytes mean the writer died mid-frame. Surface it as a crash
      // rather than silently dropping a partial envelope.
      if (pending.length > 0) {
        crashed = true;
        opts.onCrash(
          `event channel received truncated frame at EOF (${String(pending.length)} bytes, no terminator)`,
        );
        wake();
        return;
      }
    } finally {
      producerDone = true;
      wake();
    }

    // Process one decoded frame through the verify/order/validate
    // pipeline. Returns `true` when the frame tripped a crash (the
    // caller must stop pumping), `false` on a clean buffered push.
    async function processLine(raw: unknown): Promise<boolean> {
      const maced = MacedEnvelope(raw);
      if (maced instanceof type.errors) {
        crashed = true;
        opts.onCrash(
          `event channel envelope failed validation: ${maced.summary}`,
        );
        wake();
        return true;
      }

      let envelopeBytes: Uint8Array;
      try {
        envelopeBytes = encodeEnvelope(maced.envelope);
      } catch (cause) {
        crashed = true;
        opts.onCrash(
          `event channel envelope re-encode failed: ${errorMessage(cause)}`,
        );
        wake();
        return true;
      }

      let macBytes: Uint8Array;
      try {
        macBytes = hexDecode(maced.mac);
      } catch (cause) {
        crashed = true;
        opts.onCrash(`event channel MAC decode failed: ${errorMessage(cause)}`);
        wake();
        return true;
      }

      const ok = await verifyHmac(envelopeBytes, macBytes, opts.hmacKey);
      if (!ok) {
        crashed = true;
        opts.onCrash(
          `event channel HMAC did not verify (seq=${String(maced.envelope.seq)}, channelId=${maced.envelope.channelId})`,
        );
        wake();
        return true;
      }

      if (maced.envelope.channelId !== opts.channelId) {
        crashed = true;
        opts.onCrash(
          `event channel channelId mismatch: expected ${opts.channelId}, got ${maced.envelope.channelId} at seq=${String(maced.envelope.seq)}`,
        );
        wake();
        return true;
      }

      if (maced.envelope.seq <= highestSeq) {
        crashed = true;
        opts.onCrash(
          `event channel out-of-order seq: expected > ${String(highestSeq)}, got ${String(maced.envelope.seq)}`,
        );
        wake();
        return true;
      }
      if (maced.envelope.seq !== highestSeq + 1) {
        crashed = true;
        opts.onCrash(
          `event channel seq gap: expected ${String(highestSeq + 1)}, got ${String(maced.envelope.seq)}`,
        );
        wake();
        return true;
      }
      highestSeq = maced.envelope.seq;

      const payload = EventPayload(maced.envelope.payload);
      if (payload instanceof type.errors) {
        crashed = true;
        opts.onCrash(
          `event channel payload failed validation: ${payload.summary}`,
        );
        wake();
        return true;
      }

      if (buffer.length >= limit) {
        crashed = true;
        opts.onCrash(
          `event channel buffer overrun: ${String(buffer.length)} frames pending, limit ${String(limit)}`,
        );
        wake();
        return true;
      }
      buffer.push(payload);
      wake();
      return false;
    }
  })();

  try {
    while (true) {
      if (buffer.length > 0) {
        const next = buffer.shift();
        if (next === undefined) {
          throw new Error(
            "event channel receiver invariant: shift returned undefined despite non-empty buffer",
          );
        }
        yield next;
        continue;
      }
      if (crashed || producerDone) {
        return;
      }
      await new Promise<void>((resolve) => {
        waiter = resolve;
      });
    }
  } finally {
    await pump;
  }
}

function errorMessage(cause: unknown): string {
  if (cause instanceof Error) return cause.message;
  return String(cause);
}

export const DEFAULT_EVENT_BUFFER_LIMIT = 1024;

/**
 * Re-export the envelope decoder for callers that need raw access
 * to a frame's envelope outside the receiver iterator.
 */
export { decodeEnvelope };
