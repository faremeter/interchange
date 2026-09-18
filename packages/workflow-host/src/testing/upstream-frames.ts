// Reading and awaiting the control frames a supervisor writes upstream.
//
// A test that drives a supervisor asserts on the frames it emitted, and has
// to wait for the one it cares about first. That wait was written as a
// deadline plus a one-millisecond tick that re-decoded the whole buffer --
// two wall-clock numbers deciding a run whose subject is what a frame
// CONTAINS, never how quickly it appears. Waiting on the write removes the
// window instead of widening it.

import { type } from "arktype";

import { ControlPayload } from "../ipc/control-channel";
import { SignedEnvelope } from "../ipc/envelope";

/**
 * Decode every payload of `type_` from `lines`, in arrival order.
 *
 * Frames that fail envelope or payload validation are skipped, so a test
 * asserting on one payload kind is not fooled by an unrelated frame sharing
 * the stream.
 */
export function readPayloadsOfType<T extends string>(
  lines: readonly string[],
  type_: T,
): Extract<typeof ControlPayload.infer, { type: T }>[] {
  const out: Extract<typeof ControlPayload.infer, { type: T }>[] = [];
  for (const line of lines) {
    let raw: unknown;
    try {
      raw = JSON.parse(line);
    } catch {
      continue;
    }
    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) continue;
    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) continue;
    if (payload.type !== type_) continue;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the arktype narrow above pins the discriminator; the cast walks the union to the matching branch
    out.push(payload as Extract<typeof ControlPayload.infer, { type: T }>);
  }
  return out;
}

/** The runIds carried on every `trigger.fire` frame in `lines`, in order. */
export function parseTriggerFireRunIds(lines: readonly string[]): string[] {
  return readPayloadsOfType(lines, "trigger.fire").map((p) => p.data.runId);
}

/** The part of a stream double this module reads. */
export type UpstreamFrameSource = {
  flushed(): readonly string[];
  nextWrite(): Promise<void>;
};

/**
 * Resolve with the first payload of `type_` that `match` accepts, whether it
 * is already buffered or arrives later.
 *
 * The buffer is re-read only when a line actually arrives, and the wait
 * carries no deadline: a frame that never comes is caught by the lane
 * timeout, per "Synchronizing on State, Not Time" in CONVENTIONS.md.
 */
export async function waitForUpstreamPayload<T extends string>(
  stream: UpstreamFrameSource,
  type_: T,
  match: (
    payload: Extract<typeof ControlPayload.infer, { type: T }>,
  ) => boolean = () => true,
): Promise<Extract<typeof ControlPayload.infer, { type: T }>> {
  for (;;) {
    // Re-read on every pass, so a frame already in the buffer resolves this
    // rather than leaving it waiting for the next write.
    const arrived = stream.nextWrite();
    const found = readPayloadsOfType(stream.flushed(), type_).find(match);
    if (found !== undefined) return found;
    await arrived;
  }
}

/**
 * Resolve once at least `count` payloads of `type_` have been written, with
 * every matching payload in arrival order.
 *
 * The count form exists because most waits here are for the Nth frame rather
 * than for a particular one; a caller wanting a specific frame should use
 * `waitForUpstreamPayload` with a predicate.
 */
export async function waitForUpstreamPayloads<T extends string>(
  stream: UpstreamFrameSource,
  type_: T,
  count = 1,
): Promise<Extract<typeof ControlPayload.infer, { type: T }>[]> {
  for (;;) {
    const arrived = stream.nextWrite();
    const found = readPayloadsOfType(stream.flushed(), type_);
    if (found.length >= count) return found;
    await arrived;
  }
}

/**
 * Resolve once at least `count` `trigger.fire` frames have been written, with
 * every runId seen in order.
 */
export async function waitForTriggerFireRunIds(
  stream: UpstreamFrameSource,
  count: number,
): Promise<string[]> {
  for (;;) {
    const arrived = stream.nextWrite();
    const ids = parseTriggerFireRunIds(stream.flushed());
    if (ids.length >= count) return ids;
    await arrived;
  }
}
