import { describe, test, expect } from "bun:test";

import { createMemoryNdjsonStream } from "./memory-streams";
import { waitForUpstreamPayload } from "./upstream-frames";

const CHANNEL = "0123456789abcdef";

/**
 * One `trigger.fire` frame in the control channel's NDJSON wire shape.
 *
 * `readPayloadsOfType` validates structure only -- it never checks the
 * signature -- so `sig` is a placeholder rather than a real one.
 */
function triggerFireLine(seq: number, runId: string): string {
  return JSON.stringify({
    envelope: {
      seq,
      channelId: CHANNEL,
      payload: {
        type: "trigger.fire",
        data: {
          runId,
          messageId: `msg-${runId}`,
          receivedAt: 0,
          payload: null,
        },
      },
    },
    sig: "00",
  });
}

describe("waitForUpstreamPayload", () => {
  test("is satisfied by a frame already in the buffer", async () => {
    const stream = createMemoryNdjsonStream();
    await stream.writer.write(triggerFireLine(1, "run-a"));

    // The level-triggered property: the buffer is re-read on every pass, so a
    // frame written before the call resolves it. An edge wait on the next
    // write hangs here instead, because nothing further is ever written to
    // this stream.
    const found = await waitForUpstreamPayload(stream, "trigger.fire");
    expect(found.data.runId).toBe("run-a");
  });

  test("resolves on a frame written after the call", async () => {
    const stream = createMemoryNdjsonStream();
    const waited = waitForUpstreamPayload(stream, "trigger.fire");
    await stream.writer.write(triggerFireLine(1, "run-b"));
    expect((await waited).data.runId).toBe("run-b");
  });
});
