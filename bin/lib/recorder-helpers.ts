import type { InferenceEvent } from "@intx/types/runtime";

// Frame recorded SSE chunk bytes into a single text/event-stream Response —
// the shape both recording scripts and the live-replay test feed back
// through the harness.
export function sseResponse(chunks: Uint8Array[]): Response {
  let total = 0;
  for (const c of chunks) total += c.byteLength;
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    merged.set(c, offset);
    offset += c.byteLength;
  }
  return new Response(merged, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

// Pull the terminal inference.done off an event stream, attaching any
// inference.error detail so a run that never reached done is diagnosable
// rather than failing with a bare "expected inference.done".
export function expectDone(
  events: InferenceEvent[],
  label: string,
): InferenceEvent & { type: "inference.done" } {
  const done = events.find((e) => e.type === "inference.done");
  if (done === undefined || done.type !== "inference.done") {
    const error = events.find((e) => e.type === "inference.error");
    const detail =
      error !== undefined && error.type === "inference.error"
        ? `: ${JSON.stringify(error.data)}`
        : "";
    throw new Error(`${label}: expected inference.done event${detail}`);
  }
  return done;
}
