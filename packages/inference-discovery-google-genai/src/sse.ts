// Gemini streams generateContent as Server-Sent Events: each `data: {json}`
// line is a full GenerateContentResponse chunk whose candidates[0].content.parts
// carry incremental deltas. To build a turn-2 multi-turn body for a streaming
// capability, the assistant content has to be reconstructed from those chunks.
//
// Reconstruction flattens every chunk's parts in order and coalesces
// consecutive text deltas that share the same shape — plain text (`{text}`)
// with plain text, and thought text (`{text, thought: true}`) with thought
// text — but never across shapes. Every non-text part (a functionCall, or any
// part carrying a thoughtSignature) is emitted as-is, in order, so the
// signature the API requires on an echoed thinking turn survives.

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// The coalescing signature of a streamed text delta, or null when the part is
// not a plain text delta (a functionCall, a thoughtSignature-bearing part, …)
// and must therefore stand as its own part.
function textDeltaSignature(part: Record<string, unknown>): string | null {
  if (typeof part.text !== "string") return null;
  const keys = Object.keys(part);
  if (keys.length === 1) return "text";
  if (keys.length === 2 && part.thought === true) return "thought-text";
  // A text delta carrying any further key (notably a thoughtSignature) is not
  // coalescible: it stays its own part so the signature's exact placement in
  // the thought stream survives verbatim into the echoed turn-2 content.
  return null;
}

function sseDataPayloads(bytes: Uint8Array): string[] {
  const text = new TextDecoder().decode(bytes);
  const payloads: string[] = [];
  for (const line of text.split(/\r?\n/)) {
    if (!line.startsWith("data:")) continue;
    const payload = line.slice("data:".length).trim();
    if (payload.length > 0 && payload !== "[DONE]") payloads.push(payload);
  }
  return payloads;
}

// Reconstructs the shape a non-streaming turn-1 response would have —
// `{ candidates: [{ content: { role, parts } }] }` — from a Gemini SSE stream,
// so the multi-turn turn-2 builder consumes it unchanged.
export function reconstructResponseFromSSE(bytes: Uint8Array): unknown {
  const payloads = sseDataPayloads(bytes);
  if (payloads.length === 0) {
    throw new Error(
      "google-genai SSE: stream carried no data payloads to reconstruct",
    );
  }
  let role: string | undefined;
  const parts: Record<string, unknown>[] = [];
  let lastSignature: string | null = null;
  for (const payload of payloads) {
    const chunk: unknown = JSON.parse(payload);
    if (!isRecord(chunk)) {
      throw new Error("google-genai SSE: chunk is not a JSON object");
    }
    const candidates = chunk.candidates;
    // Some trailing chunks carry only usageMetadata and no candidates.
    if (candidates === undefined) continue;
    if (!Array.isArray(candidates) || candidates.length === 0) {
      throw new Error(
        "google-genai SSE: chunk.candidates is not a non-empty array",
      );
    }
    const first = candidates[0];
    if (!isRecord(first)) {
      throw new Error("google-genai SSE: candidates[0] is not an object");
    }
    const content = first.content;
    // A finishReason-only chunk closes the candidate without new content.
    if (content === undefined) continue;
    if (!isRecord(content)) {
      throw new Error(
        "google-genai SSE: candidates[0].content is not an object",
      );
    }
    if (typeof content.role === "string") role = content.role;
    const chunkParts = content.parts;
    if (chunkParts === undefined) continue;
    if (!Array.isArray(chunkParts)) {
      throw new Error(
        "google-genai SSE: candidates[0].content.parts is not an array",
      );
    }
    for (const part of chunkParts) {
      if (!isRecord(part)) {
        throw new Error("google-genai SSE: a content part is not an object");
      }
      const signature = textDeltaSignature(part);
      const previous = parts[parts.length - 1];
      if (
        signature !== null &&
        signature === lastSignature &&
        previous !== undefined
      ) {
        previous.text = `${String(previous.text)}${String(part.text)}`;
      } else {
        parts.push({ ...part });
        lastSignature = signature;
      }
    }
  }
  // The assistant role must come off the wire; defaulting it would fabricate a
  // turn-1 shape the model never sent and mask a provider change the probe
  // exists to catch. Gemini emits role on the first content-bearing chunk.
  if (role === undefined) {
    throw new Error(
      "google-genai SSE: no candidate content carried a role; cannot reconstruct the assistant turn",
    );
  }
  return { candidates: [{ content: { role, parts } }] };
}
