// Gemini streams generateContent as Server-Sent Events: each `data: {json}`
// line is a GenerateContentResponse chunk whose candidates[0].content.parts
// carry incremental deltas, so a turn-2 multi-turn body must reconstruct the
// assistant content from those chunks.
//
// Reconstruction flattens the parts in order and coalesces consecutive text
// deltas of the same shape (plain with plain, thought with thought), never
// across shapes. Non-text parts (a functionCall, a thoughtSignature-bearing
// part) are emitted as-is so the signature the API requires on an echoed
// thinking turn survives.

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// Coalescing signature of a streamed text delta, or null when the part is not
// a plain text delta and must stand alone.
function textDeltaSignature(part: Record<string, unknown>): string | null {
  if (typeof part.text !== "string") return null;
  const keys = Object.keys(part);
  if (keys.length === 1) return "text";
  if (keys.length === 2 && part.thought === true) return "thought-text";
  // A text delta carrying any further key (notably a thoughtSignature) is not
  // coalescible: it stays its own part so the signature's placement in the
  // thought stream survives into the echoed turn-2 content.
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
  // turn-1 shape the model never sent and mask a provider change. Gemini emits
  // role on the first content-bearing chunk.
  if (role === undefined) {
    throw new Error(
      "google-genai SSE: no candidate content carried a role; cannot reconstruct the assistant turn",
    );
  }
  return { candidates: [{ content: { role, parts } }] };
}
