export interface AnthropicReasoningTrace {
  blockType: "thinking" | "redacted_thinking";
  fieldPath: string;
  // For "thinking" blocks: the first slice of the surfaced text. For
  // "redacted_thinking" blocks: a short prefix of the encrypted data
  // field, just enough to confirm the round-trip is exercising real
  // bytes.
  sample: string;
  // Present on either block type when the response carries it.
  signature?: string;
}

const SAMPLE_PREFIX_LENGTH = 80;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

// Surfaces the first thinking-class block from an Anthropic /v1/messages
// response so the observed-vs-documented notes in the discovery doc can
// pin the exact wire shape Anthropic returned. We capture the block
// type, the field path, a short sample of the contents, and the
// signature when present.
export function extractReasoningTrace(
  parsed: unknown,
): AnthropicReasoningTrace | null {
  if (!isRecord(parsed)) return null;
  const content = parsed.content;
  if (!Array.isArray(content)) return null;
  for (let i = 0; i < content.length; i += 1) {
    const block = content[i];
    if (!isRecord(block)) continue;
    const type = block.type;
    if (type === "thinking") {
      const text = asString(block.thinking);
      if (text === null) continue;
      const trace: AnthropicReasoningTrace = {
        blockType: "thinking",
        fieldPath: `content[${String(i)}].thinking`,
        sample: text.slice(0, SAMPLE_PREFIX_LENGTH),
      };
      const signature = asString(block.signature);
      if (signature !== null) trace.signature = signature;
      return trace;
    }
    if (type === "redacted_thinking") {
      const data = asString(block.data);
      if (data === null) continue;
      const trace: AnthropicReasoningTrace = {
        blockType: "redacted_thinking",
        fieldPath: `content[${String(i)}].data`,
        sample: data.slice(0, SAMPLE_PREFIX_LENGTH),
      };
      const signature = asString(block.signature);
      if (signature !== null) trace.signature = signature;
      return trace;
    }
  }
  return null;
}
