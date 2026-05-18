// A deliberately oversized tool result. The example wires this into
// the agent's `tools` array so the model's first turn produces a
// tool_use → tool_result cycle whose result blows past the size-cap
// transform's threshold, causing the reactor to spill the payload to
// the context store and rewrite the in-history block to a
// `tool-output:///{callId}` URI.

import { stringTool, type AgentTool } from "@interchange/agent";

/**
 * Default size of the synthetic payload. Comfortably above the agent's
 * default `sizeCapMaxChars` of 10 000.
 */
export const DEFAULT_PAYLOAD_CHARS = 25_000;

/**
 * Build the noisy tool. The payload is deterministic so tests can
 * round-trip it byte-for-byte through the `BlobReader`.
 */
export function createNoisyTool(
  payloadChars = DEFAULT_PAYLOAD_CHARS,
): AgentTool {
  const payload = buildPayload(payloadChars);
  return stringTool({
    definition: {
      name: "fetch_full_logs",
      description:
        "Fetch a verbose service log. The result is large enough that the size-cap transform will spill it to a blob.",
      inputSchema: {
        type: "object",
        properties: {},
      },
    },
    handler: async () => payload,
  });
}

function buildPayload(chars: number): string {
  // A repeating banner-and-line shape so a human inspecting the
  // spill file under `tool-output/<callId>.txt` sees something
  // human-readable rather than a sea of identical characters.
  const banner = "----- noisy tool emission -----\n";
  const lineTemplate = "log entry %i: nothing of consequence happened\n";
  const out: string[] = [banner];
  let total = banner.length;
  let i = 0;
  while (total < chars) {
    const line = lineTemplate.replace("%i", String(i));
    out.push(line);
    total += line.length;
    i++;
  }
  return out.join("");
}
