import { describe, test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { extractContentBlocksFromSSE } from "./sse";

// Pins the sse.ts:applyDelta throw against real captured streams:
// server-side tool blocks (server_tool_use, web_search_tool_result,
// code_execution_tool_use) arrive with partial deltas here, and the
// parser does not implement delta application for them, so it must
// fail loud rather than silently drop the payloads.

// Fixtures live under sessions/anthropic/ in this package; the repo root
// is three directories up. Streaming captures are single-exchange, so the
// bytes sit at exchanges/0/response.sse.
const REPO_ROOT = resolve(
  fileURLToPath(new URL(".", import.meta.url)),
  "..",
  "..",
  "..",
);

const FIXTURES = [
  "packages/inference-discovery-anthropic/sessions/anthropic/claude-haiku-4-5-20251001/grounding-streaming/exchanges/0/response.sse",
  "packages/inference-discovery-anthropic/sessions/anthropic/claude-sonnet-5/grounding-streaming/exchanges/0/response.sse",
  "packages/inference-discovery-anthropic/sessions/anthropic/claude-haiku-4-5-20251001/code-execution-streaming/exchanges/0/response.sse",
  "packages/inference-discovery-anthropic/sessions/anthropic/claude-sonnet-5/code-execution-streaming/exchanges/0/response.sse",
];

describe("extractContentBlocksFromSSE — server-side tool fixture contract", () => {
  for (const relPath of FIXTURES) {
    test(`throws on ${relPath}`, () => {
      const bytes = readFileSync(resolve(REPO_ROOT, relPath));
      expect(() => extractContentBlocksFromSSE(new Uint8Array(bytes))).toThrow(
        /non-enumerated block type/,
      );
    });
  }
});
