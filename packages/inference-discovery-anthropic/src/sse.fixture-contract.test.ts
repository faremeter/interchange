import { describe, test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { extractContentBlocksFromSSE } from "./sse";

// Pins the empirical contract behind the throw in sse.ts:applyDelta:
// Anthropic's server-side tool blocks (server_tool_use,
// web_search_tool_result, code_execution_tool_use) arrive with partial
// deltas in real streams — input_json_delta events build up the
// `input` field across content_block_delta events. This parser does
// not yet implement delta application for those block types, so any
// caller that invokes the parser on these fixtures must surface a
// loud failure rather than silently dropping the delta payloads.
//
// As long as this test exists, anyone refactoring the parser to "pass
// through unknown deltas" will see it fail and have to confront the
// actual wire shape. It also blocks the regression where someone
// widens streaming-multi-turn to pair with a server-side tool without
// first teaching the parser the delta application for the affected
// block type.

// The test file is at packages/inference-discovery-anthropic/src/; the
// fixtures it reads live under packages/inference-testing/wire/, so the
// repo root is three directories up from this file's parent.
const REPO_ROOT = resolve(
  fileURLToPath(new URL(".", import.meta.url)),
  "..",
  "..",
  "..",
);

const FIXTURES = [
  "packages/inference-testing/wire/anthropic/claude-opus-4-1-20250805/grounding-streaming/response.sse",
  "packages/inference-testing/wire/anthropic/claude-sonnet-4-5-20250929/grounding-streaming/response.sse",
  "packages/inference-testing/wire/anthropic/claude-haiku-4-5-20251001/grounding-streaming/response.sse",
  "packages/inference-testing/wire/anthropic/claude-opus-4-1-20250805/code-execution-streaming/response.sse",
  "packages/inference-testing/wire/anthropic/claude-sonnet-4-5-20250929/code-execution-streaming/response.sse",
  "packages/inference-testing/wire/anthropic/claude-haiku-4-5-20251001/code-execution-streaming/response.sse",
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
