// Parser regression over the full session corpus. For every fixture-bearing
// (adapter × capability) cell, replay each captured response through production
// runInference and assert the resulting event stream satisfies INVARIANTS. This
// drives the one unified loader (`replayResponsesForParsing`) over the session
// tree, serves captured response headers verbatim, and exercises JSON-bodied
// and non-streaming captures alongside streaming ones.
//
// This is anything-goes body matching: the captured response is the fixed
// input and only the decode is under test, so a failure means a code-side
// parser change, never a request-body divergence. Strict canonical request-body
// equality is a separate concern (see createReplayHarness).

import { describe, test, expect } from "bun:test";
import { join } from "node:path";

import {
  SUPPORT_MATRIX,
  getSessionDir,
  isFixtureBearing,
} from "@intx/inference-discovery/catalog";
import { replayResponsesForParsing } from "@intx/inference-testing";

const repoRoot = join(import.meta.dir, "..", "..", "..");

// The only exchange the loader legitimately skips is a files-api raw upload
// (request.bin), which is not a decodable inference response.
const FILES_API_CAPABILITIES = new Set([
  "files-api-reference",
  "files-api-reference-streaming",
]);

const fixtureBearing = SUPPORT_MATRIX.filter(isFixtureBearing);

// Cells whose captured response the current adapter cannot decode without
// throwing — a genuine adapter bug this JSON-decode coverage surfaces, not a
// regression in this test. Each entry pins the specific error the defect
// raises rather than accepting any throw, so a lost or moved fixture (which
// throws ENOENT) cannot masquerade as the defect still being covered. Fixing
// the adapter makes replay resolve, the assertion then fails, and the entry
// must be removed.
const EXPECTED_PARSER_FAILURES = new Map<
  string,
  { reason: string; throws: RegExp }
>([
  [
    "anthropic/claude-opus-5/grounding",
    {
      reason:
        "a web-search citation references a content-block index the adapter " +
        "drops, so replay raises a citation/block ProtocolMismatchError",
      throws: /no matching emitted block/,
    },
  ],
]);

function cellKey(entry: {
  provider: string;
  model: string;
  capability: string;
}): string {
  return `${entry.provider}/${entry.model}/${entry.capability}`;
}

describe("session corpus parser regression", () => {
  test("the corpus has fixture-bearing cells to replay", () => {
    expect(fixtureBearing.length).toBeGreaterThan(0);
  });

  for (const entry of fixtureBearing) {
    const key = cellKey(entry);
    const expectedFailure = EXPECTED_PARSER_FAILURES.get(key);

    test(`${key} replays without violations`, async () => {
      const rel = getSessionDir(entry);
      if (rel === null) {
        throw new Error(`no session dir for ${key}`);
      }
      const sessionDir = join(repoRoot, rel);

      if (expectedFailure !== undefined) {
        let thrown: unknown;
        try {
          await replayResponsesForParsing({ sessionDir });
        } catch (err) {
          thrown = err;
        }
        if (!(thrown instanceof Error)) {
          throw new Error(
            `${key} was expected to fail parsing (${expectedFailure.reason}) ` +
              `but replay resolved without throwing`,
          );
        }
        expect(thrown.message).toMatch(expectedFailure.throws);
        return;
      }

      const results = await replayResponsesForParsing({ sessionDir });
      const replayed = results.filter((r) => r.kind === "replayed");
      const skipped = results.filter((r) => r.kind === "skipped");

      // Vacuous-pass guard: a cell that replayed nothing (all exchanges
      // skipped) would satisfy the violation check trivially. Every cell must
      // decode at least one real response.
      expect(replayed.length).toBeGreaterThan(0);

      // A skip is legitimate only for a files-api raw upload exchange.
      if (skipped.length > 0) {
        expect(FILES_API_CAPABILITIES.has(entry.capability)).toBe(true);
        for (const r of skipped) expect(r.reason).toBe("raw_request");
      }

      for (const r of replayed) {
        if (r.violations.length > 0) {
          throw new Error(
            `${entry.provider}/${entry.model}/${entry.capability} exchange ${String(
              r.index,
            )}: ${r.violations.map((v) => v.message).join("; ")}`,
          );
        }
      }
    });
  }
});
