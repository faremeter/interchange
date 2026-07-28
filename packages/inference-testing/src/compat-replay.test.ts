import * as path from "node:path";
import { describe, expect, test } from "bun:test";

import {
  runCompatReplay,
  runCompatReplayCorpus,
  type CompatReplayCorpusResult,
  type CompatReplaySkipReason,
} from "./compat-replay";

// Workspace-relative root of the opencode-zen fixture corpus, which lives
// in the openai discovery package. The compat-replay helper requires an
// absolute path; tests resolve against the workspace root by walking up
// from this file's location.
const WORKSPACE_ROOT = path.resolve(__dirname, "../../..");
const FIXTURE_ROOT = path.join(
  WORKSPACE_ROOT,
  "packages/inference-discovery-openai/wire",
);

describe("runCompatReplay — single-turn streaming SSE", () => {
  test("opencode-zen/kimi-k2.6/plain-text-streaming: all invariants pass", async () => {
    const fixtureDir = path.join(
      FIXTURE_ROOT,
      "opencode-zen/kimi-k2.6/plain-text-streaming",
    );
    const result = await runCompatReplay({
      fixtureDir,
      provider: "opencode-zen",
      model: "kimi-k2.6",
    });

    if (result.kind !== "replayed") {
      throw new Error(
        `expected replay to complete, got skipped: ${result.reason}`,
      );
    }

    // Surface every violation in the failure message so a regression is
    // immediately debuggable. Empty-array equality alone gives no context.
    expect(result.violations).toEqual([]);
    if (result.violations.length > 0) {
      throw new Error(
        `compat-replay violations:\n${JSON.stringify(result.violations, null, 2)}`,
      );
    }

    // Sanity: the replay produced events of the expected categories.
    const textDeltas = result.events.filter(
      (e) => e.type === "inference.text.delta",
    );
    const dones = result.events.filter((e) => e.type === "inference.done");
    expect(textDeltas.length).toBeGreaterThan(0);
    expect(dones).toHaveLength(1);
  });

  test("anthropic redacted-thinking-streaming turn-1: redacted events + clean invariants", async () => {
    // Real capture (Haiku 4.5): multiple one-shot
    // redacted_thinking content_block_start events before text.
    const fixtureDir = path.join(
      WORKSPACE_ROOT,
      "packages/inference-discovery-anthropic/wire/anthropic/claude-haiku-4-5-20251001/redacted-thinking-streaming/turn-1",
    );
    const result = await runCompatReplay({
      fixtureDir,
      provider: "anthropic",
      model: "claude-haiku-4-5-20251001",
    });

    if (result.kind !== "replayed") {
      throw new Error(
        `expected replay to complete, got skipped: ${result.reason}`,
      );
    }

    expect(result.violations).toEqual([]);
    const redacted = result.events.filter(
      (e) => e.type === "inference.thinking.redacted",
    );
    expect(redacted.length).toBeGreaterThan(0);
    for (const ev of redacted) {
      if (ev.type !== "inference.thinking.redacted") continue;
      expect(ev.data.redactedThinking.type).toBe("redacted_thinking");
      expect(ev.data.redactedThinking.data.length).toBeGreaterThan(0);
    }
    const dones = result.events.filter((e) => e.type === "inference.done");
    expect(dones).toHaveLength(1);
    if (dones[0]?.type === "inference.done") {
      const redactedBlocks = dones[0].data.turn.content.filter(
        (b) => b.type === "redacted_thinking",
      );
      expect(redactedBlocks.length).toBe(redacted.length);
    }
  });
});

describe("runCompatReplay — skip behavior", () => {
  test("skips with no_adapter_registered when the catalog provider has no mapped adapter", async () => {
    // The skip path fires whenever `runCompatReplay` is invoked for
    // a catalog provider that has no entry in `CATALOG_TO_ADAPTER`.
    // Every provider in today's catalog has an entry, so this test
    // uses a synthetic provider name that is guaranteed absent from
    // the mapping; the helper short-circuits before reading the
    // fixture (the early return runs before any I/O), so a real
    // fixture directory is not required and any path works for the
    // `fixtureDir` argument.
    const result = await runCompatReplay({
      fixtureDir: WORKSPACE_ROOT,
      provider: "synthetic-unmapped-provider",
      model: "synthetic-model",
    });
    expect(result).toEqual({
      kind: "skipped",
      reason: "no_adapter_registered",
    });
  });

  test("skips with non_streaming_capture when only response.json exists", async () => {
    // plain-text (non-streaming) carries response.json but no response.sse.
    // Today's adapter consumes SSE only, so the capture is not replayable
    // through the existing parser path. Skip rather than throw.
    const fixtureDir = path.join(
      FIXTURE_ROOT,
      "opencode-zen/kimi-k2.6/plain-text",
    );
    const result = await runCompatReplay({
      fixtureDir,
      provider: "opencode-zen",
      model: "kimi-k2.6",
    });
    expect(result).toEqual({
      kind: "skipped",
      reason: "non_streaming_capture",
    });
  });

  test("throws when the fixture has neither response.sse nor response.json", async () => {
    // Point at a directory that exists but contains neither response file
    // (use the workspace root itself — definitely no response files there).
    let thrown: unknown;
    try {
      await runCompatReplay({
        fixtureDir: WORKSPACE_ROOT,
        provider: "opencode-zen",
        model: "kimi-k2.6",
      });
    } catch (e) {
      thrown = e;
    }
    if (!(thrown instanceof Error)) {
      throw new Error("expected runCompatReplay to throw");
    }
    expect(thrown.message).toMatch(/fixture appears malformed/);
  });
});

describe("runCompatReplayCorpus — full SUPPORT_MATRIX iteration", () => {
  // Walk every captured row in the discovery support matrix through
  // the replay pipeline. The corpus is the contract: each captured
  // fixture must replay through the current adapter without
  // producing invariant violations, OR skip with a structured
  // reason that names why. A new invariant landing in INVARIANTS
  // surfaces here as a failure across the whole corpus — exactly
  // what the typed Invariant list was built for.
  //
  // Expected skip reasons:
  //   - no_adapter_registered: catalog providers absent from
  //     `CATALOG_TO_ADAPTER`. Every provider in today's catalog has
  //     an entry, so this reason fires only when the catalog
  //     gains a new provider before its adapter lands.
  //   - non_streaming_capture: response.json-only captures (no SSE).
  //   - raw_bytes_upload: files-api `upload/` subdirs carrying request.bin.
  //   - non_captured_outcome: misled/refused/http-error/unsupported rows.
  test("every captured leaf either replays clean or skips with a known reason", async () => {
    const results = await runCompatReplayCorpus({
      workspaceRoot: WORKSPACE_ROOT,
    });

    // Sanity: the corpus is non-empty (a future regression that
    // empties SUPPORT_MATRIX would otherwise silently pass).
    expect(results.length).toBeGreaterThan(0);

    const ALLOWED_SKIP_REASONS: readonly CompatReplaySkipReason[] = [
      "no_adapter_registered",
      "non_streaming_capture",
      "raw_bytes_upload",
      "non_captured_outcome",
    ];

    const replayedWithViolations: CompatReplayCorpusResult[] = [];
    const skippedUnknown: CompatReplayCorpusResult[] = [];

    for (const result of results) {
      if (result.kind === "skipped") {
        if (!ALLOWED_SKIP_REASONS.includes(result.reason)) {
          skippedUnknown.push(result);
        }
        continue;
      }
      if (result.violations.length > 0) {
        replayedWithViolations.push(result);
      }
    }

    if (skippedUnknown.length > 0) {
      throw new Error(
        `compat-replay corpus: unrecognized skip reasons:\n${skippedUnknown
          .map(
            (r) =>
              `  ${r.entry.provider}/${r.entry.model}/${r.entry.capability}${r.subPath === "" ? "" : `/${r.subPath}`}: ${r.kind === "skipped" ? r.reason : "???"}`,
          )
          .join("\n")}`,
      );
    }

    if (replayedWithViolations.length > 0) {
      throw new Error(
        `compat-replay corpus: ${String(replayedWithViolations.length)} captured replays produced invariant violations:\n${replayedWithViolations
          .map(
            (r) =>
              `  ${r.entry.provider}/${r.entry.model}/${r.entry.capability}${r.subPath === "" ? "" : `/${r.subPath}`}: ${r.kind === "replayed" ? JSON.stringify(r.violations).slice(0, 200) : "???"}`,
          )
          .join("\n")}`,
      );
    }

    // The corpus should include both real replays (anthropic +
    // opencode-zen + google-genai + openai captured streaming rows) and
    // structured skips (misled outcomes, raw-bytes upload leaves,
    // non-streaming captures). A complete absence of either category
    // would mean the catalog or the adapter wiring shifted in a way
    // this suite isn't covering — surface that loudly rather than as a
    // green pass.
    const replayedCount = results.filter((r) => r.kind === "replayed").length;
    const skippedCount = results.filter((r) => r.kind === "skipped").length;
    expect(replayedCount).toBeGreaterThan(0);
    expect(skippedCount).toBeGreaterThan(0);

    // Pin the first-party openai mapping so dropping
    // `openai: "openai-compatible"` cannot leave the suite green while
    // openai streaming captures silently skip as no_adapter_registered.
    const openaiResults = results.filter((r) => r.entry.provider === "openai");
    expect(openaiResults.length).toBeGreaterThan(0);
    const openaiReplayed = openaiResults.filter((r) => r.kind === "replayed");
    expect(openaiReplayed.length).toBeGreaterThan(0);
    const openaiNoAdapter = openaiResults.filter(
      (r) => r.kind === "skipped" && r.reason === "no_adapter_registered",
    );
    expect(openaiNoAdapter).toEqual([]);
  });
});
