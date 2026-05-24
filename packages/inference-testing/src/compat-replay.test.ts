import * as path from "node:path";
import { describe, expect, test } from "bun:test";

import { runCompatReplay } from "./compat-replay";

// Workspace-relative fixture root. The compat-replay helper requires an
// absolute path; tests resolve against the workspace root by walking up
// from this file's location.
const WORKSPACE_ROOT = path.resolve(__dirname, "../../..");
const FIXTURE_ROOT = path.join(
  WORKSPACE_ROOT,
  "packages/inference-testing/wire",
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
});

describe("runCompatReplay — skip behavior", () => {
  test("skips with no_adapter_registered when the catalog provider has no mapped adapter", async () => {
    // google-genai is not in CATALOG_TO_ADAPTER. The helper must skip
    // rather than throw, so a full-corpus iteration continues past it
    // without manual exclusion.
    const fixtureDir = path.join(
      FIXTURE_ROOT,
      "google-genai/gemini-2.5-flash/plain-text-streaming",
    );
    const result = await runCompatReplay({
      fixtureDir,
      provider: "google-genai",
      model: "gemini-2.5-flash",
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
