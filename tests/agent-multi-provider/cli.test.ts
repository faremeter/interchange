// agent-multi-provider end-to-end test. Verifies the policy layer
// actually drives setProvider() before each send: one short prompt
// goes to the cheap model, one long prompt goes to the smart model.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { main } from "@intx/example-agent-multi-provider";
import { setupHarness, type Harness } from "@intx/inference-testing";
import type { ProviderConfig } from "@intx/types/runtime";

const PRIMARY: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-primary",
  model: "primary-cheap",
};
const FALLBACK: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-fallback",
  model: "fallback-cheap",
};

describe("agent-multi-provider CLI", () => {
  let workDir: string;
  let contextDir: string;
  let harness: Harness;
  let stdoutBuf: string;
  let stderrBuf: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-multi-provider-"));
    contextDir = join(workDir, "ctx");
    harness = setupHarness();
    stdoutBuf = "";
    stderrBuf = "";
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("routes short prompts to the cheap model and long ones to the smart model", async () => {
    harness.scenario.replyOnce("anthropic", { text: "cheap-reply" });
    harness.scenario.replyOnce("anthropic", { text: "smart-reply" });

    const short = "boil water?";
    const long =
      "explain in detail with a worked example covering at least three" +
      " distinct angles and edge cases — this prompt is long on purpose so" +
      " that the routing heuristic chooses the smart model rather than the" +
      " cheap one";

    const run = main(
      [short, long],
      { ANTHROPIC_API_KEY: "irrelevant" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: (s) => {
          stderrBuf += s;
        },
        primaryOverride: PRIMARY,
        fallbackOverride: FALLBACK,
        deps: harness.deps,
        contextDir,
        models: { cheap: "haiku-test", smart: "sonnet-test" },
      },
    );
    await harness.run();
    const code = await run;

    expect(stderrBuf).toBe("");
    expect(code).toBe(0);
    expect(stdoutBuf).toContain("tier=cheap model=haiku-test");
    expect(stdoutBuf).toContain("tier=smart model=sonnet-test");
    expect(stdoutBuf).toContain("cheap-reply");
    expect(stdoutBuf).toContain("smart-reply");

    // The model carried by the actual inference request must match
    // the routed model. lastRequest sees only the most recent
    // matched request, so we check the smart side here.
    const last = harness.scenario.lastRequest();
    if (last === undefined) throw new Error("expected a matched request");
    const body: unknown = await last.json();
    expect(body).toMatchObject({ model: "sonnet-test" });
  });

  test("missing prompts return exit code 1", async () => {
    const code = await main(
      [],
      { ANTHROPIC_API_KEY: "irrelevant" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: (s) => {
          stderrBuf += s;
        },
        primaryOverride: PRIMARY,
        fallbackOverride: FALLBACK,
        deps: harness.deps,
        contextDir,
      },
    );
    expect(code).toBe(1);
    expect(stderrBuf).toContain("usage:");
  });
});
