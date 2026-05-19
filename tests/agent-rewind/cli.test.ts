// agent-rewind end-to-end test. Drives the example's main() with two
// scripted prompts, then verifies that the rewound agent's history
// contains only the first cycle's turns.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { main } from "@intx/example-agent-rewind";
import { setupHarness, type Harness } from "@intx/inference-testing";
import { createIsogitStore } from "@intx/storage-isogit";
import type { ProviderConfig } from "@intx/types/runtime";

const PROVIDER: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-rewind",
  model: "claude-3-5-sonnet",
};

describe("agent-rewind CLI", () => {
  let workDir: string;
  let contextDir: string;
  let rewindDir: string;
  let harness: Harness;
  let stdoutBuf: string;
  let stderrBuf: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-rewind-"));
    contextDir = join(workDir, "ctx");
    rewindDir = join(workDir, "ctx-rewound");
    harness = setupHarness();
    stdoutBuf = "";
    stderrBuf = "";
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("rewinds to the commit after the first send", async () => {
    harness.scenario.replyOnce("anthropic", { text: "Mars" });
    harness.scenario.replyOnce("anthropic", { text: "Phobos" });

    const run = main(
      ["name a planet", "now name a moon of that planet"],
      { ANTHROPIC_API_KEY: "irrelevant" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: (s) => {
          stderrBuf += s;
        },
        providerOverride: PROVIDER,
        deps: harness.deps,
        contextDir,
        rewindDir,
      },
    );
    await harness.run();
    expect(await run).toBe(0);
    expect(stderrBuf).toBe("");

    // The rewound history should reflect only the first send's
    // turn pair.
    expect(stdoutBuf).toContain("rewound history turns: 2");
    expect(stdoutBuf).toContain("Mars");
    expect(stdoutBuf).not.toContain("Phobos");

    // Cross-check by opening the rewound contextDir directly: the
    // store on disk really did move HEAD to the older commit.
    const rewoundStore = await createIsogitStore(rewindDir);
    const loaded = await rewoundStore.load();
    expect(loaded.turns.length).toBe(2);
    expect(
      loaded.turns.some(
        (t) =>
          t.role === "assistant" &&
          t.content.some((b) => b.type === "text" && b.text.includes("Mars")),
      ),
    ).toBe(true);
    expect(
      loaded.turns.some(
        (t) =>
          t.role === "assistant" &&
          t.content.some((b) => b.type === "text" && b.text.includes("Phobos")),
      ),
    ).toBe(false);

    // The original contextDir still carries both cycles.
    const originalStore = await createIsogitStore(contextDir);
    const originalLoaded = await originalStore.load();
    expect(originalLoaded.turns.length).toBe(4);
  });

  test("missing or too-many arguments returns exit code 1", async () => {
    const code = await main(
      ["only one"],
      { ANTHROPIC_API_KEY: "irrelevant" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: (s) => {
          stderrBuf += s;
        },
        providerOverride: PROVIDER,
        deps: harness.deps,
        contextDir,
        rewindDir,
      },
    );
    expect(code).toBe(1);
    expect(stderrBuf).toContain("usage:");
  });
});
