import { describe, test, expect, afterEach } from "bun:test";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { InferenceEvent } from "@intx/types/runtime";
import type { InvariantViolation } from "./invariants";
import type { ParserReplayResult } from "./session-replay";
import { classifyReplay, classifySession } from "./classify-replay";

const repoRoot = join(import.meta.dir, "..", "..", "..");

function events(...types: string[]): InferenceEvent[] {
  return types.map(
    (type) =>
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- synthetic minimal event; classifyReplay inspects only event.type, so the heavy seq/data fields are irrelevant to this unit
      ({ type }) as unknown as InferenceEvent,
  );
}

function replayed(
  index: number,
  eventTypes: string[],
  violations: InvariantViolation[] = [],
): ParserReplayResult {
  return { index, kind: "replayed", events: events(...eventTypes), violations };
}

describe("classifyReplay", () => {
  test("text content is captured", () => {
    const result = classifyReplay([
      replayed(0, [
        "inference.start",
        "inference.text.delta",
        "inference.usage",
        "inference.done",
      ]),
    ]);
    expect(result.outcome).toBe("captured");
  });

  test("a tool call is captured", () => {
    const result = classifyReplay([
      replayed(0, [
        "inference.start",
        "inference.tool_call.start",
        "inference.tool_call.delta",
        "inference.tool_call.end",
        "inference.done",
      ]),
    ]);
    expect(result.outcome).toBe("captured");
  });

  test("a refusal with no content is refused", () => {
    const result = classifyReplay([
      replayed(0, [
        "inference.start",
        "inference.refusal.delta",
        "inference.refusal.delta",
        "inference.done",
      ]),
    ]);
    expect(result.outcome).toBe("refused");
  });

  test("an inference.error is misled", () => {
    const result = classifyReplay([
      replayed(0, ["inference.start", "inference.error"]),
    ]);
    expect(result.outcome).toBe("misled");
    expect(result.reason).toMatch(/inference\.error/);
  });

  test("an invariant violation is misled", () => {
    const violation: InvariantViolation = {
      invariant: "schema_validity",
      message: "event at index 1 failed validation",
      events: [1],
    };
    const result = classifyReplay([
      replayed(0, ["inference.start", "inference.text.delta"], [violation]),
    ]);
    expect(result.outcome).toBe("misled");
    expect(result.reason).toMatch(/schema_validity/);
  });

  test("an empty stream is misled", () => {
    const result = classifyReplay([
      replayed(0, ["inference.start", "inference.usage", "inference.done"]),
    ]);
    expect(result.outcome).toBe("misled");
  });

  test("a session with only skipped exchanges is misled", () => {
    const result = classifyReplay([
      { index: 0, kind: "skipped", reason: "raw_request" },
    ]);
    expect(result.outcome).toBe("misled");
  });

  test("content in any exchange of a multi-turn flow is captured", () => {
    const result = classifyReplay([
      replayed(0, [
        "inference.start",
        "inference.tool_call.start",
        "inference.tool_call.end",
        "inference.done",
      ]),
      replayed(1, [
        "inference.start",
        "inference.text.delta",
        "inference.done",
      ]),
    ]);
    expect(result.outcome).toBe("captured");
  });
});

describe("classifySession over a committed fixture", () => {
  test("a real captured plain-text session classifies as captured", async () => {
    const sessionDir = join(
      repoRoot,
      "packages/inference-discovery-anthropic/sessions/anthropic/claude-sonnet-5/plain-text",
    );
    const result = await classifySession(sessionDir);
    expect(result.outcome).toBe("captured");
  });
});

describe("classifySession throw handling", () => {
  const created: string[] = [];
  afterEach(() => {
    for (const dir of created) rmSync(dir, { recursive: true, force: true });
    created.length = 0;
  });

  test("a session whose replay throws resolves to misled, not a crash", async () => {
    const dir = mkdtempSync(join(tmpdir(), "classify-throw-"));
    created.push(dir);
    // A session.json exists so a caller reaches replay, but the manifest is
    // malformed and the exchange body is undecodable. The documented contract
    // is that a batch caller sees this cell as misled rather than the whole run
    // crashing.
    writeFileSync(
      join(dir, "session.json"),
      JSON.stringify({ schemaVersion: "2" }),
    );
    mkdirSync(join(dir, "exchanges", "0"), { recursive: true });
    writeFileSync(join(dir, "exchanges", "0", "response.json"), "not json {{{");

    const result = await classifySession(dir);
    expect(result.outcome).toBe("misled");
    expect(result.reason).toMatch(/replay decode threw/);
  });
});
