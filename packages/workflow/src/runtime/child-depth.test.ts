import { describe, test, expect } from "bun:test";

import {
  MAX_CHILD_SPAWN_DEPTH,
  ChildSpawnDepthExceededError,
  resolveMaxChildSpawnDepth,
  assertSpawnDepthWithinLimit,
} from "./child-depth";

describe("resolveMaxChildSpawnDepth", () => {
  test("defaults to the constant when unset", () => {
    expect(resolveMaxChildSpawnDepth(undefined)).toBe(MAX_CHILD_SPAWN_DEPTH);
  });

  test("honors a lower injected ceiling", () => {
    expect(resolveMaxChildSpawnDepth(2)).toBe(2);
  });

  test("clamps an injected ceiling to the constant so it can only tighten", () => {
    expect(resolveMaxChildSpawnDepth(MAX_CHILD_SPAWN_DEPTH + 1000)).toBe(
      MAX_CHILD_SPAWN_DEPTH,
    );
  });

  test("rejects a non-finite ceiling rather than silently disabling the guard", () => {
    // Math.min(NaN, 32) is NaN, and `childDepth > NaN` is always false, so a
    // NaN ceiling would defeat the backstop entirely. Fail loud instead.
    expect(() => resolveMaxChildSpawnDepth(Number.NaN)).toThrow(/finite/);
    expect(() => resolveMaxChildSpawnDepth(Number.POSITIVE_INFINITY)).toThrow(
      /finite/,
    );
  });
});

describe("assertSpawnDepthWithinLimit", () => {
  test("permits a child at the ceiling", () => {
    expect(() => assertSpawnDepthWithinLimit(2, "spawn", 2)).not.toThrow();
  });

  test("rejects a child past the ceiling, naming the depth and the step", () => {
    let thrown: unknown;
    try {
      assertSpawnDepthWithinLimit(3, "spawnChild", 2);
    } catch (err) {
      thrown = err;
    }
    if (!(thrown instanceof ChildSpawnDepthExceededError)) {
      throw new Error("expected a ChildSpawnDepthExceededError");
    }
    expect(thrown.depth).toBe(3);
    expect(thrown.maxDepth).toBe(2);
    expect(thrown.parentStepId).toBe("spawnChild");
    // The message must name the depth so a failed run is diagnosable.
    expect(thrown.message).toContain("3");
    expect(thrown.message).toContain("spawnChild");
  });
});
