import { describe, test, expect } from "bun:test";

import { scopedStepId, baseStepId, loopBodyRunId } from "./step-scope";

describe("step-scope", () => {
  test("scopedStepId encodes the base id and iteration index", () => {
    expect(scopedStepId("foo", 0)).toBe("foo[0]");
    expect(scopedStepId("summarize", 12)).toBe("summarize[12]");
    expect(scopedStepId("step-1_a", 3)).toBe("step-1_a[3]");
  });

  test("baseStepId inverts scopedStepId", () => {
    for (const base of ["foo", "summarize", "step-1_a", "s"]) {
      for (const index of [0, 1, 9, 42, 100]) {
        expect(baseStepId(scopedStepId(base, index))).toBe(base);
      }
    }
  });

  test("baseStepId is the identity on an unscoped id", () => {
    expect(baseStepId("foo")).toBe("foo");
    expect(baseStepId("step-1")).toBe("step-1");
  });

  test("baseStepId strips only a trailing bracketed integer", () => {
    // Author step ids match STEP_ID_PATTERN (`[a-zA-Z0-9_-]+`), so they never
    // contain a bracket; the only stripping case is a trailing numeric scope.
    // A trailing non-numeric bracket is not a scope marker and is preserved.
    expect(baseStepId("foo[x]")).toBe("foo[x]");
    // A bracket that is not at the end is preserved.
    expect(baseStepId("a[1]b")).toBe("a[1]b");
    // Only the single trailing scope is stripped.
    expect(baseStepId("foo[0]")).toBe("foo");
  });

  test("loopBodyRunId prefixes the container run id", () => {
    expect(loopBodyRunId("run-abc", "rework", 0)).toBe("run-abc__rework__0");
    expect(loopBodyRunId("run-abc", "rework", 12)).toBe("run-abc__rework__12");
  });

  test("loopBodyRunId is injective even when the run id contains __", () => {
    // The store keys runs by this string, so two distinct
    // (runId, loopId, index) triples must never render equal. Injectivity does
    // not need a __-free run id: loopId carries no __ (definition-time invariant)
    // and index is digits, so the final two __ are always the separators.
    const runIds = ["a", "a__b", "a__0", "run-1_x__body__0", "x__y__z", "1"];
    const loopIds = ["l", "y", "0", "inner", "b", "z"];
    const indices = [0, 1, 12, 100];
    const seen = new Map<string, string>();
    for (const runId of runIds) {
      for (const loopId of loopIds) {
        for (const index of indices) {
          const key = loopBodyRunId(runId, loopId, index);
          const triple = JSON.stringify([runId, loopId, index]);
          const prior = seen.get(key);
          expect(prior === undefined || prior === triple).toBe(true);
          seen.set(key, triple);
        }
      }
    }
    expect(seen.size).toBe(runIds.length * loopIds.length * indices.length);
  });

  test("loopBodyRunId re-roots per nesting level", () => {
    // A loop nested in an outer iteration runs under that iteration's own body
    // run id, so an inner loop under two outer iterations gets distinct ids.
    const outerZero = loopBodyRunId("run-abc", "outer", 0);
    const outerOne = loopBodyRunId("run-abc", "outer", 1);
    expect(loopBodyRunId(outerZero, "inner", 0)).toBe(
      "run-abc__outer__0__inner__0",
    );
    expect(loopBodyRunId(outerOne, "inner", 0)).toBe(
      "run-abc__outer__1__inner__0",
    );
    expect(loopBodyRunId(outerZero, "inner", 0)).not.toBe(
      loopBodyRunId(outerOne, "inner", 0),
    );
  });
});
