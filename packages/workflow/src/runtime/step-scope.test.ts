import { describe, test, expect } from "bun:test";

import { scopedStepId, baseStepId } from "./step-scope";

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
});
