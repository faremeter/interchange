import { describe, test, expect } from "bun:test";

import {
  scopedStepId,
  baseStepId,
  loopBodyRunId,
  sectionBodyRunId,
} from "./step-scope";

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

  test("loopBodyRunId is unique for one fixed run id", () => {
    // loopId carries no __ (definition-time invariant) and index is decimal
    // digits, so pairs under one run id stay distinct even when that run id
    // itself contains __.
    const loopIds = ["l", "y", "0", "inner", "b", "z", "_b"];
    const indices = [0, 1, 12, 100];
    for (const runId of ["run-abc", "a__b", "run-1_x__body__0"]) {
      const seen = new Map<string, string>();
      for (const loopId of loopIds) {
        for (const index of indices) {
          const key = loopBodyRunId(runId, loopId, index);
          const pair = JSON.stringify([loopId, index]);
          const prior = seen.get(key);
          expect(prior === undefined || prior === pair).toBe(true);
          seen.set(key, pair);
        }
      }
      expect(seen.size).toBe(loopIds.length * indices.length);
    }
  });

  test("loopBodyRunId can alias across different run ids", () => {
    expect(loopBodyRunId("a_", "_b", 0)).toBe("a____b__0");
    expect(loopBodyRunId("a_", "_b", 0)).toBe(loopBodyRunId("a__", "b", 0));
  });

  test("sectionBodyRunId prefixes the parent run id", () => {
    expect(sectionBodyRunId("run_abc", "section", 0)).toBe(
      "run_abc__section__0",
    );
    expect(sectionBodyRunId("run_abc", "section", 12)).toBe(
      "run_abc__section__12",
    );
  });

  test("sectionBodyRunId is unique for one fixed parent run id", () => {
    const sectionIds = ["section", "other", "0", "_sec"];
    const indices = [0, 1, 12, 100];
    for (const parentRunId of ["run_abc", "run_abc__body__0"]) {
      const seen = new Map<string, string>();
      for (const sectionId of sectionIds) {
        for (const index of indices) {
          const key = sectionBodyRunId(parentRunId, sectionId, index);
          const pair = JSON.stringify([sectionId, index]);
          const prior = seen.get(key);
          expect(prior === undefined || prior === pair).toBe(true);
          seen.set(key, pair);
        }
      }
      expect(seen.size).toBe(sectionIds.length * indices.length);
    }
  });

  test("sectionBodyRunId can alias across different parent run ids", () => {
    expect(sectionBodyRunId("run_abc", "_sec", 0)).toBe("run_abc___sec__0");
    expect(sectionBodyRunId("run_abc", "_sec", 0)).toBe(
      sectionBodyRunId("run_abc_", "sec", 0),
    );
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
