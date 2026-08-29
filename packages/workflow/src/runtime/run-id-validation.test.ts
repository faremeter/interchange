// A run id enters the system at `runtimeRun` and is used verbatim as a
// durable-store path segment and a mail-address local part, so it is validated
// against `RUN_ID_PATTERN` at that boundary. `runLocal` forwards a caller
// `runId` straight through, so it is the seam that exercises the guard.

import { describe, test, expect } from "bun:test";

import { action, defineWorkflow, runLocal } from "@intx/workflow";

const oneStep = defineWorkflow({
  id: "one-step",
  trigger: { type: "manual" },
  steps: {
    only: action({ handler: "noop" }),
  },
});

describe("run id validation", () => {
  test("rejects a caller run id with a path separator", () => {
    expect(() => runLocal(oneStep, { runId: "runs/../escape" })).toThrow(
      /run id .* must match/,
    );
  });

  test("rejects a caller run id with an at sign", () => {
    expect(() => runLocal(oneStep, { runId: "a@b" })).toThrow(
      /run id .* must match/,
    );
  });

  test("accepts a run id of letters, digits, underscores, and hyphens", () => {
    expect(() =>
      runLocal(oneStep, { runId: "run-1_a__body__0" }),
    ).not.toThrow();
  });
});
