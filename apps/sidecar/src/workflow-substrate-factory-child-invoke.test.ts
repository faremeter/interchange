import { describe, test, expect } from "bun:test";

import { ChildStepNotImplementedError } from "./workflow-substrate-factory";

// The child-runtime step invoker (`childInvokeStep`) rejects with this error
// instead of fabricating a `{ reply, turn }` success, so a `childWorkflow`
// fan-out fails loud. Its message crosses into the child run's
// `StepFailed.error.message`; the child-workflow-roundtrip integration test
// asserts these same substrings, so pin the contract here in the fast unit
// pass too.
describe("ChildStepNotImplementedError", () => {
  test("names INTR-310 and the unimplemented capability", () => {
    const err = new ChildStepNotImplementedError("agent-child", "childStep");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("ChildStepNotImplementedError");
    expect(err.message).toContain("INTR-310");
    expect(err.message).toContain("not implemented");
    // Carries the step and agent identities for diagnostics.
    expect(err.message).toContain("childStep");
    expect(err.message).toContain("agent-child");
  });

  test("tolerates an undefined step id", () => {
    const err = new ChildStepNotImplementedError("agent-child", undefined);
    expect(err.message).toContain("INTR-310");
    expect(err.message).toContain("not implemented");
  });
});
