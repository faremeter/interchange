import { describe, test, expect } from "bun:test";

import type { AuthzCallResult } from "@intx/inference";

import type {
  AuthorizeContext,
  WorkflowAuthorizeFn,
} from "./authorize-context";

describe("AuthorizeContext", () => {
  test("workflow-typed closure receives the populated context", async () => {
    const observed: AuthorizeContext[] = [];
    const result: AuthzCallResult = {
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    };
    const authorize: WorkflowAuthorizeFn = async (
      _resource,
      _action,
      context,
    ) => {
      observed.push(context);
      return result;
    };

    await authorize("tool:foo", "invoke", {
      stepId: "step-a",
      attempt: 1,
      runId: "run-1",
    });
    await authorize("tool:bar", "invoke", { stepId: "step-b", attempt: 2 });

    expect(observed).toEqual([
      { stepId: "step-a", attempt: 1, runId: "run-1" },
      { stepId: "step-b", attempt: 2 },
    ]);
  });

  test("bare invocation passes an empty context", async () => {
    const observed: AuthorizeContext[] = [];
    const authorize: WorkflowAuthorizeFn = async (
      _resource,
      _action,
      context,
    ) => {
      observed.push(context);
      return { effect: "allow", matchingGrants: [], resolvedBy: null };
    };

    await authorize("tool:foo", "invoke", {});
    expect(observed[0]).toEqual({});
  });
});
