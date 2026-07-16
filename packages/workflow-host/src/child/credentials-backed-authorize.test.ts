import { describe, test, expect } from "bun:test";

import {
  createCredentialsBackedAuthorize,
  type CredentialsSnapshotRef,
  type GrantEvaluator,
} from "./run-child";

// The credentials snapshot is keyed per base step. A `map` iteration runs
// under a scoped step id `<base>[<index>]`, so the authorize closure resolves
// the scoped id to its base before looking up the step's grant entry -- every
// iteration shares the base step's grants while the scoped id remains the
// invocation identity handed to the evaluator.
describe("createCredentialsBackedAuthorize base-step grant resolution", () => {
  const summarizeGrant = { resource: "tool:posix", action: "invoke" };

  function refWithSummarize(): CredentialsSnapshotRef {
    return {
      current: {
        steps: [
          {
            stepId: "summarize",
            address: "ins_dep-summarize@example.com",
            grants: [summarizeGrant],
            contentHash: "hash",
          },
        ],
      },
    };
  }

  function recordingEvaluator(): {
    evaluate: GrantEvaluator;
    calls: Parameters<GrantEvaluator>[0][];
  } {
    const calls: Parameters<GrantEvaluator>[0][] = [];
    const evaluate: GrantEvaluator = (input) => {
      calls.push(input);
      return Promise.resolve({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      });
    };
    return { evaluate, calls };
  }

  test("a map iteration resolves the base step's grants", async () => {
    const { evaluate, calls } = recordingEvaluator();
    const authorize = createCredentialsBackedAuthorize(
      refWithSummarize(),
      evaluate,
    );

    const result = await authorize("tool:posix", "invoke", {
      stepId: "summarize[2]",
      attempt: 1,
      runId: "run-1",
    });

    expect(result.effect).toBe("allow");
    expect(calls).toHaveLength(1);
    // Grants come from the base step's snapshot entry.
    expect(calls[0]?.grants).toEqual([summarizeGrant]);
    // The evaluator still sees the scoped id as the invocation identity.
    expect(calls[0]?.stepId).toBe("summarize[2]");
  });

  test("an unscoped step id resolves unchanged", async () => {
    const { evaluate, calls } = recordingEvaluator();
    const authorize = createCredentialsBackedAuthorize(
      refWithSummarize(),
      evaluate,
    );

    await authorize("tool:posix", "invoke", {
      stepId: "summarize",
      attempt: 1,
      runId: "run-1",
    });

    expect(calls[0]?.grants).toEqual([summarizeGrant]);
    expect(calls[0]?.stepId).toBe("summarize");
  });

  test("a missing base entry reports the base id and the scoped invocation id", async () => {
    const { evaluate } = recordingEvaluator();
    const authorize = createCredentialsBackedAuthorize(
      refWithSummarize(),
      evaluate,
    );

    await expect(
      authorize("tool:posix", "invoke", {
        stepId: "other[0]",
        attempt: 1,
        runId: "run-1",
      }),
    ).rejects.toThrow(
      /no entry for stepId other \(normalized from scoped invocation id other\[0\]\)/,
    );
  });
});
