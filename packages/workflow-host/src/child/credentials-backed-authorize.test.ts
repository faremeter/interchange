import { describe, test, expect } from "bun:test";

import type { GrantRule } from "@intx/authz";

import {
  createCredentialsBackedAuthorize,
  createGrantEvaluator,
  createSnapshotGrantsReader,
  createSnapshotGrantsResolver,
  type CredentialsSnapshotRef,
  type GrantEvaluator,
} from "./credentials-backed-authorize";

function rule(
  resource: string,
  effect: GrantRule["effect"],
  expiresAt: Date | null = null,
): GrantRule {
  return {
    id: `grant:${resource}:${effect}`,
    resource,
    action: "invoke",
    effect,
    origin: "creator",
    conditions: null,
    expiresAt,
    roleId: null,
    principalId: null,
  };
}

// The grants a run child receives crossed JSON, which turns each `expiresAt`
// into a string.
function overJSON(grants: readonly GrantRule[]): unknown[] {
  const parsed: unknown = JSON.parse(JSON.stringify(grants));
  if (!Array.isArray(parsed)) throw new Error("expected an array");
  return parsed;
}

// The credentials snapshot is keyed per base step. A `map` iteration runs
// under a scoped step id `<base>[<index>]`, so the authorize closure resolves
// the scoped id to its base before looking up the step's grant entry -- every
// iteration shares the base step's grants while the scoped id remains the
// invocation identity handed to the evaluator.
describe("createCredentialsBackedAuthorize base-step grant resolution", () => {
  const summarizeGrant = rule("tool:posix", "allow");

  function refWithSummarize(): CredentialsSnapshotRef {
    return {
      current: {
        steps: [
          {
            stepId: "summarize",
            address: "run_dep-summarize@example.com",
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
      createSnapshotGrantsResolver(refWithSummarize(), "test authorize"),
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
      createSnapshotGrantsResolver(refWithSummarize(), "test authorize"),
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
      createSnapshotGrantsResolver(refWithSummarize(), "test authorize"),
      evaluate,
    );

    await expect(
      authorize("tool:posix", "invoke", {
        stepId: "other[0]",
        attempt: 1,
        runId: "run-1",
      }),
    ).rejects.toThrow(
      /no entry for step other \(normalized from scoped invocation id other\[0\]\)/,
    );
  });
});

describe("createSnapshotGrantsResolver", () => {
  function entry(stepId: string, grants: unknown[]) {
    return { stepId, address: stepId, grants, contentHash: "h" };
  }
  const oldGrant = rule("tool:old", "allow");
  const newGrant = rule("tool:new", "allow");

  test("reads the ref on every call, so a swapped snapshot takes effect", () => {
    const ref: CredentialsSnapshotRef = {
      current: { steps: [entry("s", [oldGrant])] },
    };
    const resolve = createSnapshotGrantsResolver(ref, "lbl");
    expect(resolve("s")).toEqual([oldGrant]);
    ref.current = { steps: [entry("s", [newGrant])] };
    expect(resolve("s")).toEqual([newGrant]);
  });

  test("throws with the label when no snapshot is active", () => {
    const resolve = createSnapshotGrantsResolver({ current: null }, "lbl");
    expect(() => resolve("s")).toThrow(
      "lbl: no credentials snapshot is active for step s",
    );
  });

  test("a scoped id resolves to the base entry; a missing entry names both ids", () => {
    const resolve = createSnapshotGrantsResolver(
      { current: { steps: [entry("s", [oldGrant])] } },
      "lbl",
    );
    expect(resolve("s[2]")).toEqual([oldGrant]);
    expect(() => resolve("t[1]")).toThrow(
      "lbl: the credentials snapshot has no entry for step t (normalized from scoped invocation id t[1])",
    );
  });

  test("turns an expiry that crossed JSON back into a Date", () => {
    const expiresAt = new Date("2026-07-21T00:00:00.000Z");
    const resolve = createSnapshotGrantsResolver(
      {
        current: {
          steps: [entry("s", overJSON([rule("tool:a", "allow", expiresAt)]))],
        },
      },
      "lbl",
    );
    const [grant] = resolve("s");
    expect(grant?.expiresAt).toBeInstanceOf(Date);
    expect(grant?.expiresAt?.getTime()).toBe(expiresAt.getTime());
  });

  test("returns the same parsed array until the step's grants change", () => {
    const ref: CredentialsSnapshotRef = {
      current: { steps: [entry("s", overJSON([oldGrant]))] },
    };
    const resolve = createSnapshotGrantsResolver(ref, "lbl");
    const first = resolve("s");
    expect(resolve("s")).toBe(first);
    ref.current = { steps: [entry("s", overJSON([newGrant]))] };
    expect(resolve("s")).not.toBe(first);
    expect(resolve("s")).toEqual([newGrant]);
  });

  test("throws, naming the step, when an entry is not a grant rule", () => {
    const resolve = createSnapshotGrantsResolver(
      { current: { steps: [entry("s", [{ resource: "tool:a" }])] } },
      "lbl",
    );
    expect(() => resolve("s")).toThrow(
      "lbl: the grants of step s are not grant rules",
    );
  });
});

describe("createSnapshotGrantsReader", () => {
  function entry(stepId: string, grants: unknown[]) {
    return { stepId, address: stepId, grants, contentHash: "h" };
  }

  test("returns one entry per grant id and keeps a row without an id once", () => {
    const shared = { id: "g1", resource: "tool:a" };
    const noId = { resource: "tool:b" };
    const read = createSnapshotGrantsReader(
      {
        current: {
          steps: [
            entry("s", [shared, noId]),
            entry("t", [{ ...shared }, noId, { id: "g2" }]),
          ],
        },
      },
      "lbl",
    );
    expect(read()).toEqual([shared, noId, { id: "g2" }]);
  });

  test("returns the same array until the snapshot object changes", () => {
    const ref: CredentialsSnapshotRef = {
      current: { steps: [entry("s", [{ id: "old" }])] },
    };
    const read = createSnapshotGrantsReader(ref, "lbl");
    const first = read();
    expect(read()).toBe(first);
    ref.current = { steps: [entry("s", [{ id: "new" }])] };
    const second = read();
    expect(second).not.toBe(first);
    expect(second).toEqual([{ id: "new" }]);
  });

  test("throws with the label when no snapshot is active", () => {
    const read = createSnapshotGrantsReader({ current: null }, "lbl");
    expect(() => read()).toThrow("lbl: no credentials snapshot is active");
  });
});

describe("createGrantEvaluator", () => {
  const input = (stepId: string, grants: readonly GrantRule[]) => ({
    resource: "tool:fetch",
    action: "invoke",
    stepId,
    attempt: 1,
    runId: "run-1",
    grants,
  });

  test("evaluates the snapshot grants when the step has no floor", async () => {
    const evaluate = createGrantEvaluator({ floorFor: () => [] });
    await expect(
      evaluate(input("work", [rule("tool:fetch", "allow")])),
    ).resolves.toEqual({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    });
    await expect(evaluate(input("work", []))).resolves.toEqual({
      effect: null,
      matchingGrants: [],
      resolvedBy: null,
    });
  });

  test("adds the floor of the base step, and a declared deny still wins", async () => {
    const asked: string[] = [];
    const evaluate = createGrantEvaluator({
      floorFor: (stepId) => {
        asked.push(stepId);
        return [rule("tool:fetch", "ask")];
      },
    });
    await expect(evaluate(input("fan[2]", []))).resolves.toMatchObject({
      effect: "ask",
    });
    await expect(
      evaluate(input("fan[0]", [rule("tool:fetch", "deny")])),
    ).resolves.toMatchObject({ effect: "deny" });
    expect(asked).toEqual(["fan", "fan"]);
  });

  test("evaluates a time window against the clock, so an approved ask allows inside it", async () => {
    // Windows relative to now, in UTC, an hour clear of either edge.
    const hhmm = (offsetHours: number) =>
      new Date(Date.now() + offsetHours * 3_600_000)
        .toISOString()
        .slice(11, 16);
    const window = (after: number, before: number) => ({
      time_window: {
        after: hhmm(after),
        before: hhmm(before),
        timezone: "UTC",
      },
    });
    const evaluate = createGrantEvaluator({ floorFor: () => [] });
    const approved = (conditions: GrantRule["conditions"]) =>
      evaluate(input("work", [{ ...rule("tool:fetch", "allow"), conditions }]));

    await expect(approved(window(-1, 1))).resolves.toMatchObject({
      effect: "allow",
    });
    await expect(approved(window(2, 3))).resolves.toMatchObject({
      effect: null,
    });
    await expect(
      evaluate(
        input("work", [
          rule("tool:fetch", "allow"),
          { ...rule("tool:fetch", "deny"), conditions: window(-1, 1) },
        ]),
      ),
    ).resolves.toMatchObject({ effect: "deny" });
  });

  test("fails closed on a condition the child cannot evaluate", async () => {
    const evaluate = createGrantEvaluator({ floorFor: () => [] });
    await expect(
      evaluate(
        input("work", [
          { ...rule("tool:fetch", "allow"), conditions: { tool: "tool:x" } },
        ]),
      ),
    ).rejects.toThrow('Unknown condition: "tool"');
  });
});

describe("a run child's authorize closure over grants that crossed JSON", () => {
  function authorizeOver(grants: readonly GrantRule[]) {
    return createCredentialsBackedAuthorize(
      createSnapshotGrantsResolver(
        {
          current: {
            steps: [
              {
                stepId: "work",
                address: "work",
                grants: overJSON(grants),
                contentHash: "h",
              },
            ],
          },
        },
        "lbl",
      ),
      createGrantEvaluator({ floorFor: () => [] }),
    );
  }
  const ctx = { stepId: "work", attempt: 1, runId: "run-1" };

  test("does not allow on a row whose expiry has passed", async () => {
    const authorize = authorizeOver([
      rule("tool:fetch", "allow", new Date(Date.now() - 60_000)),
    ]);
    await expect(authorize("tool:fetch", "invoke", ctx)).resolves.toMatchObject(
      { effect: null },
    );
  });

  test("allows on a row whose expiry has not passed", async () => {
    const authorize = authorizeOver([
      rule("tool:fetch", "allow", new Date(Date.now() + 3_600_000)),
    ]);
    await expect(authorize("tool:fetch", "invoke", ctx)).resolves.toMatchObject(
      { effect: "allow" },
    );
  });
});
