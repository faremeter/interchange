// createEffectContext: per-effect ledger keying and fail-closed
// capability enforcement.

import { describe, test, expect } from "bun:test";

import {
  createEffectContext,
  type EffectLedger,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

function allowAll(): WorkflowAuthorizeFn {
  return async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });
}

function inMemoryLedger(): { ledger: EffectLedger; keys: string[] } {
  const store = new Map<string, { output: unknown }>();
  const keys: string[] = [];
  return {
    keys,
    ledger: {
      async lookup(effectKey) {
        return store.get(effectKey);
      },
      async record(effectKey, output) {
        keys.push(effectKey);
        store.set(effectKey, { output });
      },
    },
  };
}

describe("createEffectContext per-effect keying", () => {
  test("two distinct effectIds in one handler do not collide", async () => {
    const { ledger, keys } = inMemoryLedger();
    let cloneRuns = 0;
    let buildRuns = 0;
    const ctx = createEffectContext({
      authorize: allowAll(),
      effects: ledger,
      requires: ["git:clone", "shell:run"],
      authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
      input: { repo: "x" },
    });

    const clone = await ctx.perform({
      effectId: "clone",
      capability: "git:clone",
      run: async () => {
        cloneRuns += 1;
        return "cloned";
      },
    });
    const build = await ctx.perform({
      effectId: "build",
      capability: "shell:run",
      run: async () => {
        buildRuns += 1;
        return "built";
      },
    });

    expect(clone).toBe("cloned");
    expect(build).toBe("built");
    expect(cloneRuns).toBe(1);
    expect(buildRuns).toBe(1);
    // Two distinct keys recorded: no collapse.
    expect(new Set(keys).size).toBe(2);
  });

  test("re-drive against a shared ledger reconstructs each effect's output", async () => {
    const shared = inMemoryLedger();
    let cloneRuns = 0;
    let buildRuns = 0;

    const runHandler = async () => {
      const ctx = createEffectContext({
        authorize: allowAll(),
        effects: shared.ledger,
        requires: ["git:clone", "shell:run"],
        authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
        input: { repo: "x" },
      });
      const clone = await ctx.perform({
        effectId: "clone",
        capability: "git:clone",
        run: async () => {
          cloneRuns += 1;
          return `clone-${String(cloneRuns)}`;
        },
      });
      const build = await ctx.perform({
        effectId: "build",
        capability: "shell:run",
        run: async () => {
          buildRuns += 1;
          return `build-${String(buildRuns)}`;
        },
      });
      return { clone, build };
    };

    const first = await runHandler();
    const second = await runHandler();

    expect(first).toEqual({ clone: "clone-1", build: "build-1" });
    // Re-drive: neither real effect ran again; each output is reconstructed
    // from its own key, not cross-wired.
    expect(second).toEqual({ clone: "clone-1", build: "build-1" });
    expect(cloneRuns).toBe(1);
    expect(buildRuns).toBe(1);
  });

  test("the same effectId under different input yields a different key", async () => {
    const { ledger, keys } = inMemoryLedger();
    const mk = (input: unknown) =>
      createEffectContext({
        authorize: allowAll(),
        effects: ledger,
        requires: ["git:commit"],
        authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
        input,
      });

    await mk({ msg: "a" }).perform({
      effectId: "commit",
      capability: "git:commit",
      run: async () => "sha-a",
    });
    await mk({ msg: "b" }).perform({
      effectId: "commit",
      capability: "git:commit",
      run: async () => "sha-b",
    });

    expect(new Set(keys).size).toBe(2);
  });
});

describe("createEffectContext fail-closed enforcement", () => {
  test("an undeclared capability throws before authorize is consulted", async () => {
    let authorizeCalls = 0;
    const authorize: WorkflowAuthorizeFn = async () => {
      authorizeCalls += 1;
      return { effect: "allow", matchingGrants: [], resolvedBy: null };
    };
    const ctx = createEffectContext({
      authorize,
      effects: inMemoryLedger().ledger,
      requires: ["git:commit"],
      authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
      input: null,
    });

    await expect(
      ctx.perform({
        effectId: "x",
        capability: "shell:run",
        run: async () => "ran",
      }),
    ).rejects.toThrow(/not in its declared requires set/);
    expect(authorizeCalls).toBe(0);
  });

  test("only an allow decision proceeds: deny, ask, and null all block", async () => {
    for (const effect of ["deny", "ask", null] as const) {
      let effectRan = 0;
      const authorize: WorkflowAuthorizeFn = async () => ({
        effect,
        matchingGrants: [],
        resolvedBy: null,
      });
      const ctx = createEffectContext({
        authorize,
        effects: inMemoryLedger().ledger,
        requires: ["git:commit"],
        authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
        input: null,
      });

      await expect(
        ctx.perform({
          effectId: "c",
          capability: "git:commit",
          run: async () => {
            effectRan += 1;
            return "x";
          },
        }),
      ).rejects.toThrow();
      expect(effectRan).toBe(0);
    }
  });
});
