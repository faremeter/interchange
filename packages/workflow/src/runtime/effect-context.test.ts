// createEffectContext: per-effect ledger keying, fail-closed capability
// enforcement, and the credentials scope each `perform` gives its `run`.

import { describe, test, expect } from "bun:test";

import type {
  CredentialCapability,
  HttpMediatedCredential,
  MediatedCredential,
} from "@intx/types";
import {
  createEffectContext,
  unavailableCredentialCapability,
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
      handler: "test-action",
      authorize: allowAll(),
      effects: ledger,
      requires: ["git:clone", "shell:run"],
      authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
      input: { repo: "x" },
      credentials: unavailableCredentialCapability("test-action"),
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
        handler: "test-action",
        authorize: allowAll(),
        effects: shared.ledger,
        requires: ["git:clone", "shell:run"],
        authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
        input: { repo: "x" },
        credentials: unavailableCredentialCapability("test-action"),
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
        handler: "test-action",
        authorize: allowAll(),
        effects: ledger,
        requires: ["git:commit"],
        authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
        input,
        credentials: unavailableCredentialCapability("test-action"),
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
      handler: "test-action",
      authorize,
      effects: inMemoryLedger().ledger,
      requires: ["git:commit"],
      authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
      input: null,
      credentials: unavailableCredentialCapability("test-action"),
    });

    await expect(
      ctx.perform({
        effectId: "x",
        capability: "shell:run",
        run: async () => "ran",
      }),
    ).rejects.toThrow(
      /action test-action effect uses capability .* which is not in its declared requires set/,
    );
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
        handler: "test-action",
        authorize,
        effects: inMemoryLedger().ledger,
        requires: ["git:commit"],
        authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
        input: null,
        credentials: unavailableCredentialCapability("test-action"),
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

// A capability over one fake http handle that records each resolve, each
// fetch, and each dispose of the shaped handle.
function recordingCapability(): {
  capability: CredentialCapability;
  resolves: string[];
  fetches: string[];
  disposals: { count: number };
} {
  const resolves: string[] = [];
  const fetches: string[] = [];
  const disposals = { count: 0 };
  const handle: HttpMediatedCredential = {
    kind: "http",
    fetch: (input) => {
      fetches.push(String(input));
      return Promise.resolve(new Response("ok"));
    },
    dispose() {
      disposals.count += 1;
    },
  };
  return {
    resolves,
    fetches,
    disposals,
    capability: {
      resolve: (h) => {
        resolves.push(h);
        return h === "github"
          ? Promise.resolve(handle)
          : Promise.reject(new Error(`unbound ${h}`));
      },
    },
  };
}

function contextWith(
  credentials: CredentialCapability,
  opts: { ledger?: EffectLedger; authorize?: WorkflowAuthorizeFn } = {},
) {
  return createEffectContext({
    handler: "push",
    authorize: opts.authorize ?? allowAll(),
    effects: opts.ledger ?? inMemoryLedger().ledger,
    requires: ["github.release"],
    authzContext: { runId: "r1", stepId: "s1", attempt: 1 },
    input: null,
    credentials,
  });
}

function asHttp(mediated: MediatedCredential): HttpMediatedCredential {
  if (mediated.kind !== "http") throw new Error(`unexpected ${mediated.kind}`);
  return mediated;
}

describe("createEffectContext credentials scope", () => {
  test("run receives a scope that resolves and fetches through the capability", async () => {
    const rec = recordingCapability();
    const output = await contextWith(rec.capability).perform({
      effectId: "release",
      capability: "github.release",
      run: async ({ credentials }) => {
        const gh = asHttp(await credentials.resolve("github"));
        return (await gh.fetch("/releases")).status;
      },
    });
    expect(output).toBe(200);
    expect(rec.resolves).toEqual(["github"]);
    expect(rec.fetches).toEqual(["/releases"]);
  });

  test("a scope and its handles refuse once run settles", async () => {
    const rec = recordingCapability();
    const kept: {
      scope?: CredentialCapability;
      handle?: HttpMediatedCredential;
    } = {};
    await contextWith(rec.capability).perform({
      effectId: "release",
      capability: "github.release",
      run: async ({ credentials }) => {
        kept.scope = credentials;
        kept.handle = asHttp(await credentials.resolve("github"));
        return null;
      },
    });
    if (kept.scope === undefined || kept.handle === undefined) {
      throw new Error("run did not keep its scope");
    }
    await expect(kept.scope.resolve("github")).rejects.toThrow(
      'action push cannot resolve credential handle "github" outside the run of its effect release',
    );
    await expect(kept.handle.fetch("/late")).rejects.toThrow(
      'action push cannot use credential handle "github" outside the run of its effect release',
    );
    expect(rec.fetches).toEqual([]);
  });

  test("a run that throws surfaces the same error and closes its scope", async () => {
    const rec = recordingCapability();
    const failure = new Error("release failed");
    let kept: CredentialCapability | undefined;
    const thrown: unknown = await contextWith(rec.capability)
      .perform({
        effectId: "release",
        capability: "github.release",
        run: async ({ credentials }) => {
          kept = credentials;
          throw failure;
        },
      })
      .catch((error: unknown) => error);
    expect(thrown).toBe(failure);
    if (kept === undefined) throw new Error("run did not keep its scope");
    await expect(kept.resolve("github")).rejects.toThrow(
      "outside the run of its effect release",
    );
  });

  test("concurrent scopes are independent", async () => {
    const rec = recordingCapability();
    const ctx = contextWith(rec.capability);
    let releaseB: () => void = () => undefined;
    const bReady = new Promise<void>((resolve) => {
      releaseB = resolve;
    });
    let fromA: HttpMediatedCredential | undefined;
    const b = ctx.perform({
      effectId: "b",
      capability: "github.release",
      run: async ({ credentials }) => {
        const gh = asHttp(await credentials.resolve("github"));
        await bReady;
        return (await gh.fetch("/b")).status;
      },
    });
    await ctx.perform({
      effectId: "a",
      capability: "github.release",
      run: async ({ credentials }) => {
        fromA = asHttp(await credentials.resolve("github"));
        // Disposing a scoped handle releases nothing the other scope uses.
        await fromA.dispose();
        return null;
      },
    });
    releaseB();
    await expect(b).resolves.toBe(200);
    expect(rec.disposals.count).toBe(0);
    expect(rec.fetches).toEqual(["/b"]);
    if (fromA === undefined) throw new Error("scope A did not resolve");
    await expect(fromA.fetch("/a-late")).rejects.toThrow(
      "outside the run of its effect a",
    );
  });

  test("a handle from one scope refuses inside a later scope", async () => {
    const rec = recordingCapability();
    const ctx = contextWith(rec.capability);
    let fromA: HttpMediatedCredential | undefined;
    await ctx.perform({
      effectId: "a",
      capability: "github.release",
      run: async ({ credentials }) => {
        fromA = asHttp(await credentials.resolve("github"));
        return null;
      },
    });
    await expect(
      ctx.perform({
        effectId: "b",
        capability: "github.release",
        run: async () => {
          if (fromA === undefined) throw new Error("scope A did not resolve");
          return fromA.fetch("/b");
        },
      }),
    ).rejects.toThrow("outside the run of its effect a");
    expect(rec.fetches).toEqual([]);
  });

  test("a record failure after run surfaces as the perform error", async () => {
    const rec = recordingCapability();
    const ledger: EffectLedger = {
      lookup: async () => undefined,
      record: async () => {
        throw new Error("record failed");
      },
    };
    await expect(
      contextWith(rec.capability, { ledger }).perform({
        effectId: "release",
        capability: "github.release",
        run: async ({ credentials }) => {
          await credentials.resolve("github");
          return null;
        },
      }),
    ).rejects.toThrow("record failed");
  });

  test("a kept handle refuses use while the effect is recorded", async () => {
    const rec = recordingCapability();
    let kept: HttpMediatedCredential | undefined;
    let duringRecord: unknown;
    const ledger: EffectLedger = {
      lookup: async () => undefined,
      record: async () => {
        if (kept === undefined) throw new Error("run did not keep its handle");
        duringRecord = await kept
          .fetch("/during-record")
          .catch((error: unknown) => error);
      },
    };
    await contextWith(rec.capability, { ledger }).perform({
      effectId: "release",
      capability: "github.release",
      run: async ({ credentials }) => {
        kept = asHttp(await credentials.resolve("github"));
        return null;
      },
    });
    expect(duringRecord).toBeInstanceOf(Error);
    expect(rec.fetches).toEqual([]);
  });

  test("a resolve in flight when run settles rejects", async () => {
    const rec = recordingCapability();
    let finish: () => void = () => undefined;
    const slow = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const slowCapability: CredentialCapability = {
      resolve: async (h) => {
        await slow;
        return rec.capability.resolve(h);
      },
    };
    let inflight: Promise<unknown> | undefined;
    await contextWith(slowCapability).perform({
      effectId: "release",
      capability: "github.release",
      run: async ({ credentials }) => {
        inflight = credentials.resolve("github");
        inflight.catch(() => undefined);
        return null;
      },
    });
    finish();
    await expect(inflight).rejects.toThrow(
      'action push cannot resolve credential handle "github" outside the run of its effect release',
    );
  });

  test("a closed scope does not reach the capability", async () => {
    const rec = recordingCapability();
    let kept: CredentialCapability | undefined;
    await contextWith(rec.capability).perform({
      effectId: "release",
      capability: "github.release",
      run: async ({ credentials }) => {
        kept = credentials;
        return null;
      },
    });
    if (kept === undefined) throw new Error("run did not keep its scope");
    await expect(kept.resolve("github")).rejects.toThrow(
      "outside the run of its effect release",
    );
    expect(rec.resolves).toEqual([]);
  });

  test("a ledger hit runs nothing and resolves no credential", async () => {
    const rec = recordingCapability();
    const shared = inMemoryLedger();
    const perform = () =>
      contextWith(rec.capability, { ledger: shared.ledger }).perform({
        effectId: "release",
        capability: "github.release",
        run: async ({ credentials }) => {
          await credentials.resolve("github");
          return "released";
        },
      });
    await expect(perform()).resolves.toBe("released");
    await expect(perform()).resolves.toBe("released");
    expect(rec.resolves).toEqual(["github"]);
  });

  test("a denied effect opens no scope", async () => {
    const rec = recordingCapability();
    let runs = 0;
    await expect(
      contextWith(rec.capability, {
        authorize: async () => ({
          effect: "deny",
          matchingGrants: [],
          resolvedBy: null,
        }),
      }).perform({
        effectId: "release",
        capability: "github.release",
        run: async ({ credentials }) => {
          runs += 1;
          await credentials.resolve("github");
          return null;
        },
      }),
    ).rejects.toThrow("was not authorized");
    expect(runs).toBe(0);
    expect(rec.resolves).toEqual([]);
  });
});
