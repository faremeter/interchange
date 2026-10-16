// The default action invoker builds each action's credentials capability
// before the handler runs, hands the handler a view that can only resolve, and
// disposes the capability when the handler settles.

import { describe, test, expect } from "bun:test";

import type {
  CredentialCapability,
  HostCredentialCapability,
  MediatedCredential,
} from "@intx/types";

import type { WorkflowAuthorizeFn } from "../authorize-context";
import type { ActionHandler } from "../definition/index";
import type {
  ActionCredentialsFactory,
  ActionInvokeRequest,
  EffectLedger,
} from "../runtime/env";
import {
  createDefaultActionInvoker,
  unavailableActionCredentials,
} from "./run-local";

const allow: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

const authzContext = { stepId: "push", attempt: 1, runId: "run-1" };

function request(
  overrides: Partial<ActionInvokeRequest> = {},
): ActionInvokeRequest {
  return {
    handler: "push",
    input: null,
    requires: ["github.release"],
    credentials: ["github"],
    authzContext,
    signal: new AbortController().signal,
    ...overrides,
  };
}

// A ledger that records every lookup, so a test can prove no effect ran.
function trackingLedger(): { ledger: EffectLedger; lookups: string[] } {
  const lookups: string[] = [];
  const store = new Map<string, { output: unknown }>();
  return {
    lookups,
    ledger: {
      async lookup(effectKey) {
        lookups.push(effectKey);
        return store.get(effectKey);
      },
      async record(effectKey, output) {
        store.set(effectKey, { output });
      },
    },
  };
}

const handle: MediatedCredential = {
  kind: "http",
  fetch: () => Promise.resolve(new Response("ok")),
  dispose() {
    /* the fake handle holds nothing to release */
  },
};

// A factory over one fake capability that records its request and disposals.
function recordingFactory(opts: { disposeError?: Error } = {}): {
  factory: ActionCredentialsFactory;
  requests: Parameters<ActionCredentialsFactory>[0][];
  disposals: { count: number };
} {
  const requests: Parameters<ActionCredentialsFactory>[0][] = [];
  const disposals = { count: 0 };
  const capability: HostCredentialCapability = {
    resolve: (h) =>
      h === "github"
        ? Promise.resolve(handle)
        : Promise.reject(new Error(`unbound ${h}`)),
    dispose: () => {
      disposals.count += 1;
      return opts.disposeError === undefined
        ? Promise.resolve()
        : Promise.reject(opts.disposeError);
    },
  };
  return {
    requests,
    disposals,
    factory: (req) => {
      requests.push(req);
      return capability;
    },
  };
}

function invokerFor(
  handler: ActionHandler,
  factory: ActionCredentialsFactory,
  ledger: EffectLedger = trackingLedger().ledger,
) {
  return createDefaultActionInvoker(allow, ledger, () => handler, factory);
}

describe("action credentials in the default invoker", () => {
  test("a handler resolves its declared handle through the factory's capability", async () => {
    const { factory, requests } = recordingFactory();
    const invoke = invokerFor(async (_input, ctx) => {
      const resolved = await ctx.credentials.resolve("github");
      return resolved.kind;
    }, factory);

    await expect(invoke(request())).resolves.toEqual({ output: "http" });
    expect(requests).toEqual([
      { handler: "push", credentials: ["github"], authzContext },
    ]);
  });

  test("the handler's view of the capability cannot dispose it", async () => {
    const { factory } = recordingFactory();
    const invoke = invokerFor(
      async (_input, ctx) => Object.keys(ctx.credentials),
      factory,
    );
    await expect(invoke(request())).resolves.toEqual({ output: ["resolve"] });
  });

  test("the capability is disposed once, after the handler's resolve settles", async () => {
    const { factory, disposals } = recordingFactory();
    const seen: number[] = [];
    await invokerFor(async (_input, ctx) => {
      seen.push(disposals.count);
      await ctx.credentials.resolve("github");
      seen.push(disposals.count);
      return "done";
    }, factory)(request());
    expect(seen).toEqual([0, 0]);
    expect(disposals.count).toBe(1);
  });

  test("the capability is disposed once, after the handler throws, and the handler error surfaces", async () => {
    const { factory, disposals } = recordingFactory();
    const seen: number[] = [];
    const invoke = invokerFor(async () => {
      seen.push(disposals.count);
      throw new Error("handler failed");
    }, factory);
    await expect(invoke(request())).rejects.toThrow("handler failed");
    expect(seen).toEqual([0]);
    expect(disposals.count).toBe(1);
  });

  test("the capability is disposed when the effect context cannot be built", async () => {
    const { factory, disposals } = recordingFactory();
    const invoke = invokerFor(async () => "done", factory);
    await expect(
      invoke(request({ authzContext: { stepId: "push", attempt: 1 } })),
    ).rejects.toThrow("createEffectContext requires runId and stepId");
    expect(disposals.count).toBe(1);
  });

  test("a view kept past the invocation refuses to resolve", async () => {
    const { factory } = recordingFactory();
    const kept: { view?: CredentialCapability } = {};
    await invokerFor(async (_input, ctx) => {
      kept.view = ctx.credentials;
      return "done";
    }, factory)(request());
    if (kept.view === undefined) throw new Error("the handler did not run");
    await expect(kept.view.resolve("github")).rejects.toThrow(
      'action push cannot resolve credential handle "github": its credentials capability is disposed',
    );
  });

  test("a dispose failure after a successful handler fails the invocation", async () => {
    const { factory } = recordingFactory({
      disposeError: new Error("dispose failed"),
    });
    const invoke = invokerFor(async () => "done", factory);
    await expect(invoke(request())).rejects.toThrow("dispose failed");
  });

  test("a handler failure and a dispose failure surface together", async () => {
    const { factory } = recordingFactory({
      disposeError: new Error("dispose failed"),
    });
    const invoke = invokerFor(async () => {
      throw new Error("handler failed");
    }, factory);
    const failure: unknown = await invoke(request()).catch(
      (error: unknown) => error,
    );
    if (!(failure instanceof AggregateError)) {
      throw new Error(`expected an AggregateError, got ${String(failure)}`);
    }
    expect(failure.message).toContain("action push");
    expect(
      failure.errors.map((error: unknown) =>
        error instanceof Error ? error.message : String(error),
      ),
    ).toEqual(["handler failed", "dispose failed"]);
  });

  test("a factory that throws runs no part of the handler and no effect", async () => {
    const { ledger, lookups } = trackingLedger();
    let handlerCalls = 0;
    const invoke = invokerFor(
      async (_input, ctx) => {
        handlerCalls += 1;
        return ctx.perform({
          effectId: "release",
          capability: "github.release",
          run: () => Promise.resolve("released"),
        });
      },
      () => {
        throw new Error("no binding for github");
      },
      ledger,
    );
    await expect(invoke(request())).rejects.toThrow("no binding for github");
    expect(handlerCalls).toBe(0);
    expect(lookups).toEqual([]);
  });

  test("a handler ref that does not resolve never builds the capability", async () => {
    const { factory, requests } = recordingFactory();
    const invoke = createDefaultActionInvoker(
      allow,
      trackingLedger().ledger,
      () => {
        throw new Error("no handler named push");
      },
      factory,
    );
    await expect(invoke(request())).rejects.toThrow("no handler named push");
    expect(requests).toEqual([]);
  });
});

describe("unavailableActionCredentials", () => {
  test("refuses an action that declares credentials before its handler runs", async () => {
    let handlerCalls = 0;
    const invoke = invokerFor(async () => {
      handlerCalls += 1;
      return null;
    }, unavailableActionCredentials);
    await expect(invoke(request())).rejects.toThrow(
      "action push declares credential handle(s) github, but no credential source is configured for action handlers",
    );
    expect(handlerCalls).toBe(0);
  });

  test("gives an action that declares none a capability that fails closed", async () => {
    const invoke = invokerFor(
      async (_input, ctx) => ctx.credentials.resolve("github"),
      unavailableActionCredentials,
    );
    await expect(invoke(request({ credentials: [] }))).rejects.toThrow(
      'action push cannot resolve credential handle "github": no credential source is configured for action handlers',
    );
  });
});
