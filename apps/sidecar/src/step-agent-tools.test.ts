// LSP-lifecycle seam test for the workflow-process child's tool-bearing
// agent factory.
//
// What this asserts, precisely:
//   - The agent factory built by `createToolBearingAgentFactory` runs
//     each materialized plugin factory when it builds the step's agent
//     (the plugin chain mirrors `default-harness.ts`).
//   - The plugin's `dispose` runs when `agent.close()` is called, and
//     `agent.close()` is what the step-invoker adapter calls in its
//     `finally` on every exit path.
//
// What this does NOT assert: a real language server protocol exchange.
// The real LSP plugin (`@intx/tools-lsp` `createLSPPlugin`) spawns its
// server subprocess LAZILY -- only when a tool touches a file -- and its
// `dispose` chains to `lsp.dispose()`, which terminates whatever server
// subprocesses were spawned. This test stands in a plugin whose factory
// spawns a REAL subprocess eagerly and whose `dispose` kills it, so the
// load-bearing seam under test -- "the child's agent.close() tears down
// the plugin's subprocess" -- is exercised against a real OS process
// without depending on a language-server binary being present in CI.
// The LSP-specific lazy-spawn behavior is covered by the `tools-lsp`
// package's own tests; what is sidecar-specific (and new in Phase 2) is
// the close -> plugin-dispose wiring proven here.

import { describe, test, expect, afterEach } from "bun:test";

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  definePlugin,
  defineTool,
  type AnnotatedToolFactory,
  type BaseEnv,
  type ToolBundle,
  type ToolDeclaration,
} from "@intx/agent";
import { createDefaultDirectorRegistry } from "@intx/agent";
import { evaluateGrants } from "@intx/authz";
import type { HostCredentialCapability } from "@intx/harness";
import { createIsogitStore } from "@intx/storage-isogit/node";
import { noopAuditStore } from "@intx/agent/testing";
import type { GrantRule } from "@intx/types/authz";
import type { InferenceSource } from "@intx/types/runtime";
import {
  createRuntimeCapabilities,
  type RuntimeCapabilities,
} from "@intx/types/runtime-capabilities";
import type { LoadedToolFactory } from "@intx/tool-packaging";

import {
  attachStepTools,
  createToolBearingAgentFactory,
  deriveToolMarkFloorGrants,
  rewrapStepToolFactory,
  stepDeployTreeDir,
  type StepToolMaterialization,
} from "./step-agent-tools";

const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(
    tempDirs
      .splice(0)
      .map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

async function tempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "step-agent-tools-test-"),
  );
  tempDirs.push(d);
  return d;
}

const SOURCE: InferenceSource = {
  id: "anthropic:mock-model",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-test",
  model: "mock-model",
};

async function buildStepEnv(): Promise<BaseEnv> {
  const dir = await tempDir();
  const workdir = path.join(dir, "workspace");
  await fs.promises.mkdir(workdir, { recursive: true });
  const storage = await createIsogitStore(dir);
  return {
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    storage,
    workdir,
    audit: noopAuditStore(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    directors: createDefaultDirectorRegistry(),
  };
}

/**
 * Returns `true` if a process with the given pid is alive. `kill(pid, 0)`
 * throws ESRCH when the process does not exist and EPERM when it exists
 * but is not signalable by this user; either non-throw / EPERM means
 * "alive", ESRCH means "gone".
 */
function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    if (err instanceof Error && "code" in err && err.code === "EPERM") {
      return true;
    }
    return false;
  }
}

describe("createToolBearingAgentFactory plugin/LSP lifecycle", () => {
  test("agent.close() runs the plugin disposer and tears down its subprocess", async () => {
    // A plugin standing in for the LSP plugin: its factory spawns a REAL
    // subprocess (a sleeping shell) and its `dispose` kills it. This is
    // the same shape `createLSPPlugin` produces -- a `ToolPlugin` whose
    // `dispose` terminates a server subprocess -- minus the lazy spawn.
    let spawnedPid: number | undefined;
    let disposeCalls = 0;
    const lspLikePlugin = definePlugin({
      id: "@intx/tools-lsp-fake/sidecar-bundle",
      factory: () => {
        const proc = Bun.spawn(["sleep", "120"], {
          stdout: "ignore",
          stderr: "ignore",
        });
        spawnedPid = proc.pid;
        return {
          tools: [],
          dispose: () => {
            disposeCalls += 1;
            proc.kill();
          },
        };
      },
    });

    // A trivial tool factory so the agent has at least one tool bundle.
    const noopTool = defineTool({
      id: "@intx/test-tool/sidecar-bundle",
      requires: [],
      definitions: [],
      factory: (): ToolBundle => ({
        definitions: [],
        run: (call) => Promise.resolve({ callId: call.id, content: "" }),
      }),
    });

    const materialization: StepToolMaterialization = {
      factories: [
        {
          packageName: "@intx/test-tool",
          declaredCredentials: [],
          factory: noopTool,
        },
      ],
      pluginFactories: [lspLikePlugin],
    };

    const env = await buildStepEnv();
    attachStepTools(env, materialization);
    // The step-invoker adapter spreads the env (`{ ...envBase, authorize }`)
    // before handing it to the agent factory; replicate that spread so the
    // test exercises the symbol-slot-survives-spread path.
    const spreadEnv: BaseEnv = { ...env };

    const agentFactory = createToolBearingAgentFactory();
    const def = {
      id: "agent-lsp-lifecycle",
      systemPrompt: "lsp lifecycle test",
      toolFactories: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    } as const;

    const agent = await agentFactory(def, spreadEnv);

    // The plugin factory ran during agent build: its subprocess is live.
    if (spawnedPid === undefined) {
      throw new Error("plugin factory did not spawn a subprocess");
    }
    expect(isAlive(spawnedPid)).toBe(true);
    expect(disposeCalls).toBe(0);

    // Closing the agent must run the plugin disposer, which kills the
    // subprocess. This is the exact call the step-invoker adapter makes
    // in its `finally`.
    await agent.close();

    expect(disposeCalls).toBe(1);
    // The process is reaped; allow a brief moment for the OS to reflect
    // the kill.
    let alive = isAlive(spawnedPid);
    for (let i = 0; i < 50 && alive; i += 1) {
      await new Promise((r) => setTimeout(r, 20));
      alive = isAlive(spawnedPid);
    }
    expect(alive).toBe(false);
  });

  test("a plugin factory that throws mid-chain disposes the already-built plugins", async () => {
    // Mirrors `default-harness.ts`'s plugin-construction rollback: if a
    // later plugin factory throws, every earlier plugin instance must be
    // disposed so a partial-success chain does not leak (the LSP server
    // subprocess being the resource that would leak in production).
    let spawnedPid: number | undefined;
    let disposed = false;
    const firstPlugin = definePlugin({
      id: "@intx/first-plugin/sidecar-bundle",
      factory: () => {
        const proc = Bun.spawn(["sleep", "120"], {
          stdout: "ignore",
          stderr: "ignore",
        });
        spawnedPid = proc.pid;
        return {
          tools: [],
          dispose: () => {
            disposed = true;
            proc.kill();
          },
        };
      },
    });
    const throwingPlugin = definePlugin({
      id: "@intx/throwing-plugin/sidecar-bundle",
      factory: () => {
        throw new Error("plugin construction failure");
      },
    });

    const materialization: StepToolMaterialization = {
      factories: [],
      pluginFactories: [firstPlugin, throwingPlugin],
    };

    const env = await buildStepEnv();
    attachStepTools(env, materialization);

    const agentFactory = createToolBearingAgentFactory();
    const def = {
      id: "agent-plugin-rollback",
      systemPrompt: "plugin rollback test",
      toolFactories: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    } as const;

    await expect(agentFactory(def, { ...env })).rejects.toThrow(
      /plugin construction failure/,
    );

    expect(disposed).toBe(true);
    if (spawnedPid === undefined) {
      throw new Error("first plugin factory did not spawn a subprocess");
    }
    let alive = isAlive(spawnedPid);
    for (let i = 0; i < 50 && alive; i += 1) {
      await new Promise((r) => setTimeout(r, 20));
      alive = isAlive(spawnedPid);
    }
    expect(alive).toBe(false);
  });

  test("agent.close() rejects with an AggregateError when a disposer fails, and still runs the rest", async () => {
    // The underlying agent close succeeds; a plugin disposer (standing in
    // for the LSP subprocess kill) then throws. That failure must surface
    // through close() rather than be swallowed -- a leaked LSP subprocess
    // is otherwise invisible -- and one failing disposer must not strand
    // the others.
    const disposeError = new Error("lsp dispose boom");
    let survivorDisposed = 0;
    const throwingPlugin = definePlugin({
      id: "@intx/throwing-disposer/sidecar-bundle",
      factory: () => ({
        tools: [],
        dispose: () => {
          throw disposeError;
        },
      }),
    });
    const survivorPlugin = definePlugin({
      id: "@intx/survivor-disposer/sidecar-bundle",
      factory: () => ({
        tools: [],
        dispose: () => {
          survivorDisposed += 1;
        },
      }),
    });

    const materialization: StepToolMaterialization = {
      factories: [],
      pluginFactories: [throwingPlugin, survivorPlugin],
    };

    const env = await buildStepEnv();
    attachStepTools(env, materialization);

    const agentFactory = createToolBearingAgentFactory();
    const def = {
      id: "agent-disposer-aggregate",
      systemPrompt: "disposer aggregate test",
      toolFactories: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    } as const;

    const agent = await agentFactory(def, { ...env });

    let thrown: unknown;
    try {
      await agent.close();
    } catch (cause) {
      thrown = cause;
    }
    expect(thrown).toBeInstanceOf(AggregateError);
    if (!(thrown instanceof AggregateError)) {
      throw new Error("expected an AggregateError from a failing teardown");
    }
    expect(thrown.errors).toContain(disposeError);
    // The loop kept going after the first disposer threw: the other ran.
    expect(survivorDisposed).toBe(1);
  });

  test("a disposer that throws during construction rollback does not mask the construction error", async () => {
    // On a construction-failure rollback the pending construction error is
    // the one worth surfacing; a disposer failure during that rollback is
    // logged but must never replace it.
    const firstPlugin = definePlugin({
      id: "@intx/rollback-throwing-disposer/sidecar-bundle",
      factory: () => ({
        tools: [],
        dispose: () => {
          throw new Error("rollback dispose boom");
        },
      }),
    });
    const throwingPlugin = definePlugin({
      id: "@intx/rollback-construction-throw/sidecar-bundle",
      factory: () => {
        throw new Error("plugin construction failure");
      },
    });

    const materialization: StepToolMaterialization = {
      factories: [],
      pluginFactories: [firstPlugin, throwingPlugin],
    };

    const env = await buildStepEnv();
    attachStepTools(env, materialization);

    const agentFactory = createToolBearingAgentFactory();
    const def = {
      id: "agent-rollback-dispose-mask",
      systemPrompt: "rollback dispose mask test",
      toolFactories: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    } as const;

    await expect(agentFactory(def, { ...env })).rejects.toThrow(
      /plugin construction failure/,
    );
  });

  test("a second close() after a failed teardown neither re-runs disposers nor rethrows", async () => {
    let disposeCalls = 0;
    const throwingPlugin = definePlugin({
      id: "@intx/double-close-disposer/sidecar-bundle",
      factory: () => ({
        tools: [],
        dispose: () => {
          disposeCalls += 1;
          throw new Error("dispose boom");
        },
      }),
    });

    const materialization: StepToolMaterialization = {
      factories: [],
      pluginFactories: [throwingPlugin],
    };

    const env = await buildStepEnv();
    attachStepTools(env, materialization);

    const agentFactory = createToolBearingAgentFactory();
    const def = {
      id: "agent-double-close",
      systemPrompt: "double close test",
      toolFactories: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    } as const;

    const agent = await agentFactory(def, { ...env });

    await expect(agent.close()).rejects.toBeInstanceOf(AggregateError);
    expect(disposeCalls).toBe(1);
    // wrapAgentClose guards teardown behind `tornDown`, so a second close
    // is a no-op: it neither re-runs the disposer nor re-surfaces the
    // failure.
    await agent.close();
    expect(disposeCalls).toBe(1);
  });
});

describe("stepDeployTreeDir base-step resolution", () => {
  const dataDir = "/data";
  const mailboxAddress = "run_dep-map@example.com";

  test("a map iteration resolves the base step's deploy tree", () => {
    // Deploy stages one deploy tree per base step; every map iteration
    // `<base>[<index>]` must read that one tree, not a per-iteration address
    // that was never staged.
    const base = stepDeployTreeDir({
      dataDir,
      mailboxAddress,
      stepId: "summarize",
      stepCount: 2,
    });
    const iter0 = stepDeployTreeDir({
      dataDir,
      mailboxAddress,
      stepId: "summarize[0]",
      stepCount: 2,
    });
    const iter1 = stepDeployTreeDir({
      dataDir,
      mailboxAddress,
      stepId: "summarize[1]",
      stepCount: 2,
    });
    expect(iter0).toBe(base);
    expect(iter1).toBe(base);
  });

  test("distinct base steps still resolve distinct deploy trees", () => {
    const a = stepDeployTreeDir({
      dataDir,
      mailboxAddress,
      stepId: "alpha[0]",
      stepCount: 2,
    });
    const b = stepDeployTreeDir({
      dataDir,
      mailboxAddress,
      stepId: "beta[0]",
      stepCount: 2,
    });
    expect(a).not.toBe(b);
  });
});

describe("rewrapStepToolFactory", () => {
  test("preserves the source factory's static definitions on the re-wrap", () => {
    const source = defineTool({
      id: "@intx/test-tool/sidecar-bundle",
      requires: ["transport"],
      definitions: [{ name: "alpha" }, { name: "beta" }],
      factory: (): ToolBundle => ({
        definitions: [],
        run: (call) => Promise.resolve({ callId: call.id, content: "" }),
      }),
    });

    // This test only inspects the re-wrapped factory's static metadata;
    // it never invokes the factory, so the disposer-capture callback
    // must not fire.
    const rewrapped = rewrapStepToolFactory(
      source,
      () => {
        throw new Error("onDispose must not be called: factory is not invoked");
      },
      undefined,
    );

    expect(rewrapped.definitions).toEqual(source.definitions);
    expect(rewrapped.id).toBe(source.id);
    expect(rewrapped.requires).toEqual(source.requires);
  });

  test("layers the given credentials capability onto the bundle's env", async () => {
    let seenEnv: BaseEnv | undefined;
    const source = defineTool({
      id: "@intx/test-tool/sidecar-bundle",
      requires: ["capabilities"],
      definitions: [],
      factory: (factoryEnv): ToolBundle => {
        seenEnv = factoryEnv;
        return {
          definitions: [],
          run: (call) => Promise.resolve({ callId: call.id, content: "" }),
        };
      },
    });
    const capability: HostCredentialCapability = {
      resolve: () => Promise.reject(new Error("resolve unused in this test")),
      dispose: () => Promise.resolve(),
    };

    const rewrapped = rewrapStepToolFactory(
      source,
      () => {
        /* the bundle returns no disposer, so this never fires */
      },
      capability,
    );

    const env = await buildStepEnv();
    // The base bag buildEnv would set; an empty one suffices to prove the
    // credentials key is layered on top of it for this bundle.
    Reflect.set(env, "capabilities", createRuntimeCapabilities({}));
    rewrapped(env);

    if (seenEnv === undefined) {
      throw new Error("the bundle factory was not invoked");
    }
    // The bundle sees a layered bag resolving THIS package's capability -- the
    // seam that must never hand a bundle another package's capability.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the slot is the RuntimeCapabilities resolver this test set
    const bag = Reflect.get(seenEnv, "capabilities") as RuntimeCapabilities;
    expect(bag.resolve("credentials")).toBe(capability);
  });

  test("passes the base env through unchanged when no capability is layered", async () => {
    let seenEnv: BaseEnv | undefined;
    const source = defineTool({
      id: "@intx/test-tool/sidecar-bundle",
      requires: ["capabilities"],
      definitions: [],
      factory: (factoryEnv): ToolBundle => {
        seenEnv = factoryEnv;
        return {
          definitions: [],
          run: (call) => Promise.resolve({ callId: call.id, content: "" }),
        };
      },
    });

    const rewrapped = rewrapStepToolFactory(
      source,
      () => {
        /* no disposer captured in this test */
      },
      undefined,
    );

    const env = await buildStepEnv();
    Reflect.set(env, "capabilities", createRuntimeCapabilities({}));
    rewrapped(env);

    if (seenEnv === undefined) {
      throw new Error("the bundle factory was not invoked");
    }
    // No capability -> the exact base env is passed through, no credentials key
    // layered on, so a resolve fails closed as not-provided.
    expect(seenEnv).toBe(env);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the slot is the RuntimeCapabilities resolver this test set
    const bag = Reflect.get(seenEnv, "capabilities") as RuntimeCapabilities;
    expect(() => bag.resolve("credentials")).toThrow();
  });
});

describe("deriveToolMarkFloorGrants", () => {
  // A loaded tool factory carrying only the static metadata the floor
  // deriver reads (`definitions`), shaped like the pinned factories the
  // child's loader hands back. Never invoked here.
  function loadedFactory(
    id: string,
    definitions: readonly ToolDeclaration[],
  ): LoadedToolFactory {
    const factory: AnnotatedToolFactory<BaseEnv> = Object.assign(
      (_env: BaseEnv) => ({
        definitions: [],
        run: () =>
          Promise.resolve({ callId: "", content: "", isError: false as const }),
      }),
      { id, requires: [] as readonly string[], definitions },
    );
    return factory;
  }

  // Mirror the sidecar's grant evaluator merge: the credentials snapshot's
  // grants plus the derived floor, resolved via `evaluateGrants`. This is
  // the exact composition the `evaluateGrantsAdapter` performs.
  async function resolveWithFloor(
    toolName: string,
    factories: readonly LoadedToolFactory[],
    snapshotGrants: readonly GrantRule[],
  ): Promise<"allow" | "deny" | "ask" | null> {
    const floor = deriveToolMarkFloorGrants(factories);
    const result = await evaluateGrants(
      [...snapshotGrants, ...floor],
      `tool:${toolName}`,
      "invoke",
    );
    return result.effect;
  }

  test("a pinned ask-marked tool resolves to ask on its floor alone", async () => {
    const factories = [
      loadedFactory("@intx/tools-posix/sidecar-bundle", [
        { name: "run_shell", approval: "ask" },
      ]),
    ];
    expect(await resolveWithFloor("run_shell", factories, [])).toBe("ask");
  });

  test("a pinned unmarked tool resolves to allow on its floor alone", async () => {
    const factories = [
      loadedFactory("@intx/tools-mail/sidecar-bundle", [{ name: "mail_send" }]),
    ];
    expect(await resolveWithFloor("mail_send", factories, [])).toBe("allow");
  });

  test("a declared deny still beats the derived floor", async () => {
    const factories = [
      loadedFactory("@intx/tools-posix/sidecar-bundle", [
        { name: "run_shell", approval: "ask" },
      ]),
    ];
    // A credentials-snapshot grant explicitly denying the tool at equal
    // specificity. `deny` (priority 2) outranks the derived `ask`, so the
    // floor never overrides an explicit denial.
    const declaredDeny: GrantRule = {
      id: "declared-deny",
      resource: "tool:run_shell",
      action: "invoke",
      effect: "deny",
      origin: "creator",
      conditions: null,
      expiresAt: null,
      roleId: null,
      principalId: null,
    };
    expect(await resolveWithFloor("run_shell", factories, [declaredDeny])).toBe(
      "deny",
    );
  });

  test("an allow grant at equal specificity does not drop the ask floor", async () => {
    const factories = [
      loadedFactory("@intx/tools-posix/sidecar-bundle", [
        { name: "run_shell", approval: "ask" },
      ]),
    ];
    // A run grant explicitly ALLOWING the tool at the SAME specificity as the
    // derived floor (both `tool:run_shell`/`invoke`, exact match). `ask`
    // (priority 1) outranks `allow` (priority 0) at equal specificity, so the
    // floor holds -- a workflow cannot declare its way below a tool's
    // approval gate. This is the `ask > allow` half of the effect ordering in
    // packages/authz/src/evaluate.ts.
    const declaredAllow: GrantRule = {
      id: "declared-allow",
      resource: "tool:run_shell",
      action: "invoke",
      effect: "allow",
      origin: "creator",
      conditions: null,
      expiresAt: null,
      roleId: null,
      principalId: null,
    };
    expect(
      await resolveWithFloor("run_shell", factories, [declaredAllow]),
    ).toBe("ask");
  });

  test("a broader allow glob loses to the exact ask floor on specificity", async () => {
    const factories = [
      loadedFactory("@intx/tools-posix/sidecar-bundle", [
        { name: "run_shell", approval: "ask" },
      ]),
    ];
    // A run grant allowing EVERY tool via a `tool:*` glob. The glob is far
    // less specific than the exact `tool:run_shell` floor, so specificity --
    // ranked before effect -- resolves to the exact floor regardless of
    // effect. The exact `ask` wins; the broad `allow` never enters the effect
    // tie-break.
    const broadAllow: GrantRule = {
      id: "broad-allow",
      resource: "tool:*",
      action: "invoke",
      effect: "allow",
      origin: "creator",
      conditions: null,
      expiresAt: null,
      roleId: null,
      principalId: null,
    };
    expect(await resolveWithFloor("run_shell", factories, [broadAllow])).toBe(
      "ask",
    );
  });
});
