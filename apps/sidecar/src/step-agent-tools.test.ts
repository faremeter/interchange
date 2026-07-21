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
  type BaseEnv,
  type ToolBundle,
} from "@intx/agent";
import { createDefaultDirectorRegistry } from "@intx/agent";
import { createIsogitStore } from "@intx/storage-isogit";
import { noopAuditStore } from "@intx/agent/testing";
import type { InferenceSource } from "@intx/types/runtime";

import {
  attachStepTools,
  createToolBearingAgentFactory,
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
  apiKey: "sk-test",
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
      factories: [noopTool],
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
});

describe("stepDeployTreeDir base-step resolution", () => {
  const dataDir = "/data";
  const mailboxAddress = "ins_dep-map@example.com";

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
    const rewrapped = rewrapStepToolFactory(source, () => {
      throw new Error("onDispose must not be called: factory is not invoked");
    });

    expect(rewrapped.definitions).toEqual(source.definitions);
    expect(rewrapped.id).toBe(source.id);
    expect(rewrapped.requires).toEqual(source.requires);
  });
});
