import { describe, test, expect } from "bun:test";

import { type } from "arktype";

import type { ContextStore, InferenceSource } from "@intx/types/runtime";

import { createAgent } from "./agent";
import { defineAgent } from "./definition";
import { defineDirector } from "./director";
import {
  createDefaultDirectorRegistry,
  createDirectorRegistry,
} from "./director-registry";
import { AgentEnvError, type BaseEnv } from "./env";
import { noopAuditStore } from "./testing/audit-noop";
import { permissiveAuthorize } from "./testing/authorize-allow";
import { defineTool } from "./tool";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test",
  model: "claude-3-5-sonnet",
};

function stubContextStore(): ContextStore {
  // Env-validation tests reject before the agent touches the store; the
  // stub never has any of its methods called.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub, never invoked on the validation path
  return {} as ContextStore;
}

function minimalDef() {
  return defineAgent({
    id: "agent",
    systemPrompt: "test",
    tools: [],
    capabilities: [],
    inference: {
      sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
    },
  });
}

function baseEnv(workdir: string): BaseEnv {
  return {
    source: SOURCE,
    storage: stubContextStore(),
    workdir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
  };
}

describe("createAgent env validation", () => {
  test("throws AgentEnvError when storage is missing", async () => {
    const env = baseEnv("/tmp/agent-env-test-storage");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional gap to exercise validateEnv
    const bad = { ...env, storage: undefined as unknown as ContextStore };
    await expect(createAgent(minimalDef(), bad)).rejects.toBeInstanceOf(
      AgentEnvError,
    );
  });

  test("throws AgentEnvError when audit is missing", async () => {
    const env = baseEnv("/tmp/agent-env-test-audit");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional gap to exercise validateEnv
    const bad = { ...env, audit: undefined as unknown as BaseEnv["audit"] };
    await expect(createAgent(minimalDef(), bad)).rejects.toBeInstanceOf(
      AgentEnvError,
    );
  });

  test("throws AgentEnvError when authorize is missing", async () => {
    const env = baseEnv("/tmp/agent-env-test-authorize");
    const bad = {
      ...env,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional gap to exercise validateEnv
      authorize: undefined as unknown as BaseEnv["authorize"],
    };
    await expect(createAgent(minimalDef(), bad)).rejects.toBeInstanceOf(
      AgentEnvError,
    );
  });

  test("blames a tool factory whose required env key is missing", async () => {
    // Tool factory declaring `requires: ["transport"]`; env omits it.
    interface MailEnv extends BaseEnv {
      transport: unknown;
    }
    const sendFactory = defineTool<MailEnv>({
      id: "@intx/tools-mail/send",
      requires: ["transport"],
      factory: () => ({
        definitions: [],
        async run(call) {
          return { callId: call.id, content: "" };
        },
      }),
    });

    const def = defineAgent({
      id: "mail",
      systemPrompt: "mail",
      tools: [sendFactory],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    // The type system already rejects this call -- def requires MailEnv,
    // env is BaseEnv. Widen the def at the call site to exercise the
    // runtime presence check, which is the whole point of the test.
    const env = baseEnv("/tmp/agent-env-test-mail");

    let caught: unknown;
    try {
      await createAgent(
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- bypass the type-level check to exercise runtime validateEnv
        def as unknown as Parameters<typeof createAgent<BaseEnv>>[0],
        env,
      );
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof AgentEnvError)) {
      throw new Error("expected AgentEnvError");
    }
    expect(caught.missing).toContain("transport");
    expect(caught.contributors).toContain("tool:@intx/tools-mail/send");
  });
});

describe("createAgent lock release on construction failure", () => {
  // The lock acquired against env.workdir must be released on every
  // failure path during construction. Otherwise a later createAgent
  // against the same workdir throws AgentContextLockError indefinitely
  // until the process exits. The narrow try/catch shape this test
  // pins guards against the regression where only the early tool /
  // source / director resolution path released the lock.
  function failingToolFactory() {
    return defineTool({
      id: "@intx-test/agent/failing",
      factory: () => {
        throw new Error("tool construction failed");
      },
    });
  }

  test("releases the workdir lock when tool factory construction throws", async () => {
    const tmpdir = `/tmp/agent-lock-leak-${Math.random().toString(36).slice(2)}`;
    const def = defineAgent({
      id: "agent",
      systemPrompt: "test",
      tools: [failingToolFactory()],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    let first: unknown;
    try {
      await createAgent(def, baseEnv(tmpdir));
    } catch (err) {
      first = err;
    }
    expect(first).toBeInstanceOf(Error);

    // The lock must be releasable; a second createAgent against the
    // same workdir hits the same failure and not AgentContextLockError.
    let second: unknown;
    try {
      await createAgent(def, baseEnv(tmpdir));
    } catch (err) {
      second = err;
    }
    expect(second).toBeInstanceOf(Error);
    if (!(second instanceof Error)) throw new Error("unreachable");
    expect(second.name).not.toBe("AgentContextLockError");
  });
});

describe("createAgent tool-rollback dispose handling", () => {
  // When a later tool factory throws (e.g. duplicate-name
  // DuplicateToolError), the agent disposes every bundle it has
  // already constructed so resources allocated at factory time do not
  // leak. The dispose contract is async, so a rejecting disposer must
  // not escape as an unhandled promise rejection -- the rollback path
  // discards the disposer's return value but has to wire any rejection
  // through a swallow handler. `void promise` would leave the
  // rejection in flight, which the surrounding synchronous try/catch
  // cannot observe and the runtime surfaces as
  // `unhandledRejection` -- producing the very noise the swallow
  // comment promises to prevent and, under a strict
  // `unhandledRejection: 'throw'` policy, crashing the process.

  test("absorbs an async disposer's rejection during rollback", async () => {
    let observedRejection: unknown = null;
    const onUnhandled = (err: unknown): void => {
      observedRejection = err;
    };
    process.on("unhandledRejection", onUnhandled);
    try {
      // Two factories that collide on tool name `dup`. The first
      // ships an async dispose that rejects after a microtask; the
      // second triggers the duplicate-name throw that drives the
      // rollback through the first's dispose.
      const firstFactory = defineTool({
        id: "@intx-test/agent/first",
        factory: () => ({
          definitions: [
            {
              name: "dup",
              description: "first",
              inputSchema: { type: "object" },
            },
          ],
          async run(call) {
            return { callId: call.id, content: "" };
          },
          async dispose() {
            await Promise.resolve();
            throw new Error("async dispose boom");
          },
        }),
      });
      const secondFactory = defineTool({
        id: "@intx-test/agent/second",
        factory: () => ({
          definitions: [
            {
              name: "dup",
              description: "second",
              inputSchema: { type: "object" },
            },
          ],
          async run(call) {
            return { callId: call.id, content: "" };
          },
        }),
      });

      const def = defineAgent({
        id: "agent",
        systemPrompt: "test",
        tools: [firstFactory, secondFactory],
        capabilities: [],
        inference: {
          sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
        },
      });

      const tmpdir = `/tmp/agent-dispose-rollback-${Math.random().toString(36).slice(2)}`;
      let caught: unknown;
      try {
        await createAgent(def, baseEnv(tmpdir));
      } catch (err) {
        caught = err;
      }
      // The original construction failure is what the caller sees;
      // the disposer's rejection is absorbed.
      expect(caught).toBeInstanceOf(Error);
      if (!(caught instanceof Error)) throw new Error("unreachable");
      expect(caught.name).toBe("DuplicateToolError");

      // Let any in-flight microtasks settle so an escaped rejection
      // would land.
      await new Promise((resolve) => setTimeout(resolve, 25));
      expect(observedRejection).toBeNull();
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  test("disposes constructed bundles when a post-resolveTools step throws", async () => {
    // The intra-resolveTools rollback handles failures inside the
    // tool-walk loop. A later createAgent step (resolveDirector here,
    // exercised via a registry whose default factory throws) leaves
    // the bundles `resolveTools` already built unreferenced: the
    // caller never reaches the returned agent, so the per-`ToolBundle`
    // "caller owns lifetime" contract has no caller to honor it.
    // The outer try/finally in createAgent must dispose them.
    let disposeCalls = 0;
    const probingFactory = defineTool({
      id: "@intx-test/agent/probing",
      factory: () => ({
        definitions: [
          {
            name: "probe",
            description: "noop",
            inputSchema: { type: "object" },
          },
        ],
        async run(call) {
          return { callId: call.id, content: "" };
        },
        async dispose() {
          disposeCalls += 1;
        },
      }),
    });

    const throwingDefault = defineDirector({
      id: "@intx-test/agent/throwing-default",
      configSchema: type({}),
      factory: () => {
        throw new Error("director construction failed");
      },
    });
    const directors = createDirectorRegistry({
      factories: [throwingDefault.factory],
      defaultId: throwingDefault.factory.id,
    });

    const def = defineAgent({
      id: "agent",
      systemPrompt: "test",
      tools: [probingFactory],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    const tmpdir = `/tmp/agent-post-resolvetools-${Math.random().toString(36).slice(2)}`;
    let caught: unknown;
    try {
      await createAgent(def, { ...baseEnv(tmpdir), directors });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);

    // The bundle was constructed before the director throw, so it
    // must be disposed.
    expect(disposeCalls).toBe(1);
  });
});
