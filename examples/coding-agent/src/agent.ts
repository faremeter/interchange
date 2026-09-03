// Factory for the coding-agent reference consumer.
//
// `createCodingAgent` builds an `@intx/agent` Agent wired against the
// posix tools (with the LSP plugin attached) and a real workdir-backed
// isogit store. The factory is separated from the CLI entry so tests
// can construct the same agent with a stubbed inference source.
//
// The working directory flows through env-DI: `env.toolCwd` is the tree
// the model reads and edits, distinct from `env.workdir` (the isogit
// lock and storage boundary). The posix bundle and the LSP plugin both
// read `toolCwd`, so the reference consumer demonstrates the env-DI
// surface rather than bypassing it with constructor arguments.

import { mkdirSync } from "node:fs";

import {
  createAgent,
  createDefaultDirectorRegistry,
  defineAgent,
  defineTool,
  type Agent,
  type Dependencies,
} from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit/node";
import { lsp } from "@intx/tools-lsp/sidecar-bundle";
import { posix, type PosixToolEnv } from "@intx/tools-posix/sidecar-bundle";
import type { InferenceSource } from "@intx/types/runtime";

import { CODING_AGENT_SYSTEM_PROMPT } from "./prompt";

export const DEFAULT_MODEL = "claude-sonnet-5";
export const DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com";

export type CodingAgentOptions = {
  /** Where to persist conversation history and audit records. */
  contextDir: string;
  /** Working directory for tool calls (read/write/grep are scoped here). */
  cwd: string;
  /**
   * Anthropic API key. Required unless `sourceOverride` is set, in
   * which case it is ignored.
   */
  apiKey?: string;
  /** Override the default Anthropic Claude model. */
  model?: string;
  /**
   * Inject a fully-formed inference source (typically used by tests that
   * stub the inference layer). When provided, `apiKey` and `model` are
   * ignored.
   */
  sourceOverride?: InferenceSource;
  /**
   * Inference dependencies for the underlying agent. Production callers
   * leave this undefined; tests pass `setupHarness().deps` from
   * `@intx/inference-testing` to swap the fetch implementation.
   */
  deps?: Dependencies;
};

export type CodingAgent = {
  agent: Agent;
  /**
   * Close the agent and dispose of the tool runner. Idempotent enough to
   * be called from a `finally` block.
   */
  close(): Promise<void>;
};

export async function createCodingAgent(
  opts: CodingAgentOptions,
): Promise<CodingAgent> {
  // Resolve the inference source before building any tool resource, so a
  // missing apiKey fails before there is anything to tear down.
  let source: InferenceSource;
  if (opts.sourceOverride !== undefined) {
    source = opts.sourceOverride;
  } else {
    if (opts.apiKey === undefined) {
      throw new Error(
        "createCodingAgent: either apiKey or sourceOverride is required",
      );
    }
    const model = opts.model ?? DEFAULT_MODEL;
    source = {
      id: `anthropic:${model}`,
      provider: "anthropic",
      baseURL: DEFAULT_ANTHROPIC_BASE_URL,
      apiKey: opts.apiKey,
      model,
    };
  }

  mkdirSync(opts.contextDir, { recursive: true });
  const storage = await createIsogitStore(opts.contextDir);

  // Wrap the shipped posix sidecar bundle to capture its disposer as the
  // factory runs inside createAgent. Bundle lifetimes are caller-owned on
  // the success path, and disposing the posix bundle chains through
  // createPosixTools to the LSP subprocess. Forwarding posix.requires
  // keeps the toolCwd env-DI contract enforced by validateEnv.
  //
  // This capture-dispose shape is duplicated in apps/sidecar
  // (rewrapStepToolFactory); it is inlined here rather than shared
  // because that helper carries a sidecar-specific credentials parameter
  // and lives in an app that examples must not depend on. A shared helper
  // belongs next to defineTool in @intx/agent, but only once a third
  // consumer justifies touching that package's public surface.
  let disposeTools: (() => Promise<void>) | undefined;
  const posixFactory = defineTool<PosixToolEnv>({
    id: posix.id,
    requires: posix.requires,
    definitions: posix.definitions,
    factory: (env) => {
      const bundle = posix(env);
      disposeTools = bundle.dispose;
      return bundle;
    },
  });

  const def = defineAgent({
    id: "coding-agent",
    systemPrompt: CODING_AGENT_SYSTEM_PROMPT,
    tools: [posixFactory],
    capabilities: [],
    inference: {
      sources: [{ provider: source.provider, model: source.model }],
    },
  });

  const env: PosixToolEnv = {
    sources: [source],
    defaultSource: source.id,
    storage,
    workdir: opts.contextDir,
    toolCwd: opts.cwd,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
    ...(opts.deps !== undefined ? { deps: opts.deps } : {}),
  };

  // The LSP plugin is a host responsibility: instantiate it (it reads
  // env.toolCwd) and hand it to the agent through a fresh env carrying
  // plugins, the same composition the deploy path performs. posix's
  // factory reads it back off env.plugins and attaches it to its bundle.
  const lspInstance = lsp(env);
  const envWithPlugins: PosixToolEnv = { ...env, plugins: [lspInstance] };

  let agent: Agent;
  try {
    agent = await createAgent(def, envWithPlugins);
  } catch (cause) {
    // Idempotent dual-dispose: the posix disposer if its factory ran
    // (which chains to the LSP plugin), plus the LSP instance directly in
    // case createAgent threw before the posix factory ran.
    if (disposeTools !== undefined) await disposeTools();
    if (lspInstance.dispose !== undefined) await lspInstance.dispose();
    throw cause;
  }

  return {
    agent,
    async close() {
      await agent.close();
      if (disposeTools !== undefined) await disposeTools();
    },
  };
}
