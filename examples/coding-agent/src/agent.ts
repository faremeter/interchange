// Factory for the coding-agent reference consumer.
//
// `createCodingAgent` builds an `@intx/agent` Agent wired against
// the posix tools (with the LSP plugin attached) and a real
// `contextDir`-backed isogit store. The factory is separated from the
// CLI entry so tests can construct the same agent with a stubbed
// inference source.

import { mkdirSync } from "node:fs";

import { createAgent, fromToolRunner, type Agent } from "@intx/agent";
import type { Dependencies } from "@intx/inference";
import { createLSPPlugin } from "@intx/tools-lsp";
import { createPosixTools, type PosixTools } from "@intx/tools-posix";
import type { InferenceSource } from "@intx/types/runtime";

import { CODING_AGENT_SYSTEM_PROMPT } from "./prompt";

export const DEFAULT_MODEL = "claude-3-5-sonnet-20241022";
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
  posixTools: PosixTools;
  /**
   * Close the agent and dispose of the tool runner. Idempotent enough to
   * be called from a `finally` block.
   */
  close(): Promise<void>;
};

export async function createCodingAgent(
  opts: CodingAgentOptions,
): Promise<CodingAgent> {
  mkdirSync(opts.contextDir, { recursive: true });

  const posixTools = createPosixTools({
    cwd: opts.cwd,
    plugins: [createLSPPlugin({ cwd: opts.cwd })],
  });

  let source: InferenceSource;
  if (opts.sourceOverride !== undefined) {
    source = opts.sourceOverride;
  } else {
    if (opts.apiKey === undefined) {
      await posixTools.dispose();
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

  let agent: Agent;
  try {
    agent = await createAgent({
      contextDir: opts.contextDir,
      sources: [source],
      defaultSource: source.id,
      systemPrompt: CODING_AGENT_SYSTEM_PROMPT,
      tools: fromToolRunner(posixTools),
      ...(opts.deps !== undefined ? { deps: opts.deps } : {}),
    });
  } catch (cause) {
    await posixTools.dispose();
    throw cause;
  }

  return {
    agent,
    posixTools,
    async close() {
      await agent.close();
      await posixTools.dispose();
    },
  };
}
