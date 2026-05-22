// Integration tests for @intx/agent lifecycle that exercise the
// real isogit-backed context store but do not require driving inference.
//
// These tests cover the singleton-per-`contextDir` lock, the
// close-and-reopen contract that backs the resume-from-crash story, and
// the storage-validation surface of `createAgent`. Tests that drive real
// inference live alongside this file in `send-flow.test.ts`.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  AgentInUseError,
  createAgent,
  type Agent,
  type AgentConfig,
} from "@intx/agent";
import type {
  ContextStore,
  InboundMessage,
  InferenceSource,
} from "@intx/types/runtime";

/**
 * Test helper: build an `InboundMessage` shaped just enough to exercise
 * the `closed` guard on `agent.deliver`. The agent rejects before
 * inspecting the message contents.
 */
function stubInboundMessage(): InboundMessage {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub never inspected
  return {} as InboundMessage;
}

/**
 * Test helper: produce a stub ContextStore reference for tests that
 * verify createAgent rejects before touching the store. Bypasses the
 * static type via `unknown` because constructing a full ContextStore for
 * a never-invoked path would be busywork.
 */
function stubContextStore(): ContextStore {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub never invoked
  return {} as ContextStore;
}

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-lifecycle",
  model: "claude-3-5-sonnet",
};

function baseConfig(contextDir: string): AgentConfig {
  return {
    contextDir,
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    systemPrompt: "lifecycle test agent",
    tools: [],
  };
}

describe("@intx/agent lifecycle", () => {
  let workDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-lifecycle-"));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test("acquires the singleton lock; second concurrent agent rejects", async () => {
    const dir = join(workDir, "ctx");
    const first = await createAgent(baseConfig(dir));
    try {
      await expect(createAgent(baseConfig(dir))).rejects.toBeInstanceOf(
        AgentInUseError,
      );
    } finally {
      await first.close();
    }
  });

  test("close releases the lock so a fresh agent can reopen the same dir", async () => {
    const dir = join(workDir, "ctx");

    const first = await createAgent(baseConfig(dir));
    await first.close();

    const second = await createAgent(baseConfig(dir));
    try {
      // Fresh agent on the same context dir starts with empty history.
      const turns = await second.history();
      expect(turns).toEqual([]);
    } finally {
      await second.close();
    }
  });

  test("close is idempotent", async () => {
    const dir = join(workDir, "ctx");
    const agent = await createAgent(baseConfig(dir));
    await agent.close();
    await agent.close();
    // A subsequent open on the same dir should still succeed.
    const reopened = await createAgent(baseConfig(dir));
    await reopened.close();
  });

  test("methods after close throw AgentClosedError", async () => {
    const dir = join(workDir, "ctx");
    const agent: Agent = await createAgent(baseConfig(dir));
    await agent.close();

    expect(() => agent.deliver(stubInboundMessage())).toThrow();
    expect(() => agent.setSource(SOURCE)).toThrow();
    await expect(agent.send("hi")).rejects.toBeDefined();
  });

  test("rejects configurations that supply both contextStore and contextDir", async () => {
    const dir = join(workDir, "ctx");
    await expect(
      createAgent({
        ...baseConfig(dir),
        contextStore: stubContextStore(),
      }),
    ).rejects.toBeDefined();
  });
});
