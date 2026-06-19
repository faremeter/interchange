// Integration tests for @intx/agent lifecycle that exercise the real
// isogit-backed context store but do not require driving inference.
//
// These tests cover the singleton-per-workdir lock, the
// close-and-reopen contract that backs the resume-from-crash story,
// and the post-close-method guards. Tests that drive real inference
// live alongside this file in `send-flow.test.ts`.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  AgentContextLockError,
  createAgent,
  createDefaultDirectorRegistry,
  defineAgent,
  type Agent,
  type AgentDefinition,
  type BaseEnv,
} from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit";
import type { InboundMessage, InferenceSource } from "@intx/types/runtime";

/**
 * Test helper: build an `InboundMessage` shaped just enough to exercise
 * the `closed` guard on `agent.deliver`. The agent rejects before
 * inspecting the message contents.
 */
function stubInboundMessage(): InboundMessage {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub never inspected
  return {} as InboundMessage;
}

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-lifecycle",
  model: "claude-3-5-sonnet",
};

function definition(): AgentDefinition<BaseEnv> {
  return defineAgent({
    id: "lifecycle-test",
    systemPrompt: "lifecycle test agent",
    tools: [],
    capabilities: [],
    inference: {
      sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
    },
  });
}

async function envFor(workdir: string): Promise<BaseEnv> {
  const storage = await createIsogitStore(workdir);
  return {
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    storage,
    workdir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
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
    const first = await createAgent(definition(), await envFor(dir));
    try {
      await expect(
        createAgent(definition(), await envFor(dir)),
      ).rejects.toBeInstanceOf(AgentContextLockError);
    } finally {
      await first.close();
    }
  });

  test("close releases the lock so a fresh agent can reopen the same dir", async () => {
    const dir = join(workDir, "ctx");

    const first = await createAgent(definition(), await envFor(dir));
    await first.close();

    const second = await createAgent(definition(), await envFor(dir));
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
    const agent = await createAgent(definition(), await envFor(dir));
    await agent.close();
    await agent.close();
    // A subsequent open on the same dir should still succeed.
    const reopened = await createAgent(definition(), await envFor(dir));
    await reopened.close();
  });

  test("methods after close throw AgentClosedError", async () => {
    const dir = join(workDir, "ctx");
    const agent: Agent = await createAgent(definition(), await envFor(dir));
    await agent.close();

    expect(() => agent.deliver(stubInboundMessage())).toThrow();
    expect(() => agent.setSource(SOURCE)).toThrow();
    await expect(agent.send("hi")).rejects.toBeDefined();
  });
});
