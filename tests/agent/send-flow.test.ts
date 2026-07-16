// Integration tests for @intx/agent send/close flow driven by the
// @intx/inference-testing harness.
//
// Each test wires a deterministic Anthropic SSE response, constructs an
// agent backed by a real isogit context store and the harness's stubbed
// fetch, and drives the reactor through a real send cycle. These tests
// are what prove the agent's wiring actually carries inference events
// through to `connector.reply` and persists turns.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  AgentClosedError,
  createAgent,
  createDefaultDirectorRegistry,
  defineAgent,
  SendQueueFullError,
  type Agent,
  type AgentDefinition,
  type BaseEnv,
} from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { setupHarness, type Harness } from "@intx/inference-testing";
import { createIsogitStore } from "@intx/storage-isogit";
import type { ConversationTurn, InferenceSource } from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-send-flow",
  model: "claude-3-5-sonnet",
};

function definition(): AgentDefinition<BaseEnv> {
  return defineAgent({
    id: "send-flow-test",
    systemPrompt: "send-flow test",
    tools: [],
    capabilities: [],
    inference: {
      sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
    },
  });
}

async function envFor(
  workdir: string,
  harness: Harness,
  extras: Partial<BaseEnv> = {},
): Promise<BaseEnv> {
  const storage = await createIsogitStore(workdir);
  return {
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    storage,
    workdir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
    deps: harness.deps,
    ...extras,
  };
}

describe("@intx/agent send-flow integration", () => {
  let workDir: string;
  let harness: Harness;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-send-flow-"));
    harness = setupHarness();
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("end-to-end send round-trips to a connector.reply", async () => {
    harness.scenario.replyOnce("anthropic", { text: "Hi there!" });

    const agent = await createAgent(
      definition(),
      await envFor(join(workDir, "ctx"), harness),
    );

    try {
      const sendPromise = agent.send("Hello");
      await harness.run();
      const result = await sendPromise;
      if (result.type !== "reply") {
        throw new Error(`expected a reply outcome, got ${result.type}`);
      }
      expect(result.reply).toBe("Hi there!");
      expect(result.turn.role).toBe("assistant");
    } finally {
      await agent.close();
    }
  });

  test("persists the assistant turn so history() and reopened agents see it", async () => {
    harness.scenario.replyOnce("anthropic", { text: "Persisted reply" });

    const dir = join(workDir, "ctx");
    const agent = await createAgent(definition(), await envFor(dir, harness));

    let firstHistory: ConversationTurn[] = [];
    try {
      const sendPromise = agent.send("Hello");
      await harness.run();
      await sendPromise;
      firstHistory = await agent.history();
    } finally {
      await agent.close();
    }

    expect(firstHistory.length).toBeGreaterThan(0);
    expect(firstHistory.some((t) => t.role === "assistant")).toBe(true);

    // Reopen on the same directory and verify the projection survives.
    // The reopened agent does not need a working provider; we only
    // exercise history(), which reads from the store directly.
    const reopened = await createAgent(
      definition(),
      await envFor(dir, harness),
    );

    try {
      const turns = await reopened.history();
      expect(turns.length).toBe(firstHistory.length);
      expect(turns.some((t) => t.role === "assistant")).toBe(true);
    } finally {
      await reopened.close();
    }
  });

  test("readAt returns the conversation at the recorded commit hash", async () => {
    harness.scenario.replyOnce("anthropic", { text: "Checkpoint reply" });

    const agent = await createAgent(
      definition(),
      await envFor(join(workDir, "ctx"), harness),
    );

    try {
      const sendPromise = agent.send("Hello");
      await harness.run();
      await sendPromise;

      const checkpoints = await agent.checkpoints(10);
      expect(checkpoints.length).toBeGreaterThan(0);
      const latest = checkpoints[0];
      if (latest === undefined)
        throw new Error("expected at least one checkpoint");

      const turns = await agent.readAt(latest.hash);
      expect(turns.length).toBeGreaterThan(0);
    } finally {
      await agent.close();
    }
  });

  test("close-while-pending rejects queued sends with AgentClosedError", async () => {
    harness.scenario.replyOnce("anthropic", { text: "should never observe" });

    const agent: Agent = await createAgent(
      definition(),
      await envFor(join(workDir, "ctx"), harness),
    );

    let firstReason: unknown;
    let secondReason: unknown;
    const first = agent.send("first").catch((err: unknown) => {
      firstReason = err;
    });
    const second = agent.send("second").catch((err: unknown) => {
      secondReason = err;
    });

    await agent.close();

    await first;
    await second;

    expect(firstReason).toBeInstanceOf(AgentClosedError);
    expect(secondReason).toBeInstanceOf(AgentClosedError);
  });

  test("setSource hot-swaps credentials before the next inference", async () => {
    harness.scenario.replyOnce("anthropic", { text: "first-provider reply" });

    const agent = await createAgent(
      definition(),
      await envFor(join(workDir, "ctx"), harness),
    );

    try {
      const first = agent.send("Hello");
      await harness.run();
      await first;

      agent.setSource({
        id: SOURCE.id,
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "sk-rotated",
        model: SOURCE.model,
      });

      harness.scenario.replyOnce("anthropic", { text: "rotated reply" });

      const second = agent.send("Hello again");
      await harness.run();
      const r = await second;
      if (r.type !== "reply") {
        throw new Error(`expected a reply outcome, got ${r.type}`);
      }
      expect(r.reply).toBe("rotated reply");

      const matched = harness.scenario.matchedRequests().at(-1);
      if (matched === undefined) {
        throw new Error("expected a matched request after setSource swap");
      }
      expect(matched.headers.get("x-api-key")).toBe("sk-rotated");
    } finally {
      await agent.close();
    }
  });

  test("setSource rotates the model in the next inference request", async () => {
    harness.scenario.replyOnce("anthropic", { text: "first-model reply" });

    const agent = await createAgent(
      definition(),
      await envFor(join(workDir, "ctx"), harness),
    );

    try {
      const first = agent.send("Hello");
      await harness.run();
      await first;

      const NEW_MODEL = "claude-3-5-haiku";
      agent.setSource({
        id: `anthropic:${NEW_MODEL}`,
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: SOURCE.apiKey,
        model: NEW_MODEL,
      });

      harness.scenario.replyOnce("anthropic", {
        text: "second-model reply",
      });

      const second = agent.send("Hello again");
      await harness.run();
      const r = await second;
      if (r.type !== "reply") {
        throw new Error(`expected a reply outcome, got ${r.type}`);
      }
      expect(r.reply).toBe("second-model reply");

      const matched = harness.scenario.matchedRequests().at(-1);
      if (matched === undefined) {
        throw new Error("expected a matched request after model swap");
      }
      const body: unknown = await matched.json();
      if (typeof body !== "object" || body === null || !("model" in body)) {
        throw new Error("expected inference request body to include `model`");
      }
      expect(body.model).toBe(NEW_MODEL);
    } finally {
      await agent.close();
    }
  });

  test("abort signal on in-flight send rejects without blocking the agent", async () => {
    harness.scenario.replyOnce("anthropic", { text: "abandoned reply" });

    const agent = await createAgent(
      definition(),
      await envFor(join(workDir, "ctx"), harness),
    );

    try {
      const ctl = new AbortController();
      let abortedReason: unknown;
      const sendPromise = agent
        .send("Hello", { signal: ctl.signal })
        .catch((err: unknown) => {
          abortedReason = err;
        });

      ctl.abort();
      await sendPromise;
      expect(abortedReason).toBeDefined();

      await harness.run();

      harness.scenario.replyOnce("anthropic", { text: "post-abort reply" });
      const next = agent.send("Round two");
      await harness.run();
      const result = await next;
      if (result.type !== "reply") {
        throw new Error(`expected a reply outcome, got ${result.type}`);
      }
      expect(result.reply).toBe("post-abort reply");
    } finally {
      await agent.close();
    }
  });

  test("synchronously throws SendQueueFullError past the configured cap", async () => {
    harness.scenario.replyOnce("anthropic", { text: "stalled response" });

    const agent = await createAgent(
      definition(),
      await envFor(join(workDir, "ctx"), harness, { sendQueueMax: 2 }),
    );

    try {
      const p1 = agent.send("a");
      const p2 = agent.send("b");
      expect(() => agent.send("c")).toThrow(SendQueueFullError);
      p1.catch(() => undefined);
      p2.catch(() => undefined);
    } finally {
      await agent.close();
    }
  });
});
