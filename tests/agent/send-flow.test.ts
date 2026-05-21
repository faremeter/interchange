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
  SendQueueFullError,
  type Agent,
} from "@intx/agent";
import { setupHarness, type Harness } from "@intx/inference-testing";
import type { ConversationTurn, ProviderConfig } from "@intx/types/runtime";

const PROVIDER: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-send-flow",
  model: "claude-3-5-sonnet",
};

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

    const agent = await createAgent({
      contextDir: join(workDir, "ctx"),
      providers: [PROVIDER],
      defaultModel: PROVIDER.model ?? "claude-3-5-sonnet",
      systemPrompt: "send-flow test",
      tools: [],
      deps: harness.deps,
    });

    try {
      const sendPromise = agent.send("Hello");
      await harness.run();
      const result = await sendPromise;
      expect(result.reply).toBe("Hi there!");
      expect(result.turn.role).toBe("assistant");
    } finally {
      await agent.close();
    }
  });

  test("persists the assistant turn so history() and reopened agents see it", async () => {
    harness.scenario.replyOnce("anthropic", { text: "Persisted reply" });

    const dir = join(workDir, "ctx");
    const agent = await createAgent({
      contextDir: dir,
      providers: [PROVIDER],
      defaultModel: PROVIDER.model ?? "claude-3-5-sonnet",
      systemPrompt: "send-flow test",
      tools: [],
      deps: harness.deps,
    });

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
    const reopened = await createAgent({
      contextDir: dir,
      providers: [PROVIDER],
      defaultModel: PROVIDER.model ?? "claude-3-5-sonnet",
      systemPrompt: "send-flow test",
      tools: [],
      deps: harness.deps,
    });

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

    const agent = await createAgent({
      contextDir: join(workDir, "ctx"),
      providers: [PROVIDER],
      defaultModel: PROVIDER.model ?? "claude-3-5-sonnet",
      systemPrompt: "send-flow test",
      tools: [],
      deps: harness.deps,
    });

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

    const agent: Agent = await createAgent({
      contextDir: join(workDir, "ctx"),
      providers: [PROVIDER],
      defaultModel: PROVIDER.model ?? "claude-3-5-sonnet",
      systemPrompt: "send-flow test",
      tools: [],
      deps: harness.deps,
    });

    // Kick off an in-flight send, then a queued second one.
    let firstReason: unknown;
    let secondReason: unknown;
    const first = agent.send("first").catch((err: unknown) => {
      firstReason = err;
    });
    const second = agent.send("second").catch((err: unknown) => {
      secondReason = err;
    });

    // Close without advancing the clock — the in-flight inference is
    // parked on a matcher, so the active send hasn't completed yet.
    await agent.close();

    // Let any settle-microtasks complete.
    await first;
    await second;

    expect(firstReason).toBeInstanceOf(AgentClosedError);
    expect(secondReason).toBeInstanceOf(AgentClosedError);
  });

  test("setProvider hot-swaps credentials before the next inference", async () => {
    harness.scenario.replyOnce("anthropic", { text: "first-provider reply" });

    const agent = await createAgent({
      contextDir: join(workDir, "ctx"),
      providers: [PROVIDER],
      defaultModel: PROVIDER.model ?? "claude-3-5-sonnet",
      systemPrompt: "send-flow test",
      tools: [],
      deps: harness.deps,
    });

    try {
      const first = agent.send("Hello");
      await harness.run();
      await first;

      // Swap to a new credential. The next send picks up the new apiKey
      // when the reactor reads providerConfig at start-of-inference.
      agent.setProvider({
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "sk-rotated",
        model: PROVIDER.model ?? "claude-3-5-sonnet",
      });

      harness.scenario.replyOnce("anthropic", { text: "rotated reply" });

      const second = agent.send("Hello again");
      await harness.run();
      const r = await second;
      expect(r.reply).toBe("rotated reply");

      const matched = harness.scenario.matchedRequests().at(-1);
      if (matched === undefined) {
        throw new Error("expected a matched request after setProvider swap");
      }
      expect(matched.headers.get("x-api-key")).toBe("sk-rotated");
    } finally {
      await agent.close();
    }
  });

  test("setProvider rotates the model in the next inference request", async () => {
    harness.scenario.replyOnce("anthropic", { text: "first-model reply" });

    const agent = await createAgent({
      contextDir: join(workDir, "ctx"),
      providers: [PROVIDER],
      defaultModel: PROVIDER.model ?? "claude-3-5-sonnet",
      systemPrompt: "send-flow test",
      tools: [],
      deps: harness.deps,
    });

    try {
      const first = agent.send("Hello");
      await harness.run();
      await first;

      // Swap to a completely different model. The wrapped director's
      // capabilities.infer substitutes the live model on each call, so
      // the next request body should carry the new model name.
      const NEW_MODEL = "claude-3-5-haiku";
      agent.setProvider({
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: PROVIDER.apiKey,
        model: NEW_MODEL,
      });

      harness.scenario.replyOnce("anthropic", {
        text: "second-model reply",
      });

      const second = agent.send("Hello again");
      await harness.run();
      const r = await second;
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

    const agent = await createAgent({
      contextDir: join(workDir, "ctx"),
      providers: [PROVIDER],
      defaultModel: PROVIDER.model ?? "claude-3-5-sonnet",
      systemPrompt: "send-flow test",
      tools: [],
      deps: harness.deps,
    });

    try {
      const ctl = new AbortController();
      let abortedReason: unknown;
      const sendPromise = agent
        .send("Hello", { signal: ctl.signal })
        .catch((err: unknown) => {
          abortedReason = err;
        });

      // Abort while the inference is parked on the matcher.
      ctl.abort();
      await sendPromise;
      expect(abortedReason).toBeDefined();

      // Even though the previous cycle is still draining in the
      // background, advancing the clock allows it to complete so the
      // next send can run cleanly on the same agent.
      await harness.run();

      // Wire a fresh reply for the next send.
      harness.scenario.replyOnce("anthropic", { text: "post-abort reply" });
      const next = agent.send("Round two");
      await harness.run();
      const result = await next;
      expect(result.reply).toBe("post-abort reply");
    } finally {
      await agent.close();
    }
  });

  test("synchronously throws SendQueueFullError past the configured cap", async () => {
    harness.scenario.replyOnce("anthropic", { text: "stalled response" });

    const agent = await createAgent({
      contextDir: join(workDir, "ctx"),
      providers: [PROVIDER],
      defaultModel: PROVIDER.model ?? "claude-3-5-sonnet",
      systemPrompt: "send-flow test",
      tools: [],
      deps: harness.deps,
      sendQueueMax: 2,
    });

    try {
      // Two sends saturate the queue; the inference is parked on the
      // matcher with no clock advance, so none complete.
      const p1 = agent.send("a");
      const p2 = agent.send("b");
      expect(() => agent.send("c")).toThrow(SendQueueFullError);
      // Avoid unhandled-rejection warnings on the parked sends.
      p1.catch(() => undefined);
      p2.catch(() => undefined);
    } finally {
      await agent.close();
    }
  });
});
