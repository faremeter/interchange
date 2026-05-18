// Integration tests for @interchange/agent send/close flow driven by the
// @interchange/inference-testing harness.
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
} from "@interchange/agent";
import {
  setupHarness,
  wire,
  type Harness,
} from "@interchange/inference-testing";
import type {
  ConversationTurn,
  ProviderConfig,
} from "@interchange/types/runtime";

const PROVIDER: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-send-flow",
  model: "claude-3-5-sonnet",
};

const USAGE_HEAD = {
  input: 10,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

const USAGE_TAIL = {
  input: 0,
  output: 5,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

function wireSingleReply(harness: Harness, text: string): void {
  const stream = harness.scenario.createStream();
  const chunks = wire.completeResponse("anthropic", {
    text,
    headUsage: USAGE_HEAD,
    tailUsage: USAGE_TAIL,
  });
  // Schedule the chunks relative to the harness's current virtual time
  // so multiple wireSingleReply calls in the same test never schedule
  // into the past after an earlier round-trip has advanced the clock.
  stream.enqueueAll(chunks, { startAt: harness.clock.now() + 10 });
  harness.scenario.whenRequestMatches(() => true, stream);
}

describe("@interchange/agent send-flow integration", () => {
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
    wireSingleReply(harness, "Hi there!");

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
    wireSingleReply(harness, "Persisted reply");

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
    wireSingleReply(harness, "Checkpoint reply");

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
    wireSingleReply(harness, "should never observe");

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
    wireSingleReply(harness, "first-provider reply");

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

      let observedKey: string | undefined;
      const stream = harness.scenario.createStream();
      const chunks = wire.completeResponse("anthropic", {
        text: "rotated reply",
        headUsage: USAGE_HEAD,
        tailUsage: USAGE_TAIL,
      });
      stream.enqueueAll(chunks, { startAt: harness.clock.now() + 10 });
      harness.scenario.whenRequestMatches((req) => {
        observedKey = req.headers.get("x-api-key") ?? undefined;
        return true;
      }, stream);

      const second = agent.send("Hello again");
      await harness.run();
      const r = await second;
      expect(r.reply).toBe("rotated reply");
      expect(observedKey).toBe("sk-rotated");
    } finally {
      await agent.close();
    }
  });

  test("abort signal on in-flight send rejects without blocking the agent", async () => {
    wireSingleReply(harness, "abandoned reply");

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
      wireSingleReply(harness, "post-abort reply");
      const next = agent.send("Round two");
      await harness.run();
      const result = await next;
      expect(result.reply).toBe("post-abort reply");
    } finally {
      await agent.close();
    }
  });

  test("synchronously throws SendQueueFullError past the configured cap", async () => {
    wireSingleReply(harness, "stalled response");

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
