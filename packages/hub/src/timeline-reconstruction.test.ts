import { describe, test, expect, afterAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import {
  IsogitStore,
  initAgentRepo,
  createMailAuditStore,
} from "@interchange/storage-isogit";
import type { ConversationMessage } from "@interchange/types/runtime";
import type { ErrorRecord } from "@interchange/types/audit";
import { reconstructTimeline } from "./timeline-reconstruction";

const tempDirs: string[] = [];

async function makeTempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "timeline-recon-"),
  );
  tempDirs.push(d);
  return d;
}

afterAll(async () => {
  for (const d of tempDirs) {
    await fs.promises.rm(d, { recursive: true, force: true });
  }
});

const NO_OPS: [] = [];
const NO_USAGE = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

function userMessage(
  text: string,
  timestamp = Date.now(),
): ConversationMessage {
  return { role: "user", content: [{ type: "text", text }], timestamp };
}

function assistantMessage(
  text: string,
  timestamp = Date.now(),
): ConversationMessage {
  return {
    role: "assistant",
    content: [{ type: "text", text }],
    model: "test-model",
    timestamp,
  };
}

function toolCallMessage(
  callId: string,
  name: string,
  args: Record<string, unknown>,
  timestamp = Date.now(),
): ConversationMessage {
  return {
    role: "assistant",
    content: [{ type: "tool_call", id: callId, name, arguments: args }],
    model: "test-model",
    timestamp,
  };
}

function toolResultMessage(
  callId: string,
  result: string,
  timestamp = Date.now(),
): ConversationMessage {
  return {
    role: "user",
    content: [
      {
        type: "tool_result",
        callId,
        content: [{ type: "text", text: result }],
      },
    ],
    timestamp,
  };
}

function buildRawMessage(opts: {
  messageId: string;
  from?: string;
  to?: string;
  inReplyTo?: string;
  body?: string;
}): Uint8Array {
  const lines: string[] = [];
  lines.push(`Message-ID: ${opts.messageId}`);
  lines.push(`From: ${opts.from ?? "sender@example.com"}`);
  lines.push(`To: ${opts.to ?? "recipient@example.com"}`);
  lines.push(`Date: ${new Date().toUTCString()}`);
  if (opts.inReplyTo !== undefined) {
    lines.push(`In-Reply-To: ${opts.inReplyTo}`);
  }
  lines.push("");
  lines.push(opts.body ?? "test body");
  return new TextEncoder().encode(lines.join("\r\n"));
}

describe("reconstructTimeline", () => {
  test("reconstructs a single-turn conversation", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    const t = 1700000000000;
    const messages: ConversationMessage[] = [
      userMessage("Hello", t),
      assistantMessage("Hi there", t + 1000),
    ];
    await store.commit(
      messages,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    const result = await reconstructTimeline(dir);

    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
    expect(turns[0]?.content).toBe("Hi there");
    // Should use the per-message timestamp, not the git commit timestamp
    expect(turns[0]?.timestamp).toBe(t + 1000);
    // Status derived from checkpoint reason
    expect(turns[0]?.kind === "turn" && turns[0].status).toBe("completed");
  });

  test("reconstructs multi-turn conversations across checkpoints", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    const t = 1700000000000;

    // First turn
    const messages1: ConversationMessage[] = [
      userMessage("Hello", t),
      assistantMessage("Hi there", t + 1000),
    ];
    await store.commit(
      messages1,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    // Second turn (appends to the same message array)
    const messages2: ConversationMessage[] = [
      ...messages1,
      userMessage("How are you?", t + 5000),
      assistantMessage("I'm doing well", t + 6000),
    ];
    await store.commit(
      messages2,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    const result = await reconstructTimeline(dir);

    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(2);
    expect(turns[0]?.content).toBe("Hi there");
    expect(turns[1]?.content).toBe("I'm doing well");

    // Should use per-message timestamps
    expect(turns[0]?.timestamp).toBe(t + 1000);
    expect(turns[1]?.timestamp).toBe(t + 6000);
  });

  test("treats a tool-use loop as a single turn", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    const messages: ConversationMessage[] = [
      userMessage("What's the weather?"),
      toolCallMessage("call-1", "get_weather", { city: "SF" }),
      toolResultMessage("call-1", "72F and sunny"),
      assistantMessage("The weather in SF is 72F and sunny."),
    ];
    await store.commit(
      messages,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    const result = await reconstructTimeline(dir);

    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
    expect(turns[0]?.content).toBe("The weather in SF is 72F and sunny.");
  });

  test("reconstructs mail events", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);

    const mailStore = await createMailAuditStore(dir);

    const inbound = buildRawMessage({
      messageId: "<msg-1@test>",
      from: "alice@example.com",
      to: "agent@example.com",
      body: "Please help me",
    });
    await mailStore.commitMail(inbound, "in");

    const outbound = buildRawMessage({
      messageId: "<msg-2@test>",
      from: "agent@example.com",
      to: "alice@example.com",
      inReplyTo: "<msg-1@test>",
      body: "Sure, how can I help?",
    });
    await mailStore.commitMail(outbound, "out");

    const result = await reconstructTimeline(dir);

    const mailEvents = result.events.filter((e) => e.kind === "mail");
    expect(mailEvents).toHaveLength(2);

    const inboundEvent = mailEvents.find(
      (e) => e.kind === "mail" && e.direction === "in",
    );
    expect(inboundEvent).toBeDefined();
    if (inboundEvent !== undefined && inboundEvent.kind === "mail") {
      expect(inboundEvent.messageId).toBe("<msg-1@test>");
    }

    const outboundEvent = mailEvents.find(
      (e) => e.kind === "mail" && e.direction === "out",
    );
    expect(outboundEvent).toBeDefined();
    if (outboundEvent !== undefined && outboundEvent.kind === "mail") {
      expect(outboundEvent.messageId).toBe("<msg-2@test>");
    }
  });

  test("links outbound mail to checkpoint via Checkpoint trailer", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    const t = 1700000000000;
    const messages: ConversationMessage[] = [
      userMessage("Hello", t),
      assistantMessage("Hi there", t + 1000),
    ];
    const commit = await store.commit(
      messages,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    const mailStore = await createMailAuditStore(dir);
    const outbound = buildRawMessage({
      messageId: "<reply@test>",
      from: "agent@example.com",
      to: "alice@example.com",
      body: "Hi there",
    });
    await mailStore.commitMail(outbound, "out", {
      checkpointHash: commit.hash,
    });

    const result = await reconstructTimeline(dir);

    const mailEvents = result.events.filter((e) => e.kind === "mail");
    expect(mailEvents).toHaveLength(1);
    const mailEvent = mailEvents[0];
    expect(mailEvent).toBeDefined();
    if (mailEvent !== undefined && mailEvent.kind === "mail") {
      expect(mailEvent.checkpointHash).toBe(commit.hash);
    }
  });

  test("associates error records from git with the preceding turn", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    // Write a conversation first
    const messages: ConversationMessage[] = [
      userMessage("Do something risky"),
      assistantMessage("Attempting..."),
    ];
    await store.commit(
      messages,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    // Write error records (committed to git by the store)
    const errors: ErrorRecord[] = [
      {
        source: "inference",
        category: "rate_limit",
        message: "Rate limit exceeded",
        fatal: false,
        timestamp: new Date().toISOString(),
        sessionId: "test-session",
        seq: 0,
      },
    ];
    await store.commitErrors(errors);

    const result = await reconstructTimeline(dir);

    // Errors should be attached to the preceding turn, not a synthetic one
    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
    const turn = turns[0];
    expect(turn?.kind === "turn" && turn.isError).toBe(true);
    expect(turn?.kind === "turn" && turn.errors).toHaveLength(1);
    if (turn?.kind === "turn") {
      expect(turn.errors?.[0]?.category).toBe("rate_limit");
    }

    // No error-association or working-tree gaps should be present
    const gapKinds = result.gaps.map((g) => g.kind);
    expect(gapKinds).not.toContain("no-error-turn-association");
    expect(gapKinds).not.toContain("errors-from-working-tree");
  });

  test("handles message-count regression gracefully", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    // First checkpoint with 4 messages
    const messages1: ConversationMessage[] = [
      userMessage("Hello"),
      assistantMessage("Hi"),
      userMessage("More"),
      assistantMessage("Sure"),
    ];
    await store.commit(
      messages1,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    // Second checkpoint with only 2 messages (regression)
    const messages2: ConversationMessage[] = [
      userMessage("Fresh start"),
      assistantMessage("OK"),
    ];
    await store.commit(
      messages2,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    const result = await reconstructTimeline(dir);

    // Should not crash — should produce a gap record
    const regressionGap = result.gaps.find(
      (g) => g.kind === "message-count-regression",
    );
    expect(regressionGap).toBeDefined();

    // Should still produce turn events from what it can reconstruct
    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns.length).toBeGreaterThan(0);
  });

  test("interleaves mail and turn events by timestamp", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);
    const mailStore = await createMailAuditStore(dir);

    // Inbound mail first
    const inbound = buildRawMessage({
      messageId: "<msg-1@test>",
      body: "Hello agent",
    });
    await mailStore.commitMail(inbound, "in");

    // Then a turn
    const messages: ConversationMessage[] = [
      userMessage("Hello"),
      assistantMessage("Hi there"),
    ];
    await store.commit(
      messages,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    const result = await reconstructTimeline(dir);

    // Both kinds should be present
    const mailEvents = result.events.filter((e) => e.kind === "mail");
    const turnEvents = result.events.filter((e) => e.kind === "turn");
    expect(mailEvents).toHaveLength(1);
    expect(turnEvents).toHaveLength(1);

    // Mail should come before turn (committed first)
    if (result.events[0] !== undefined && result.events[1] !== undefined) {
      expect(result.events[0].timestamp).toBeLessThanOrEqual(
        result.events[1].timestamp,
      );
    }
  });

  test("produces no gaps for a well-formed checkpoint", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    const t = 1700000000000;
    const messages: ConversationMessage[] = [
      userMessage("Hello", t),
      assistantMessage("Hi", t + 1000),
    ];
    await store.commit(
      messages,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    const result = await reconstructTimeline(dir);

    expect(result.gaps).toHaveLength(0);
  });

  test("marks turns as error when checkpoint reason is inference-error", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    const t = 1700000000000;
    const messages: ConversationMessage[] = [
      userMessage("Hello", t),
      assistantMessage("Something went wrong", t + 1000),
    ];
    await store.commit(
      messages,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-error",
    );

    const result = await reconstructTimeline(dir);

    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
    expect(turns[0]?.kind === "turn" && turns[0].status).toBe("error");
    expect(turns[0]?.kind === "turn" && turns[0].isError).toBe(true);
  });

  test("marks turns as in-progress for mid-turn checkpoints", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    const t = 1700000000000;
    const messages: ConversationMessage[] = [
      userMessage("What's the weather?", t),
      toolCallMessage("call-1", "get_weather", { city: "SF" }, t + 1000),
    ];
    await store.commit(
      messages,
      NO_OPS,
      NO_USAGE,
      "checkpoint: tool-execution",
    );

    // Add tool result and final response
    const messages2: ConversationMessage[] = [
      ...messages,
      toolResultMessage("call-1", "72F", t + 2000),
      assistantMessage("It's 72F in SF", t + 3000),
    ];
    await store.commit(
      messages2,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    const result = await reconstructTimeline(dir);

    const turns = result.events.filter((e) => e.kind === "turn");
    // tool-execution checkpoint has no text content, so no turn emitted
    // inference-done checkpoint has the final response
    expect(turns).toHaveLength(1);
    expect(turns[0]?.kind === "turn" && turns[0].status).toBe("completed");
  });

  test("handles empty repo with no checkpoints", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);

    const result = await reconstructTimeline(dir);

    expect(result.events).toHaveLength(0);
    expect(result.gaps).toBeDefined();
  });

  test("records a gap when a checkpoint commit has corrupt context", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    // Write a valid checkpoint first
    const messages: ConversationMessage[] = [
      userMessage("Hello"),
      assistantMessage("Hi"),
    ];
    await store.commit(
      messages,
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    // Write a corrupt checkpoint directly via git
    const contextPath = path.join(dir, "state/context.json");
    await fs.promises.writeFile(contextPath, "NOT VALID JSON");
    await git.add({ fs, dir, filepath: "state/context.json" });
    await git.commit({
      fs,
      dir,
      message: "checkpoint: inference-done",
      author: { name: "test", email: "test@test.dev" },
    });

    const result = await reconstructTimeline(dir);

    // Should still have the valid turn from the first checkpoint
    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);

    // Should record a gap for the corrupt checkpoint
    const corruptGaps = result.gaps.filter(
      (g) => g.kind === "corrupt-checkpoint",
    );
    expect(corruptGaps).toHaveLength(1);
  });

  test("records gaps for corrupt error record files in git", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    await store.commit(
      [userMessage("Hello"), assistantMessage("Hi")],
      NO_OPS,
      NO_USAGE,
      "checkpoint: inference-done",
    );

    // Commit a valid error record via the store
    await store.commitErrors([
      {
        source: "inference",
        category: "rate_limit",
        message: "Rate limited",
        fatal: false,
        timestamp: new Date().toISOString(),
        sessionId: "test-session",
        seq: 0,
      },
    ]);

    // Manually commit a corrupt error file directly via git
    const errorsDir = path.join(dir, "state/errors/test-session");
    await fs.promises.writeFile(
      path.join(errorsDir, "00000001-corrupt.json"),
      "NOT VALID JSON",
    );
    await git.add({
      fs,
      dir,
      filepath: "state/errors/test-session/00000001-corrupt.json",
    });
    await git.commit({
      fs,
      dir,
      message: "Record 1 error record",
      author: { name: "test", email: "test@test.dev" },
    });

    const result = await reconstructTimeline(dir);

    // Valid error should be associated with the preceding turn
    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
    const turn = turns[0];
    expect(turn?.kind === "turn" && turn.isError).toBe(true);
    expect(turn?.kind === "turn" && turn.errors?.length).toBeGreaterThanOrEqual(
      1,
    );

    // Corrupt file should produce a gap
    const corruptGaps = result.gaps.filter(
      (g) => g.kind === "corrupt-error-record",
    );
    expect(corruptGaps).toHaveLength(1);
    expect(corruptGaps[0]?.description).toContain("00000001-corrupt.json");
  });

  test("records multiple gaps for multiple corrupt checkpoints", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);

    // Write two corrupt checkpoints
    for (let i = 0; i < 2; i++) {
      const contextPath = path.join(dir, "state/context.json");
      await fs.promises.writeFile(contextPath, `CORRUPT ${i}`);
      await git.add({ fs, dir, filepath: "state/context.json" });
      await git.commit({
        fs,
        dir,
        message: "checkpoint: inference-done",
        author: { name: "test", email: "test@test.dev" },
      });
    }

    const result = await reconstructTimeline(dir);

    const corruptGaps = result.gaps.filter(
      (g) => g.kind === "corrupt-checkpoint",
    );
    expect(corruptGaps).toHaveLength(2);
  });
});
