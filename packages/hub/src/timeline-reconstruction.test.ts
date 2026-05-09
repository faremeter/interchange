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

function userMessage(text: string): ConversationMessage {
  return { role: "user", content: [{ type: "text", text }] };
}

function assistantMessage(text: string): ConversationMessage {
  return {
    role: "assistant",
    content: [{ type: "text", text }],
    model: "test-model",
  };
}

function toolCallMessage(
  callId: string,
  name: string,
  args: Record<string, unknown>,
): ConversationMessage {
  return {
    role: "assistant",
    content: [{ type: "tool_call", id: callId, name, arguments: args }],
    model: "test-model",
  };
}

function toolResultMessage(
  callId: string,
  result: string,
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

    const messages: ConversationMessage[] = [
      userMessage("Hello"),
      assistantMessage("Hi there"),
    ];
    await store.commit(messages, NO_OPS, NO_USAGE, "checkpoint");

    const result = await reconstructTimeline(dir);

    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
    expect(turns[0]?.content).toBe("Hi there");
    expect(turns[0]?.timestamp).toBeGreaterThan(0);

    // Gap: no per-message timestamps
    const timestampGap = result.gaps.find(
      (g) => g.kind === "no-per-message-timestamps",
    );
    expect(timestampGap).toBeDefined();
  });

  test("reconstructs multi-turn conversations across checkpoints", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    // First turn
    const messages1: ConversationMessage[] = [
      userMessage("Hello"),
      assistantMessage("Hi there"),
    ];
    await store.commit(messages1, NO_OPS, NO_USAGE, "checkpoint");

    // Second turn (appends to the same message array)
    const messages2: ConversationMessage[] = [
      ...messages1,
      userMessage("How are you?"),
      assistantMessage("I'm doing well"),
    ];
    await store.commit(messages2, NO_OPS, NO_USAGE, "checkpoint");

    const result = await reconstructTimeline(dir);

    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(2);
    expect(turns[0]?.content).toBe("Hi there");
    expect(turns[1]?.content).toBe("I'm doing well");

    // Second turn should have a later or equal timestamp
    if (turns[0] !== undefined && turns[1] !== undefined) {
      expect(turns[1].timestamp).toBeGreaterThanOrEqual(turns[0].timestamp);
    }
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
    await store.commit(messages, NO_OPS, NO_USAGE, "checkpoint");

    const result = await reconstructTimeline(dir);

    const turns = result.events.filter((e) => e.kind === "turn");
    expect(turns).toHaveLength(1);
    expect(turns[0]?.content).toBe("The weather in SF is 72F and sunny.");

    // Gap: turn boundaries are heuristic
    const turnBoundaryGap = result.gaps.find(
      (g) => g.kind === "no-turn-boundaries",
    );
    expect(turnBoundaryGap).toBeDefined();
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

    // Gap: no link between assistant messages and outbound mail
    const linkGap = result.gaps.find(
      (g) => g.kind === "no-assistant-mail-linkage",
    );
    expect(linkGap).toBeDefined();
  });

  test("surfaces error records and documents the turn association gap", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    // Write a conversation first
    const messages: ConversationMessage[] = [
      userMessage("Do something risky"),
      assistantMessage("Attempting..."),
    ];
    await store.commit(messages, NO_OPS, NO_USAGE, "checkpoint");

    // Write error records
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

    // Errors should be surfaced
    const errorEvents = result.events.filter(
      (e) => e.kind === "turn" && e.isError === true,
    );
    expect(errorEvents).toHaveLength(1);
    if (errorEvents[0] !== undefined && errorEvents[0].kind === "turn") {
      expect(errorEvents[0].errors).toHaveLength(1);
      expect(errorEvents[0].errors?.[0]?.category).toBe("rate_limit");
    }

    // Gap: errors have no turn association
    const errorGap = result.gaps.find(
      (g) => g.kind === "no-error-turn-association",
    );
    expect(errorGap).toBeDefined();

    // Gap: reading errors from working tree, not git objects
    const workingTreeGap = result.gaps.find(
      (g) => g.kind === "errors-from-working-tree",
    );
    expect(workingTreeGap).toBeDefined();
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
    await store.commit(messages1, NO_OPS, NO_USAGE, "checkpoint");

    // Second checkpoint with only 2 messages (regression)
    const messages2: ConversationMessage[] = [
      userMessage("Fresh start"),
      assistantMessage("OK"),
    ];
    await store.commit(messages2, NO_OPS, NO_USAGE, "checkpoint");

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
    await store.commit(messages, NO_OPS, NO_USAGE, "checkpoint");

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

  test("produces all known gap types", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    const messages: ConversationMessage[] = [
      userMessage("Hello"),
      assistantMessage("Hi"),
    ];
    await store.commit(messages, NO_OPS, NO_USAGE, "checkpoint");

    const result = await reconstructTimeline(dir);

    const gapKinds = result.gaps.map((g) => g.kind);
    expect(gapKinds).toContain("no-per-message-timestamps");
    expect(gapKinds).toContain("no-turn-boundaries");
    expect(gapKinds).toContain("no-assistant-mail-linkage");
    expect(gapKinds).toContain("no-turn-status");
    expect(gapKinds).toContain("no-had-error");
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
    await store.commit(messages, NO_OPS, NO_USAGE, "checkpoint");

    // Write a corrupt checkpoint directly via git
    const contextPath = path.join(dir, "state/context.json");
    await fs.promises.writeFile(contextPath, "NOT VALID JSON");
    await git.add({ fs, dir, filepath: "state/context.json" });
    await git.commit({
      fs,
      dir,
      message: "checkpoint",
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

  test("records gaps for corrupt error record files", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    await store.commit(
      [userMessage("Hello"), assistantMessage("Hi")],
      NO_OPS,
      NO_USAGE,
      "checkpoint",
    );

    // Write one valid and one corrupt error record
    const errorsDir = path.join(dir, "state/errors/test-session");
    await fs.promises.mkdir(errorsDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(errorsDir, "00000000-valid.json"),
      JSON.stringify({
        source: "inference",
        category: "rate_limit",
        message: "Rate limited",
        fatal: false,
        timestamp: new Date().toISOString(),
        sessionId: "test-session",
        seq: 0,
      }),
    );
    await fs.promises.writeFile(
      path.join(errorsDir, "00000001-corrupt.json"),
      "NOT VALID JSON",
    );

    const result = await reconstructTimeline(dir);

    // Valid error should be surfaced
    const errorEvents = result.events.filter(
      (e) => e.kind === "turn" && e.isError === true,
    );
    expect(errorEvents).toHaveLength(1);

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
        message: "checkpoint",
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
