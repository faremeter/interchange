import fs from "node:fs";
import path from "node:path";
import { type } from "arktype";
import {
  IsogitStore,
  listMail,
  type MailDirection,
} from "@interchange/storage-isogit";
import type { ConversationMessage } from "@interchange/types/runtime";
import { ErrorRecord } from "@interchange/types/audit";

export type ReconstructedEvent =
  | {
      kind: "mail";
      direction: MailDirection;
      messageId: string;
      timestamp: number;
      raw: Uint8Array;
    }
  | {
      kind: "turn";
      content: string;
      timestamp: number;
      status: "completed" | "error" | "in-progress";
      isError?: boolean;
      errors?: { category: string; message: string }[];
    };

export type GapKind =
  | "no-assistant-mail-linkage"
  | "no-error-turn-association"
  | "errors-from-working-tree"
  | "message-count-regression"
  | "corrupt-checkpoint"
  | "corrupt-error-record";

export type ReconstructionGap = {
  kind: GapKind;
  description: string;
};

export type ReconstructionResult = {
  events: ReconstructedEvent[];
  gaps: ReconstructionGap[];
};

const ERRORS_DIR = "state/errors";
const MAX_LOG_DEPTH = 10000;

type CheckpointReason =
  | "inference-done"
  | "tool-execution"
  | "tool-done"
  | "inference-error"
  | "gate-cleared";

function isCheckpointReason(value: string): value is CheckpointReason {
  return (
    value === "inference-done" ||
    value === "tool-execution" ||
    value === "tool-done" ||
    value === "inference-error" ||
    value === "gate-cleared"
  );
}

function parseCheckpointReason(commitMessage: string): CheckpointReason | null {
  const match = /^checkpoint: (.+)$/.exec(commitMessage);
  if (match?.[1] === undefined) return null;
  if (!isCheckpointReason(match[1])) return null;
  return match[1];
}

function reasonToStatus(
  reason: CheckpointReason,
): "completed" | "error" | "in-progress" {
  switch (reason) {
    case "inference-done":
      return "completed";
    case "inference-error":
      return "error";
    case "tool-execution":
    case "tool-done":
    case "gate-cleared":
      return "in-progress";
  }
}

function isToolResultMessage(msg: ConversationMessage): boolean {
  const first = msg.content[0];
  return (
    msg.role === "user" && first !== undefined && first.type === "tool_result"
  );
}

function extractTextContent(msg: ConversationMessage): string {
  return msg.content
    .filter((b) => b.type === "text")
    .map((b) => {
      if (b.type !== "text") return "";
      return b.text;
    })
    .join("");
}

type TurnAccumulator = {
  texts: string[];
  timestamp: number;
};

function extractTurns(
  newMessages: ConversationMessage[],
  status: "completed" | "error" | "in-progress",
): ReconstructedEvent[] {
  const events: ReconstructedEvent[] = [];
  let current: TurnAccumulator | null = null;

  for (const msg of newMessages) {
    if (msg.role === "user" && !isToolResultMessage(msg)) {
      // New user message (not a tool result) starts a new turn
      if (current !== null && current.texts.length > 0) {
        events.push({
          kind: "turn",
          content: current.texts.join(""),
          timestamp: current.timestamp,
          status,
        });
      }
      current = { texts: [], timestamp: msg.timestamp };
    } else if (msg.role === "assistant") {
      if (current === null) {
        current = { texts: [], timestamp: msg.timestamp };
      } else {
        // Update timestamp to the latest assistant message in this turn
        current.timestamp = msg.timestamp;
      }
      const text = extractTextContent(msg);
      if (text.length > 0) {
        current.texts.push(text);
      }
    }
    // tool_result user messages and system messages are continuations, not new turns
  }

  if (current !== null && current.texts.length > 0) {
    events.push({
      kind: "turn",
      content: current.texts.join(""),
      timestamp: current.timestamp,
      status,
      ...(status === "error" ? { isError: true } : {}),
    });
  }

  return events;
}

type ErrorReadResult = {
  errors: { category: string; message: string; timestamp: string }[];
  corruptFiles: string[];
};

async function readErrorRecords(dir: string): Promise<ErrorReadResult> {
  const errorsDir = path.join(dir, ERRORS_DIR);
  let sessionDirs: string[];
  try {
    sessionDirs = await fs.promises.readdir(errorsDir);
  } catch (e: unknown) {
    if (e instanceof Error && "code" in e && e.code === "ENOENT") {
      return { errors: [], corruptFiles: [] };
    }
    throw e;
  }

  const errors: { category: string; message: string; timestamp: string }[] = [];
  const corruptFiles: string[] = [];

  for (const sessionId of sessionDirs) {
    const sessionPath = path.join(errorsDir, sessionId);
    const stat = await fs.promises.stat(sessionPath);
    if (!stat.isDirectory()) continue;

    const files = await fs.promises.readdir(sessionPath);
    for (const file of files) {
      if (!file.endsWith(".json")) continue;
      const fullPath = path.join(sessionPath, file);
      const raw = await fs.promises.readFile(fullPath, "utf-8");
      let parsed: unknown;
      try {
        parsed = JSON.parse(raw);
      } catch {
        corruptFiles.push(`${sessionId}/${file}`);
        continue;
      }
      const result = ErrorRecord(parsed);
      if (result instanceof type.errors) {
        corruptFiles.push(`${sessionId}/${file}`);
        continue;
      }
      errors.push({
        category: result.category,
        message: result.message,
        timestamp: result.timestamp,
      });
    }
  }

  return { errors, corruptFiles };
}

export async function reconstructTimeline(
  dir: string,
): Promise<ReconstructionResult> {
  const store = new IsogitStore(dir);
  const events: ReconstructedEvent[] = [];
  const gaps: ReconstructionGap[] = [];
  const gapKindsAdded = new Set<GapKind>();

  function addGap(kind: GapKind, description: string): void {
    if (gapKindsAdded.has(kind)) return;
    gapKindsAdded.add(kind);
    gaps.push({ kind, description });
  }

  // Walk the full commit log, oldest first
  const commits = await store.log(MAX_LOG_DEPTH);
  commits.reverse();

  // Process checkpoint commits to extract turns
  let prevMessageCount = 0;
  let hasCheckpoints = false;

  for (const commit of commits) {
    const reason = parseCheckpointReason(commit.message);
    if (reason === null) continue;

    hasCheckpoints = true;
    const status = reasonToStatus(reason);

    let messages: ConversationMessage[];
    try {
      messages = await store.readAt(commit.hash);
    } catch {
      gaps.push({
        kind: "corrupt-checkpoint",
        description: `Checkpoint commit ${commit.hash} could not be read; this checkpoint is missing from the reconstructed timeline`,
      });
      continue;
    }

    if (messages.length < prevMessageCount) {
      addGap(
        "message-count-regression",
        `Message count dropped from ${prevMessageCount} to ${messages.length} at commit ${commit.hash}`,
      );
      // Treat the entire message array as new for this checkpoint
      const turnEvents = extractTurns(messages, status);
      events.push(...turnEvents);
      prevMessageCount = messages.length;
      continue;
    }

    const newMessages = messages.slice(prevMessageCount);
    prevMessageCount = messages.length;

    if (newMessages.length === 0) continue;

    const turnEvents = extractTurns(newMessages, status);
    events.push(...turnEvents);
  }

  // Process mail entries
  const mailEntries = await listMail(dir);
  // Correlate mail timestamps with git commits by matching commit messages
  const mailCommitTimestamps = new Map<string, number>();
  for (const commit of commits) {
    const match = /^Record (?:inbound|outbound) mail (.+)$/.exec(
      commit.message,
    );
    if (match?.[1] !== undefined) {
      mailCommitTimestamps.set(match[1], commit.timestamp);
    }
  }

  for (const entry of mailEntries) {
    const timestamp = mailCommitTimestamps.get(entry.messageId) ?? 0;
    events.push({
      kind: "mail",
      direction: entry.direction,
      messageId: entry.messageId,
      timestamp,
      raw: entry.raw,
    });
  }

  // Process error records from the working tree
  const { errors, corruptFiles } = await readErrorRecords(dir);

  for (const file of corruptFiles) {
    gaps.push({
      kind: "corrupt-error-record",
      description: `Error record ${file} failed validation and was excluded from the timeline`,
    });
  }

  if (errors.length > 0) {
    // Errors have no turn association, so use the last checkpoint as a best-effort timestamp
    const fallbackTimestamp =
      commits.filter((c) => parseCheckpointReason(c.message) !== null).at(-1)
        ?.timestamp ?? 0;

    events.push({
      kind: "turn",
      content: "",
      timestamp: fallbackTimestamp,
      status: "error",
      isError: true,
      errors: errors.map((e) => ({ category: e.category, message: e.message })),
    });

    addGap(
      "no-error-turn-association",
      "Error records have sessionId and seq but no turn ID; cannot associate errors with specific turns",
    );
    addGap(
      "errors-from-working-tree",
      "Error records are read from the working tree, not from git objects; this breaks audit integrity for bare repos",
    );
  }

  // Sort all events by timestamp
  events.sort((a, b) => a.timestamp - b.timestamp);

  if (mailEntries.length > 0 || hasCheckpoints) {
    addGap(
      "no-assistant-mail-linkage",
      "The mail store has messageId and the context store has ConversationMessage, but nothing connects assistant messages to their corresponding outbound mail",
    );
  }

  return { events, gaps };
}
