import fs from "node:fs";
import git from "isomorphic-git";
import { type } from "arktype";
import {
  IsogitStore,
  listMail,
  type MailDirection,
} from "@interchange/storage-isogit";
import type { ConversationMessage } from "@interchange/types/runtime";
import {
  ErrorRecord,
  type ErrorRecord as ErrorRecordType,
} from "@interchange/types/audit";

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
  errors: ErrorRecordType[];
  corruptFiles: string[];
};

const ERROR_COMMIT_PATTERN = /^Record \d+ error records?$/;

async function readErrorRecordsFromCommit(
  dir: string,
  oid: string,
): Promise<ErrorReadResult> {
  const errors: ErrorRecordType[] = [];
  const corruptFiles: string[] = [];

  // Walk state/errors/ in the commit tree
  let sessionTree;
  try {
    sessionTree = await git.readTree({ fs, dir, oid, filepath: ERRORS_DIR });
  } catch {
    return { errors: [], corruptFiles: [] };
  }

  for (const sessionEntry of sessionTree.tree) {
    if (sessionEntry.type !== "tree") continue;
    const sessionId = sessionEntry.path;

    let fileTree;
    try {
      fileTree = await git.readTree({
        fs,
        dir,
        oid,
        filepath: `${ERRORS_DIR}/${sessionId}`,
      });
    } catch {
      continue;
    }

    for (const fileEntry of fileTree.tree) {
      if (fileEntry.type !== "blob" || !fileEntry.path.endsWith(".json"))
        continue;

      let blob: Uint8Array;
      try {
        ({ blob } = await git.readBlob({ fs, dir, oid: fileEntry.oid }));
      } catch {
        corruptFiles.push(`${sessionId}/${fileEntry.path}`);
        continue;
      }

      const text = new TextDecoder().decode(blob);
      let parsed: unknown;
      try {
        parsed = JSON.parse(text);
      } catch {
        corruptFiles.push(`${sessionId}/${fileEntry.path}`);
        continue;
      }

      const result = ErrorRecord(parsed);
      if (result instanceof type.errors) {
        corruptFiles.push(`${sessionId}/${fileEntry.path}`);
        continue;
      }
      errors.push(result);
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

  // Process commits to extract turns and associate errors
  let prevMessageCount = 0;
  let hasCheckpoints = false;
  // Track the index of the last turn event so error commits can attach to it
  let lastTurnEventIndex = -1;

  for (const commit of commits) {
    // Handle error commits — associate with the most recent turn
    if (ERROR_COMMIT_PATTERN.test(commit.message)) {
      const { errors: errorRecords, corruptFiles } =
        await readErrorRecordsFromCommit(dir, commit.hash);

      for (const file of corruptFiles) {
        gaps.push({
          kind: "corrupt-error-record",
          description: `Error record ${file} failed validation and was excluded from the timeline`,
        });
      }

      if (errorRecords.length > 0 && lastTurnEventIndex >= 0) {
        const turnEvent = events[lastTurnEventIndex];
        if (turnEvent !== undefined && turnEvent.kind === "turn") {
          turnEvent.isError = true;
          turnEvent.errors = [
            ...(turnEvent.errors ?? []),
            ...errorRecords.map((e) => ({
              category: e.category,
              message: e.message,
            })),
          ];
        }
      }
      continue;
    }

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
      if (turnEvents.length > 0) {
        lastTurnEventIndex = events.length - 1;
      }
      prevMessageCount = messages.length;
      continue;
    }

    const newMessages = messages.slice(prevMessageCount);
    prevMessageCount = messages.length;

    if (newMessages.length === 0) continue;

    const turnEvents = extractTurns(newMessages, status);
    events.push(...turnEvents);
    if (turnEvents.length > 0) {
      lastTurnEventIndex = events.length - 1;
    }
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
