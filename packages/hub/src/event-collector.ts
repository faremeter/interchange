// Persists assistant inference turns from the InferenceEvent stream.
//
// One collector per active session. Events are written eagerly to the DB
// so data survives crashes. The collector does not block the websocket
// message loop — callers should fire-and-forget and log errors.

import { eq } from "drizzle-orm";

import { inferenceTurn, turnPart } from "@interchange/db/schema";
import { getLogger } from "@interchange/log";
import type { InferenceEvent, ContentBlock } from "@interchange/types/runtime";
import { type DB, parseTurnPartType } from "@interchange/db";

import { generateId } from "./ids";

const log = getLogger(["hub", "event-collector"]);

export type TurnFinalized = {
  turnId: string;
  status: "completed" | "failed";
  text: string;
  hadReply: boolean;
  hadError: boolean;
  errors: { category: string; message: string }[];
  toolErrors: { name: string; content: string }[];
};

export type EventCollector = {
  onEvent(event: InferenceEvent): Promise<void>;
  abandon(): Promise<void>;
  getAccumulatedText(): string;
  getCurrentTurnId(): string | null;
  getLastTurnId(): string | null;
};

export type EventCollectorConfig = {
  db: DB["db"];
  sessionId: string;
  instanceId: string;
  tenantId: string;
  onTurnFinalized?: (turn: TurnFinalized) => void;
};

export function createEventCollector(
  config: EventCollectorConfig,
): EventCollector {
  const { db, sessionId, instanceId, tenantId, onTurnFinalized } = config;

  // Current inference turn being accumulated. A new turn is created on each
  // inference.start. Finalized on connector.reply, reactor.done,
  // reactor.error (fatal), or abandon. Null when no turn is active.
  let currentTurnId: string | null = null;
  // Most recent turn ID, set in beginTurn. Unlike currentTurnId this is NOT
  // cleared on finalization — the SSE replay endpoint needs the turn ID
  // after the turn commits but before the collector is removed.
  let lastTurnId: string | null = null;
  let ordinal = 0;
  // Prevents double-finalization when reactor.done and abandon() race.
  let finalized = false;
  // Set when inference.error fires so connector.reply knows to persist its
  // content (on normal turns, inference.done already persisted the text).
  let pendingError = false;
  // Accumulated visible text content for the current turn. Only text blocks
  // from inference.done (not thinking/reasoning) are included. Reset on each
  // new turn.
  let accumulatedText = "";
  // In-progress text from inference.text.delta events during the current
  // inference step. Reset on inference.done (when accumulatedText takes over).
  let streamingText = "";
  // Set when inference.error fires. Unlike pendingError (which resets on
  // connector.reply), this persists until finalization so the callback can
  // report whether an inference error occurred during the turn.
  let turnHadError = false;
  // Structured error details accumulated during the turn for inclusion in
  // TurnFinalized. Reset on each new turn.
  let accumulatedErrors: { category: string; message: string }[] = [];
  // Maps tool call IDs to tool names for correlating tool results with their
  // originating calls. Populated from inference.done tool_call blocks.
  const callNames = new Map<string, string>();
  // Tool results that reported isError, accumulated for TurnFinalized.
  let accumulatedToolErrors: { name: string; content: string }[] = [];

  async function onEvent(event: InferenceEvent): Promise<void> {
    switch (event.type) {
      case "inference.start":
        await beginTurn(event.data.model);
        await insertPart("step-start", null, { model: event.data.model });
        break;
      case "inference.text.delta":
        streamingText += event.data.token;
        break;
      case "inference.done":
        await handleInferenceDone(event.data.turn.content);
        streamingText = "";
        break;
      case "tool.done": {
        const isError = event.data.result.isError ?? false;
        await insertPart("tool", null, {
          kind: "result",
          callId: event.data.result.callId,
          content: event.data.result.content,
          isError,
        });
        if (isError) {
          const name =
            callNames.get(event.data.result.callId) ?? event.data.result.callId;
          const raw = event.data.result.content;
          const content = typeof raw === "string" ? raw : JSON.stringify(raw);
          accumulatedToolErrors.push({ name, content });
        }
        break;
      }
      case "inference.error":
        pendingError = true;
        turnHadError = true;
        await insertPart("error", event.data.error.message, {
          category: event.data.error.category,
          ...(event.data.error.statusCode !== undefined
            ? { statusCode: event.data.error.statusCode }
            : {}),
        });
        accumulatedErrors.push({
          category: event.data.error.category,
          message: event.data.error.message,
        });
        break;
      case "connector.reply":
        if (finalized) break;
        // Only persist reply content when it originated from an error path.
        // On normal turns inference.done already persisted the text parts.
        if (pendingError) {
          accumulatedText += event.data.content;
          await insertPart("text", event.data.content, null);
          pendingError = false;
        }
        await finalizeTurn("completed", true, true);
        break;
      case "reactor.done":
        await finalizeTurn("completed", true, false);
        break;
      case "reactor.error":
        if (event.data.fatal && !finalized) {
          // The reactor failed before any inference started (e.g., context
          // store load failure), but the user needs to see why their agent
          // failed, and without a turn there is no container for the error.
          if (currentTurnId === null) {
            await beginTurn("unknown");
          }
          turnHadError = true;
          // Push after beginTurn so the error survives the array reset.
          accumulatedErrors.push({
            category: "reactor_error",
            message: event.data.error,
          });
          await insertPart("error", event.data.error, {
            category: "reactor_error",
          });
          await finalizeTurn("failed", true, false);
        } else if (!event.data.fatal && !finalized) {
          if (currentTurnId === null) {
            await beginTurn("unknown");
          }
          turnHadError = true;
          accumulatedErrors.push({
            category: "reactor_error",
            message: event.data.error,
          });
          await insertPart("error", event.data.error, {
            category: "reactor_error",
          });
        }
        break;
      default:
        // reactor.start, streaming deltas, usage, and other events are
        // not persisted.
        break;
    }
  }

  async function beginTurn(model: string): Promise<void> {
    // A previous turn is still open — this is normal in multi-step tool-use
    // loops where inference.start fires again after tools return.
    if (currentTurnId !== null && !finalized) {
      await finalizeTurn("completed", true, false);
    }

    currentTurnId = generateId("inferenceTurn");
    lastTurnId = currentTurnId;
    ordinal = 0;
    finalized = false;
    pendingError = false;
    accumulatedText = "";
    streamingText = "";
    turnHadError = false;
    accumulatedErrors = [];
    callNames.clear();
    accumulatedToolErrors = [];

    await db.insert(inferenceTurn).values({
      id: currentTurnId,
      sessionId,
      instanceId,
      tenantId,
      model,
      status: "running",
      startedAt: new Date(),
    });
  }

  async function handleInferenceDone(content: ContentBlock[]): Promise<void> {
    for (const block of content) {
      switch (block.type) {
        case "text":
          accumulatedText += block.text;
          await insertPart("text", block.text, null);
          break;
        case "thinking":
          await insertPart("reasoning", block.thinking, null);
          break;
        case "tool_call":
          callNames.set(block.id, block.name);
          await insertPart("tool", null, {
            kind: "call",
            callId: block.id,
            name: block.name,
            arguments: block.arguments,
          });
          break;
        case "tool_result":
          // Tool results in the content block are echoes of earlier
          // tool.done events. Skip to avoid duplication.
          break;
        case "image":
          await insertPart("file", null, {
            mimeType: block.mimeType,
            dataLength: block.data.length,
          });
          break;
      }
    }

    // Mark the end of this inference step.
    await insertPart("step-finish", null, null);
  }

  async function finalizeTurn(
    status: "completed" | "failed",
    notify: boolean,
    hadReply: boolean,
  ): Promise<void> {
    if (currentTurnId === null || finalized) return;
    finalized = true;

    const turnId = currentTurnId;

    await db
      .update(inferenceTurn)
      .set({ status, endedAt: new Date() })
      .where(eq(inferenceTurn.id, turnId));

    if (notify && onTurnFinalized) {
      onTurnFinalized({
        turnId,
        status,
        text: accumulatedText,
        hadReply,
        hadError: turnHadError,
        errors: [...accumulatedErrors],
        toolErrors: [...accumulatedToolErrors],
      });
    }

    currentTurnId = null;
  }

  async function abandon(): Promise<void> {
    if (currentTurnId === null || finalized) return;

    log.warn`Abandoning running turn ${currentTurnId} for session ${sessionId}`;

    await finalizeTurn("failed", false, false);
  }

  async function insertPart(
    partType: string,
    content: string | null,
    metadata: Record<string, unknown> | null,
  ): Promise<void> {
    if (currentTurnId === null) {
      log.warn`Dropping ${partType} part: no active turn for session ${sessionId}`;
      return;
    }

    const values: typeof turnPart.$inferInsert = {
      id: generateId("turnPart"),
      turnId: currentTurnId,
      sessionId,
      type: parseTurnPartType(partType),
      ordinal: ordinal++,
    };

    if (content !== null) {
      values.content = content;
    }

    if (metadata !== null) {
      values.metadata = metadata;
    }

    await db.insert(turnPart).values(values);
  }

  function getAccumulatedText(): string {
    return accumulatedText + streamingText;
  }

  function getCurrentTurnId(): string | null {
    return currentTurnId;
  }

  function getLastTurnId(): string | null {
    return lastTurnId;
  }

  return {
    onEvent,
    abandon,
    getAccumulatedText,
    getCurrentTurnId,
    getLastTurnId,
  };
}
