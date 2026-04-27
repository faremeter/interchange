// Persists assistant inference turns from the InferenceEvent stream.
//
// One collector per active session. Events are written eagerly to the DB
// so data survives crashes. The collector does not block the websocket
// message loop — callers should fire-and-forget and log errors.

import { eq } from "drizzle-orm";

import { inferenceTurn, turnPart } from "@interchange/db/schema";
import { getLogger } from "@interchange/log";
import type { InferenceEvent, ContentBlock } from "@interchange/types/runtime";
import type { DB } from "@interchange/db";

import { generateId } from "./ids";

const log = getLogger(["hub", "event-collector"]);

export type EventCollector = {
  onEvent(event: InferenceEvent): Promise<void>;
  abandon(): Promise<void>;
};

export type EventCollectorConfig = {
  db: DB["db"];
  sessionId: string;
  instanceId: string;
  tenantId: string;
};

export function createEventCollector(
  config: EventCollectorConfig,
): EventCollector {
  const { db, sessionId, instanceId, tenantId } = config;

  // Current inference turn being accumulated. A new turn is created on each
  // inference.start. Finalized on connector.reply, reactor.done,
  // reactor.error (fatal), or abandon.
  let currentTurnId: string | null = null;
  let ordinal = 0;
  // Prevents double-finalization when reactor.done and abandon() race.
  let finalized = false;
  // Set when inference.error fires so connector.reply knows to persist its
  // content (on normal turns, inference.done already persisted the text).
  let pendingError = false;

  async function onEvent(event: InferenceEvent): Promise<void> {
    switch (event.type) {
      case "inference.start":
        await beginTurn(event.data.model);
        await insertPart("step-start", null, { model: event.data.model });
        break;
      case "inference.done":
        await handleInferenceDone(event.data.message.content);
        break;
      case "tool.done":
        await insertPart("tool", null, {
          kind: "result",
          callId: event.data.result.callId,
          content: event.data.result.content,
          isError: event.data.result.isError ?? false,
        });
        break;
      case "inference.error":
        pendingError = true;
        await insertPart("error", event.data.error.message, {
          category: event.data.error.category,
          ...(event.data.error.statusCode !== undefined
            ? { statusCode: event.data.error.statusCode }
            : {}),
        });
        break;
      case "connector.reply":
        // Only persist reply content when it originated from an error path.
        // On normal turns inference.done already persisted the text parts.
        if (pendingError) {
          await insertPart("text", event.data.content, null);
          pendingError = false;
        }
        await finalizeTurn("completed");
        break;
      case "reactor.done":
        await finalizeTurn("completed");
        break;
      case "reactor.error":
        if (event.data.fatal) {
          await finalizeTurn("failed");
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
      await finalizeTurn("completed");
    }

    currentTurnId = generateId("inferenceTurn");
    ordinal = 0;
    finalized = false;
    pendingError = false;

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
          await insertPart("text", block.text, null);
          break;
        case "thinking":
          await insertPart("reasoning", block.thinking, null);
          break;
        case "tool_call":
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

  async function finalizeTurn(status: "completed" | "failed"): Promise<void> {
    if (currentTurnId === null || finalized) return;
    finalized = true;

    await db
      .update(inferenceTurn)
      .set({ status, endedAt: new Date() })
      .where(eq(inferenceTurn.id, currentTurnId));

    currentTurnId = null;
  }

  async function abandon(): Promise<void> {
    if (currentTurnId === null || finalized) return;

    log.warn`Abandoning running turn ${currentTurnId} for session ${sessionId}`;

    await finalizeTurn("failed");
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
      type: partType as typeof turnPart.$inferInsert.type,
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

  return { onEvent, abandon };
}
