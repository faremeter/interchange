// Persists assistant message parts from the InferenceEvent stream.
//
// One collector per active session. Events are written eagerly to the DB
// so data survives crashes. The collector does not block the websocket
// message loop — callers should fire-and-forget and log errors.

import { eq } from "drizzle-orm";

import { sessionMessage, messagePart } from "@interchange/db/schema";
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
  tenantId: string;
};

export function createEventCollector(
  config: EventCollectorConfig,
): EventCollector {
  const { db, sessionId, tenantId } = config;

  // Current assistant message being accumulated. Created on reactor.start,
  // finalized on reactor.done, reactor.error (fatal), or abandon.
  let currentMessageId: string | null = null;
  let ordinal = 0;
  // Prevents double-finalization when reactor.done and abandon() race.
  let finalized = false;

  async function onEvent(event: InferenceEvent): Promise<void> {
    switch (event.type) {
      case "reactor.start":
        await handleReactorStart();
        break;
      case "inference.start":
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
      case "reactor.done":
        await finalizeMessage("delivered");
        break;
      case "reactor.error":
        if (event.data.fatal) {
          await finalizeMessage("failed");
        }
        break;
      default:
        // Streaming deltas, usage, and other events are not persisted.
        break;
    }
  }

  async function handleReactorStart(): Promise<void> {
    // If a previous message is still pending, finalize it as failed.
    if (currentMessageId !== null && !finalized) {
      log.warn`Orphaned pending message ${currentMessageId} for session ${sessionId}`;
      await finalizeMessage("failed");
    }

    currentMessageId = generateId("message");
    ordinal = 0;
    finalized = false;

    await db.insert(sessionMessage).values({
      id: currentMessageId,
      sessionId,
      tenantId,
      role: "assistant",
      status: "pending",
      createdAt: new Date(),
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

    // Mark the end of this inference turn.
    await insertPart("step-finish", null, null);
  }

  async function finalizeMessage(
    status: "delivered" | "failed",
  ): Promise<void> {
    if (currentMessageId === null || finalized) return;
    finalized = true;

    await db
      .update(sessionMessage)
      .set({ status })
      .where(eq(sessionMessage.id, currentMessageId));

    currentMessageId = null;
  }

  async function abandon(): Promise<void> {
    if (currentMessageId === null || finalized) return;

    log.warn`Abandoning pending message ${currentMessageId} for session ${sessionId}`;

    await finalizeMessage("failed");
  }

  async function insertPart(
    partType: string,
    content: string | null,
    metadata: Record<string, unknown> | null,
  ): Promise<void> {
    if (currentMessageId === null) {
      log.warn`Dropping ${partType} part: no active message for session ${sessionId}`;
      return;
    }

    const values: typeof messagePart.$inferInsert = {
      id: generateId("messagePart"),
      messageId: currentMessageId,
      sessionId,
      type: partType as typeof messagePart.$inferInsert.type,
      ordinal: ordinal++,
    };

    if (content !== null) {
      values.content = content;
    }

    if (metadata !== null) {
      values.metadata = metadata;
    }

    await db.insert(messagePart).values(values);
  }

  return { onEvent, abandon };
}
