// Persists assistant inference turns from the InferenceEvent stream.
//
// One collector per active session. Events are written eagerly to the DB
// so data survives crashes. The collector does not block the websocket
// message loop — callers should fire-and-forget and log errors.

import { eq } from "drizzle-orm";

import { inferenceTurn, turnPart } from "@intx/db/schema";
import { getLogger } from "@intx/log";
import type {
  InferenceEvent,
  ContentBlock,
  TokenUsage,
  LastCycleSource,
} from "@intx/types/runtime";
import { type DB, parseTurnPartType } from "@intx/db";

import { generateId } from "@intx/hub-common";

const log = getLogger(["hub", "event-collector"]);

export type TurnToolCall = {
  name: string;
  arguments: Record<string, unknown>;
  result: string;
  isError: boolean;
};

export type TurnFinalized = {
  turnId: string;
  status: "completed" | "failed";
  text: string;
  hadReply: boolean;
  hadError: boolean;
  errors: { category: string; message: string }[];
  toolCalls: TurnToolCall[];
  toolErrors: { name: string; content: string }[];
};

// Per-turn token usage, emitted once when a turn finalizes. Carries full run
// identity so a host accounting sink never has to join against a separate
// address-to-tenant map (which would go stale at run teardown).
export type TurnUsage = {
  tenantId: string;
  sessionId: string;
  runId: string;
  turnId: string;
  provider: string;
  model: string;
  usage: TokenUsage;
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
  runId: string;
  tenantId: string;
  onTurnFinalized?: (turn: TurnFinalized) => void;
  onUsage?: (usage: TurnUsage) => void;
};

export function createEventCollector(
  config: EventCollectorConfig,
): EventCollector {
  const { db, sessionId, runId, tenantId, onTurnFinalized, onUsage } = config;

  // One inference_turn row and what the collector accumulates for it. A row
  // opens on each inference.start and is finalized on connector.reply, the
  // reactor.turn.ended of its reactor turn, reactor.done, reactor.error
  // (fatal), or abandon.
  type Row = {
    id: string;
    // The reactor turn (one inbound request) the row belongs to. A reactor
    // turn can span many rows; this is a different id from the row id.
    // Undefined for a row whose event carried no turn id.
    reactorTurnId: string | undefined;
    ordinal: number;
    // Prevents double-finalization when reactor.done and abandon() race.
    finalized: boolean;
    // Set when inference.error fires so connector.reply knows to persist
    // its content (on normal turns, inference.done already persisted it).
    pendingError: boolean;
    // Visible text from inference.done (not thinking/reasoning).
    accumulatedText: string;
    // In-progress text from inference.text.delta events of the current
    // inference step. Reset on inference.done.
    streamingText: string;
    // Set when an error fires. Unlike pendingError, this persists until
    // finalization so the callback can report it.
    turnHadError: boolean;
    accumulatedErrors: { category: string; message: string }[];
    accumulatedToolCalls: TurnToolCall[];
    accumulatedToolErrors: { name: string; content: string }[];
    // Final cumulative token usage. `inference.usage` events carry a
    // running cumulative total and fire several times per step, so these
    // are OVERWRITTEN (not summed); the last value before finalize is the
    // authoritative per-row total.
    turnUsage: TokenUsage | null;
    turnSource: LastCycleSource | null;
  };

  // The row of the latest inference. Events that carry no turn id (deltas,
  // usage, inference errors) belong to it; a tool result goes to the row
  // of the turn that asked for its call (see calls). Null once that row is
  // finalized.
  let current: Row | null = null;
  // The latest row, also after it is finalized. The SSE replay endpoint
  // needs its id after the row commits but before the collector is
  // removed.
  let last: Row | null = null;
  // The open row of each reactor turn. Another turn's inference opens its
  // own row and leaves this one open (a parked turn); the turn's own next
  // inference or its end finalizes it.
  const openRows = new Map<string, Row>();
  // The latest reactor turn that started and has not ended, and whether
  // it has a row. An error before any inference of that turn opens the
  // turn's row, so the turn's end finalizes it.
  let startedTurnId: string | undefined;
  const turnsWithRows = new Set<string>();
  // Each tool call the collector saw asked for, from the inference.done
  // tool_call blocks: the reactor turn whose inference asked for it, and its
  // name and arguments. A call's result goes to that turn's open row, also
  // when it comes after another turn's inference (a parked turn's re-run
  // call). An entry is dropped when its result is recorded or its turn
  // ends.
  const calls = new Map<
    string,
    {
      reactorTurnId: string | undefined;
      name: string;
      arguments: Record<string, unknown>;
    }
  >();

  // The row a tool result belongs to: the open row of the turn that asked
  // for the call. For a call the collector never saw (a new collector after
  // a restart, before any inference of that turn), the open row of the
  // latest started turn, else the row of the latest inference.
  function rowForCall(callId: string): Row | null {
    const turnId = calls.get(callId)?.reactorTurnId;
    const own = turnId !== undefined ? openRows.get(turnId) : undefined;
    if (own !== undefined) return own;
    const started =
      startedTurnId !== undefined ? openRows.get(startedTurnId) : undefined;
    return started ?? current;
  }

  function dropCallsOf(reactorTurnId: string): void {
    for (const [callId, call] of calls) {
      if (call.reactorTurnId === reactorTurnId) calls.delete(callId);
    }
  }

  async function onEvent(event: InferenceEvent): Promise<void> {
    switch (event.type) {
      case "reactor.turn.started":
        startedTurnId = event.data.turnId;
        break;
      case "inference.start": {
        const row = await beginRow(event.data.model, event.data.turnId);
        await insertPart(row, "step-start", null, { model: event.data.model });
        break;
      }
      case "inference.text.delta":
        if (current !== null) current.streamingText += event.data.token;
        break;
      case "inference.done": {
        const turnId = event.data.turnId;
        const row =
          (turnId !== undefined ? openRows.get(turnId) : undefined) ?? current;
        if (row === null) {
          log.warn`Dropping inference.done: no active turn for session ${sessionId}`;
          break;
        }
        await handleInferenceDone(row, event.data.turn.content, turnId);
        row.streamingText = "";
        row.turnUsage = event.data.usage;
        row.turnSource = event.data.source;
        break;
      }
      case "tool.done": {
        const callId = event.data.result.callId;
        const row = rowForCall(callId);
        const call = calls.get(callId);
        calls.delete(callId);
        if (row === null) {
          log.warn`Dropping tool part: no active turn for session ${sessionId}`;
          break;
        }
        const isError = event.data.result.isError ?? false;
        await insertPart(row, "tool", null, {
          kind: "result",
          callId,
          content: event.data.result.content,
          isError,
        });
        const name = call?.name ?? callId;
        const raw = event.data.result.content;
        const content = typeof raw === "string" ? raw : JSON.stringify(raw);
        row.accumulatedToolCalls.push({
          name,
          arguments: call?.arguments ?? {},
          result: content,
          isError,
        });
        if (isError) {
          row.accumulatedToolErrors.push({ name, content });
        }
        break;
      }
      case "inference.error": {
        const row = current;
        if (row === null) {
          log.warn`Dropping error part: no active turn for session ${sessionId}`;
          break;
        }
        row.pendingError = true;
        row.turnHadError = true;
        await insertPart(row, "error", event.data.error.message, {
          category: event.data.error.category,
          ...(event.data.error.statusCode !== undefined
            ? { statusCode: event.data.error.statusCode }
            : {}),
        });
        row.accumulatedErrors.push({
          category: event.data.error.category,
          message: event.data.error.message,
        });
        break;
      }
      case "connector.reply": {
        // The reply finalizes the row of the turn it names, else the row
        // of the latest inference.
        const named = event.data.turnId;
        const row =
          named !== undefined ? (openRows.get(named) ?? null) : current;
        if (row === null || row.finalized) break;
        // Only persist reply content when it originated from an error path.
        // On normal turns inference.done already persisted the text parts.
        if (row.pendingError) {
          row.accumulatedText += event.data.content;
          await insertPart(row, "text", event.data.content, null);
          row.pendingError = false;
        }
        await finalizeRow(
          row,
          row.turnHadError ? "failed" : "completed",
          true,
          true,
        );
        break;
      }
      case "reactor.turn.ended": {
        // A turn that ends with no reply (or after a reply that a later
        // inference followed) leaves its last row running. Finalize the row
        // when the turn it belongs to ends. A `joined` turn did no work of
        // its own.
        const ended = event.data;
        if (ended.outcome === "joined") break;
        if (startedTurnId === ended.turnId) startedTurnId = undefined;
        turnsWithRows.delete(ended.turnId);
        dropCallsOf(ended.turnId);
        const row = openRows.get(ended.turnId);
        if (row === undefined) break;
        switch (ended.outcome) {
          case "replied":
          case "unanswered":
            await finalizeRow(row, "completed", true, false);
            break;
          case "failed":
            await finalizeRow(row, "failed", true, false);
            break;
          default:
            ended satisfies never;
            throw new Error("unreachable: unknown turn outcome");
        }
        break;
      }
      case "reactor.done":
        await finalizeOpenRows("completed", true);
        break;
      case "reactor.error": {
        // An error with no open row gets a row of its own, so the user sees
        // why the agent failed: before any inference of the started turn
        // (e.g. a director error), or before any inference at all (e.g. a
        // context store load failure). An error after a row was finalized,
        // with no new turn, opens none.
        const row = current ?? (await rowForError());
        if (row === null) {
          if (event.data.fatal) await finalizeOpenRows("failed", true);
          break;
        }
        row.turnHadError = true;
        row.accumulatedErrors.push({
          category: "reactor_error",
          message: event.data.error,
        });
        await insertPart(row, "error", event.data.error, {
          category: "reactor_error",
        });
        if (event.data.fatal) await finalizeOpenRows("failed", true);
        break;
      }
      case "inference.usage":
        // Cumulative running total that fires several times per step; overwrite
        // so the last value before finalize is the authoritative per-turn
        // usage, emitted once via onUsage from finalizeRow.
        if (current !== null) {
          current.turnUsage = event.data.usage;
          current.turnSource = event.data.source;
        }
        break;
      default:
        // reactor.start, streaming deltas, and other events are not persisted.
        break;
    }
  }

  // The row an error opens when no row is open: one for the started turn
  // that has no row yet, or the first row of the session.
  async function rowForError(): Promise<Row | null> {
    if (startedTurnId !== undefined && !turnsWithRows.has(startedTurnId)) {
      return beginRow("unknown", startedTurnId);
    }
    if (last === null) return beginRow("unknown", undefined);
    return null;
  }

  async function beginRow(
    model: string,
    reactorTurnId: string | undefined,
  ): Promise<Row> {
    // The turn's previous row is still open: normal in multi-step tool-use
    // loops where inference.start fires again after tools return. Another
    // turn's open row stays open. A row with no reactor turn id is
    // finalized by the next inference, whatever its turn.
    const previous =
      reactorTurnId !== undefined ? openRows.get(reactorTurnId) : undefined;
    if (previous !== undefined) {
      await finalizeRow(previous, "completed", true, false);
    }
    if (
      current !== null &&
      !current.finalized &&
      (current.reactorTurnId === undefined || reactorTurnId === undefined)
    ) {
      await finalizeRow(current, "completed", true, false);
    }

    const row: Row = {
      id: generateId("inferenceTurn"),
      reactorTurnId,
      ordinal: 0,
      finalized: false,
      pendingError: false,
      accumulatedText: "",
      streamingText: "",
      turnHadError: false,
      accumulatedErrors: [],
      accumulatedToolCalls: [],
      accumulatedToolErrors: [],
      turnUsage: null,
      turnSource: null,
    };
    current = row;
    last = row;
    if (reactorTurnId !== undefined) {
      openRows.set(reactorTurnId, row);
      turnsWithRows.add(reactorTurnId);
    }

    await db.insert(inferenceTurn).values({
      id: row.id,
      sessionId,
      runId,
      tenantId,
      model,
      status: "running",
      startedAt: new Date(),
    });
    return row;
  }

  async function handleInferenceDone(
    row: Row,
    content: ContentBlock[],
    reactorTurnId: string | undefined,
  ): Promise<void> {
    for (const block of content) {
      switch (block.type) {
        case "text":
          row.accumulatedText += block.text;
          await insertPart(row, "text", block.text, null);
          break;
        case "thinking":
          await insertPart(row, "reasoning", block.thinking, null);
          break;
        case "redacted_thinking":
          // The opaque `data` blob is meaningless to humans and
          // must be preserved verbatim for echo-back on follow-up
          // turns; persisting it as a reasoning row would invite
          // truncation or display. Skip and let the adapter layer
          // own the round-trip.
          break;
        case "refusal":
          // Refusal blocks carry human-readable text the model
          // emitted when it declined a structured-output request.
          // A dedicated `refusal` part kind keeps the signal
          // distinct from ordinary `text` (the model produced
          // schema-conformant content) and from `error` (the HTTP
          // call failed or the protocol mismatched). Session
          // readers can branch on the part type to render policy
          // declines differently from regular assistant output.
          await insertPart(row, "refusal", block.reason, null);
          break;
        case "safety_rating":
          // Structured safety signals (e.g. Gemini
          // promptFeedback.blockReason). Persist the reason under a
          // dedicated part kind so a blocked turn is not empty in
          // the audit trail. Content is the provider-native reason
          // string (observed: PROHIBITED_CONTENT).
          await insertPart(row, "safety_rating", block.blockReason, null);
          break;
        case "tool_call":
          calls.set(block.id, {
            reactorTurnId: reactorTurnId ?? row.reactorTurnId,
            name: block.name,
            arguments: block.arguments,
          });
          await insertPart(row, "tool", null, {
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
        case "citation":
          // Citations annotate model output by reference (URI or
          // document index). Persistence semantics — whether the
          // cited source warrants an audit row of its own — are not
          // yet settled; skip until they are.
          break;
        case "code_execution_request":
        case "code_execution_result":
          // Server-side code execution requests and results.
          // Persistence semantics — whether the code and its output
          // warrant audit rows of their own, and how to relate the
          // pair through the requestId back-pointer — are not yet
          // settled; skip until they are.
          break;
        case "image":
        case "audio":
        case "video":
        case "document": {
          // All media block variants persist into the generic "file"
          // part bucket. The block's own `type` distinguishes the
          // semantic role; `mimeType` distinguishes the encoding; the
          // MediaSource discriminant distinguishes inline vs reference.
          const source = block.source;
          if (source.kind === "base64") {
            await insertPart(row, "file", null, {
              kind: "base64",
              mimeType: source.mimeType,
              dataLength: source.data.length,
            });
          } else if (source.kind === "file-reference") {
            await insertPart(row, "file", null, {
              kind: "file-reference",
              mimeType: source.mimeType,
              reference: source.reference,
            });
          } else if (source.kind === "url") {
            // The `url` MediaSource variant carries a self-contained
            // dereferenceable HTTP(S) URL (Gemini accepts these in
            // `fileData/fileUri`; other adapters route similarly).
            // The session record keeps the URL on a distinct `url`
            // field rather than reusing the `reference` slot that
            // `file-reference` uses: a session reader that filters
            // or joins on `metadata->>'reference'` should see
            // provider-opaque file ids only, with public URLs
            // surfaced under their own field. The discriminant
            // `kind` is the structural source of truth, and the
            // field-per-variant shape keeps naive substring queries
            // honest.
            await insertPart(row, "file", null, {
              kind: "url",
              mimeType: source.mimeType,
              url: source.url,
            });
          } else {
            source satisfies never;
            throw new Error(`unreachable: unknown MediaSource kind`);
          }
          break;
        }
      }
    }

    // Mark the end of this inference step.
    await insertPart(row, "step-finish", null, null);
  }

  async function finalizeRow(
    row: Row,
    status: "completed" | "failed",
    notify: boolean,
    hadReply: boolean,
  ): Promise<void> {
    if (row.finalized) return;
    row.finalized = true;
    if (row.reactorTurnId !== undefined) {
      if (openRows.get(row.reactorTurnId) === row) {
        openRows.delete(row.reactorTurnId);
      }
    }
    if (current === row) current = null;

    await db
      .update(inferenceTurn)
      .set({ status, endedAt: new Date() })
      .where(eq(inferenceTurn.id, row.id));

    if (notify && onTurnFinalized) {
      onTurnFinalized({
        turnId: row.id,
        status,
        text: row.accumulatedText,
        hadReply,
        hadError: row.turnHadError,
        errors: [...row.accumulatedErrors],
        toolCalls: [...row.accumulatedToolCalls],
        toolErrors: [...row.accumulatedToolErrors],
      });
    }

    // Report per-turn usage once, alongside the finalize notify. Gated on
    // turnUsage being present so a turn that ran no inference emits nothing.
    // NOTE: abandon() finalizes with notify=false, so a turn abandoned
    // mid-step (e.g. a sidecar disconnect) reports no usage even if the
    // provider already billed input tokens -- an accepted gap until durable
    // usage persistence lands on the turn row.
    if (
      notify &&
      onUsage &&
      row.turnUsage !== null &&
      row.turnSource !== null
    ) {
      onUsage({
        tenantId,
        sessionId,
        runId,
        turnId: row.id,
        provider: row.turnSource.provider,
        model: row.turnSource.model,
        usage: row.turnUsage,
      });
    }
  }

  // Every open row: the latest one first, then each reactor turn's.
  function openRowList(): Row[] {
    const rows: Row[] = [];
    if (current !== null && !current.finalized) rows.push(current);
    for (const row of openRows.values()) {
      if (!row.finalized && row !== current) rows.push(row);
    }
    return rows;
  }

  async function finalizeOpenRows(
    status: "completed" | "failed",
    notify: boolean,
  ): Promise<void> {
    for (const row of openRowList()) {
      await finalizeRow(row, status, notify, false);
    }
    calls.clear();
  }

  async function abandon(): Promise<void> {
    const rows = openRowList();
    if (rows.length === 0) return;

    for (const row of rows) {
      log.warn`Abandoning running turn ${row.id} for session ${sessionId}`;
      await finalizeRow(row, "failed", false, false);
    }
    calls.clear();
  }

  async function insertPart(
    row: Row,
    partType: string,
    content: string | null,
    metadata: Record<string, unknown> | null,
  ): Promise<void> {
    if (row.finalized) {
      log.warn`Dropping ${partType} part: no active turn for session ${sessionId}`;
      return;
    }

    const values: typeof turnPart.$inferInsert = {
      id: generateId("turnPart"),
      turnId: row.id,
      sessionId,
      type: parseTurnPartType(partType),
      ordinal: row.ordinal++,
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
    if (current === null) return "";
    return current.accumulatedText + current.streamingText;
  }

  function getCurrentTurnId(): string | null {
    return current?.id ?? null;
  }

  function getLastTurnId(): string | null {
    return last?.id ?? null;
  }

  return {
    onEvent,
    abandon,
    getAccumulatedText,
    getCurrentTurnId,
    getLastTurnId,
  };
}
