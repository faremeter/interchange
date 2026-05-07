import { type } from "arktype";

import type { InferenceTurnResponse, MailResponse } from "@interchange/types";
import {
  InferenceEvent,
  type InferenceEvent as InferenceEventType,
} from "@interchange/types/runtime";

import {
  mailDeliveryToEvent,
  mailToEvent,
  shouldShowMail,
  turnToEvent,
} from "./transforms";
import type { Transport } from "./transport";
import type { AgentActivity, InstanceEvent } from "./types";
import {
  InferenceTextReplayEvent,
  MailDeliveredEvent,
  sessionEndedEvent,
  TurnCommittedEvent,
} from "./validators";

export interface InstanceSession {
  readonly events: InstanceEvent[];
  readonly streaming: string;
  readonly activity: AgentActivity | null;
  readonly hydrated: boolean;

  start(): () => void;
  sendMail(content: string): Promise<void>;
  destroy(): void;
}

type MailListResponse = { data: MailResponse[] };
type TurnListResponse = { data: InferenceTurnResponse[] };

export function createInstanceSession(opts: {
  tenantId: string;
  instanceId: string;
  transport: Transport;
  onChange: () => void;
  onSessionEnded?: () => void;
}): InstanceSession {
  const { tenantId, instanceId, transport, onChange, onSessionEnded } = opts;

  const basePath = `/api/tenants/${tenantId}/agents/instances/${instanceId}`;

  let events: InstanceEvent[] = [];
  let streaming = "";
  let streamingFromReplay = false;
  let activity: AgentActivity | null = null;
  let hydrated = false;

  // Null means hydration is complete (or SSE buffering is not active).
  // Non-null means we are in the hydration window and buffering SSE events.
  let sseBuffer: InstanceEvent[] | null = null;

  let destroyed = false;
  let started = false;
  let stopSSE: (() => void) | null = null;

  function pushOrBuffer(event: InstanceEvent): void {
    if (hydrated) {
      events.push(event);
      onChange();
    } else if (sseBuffer !== null) {
      sseBuffer.push(event);
    } else {
      sseBuffer = [event];
    }
  }

  function handleSSEEvent(raw: unknown): void {
    if (destroyed) return;

    if (!(sessionEndedEvent(raw) instanceof type.errors)) {
      onSessionEnded?.();
      return;
    }

    const mailEvent = MailDeliveredEvent(raw);
    if (!(mailEvent instanceof type.errors)) {
      const mailId = mailEvent.data.id;
      const alreadyInEvents = events.some(
        (e) => e.kind === "mail" && e.id === mailId,
      );
      const alreadyInBuffer =
        sseBuffer !== null &&
        sseBuffer.some((e) => e.kind === "mail" && e.id === mailId);
      if (!alreadyInEvents && !alreadyInBuffer) {
        const d = mailEvent.data;
        if (!shouldShowMail(d)) return;
        pushOrBuffer(mailDeliveryToEvent(d));
      }
      return;
    }

    const turnEvent = TurnCommittedEvent(raw);
    if (!(turnEvent instanceof type.errors)) {
      const { turnId, status, text, hadError, errors, toolErrors } =
        turnEvent.data;
      const isError = hadError || status === "failed";
      const alreadyInEvents = events.some(
        (e) => e.kind === "turn" && e.turnId === turnId,
      );
      const alreadyInBuffer =
        sseBuffer !== null &&
        sseBuffer.some((e) => e.kind === "turn" && e.turnId === turnId);

      if (!alreadyInEvents && !alreadyInBuffer) {
        if (text || isError || toolErrors.length > 0) {
          const newEvent: InstanceEvent = {
            kind: "turn",
            turnId,
            content: text || "An error occurred during inference.",
            timestamp: new Date().toISOString(),
            ...(isError ? { isError: true } : {}),
            ...(errors.length > 0 ? { errors } : {}),
            ...(toolErrors.length > 0 ? { toolErrors } : {}),
          };
          // Inline push/buffer instead of pushOrBuffer to avoid a double
          // onChange — this handler always calls onChange at the end for
          // streaming/activity state changes.
          if (hydrated) {
            events.push(newEvent);
          } else if (sseBuffer !== null) {
            sseBuffer.push(newEvent);
          } else {
            sseBuffer = [newEvent];
          }
        }
      }

      // Only clear streaming if it still holds text from this committed turn.
      // In multi-step tool loops, turn.committed for turn N may arrive after
      // deltas for turn N+1 have already started populating the buffer.
      if (!streaming || streaming === text) {
        streaming = "";
        streamingFromReplay = false;
      }
      activity = null;
      onChange();
      return;
    }

    const replayEvent = InferenceTextReplayEvent(raw);
    if (!(replayEvent instanceof type.errors)) {
      if (streaming === "") {
        streaming = replayEvent.data.text;
        streamingFromReplay = true;
        onChange();
      }
      return;
    }

    const validated = InferenceEvent(raw);
    if (validated instanceof type.errors) return;
    // The validator returns typeof InferenceEvent.infer which loses some
    // discriminated union narrowing. The manually-defined InferenceEvent type
    // preserves it correctly for switch statement narrowing.
    const event = validated as InferenceEventType;

    switch (event.type) {
      case "inference.start":
        activity = { type: "inferring" };
        onChange();
        break;
      case "inference.text.delta":
        streaming += event.data.token;
        streamingFromReplay = false;
        activity = null;
        onChange();
        break;
      case "inference.tool_call.start":
        activity = { type: "tool_call", name: event.data.name };
        onChange();
        break;
      case "tool.start":
        activity = { type: "tool_running", name: event.data.call.name };
        onChange();
        break;
      case "tool.done":
        activity = null;
        onChange();
        break;
      case "inference.done":
        activity = null;
        onChange();
        break;
      case "inference.error":
        streaming = "";
        streamingFromReplay = false;
        activity = null;
        onChange();
        break;
      case "reactor.done":
        streaming = "";
        streamingFromReplay = false;
        activity = null;
        onChange();
        break;
    }
  }

  return {
    get events() {
      return events;
    },
    get streaming() {
      return streaming;
    },
    get activity() {
      return activity;
    },
    get hydrated() {
      return hydrated;
    },

    start(): () => void {
      if (started) {
        throw new Error("start() called on an already-started session");
      }
      started = true;

      // Open SSE first to avoid losing events during hydration fetch.
      sseBuffer = [];
      stopSSE = transport.subscribe(`${basePath}/events`, handleSSEEvent, {
        eventName: "agent.event",
      });

      let cancelled = false;

      void (async () => {
        let fetchError: unknown;
        const all: InstanceEvent[] = [];

        try {
          const [mailRes, turnsRes] = await Promise.all([
            transport.fetch<MailListResponse>(
              "GET",
              `${basePath}/mail?limit=100`,
            ),
            transport.fetch<TurnListResponse>(
              "GET",
              `${basePath}/turns?limit=100`,
            ),
          ]);

          for (const m of mailRes.data) {
            if (shouldShowMail(m)) all.push(mailToEvent(m));
          }
          for (const t of turnsRes.data) {
            const event = turnToEvent(t);
            if (event) all.push(event);
          }
        } catch (err) {
          fetchError = err;
        }

        if (cancelled || destroyed) return;

        // Drain SSE buffer and merge with hydration results, deduplicating
        // by mail id or turn id so events that arrived during the fetch
        // window are not duplicated.
        const buffered = sseBuffer;
        sseBuffer = null;

        if (buffered !== null) {
          const seenMailIds = new Set<string>();
          const seenTurnIds = new Set<string>();
          for (const e of all) {
            if (e.kind === "mail") seenMailIds.add(e.id);
            else seenTurnIds.add(e.turnId);
          }
          for (const e of buffered) {
            if (e.kind === "mail" && !seenMailIds.has(e.id)) all.push(e);
            else if (e.kind === "turn" && !seenTurnIds.has(e.turnId))
              all.push(e);
          }
        }

        all.sort((a, b) =>
          a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0,
        );

        // If streaming was set by replay and no live deltas have arrived
        // since, the corresponding turn is already in the hydrated events.
        // Clear it so the UI doesn't show a stale streaming preview.
        if (streamingFromReplay) {
          streaming = "";
          streamingFromReplay = false;
        }

        events = all;
        hydrated = true;
        onChange();

        if (fetchError) {
          throw new Error("Failed to hydrate chat history", {
            cause: fetchError,
          });
        }
      })();

      return () => {
        cancelled = true;
        sseBuffer = null;
        stopSSE?.();
        stopSSE = null;
      };
    },

    async sendMail(content: string): Promise<void> {
      await transport.fetch("POST", `${basePath}/mail`, { content });
    },

    destroy(): void {
      if (destroyed) return;
      destroyed = true;
      stopSSE?.();
      stopSSE = null;
      sseBuffer = null;
      activity = null;
    },
  };
}
