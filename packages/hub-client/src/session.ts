import { type } from "arktype";

import type { Transport } from "./transport";
import type { WorkflowRunEvent } from "./validators";
import { WorkflowRunEvents } from "./validators";
import { isTerminalRunEvents } from "./transforms";

// How often to re-read the run's event log while it is still live. The log is
// git-backed and settles quickly, so a few-second cadence keeps the timeline
// fresh without hammering the substrate.
const DEFAULT_RUN_POLL_INTERVAL_MS = 2000;

// A read-only view of one workflow run's committed event log. The session
// polls the run's `/events` endpoint, replacing its timeline with each read,
// and stops once the run reaches a terminal event.
export interface RunSession {
  readonly events: WorkflowRunEvent[];
  readonly hydrated: boolean;
  readonly terminal: boolean;

  start(): () => void;
  destroy(): void;
}

export function createRunSession(opts: {
  tenantId: string;
  runId: string;
  transport: Transport;
  onChange: () => void;
  onError?: (error: Error) => void;
  pollIntervalMs?: number;
}): RunSession {
  const {
    tenantId,
    runId,
    transport,
    onChange,
    onError,
    pollIntervalMs = DEFAULT_RUN_POLL_INTERVAL_MS,
  } = opts;

  const basePath = `/api/tenants/${tenantId}/workflows/runs/${runId}`;

  let events: WorkflowRunEvent[] = [];
  let hydrated = false;
  let terminal = false;

  // `stopped` halts scheduling and discards any in-flight read, so a late
  // response cannot mutate state after start()'s cleanup or destroy().
  let stopped = false;
  let started = false;
  let timer: ReturnType<typeof setTimeout> | null = null;

  function reportError(error: Error): void {
    if (onError) {
      onError(error);
    } else {
      throw error;
    }
  }

  async function poll(): Promise<void> {
    let raw: unknown;
    try {
      raw = await transport.fetch<unknown>("GET", `${basePath}/events`);
    } catch (err) {
      if (stopped) return;
      reportError(new Error("Failed to read run events", { cause: err }));
      return;
    }
    if (stopped) return;

    const validated = WorkflowRunEvents(raw);
    if (validated instanceof type.errors) {
      reportError(
        new Error(`Invalid run events response: ${validated.summary}`),
      );
      return;
    }

    // The endpoint returns the full seq-ordered log on every call, so the
    // latest read replaces the timeline outright -- there is no delta to merge,
    // and a replay of the same seqs cannot duplicate an entry.
    events = validated.events;
    hydrated = true;
    terminal = isTerminalRunEvents(events);
    onChange();
  }

  function schedule(): void {
    if (stopped || terminal) return;
    timer = setTimeout(() => {
      void (async () => {
        await poll();
        schedule();
      })();
    }, pollIntervalMs);
  }

  function stopPolling(): void {
    stopped = true;
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
  }

  return {
    get events() {
      return events;
    },
    get hydrated() {
      return hydrated;
    },
    get terminal() {
      return terminal;
    },

    start(): () => void {
      if (started) {
        throw new Error("start() called on an already-started session");
      }
      started = true;

      // Poll immediately so the timeline hydrates without waiting a full
      // interval, then keep polling until the run settles.
      void (async () => {
        await poll();
        schedule();
      })();

      return stopPolling;
    },

    destroy(): void {
      stopPolling();
    },
  };
}
