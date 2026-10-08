import { type } from "arktype";

import type { Transport } from "./transport";
import type { WorkflowRunEvent } from "./validators";
import { WorkflowRunEvents } from "./validators";
import { isTerminalRunEvents } from "./transforms";

// Re-read the run's event log on this cadence while it is live; the log
// is git-backed, so a few seconds keeps it fresh without hammering it.
const DEFAULT_RUN_POLL_INTERVAL_MS = 2000;

// Read-only view of one run's committed event log: polls `/events`,
// replaces the timeline on each read, stops at a terminal event.
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

    // The endpoint returns the full seq-ordered log each call, so the
    // latest read replaces the timeline outright -- no delta to merge.
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

      // Poll immediately so the timeline hydrates without a full interval
      // wait, then keep polling until the run settles.
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
