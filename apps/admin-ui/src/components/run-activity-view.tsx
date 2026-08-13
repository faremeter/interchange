import type { AwaitingSignal, WorkflowRunEvent } from "@intx/hub-client";

import { Badge } from "@/components/ui/badge";
import { RunEventList } from "@/components/run-event-list";

// Presentational view of a run's activity timeline. Given the session-derived
// state -- the committed events, whether the first read has landed, whether the
// run has settled, any awaited signal, and any read error -- it renders the
// live/terminal badge, the awaiting-signal notice, and the event log. The
// effectful session lives in the caller; this component only renders.
export function RunActivityView({
  events,
  hydrated,
  terminal,
  awaiting,
  error,
}: {
  events: WorkflowRunEvent[];
  hydrated: boolean;
  terminal: boolean;
  awaiting: AwaitingSignal | null;
  error: Error | null;
}) {
  return (
    <div className="mt-6">
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold">Activity</h3>
        {hydrated && (
          <Badge variant={terminal ? "outline" : "secondary"}>
            {terminal ? "terminal" : "live"}
          </Badge>
        )}
      </div>

      {error && (
        <p className="mb-2 text-xs text-destructive">
          Could not read the run's activity: {error.message}
        </p>
      )}

      {awaiting && (
        <p className="mb-2 rounded-md border border-dashed p-2 text-xs text-muted-foreground">
          Awaiting signal{" "}
          <span className="font-mono">{awaiting.signalName}</span>
        </p>
      )}

      <div className="rounded-lg border bg-background p-3">
        {!hydrated ? (
          <p className="text-xs text-muted-foreground">Loading activity...</p>
        ) : (
          <RunEventList events={events} />
        )}
      </div>
    </div>
  );
}
