import type { WorkflowRunEvent } from "@intx/hub-client";

// Renders a workflow run's committed event log as a seq-ordered list: the seq,
// the event type, and a compact dump of any per-event body. Presentational
// only -- the caller supplies the events (a polled read or a query).
export function RunEventList({ events }: { events: WorkflowRunEvent[] }) {
  if (events.length === 0) {
    return (
      <p className="mt-2 text-xs text-muted-foreground">
        No events recorded yet.
      </p>
    );
  }

  return (
    <ol className="mt-2 space-y-1">
      {events.map((event) => (
        <li
          key={event.seq}
          className="grid grid-cols-[2.5rem_1fr] gap-2 text-xs"
        >
          <span className="font-mono text-muted-foreground">{event.seq}</span>
          <span>
            <span className="font-medium">{event.type}</span>
            {Object.keys(event.body).length > 0 && (
              <span className="ml-2 font-mono text-muted-foreground">
                {JSON.stringify(event.body)}
              </span>
            )}
          </span>
        </li>
      ))}
    </ol>
  );
}
