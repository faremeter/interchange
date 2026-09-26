import { type } from "arktype";

const TerminalEventTime = type({ "at?": "string" });

/** Bound event time to the run's lifetime; absent or invalid times use observation. */
export function getWorkflowRunEndedAt(
  eventBody: unknown,
  createdAt: Date,
  observedAt: Date,
): Date {
  const event = TerminalEventTime(eventBody);
  const at =
    event instanceof type.errors || event.at === undefined
      ? NaN
      : Date.parse(event.at);
  return new Date(
    Math.max(
      createdAt.getTime(),
      Number.isFinite(at)
        ? Math.min(observedAt.getTime(), at)
        : observedAt.getTime(),
    ),
  );
}
