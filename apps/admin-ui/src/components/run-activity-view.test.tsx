import { describe, test, expect } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import type { AwaitingSignal, WorkflowRunEvent } from "@intx/hub-client";

import { RunActivityView } from "./run-activity-view";

function event(
  seq: number,
  type: string,
  body: Record<string, unknown> = {},
): WorkflowRunEvent {
  return { seq, type, body };
}

function render(props: {
  events?: WorkflowRunEvent[];
  hydrated?: boolean;
  terminal?: boolean;
  awaiting?: AwaitingSignal | null;
  error?: Error | null;
}) {
  return renderToStaticMarkup(
    <RunActivityView
      events={props.events ?? []}
      hydrated={props.hydrated ?? false}
      terminal={props.terminal ?? false}
      awaiting={props.awaiting ?? null}
      error={props.error ?? null}
    />,
  );
}

describe("RunActivityView", () => {
  test("shows the loading state and no status badge before the first read", () => {
    const html = render({ hydrated: false });
    expect(html).toContain("Loading activity...");
    expect(html).not.toContain('data-slot="badge"');
  });

  test("renders a live badge and the timeline for an in-progress run", () => {
    const html = render({
      hydrated: true,
      terminal: false,
      events: [event(0, "RunStarted"), event(1, "StepStarted")],
    });
    expect(html).toContain('data-variant="secondary"');
    expect(html).toContain(">live<");
    expect(html).not.toContain(">terminal<");
    expect(html).not.toContain("Loading activity...");
    expect(html).toContain("RunStarted");
    expect(html).toContain("StepStarted");
  });

  test("renders a terminal badge once the run has settled", () => {
    const html = render({
      hydrated: true,
      terminal: true,
      events: [event(0, "RunStarted"), event(1, "RunCompleted")],
    });
    expect(html).toContain('data-variant="outline"');
    expect(html).toContain(">terminal<");
    expect(html).not.toContain(">live<");
  });

  test("renders the empty-log state for a hydrated run with no events", () => {
    const html = render({ hydrated: true, events: [] });
    expect(html).toContain("No events recorded yet.");
    expect(html).not.toContain("Loading activity...");
  });

  test("surfaces the awaited signal name while the run is parked", () => {
    const html = render({
      hydrated: true,
      awaiting: { seq: 2, signalName: "approve" },
    });
    expect(html).toContain("Awaiting signal");
    expect(html).toContain("approve");
  });

  test("omits the awaiting notice when the run is not parked", () => {
    const html = render({ hydrated: true, awaiting: null });
    expect(html).not.toContain("Awaiting signal");
  });

  test("surfaces a read error with its message", () => {
    const html = render({
      hydrated: true,
      error: new Error("connection refused"),
    });
    expect(html).toContain("Could not read the run&#x27;s activity:");
    expect(html).toContain("connection refused");
  });

  test("omits the error line when there is no error", () => {
    const html = render({ hydrated: true, error: null });
    expect(html).not.toContain("Could not read the run");
  });
});
