import { describe, test, expect } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import type { WorkflowRunEvent } from "@intx/hub-client";

import { RunEventList } from "./run-event-list";

function event(
  seq: number,
  type: string,
  body: Record<string, unknown> = {},
): WorkflowRunEvent {
  return { seq, type, body };
}

function render(events: WorkflowRunEvent[]) {
  return renderToStaticMarkup(<RunEventList events={events} />);
}

describe("RunEventList", () => {
  test("renders the empty-log state when there are no events", () => {
    const html = render([]);
    expect(html).toContain("No events recorded yet.");
    expect(html).not.toContain("<ol");
    expect(html).not.toContain("<li");
  });

  test("renders events as a list labelled by seq in the order supplied", () => {
    const html = render([
      event(5, "RunStarted"),
      event(12, "StepStarted"),
      event(30, "RunCompleted"),
    ]);
    expect(html).toContain("<ol");
    expect(html.indexOf("RunStarted")).toBeLessThan(
      html.indexOf("StepStarted"),
    );
    expect(html.indexOf("StepStarted")).toBeLessThan(
      html.indexOf("RunCompleted"),
    );
    // The non-contiguous seqs cannot equal their array indices, so this also
    // proves the label is the event's seq rather than its position.
    for (const seq of ["5", "12", "30"]) {
      expect(html).toContain(`>${seq}<`);
    }
  });

  test("dumps the per-event body as JSON when it carries fields", () => {
    const html = render([
      event(12, "SignalAwaited", { signalName: "approve" }),
    ]);
    expect(html).toContain("SignalAwaited");
    expect(html).toContain("signalName");
    expect(html).toContain("approve");
  });

  test("omits the body dump when the event body is empty", () => {
    const html = render([event(5, "RunStarted")]);
    expect(html).toContain("RunStarted");
    // An empty body renders no serialized object; a regression that always
    // dumped the body would emit the "{}" of `JSON.stringify({})`.
    expect(html).not.toContain("{}");
  });
});
