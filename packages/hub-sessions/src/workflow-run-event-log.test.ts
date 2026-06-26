import { describe, test, expect } from "bun:test";

import { splitCombinedEventLog } from "./workflow-run-event-log";

describe("splitCombinedEventLog", () => {
  test("returns no entries for an empty file", () => {
    expect(splitCombinedEventLog("")).toEqual([]);
  });

  test("splits one event per line and drops the trailing newline", () => {
    const combined = ['{"seq":0}', '{"seq":1}', '{"seq":2}'].join("\n") + "\n";
    expect(splitCombinedEventLog(combined)).toEqual([
      '{"seq":0}',
      '{"seq":1}',
      '{"seq":2}',
    ]);
  });

  test("keeps the last line when there is no trailing newline", () => {
    expect(splitCombinedEventLog('{"seq":0}\n{"seq":1}')).toEqual([
      '{"seq":0}',
      '{"seq":1}',
    ]);
  });

  test("drops blank interior lines rather than yielding empty entries", () => {
    expect(splitCombinedEventLog('{"seq":0}\n\n{"seq":1}\n')).toEqual([
      '{"seq":0}',
      '{"seq":1}',
    ]);
  });

  test("round-trips the verbatim per-event bytes the writer joins", () => {
    // Each line is the exact text a per-event blob held; JSON.stringify
    // never emits a literal newline, so a line split is a faithful inverse.
    const perEvent = [
      JSON.stringify({ seq: 0, type: "RunStarted", note: "a\nb" }),
      JSON.stringify({ seq: 1, type: "RunCompleted" }),
    ];
    const combined = perEvent.join("\n") + "\n";
    expect(splitCombinedEventLog(combined)).toEqual(perEvent);
  });
});
