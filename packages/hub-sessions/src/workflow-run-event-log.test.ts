import { describe, test, expect } from "bun:test";

import {
  splitCombinedEventLog,
  encodeCombinedEventLog,
} from "./workflow-run-event-log";

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

describe("encodeCombinedEventLog", () => {
  const enc = new TextEncoder();
  const dec = new TextDecoder();

  test("concatenates each blob followed by a newline", () => {
    const out = encodeCombinedEventLog([
      enc.encode('{"seq":0}'),
      enc.encode('{"seq":1}'),
    ]);
    expect(dec.decode(out)).toBe('{"seq":0}\n{"seq":1}\n');
  });

  test("an empty input yields an empty file", () => {
    expect(encodeCombinedEventLog([]).byteLength).toBe(0);
  });

  test("preserves the exact blob bytes with no decode round-trip", () => {
    // A byte sequence that decoding would normalise (a lone 0x80 maps to
    // U+FFFD): the encoder must carry it verbatim, since events are signed
    // over their own bytes.
    const blob = new Uint8Array([0x7b, 0x80, 0x7d]);
    expect(Array.from(encodeCombinedEventLog([blob]))).toEqual([
      0x7b, 0x80, 0x7d, 0x0a,
    ]);
  });
});
