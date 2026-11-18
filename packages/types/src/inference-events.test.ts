import { describe, expect, test } from "bun:test";
import { type } from "arktype";

import { parseInferenceEvent } from "./inference-events";

describe("parseInferenceEvent turn and gate events", () => {
  test("accepts a turn that ended abandoned", () => {
    const parsed = parseInferenceEvent({
      type: "reactor.turn.ended",
      seq: 1,
      data: { turnId: "t1", outcome: "abandoned" },
    });
    expect(parsed instanceof type.errors).toBe(false);
  });

  test("accepts a gate cleared because its turn was abandoned", () => {
    const parsed = parseInferenceEvent({
      type: "reactor.gate.cleared",
      seq: 1,
      data: { gateId: "g1", reason: "abandoned" },
    });
    expect(parsed instanceof type.errors).toBe(false);
  });

  test("rejects a joined end with no joined turn", () => {
    const parsed = parseInferenceEvent({
      type: "reactor.turn.ended",
      seq: 1,
      data: { turnId: "t1", outcome: "joined" },
    });
    expect(parsed instanceof type.errors).toBe(true);
  });
});
