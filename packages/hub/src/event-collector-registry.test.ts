import { describe, test, expect } from "bun:test";
import type { InferenceEvent } from "@interchange/types/runtime";
import { deriveStatus } from "./event-collector-registry";

function event(
  type: string,
  data: Record<string, unknown> = {},
): InferenceEvent {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test helper: type string is the correct discriminant
  return { type, seq: 1, data } as InferenceEvent;
}

describe("deriveStatus", () => {
  test("reactor.start does not set busy", () => {
    expect(deriveStatus(event("reactor.start"))).toBeNull();
  });

  test("inference.start sets busy", () => {
    expect(deriveStatus(event("inference.start", { model: "test" }))).toEqual({
      status: "busy",
    });
  });

  test("connector.reply sets idle", () => {
    expect(deriveStatus(event("connector.reply", { content: "hi" }))).toEqual({
      status: "idle",
    });
  });

  test("reactor.done sets idle", () => {
    expect(deriveStatus(event("reactor.done"))).toEqual({ status: "idle" });
  });

  test("reactor.gate.cleared sets busy", () => {
    expect(
      deriveStatus(event("reactor.gate.cleared", { gateId: "g1" })),
    ).toEqual({ status: "busy" });
  });

  test("reactor.gate.blocked with approval sets waiting_approval", () => {
    expect(
      deriveStatus(
        event("reactor.gate.blocked", { reason: "approval", gateId: "g1" }),
      ),
    ).toEqual({ status: "waiting_approval" });
  });

  test("reactor.gate.blocked with non-approval reason returns null", () => {
    expect(
      deriveStatus(
        event("reactor.gate.blocked", { reason: "payment", gateId: "g1" }),
      ),
    ).toBeNull();
  });

  test("fatal reactor.error sets idle", () => {
    expect(
      deriveStatus(event("reactor.error", { error: "boom", fatal: true })),
    ).toEqual({ status: "idle" });
  });

  test("non-fatal reactor.error returns null", () => {
    expect(
      deriveStatus(event("reactor.error", { error: "oops", fatal: false })),
    ).toBeNull();
  });

  test("unrecognized event returns null", () => {
    expect(
      deriveStatus(event("inference.text.delta", { token: "hi" })),
    ).toBeNull();
  });
});
