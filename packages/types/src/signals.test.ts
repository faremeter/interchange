import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import { GateType } from "./runtime";
import {
  ControlSignal,
  signalKinds,
  signalKindToGateType,
  signalName,
} from "./signals";

describe("signalKindToGateType exhaustiveness", () => {
  for (const kind of signalKinds) {
    test(`maps signal kind ${kind} to a valid GateType`, () => {
      const gate = signalKindToGateType(kind);
      expect(GateType(gate) instanceof type.errors).toBe(false);
    });
  }
});

describe("ControlSignal", () => {
  test("accepts a well-formed approval envelope", () => {
    const signal = ControlSignal({
      correlationId: "corr-1",
      kind: "approval",
      outcome: "approved",
      payload: { scope: "once" },
    });
    expect(signal instanceof type.errors).toBe(false);
  });

  test("rejects an outcome outside the approval vocabulary", () => {
    const signal = ControlSignal({
      correlationId: "corr-1",
      kind: "approval",
      outcome: "settled",
      payload: null,
    });
    expect(signal instanceof type.errors).toBe(true);
  });

  test("rejects an unknown kind discriminant", () => {
    const signal = ControlSignal({
      correlationId: "corr-1",
      kind: "payment",
      outcome: "approved",
      payload: null,
    });
    expect(signal instanceof type.errors).toBe(true);
  });
});

describe("signalName", () => {
  test("mints the reserved __signal__ namespace for a correlation id", () => {
    expect(signalName("corr-1")).toBe("__signal__:corr-1");
  });
});
