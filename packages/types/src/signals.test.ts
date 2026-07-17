import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import { GateType } from "./runtime";
import {
  ControlSignal,
  correlationIdFromSignalName,
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

describe("correlationIdFromSignalName", () => {
  // Round-trips the writer, including ids that themselves contain the
  // reserved prefix or a colon: the reader slices a fixed offset rather
  // than greedily stripping, so a nested prefix survives.
  for (const id of ["corr-1", "", "a:b:c", "__signal__:nested", "  spaces  "]) {
    test(`round-trips ${JSON.stringify(id)}`, () => {
      expect(correlationIdFromSignalName(signalName(id))).toBe(id);
    });
  }

  test("returns undefined for a free-form (non-reserved) signal name", () => {
    expect(correlationIdFromSignalName("approval")).toBeUndefined();
    expect(correlationIdFromSignalName("")).toBeUndefined();
    // A single-underscore near-miss is not the reserved prefix.
    expect(correlationIdFromSignalName("__signal:approval")).toBeUndefined();
  });

  test("yields an empty correlation id for the prefix alone", () => {
    expect(correlationIdFromSignalName("__signal__:")).toBe("");
  });
});
