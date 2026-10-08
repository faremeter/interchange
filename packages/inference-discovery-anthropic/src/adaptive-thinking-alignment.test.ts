import { describe, test, expect } from "bun:test";
import {
  ADAPTIVE_THINKING_MODELS as RUNTIME_ADAPTIVE_MODELS,
  ADAPTIVE_THINKING_EFFORT,
} from "@intx/inference/providers";
import {
  ADAPTIVE_THINKING_MODELS as DISCOVERY_ADAPTIVE_MODELS,
  ADAPTIVE_THINKING_CAPTURE_EFFORT,
} from "./request-body";

describe("adaptive-thinking model alignment", () => {
  test("the discovery set matches the runtime adapter's set", () => {
    // The discovery and runtime sets are hand-maintained in separate
    // packages; if they drift, a captured fixture stops proving the
    // production wire. Pin them equal.
    expect(new Set(DISCOVERY_ADAPTIVE_MODELS)).toEqual(
      new Set(RUNTIME_ADAPTIVE_MODELS),
    );
  });
});

describe("adaptive-thinking effort pair", () => {
  test("production sends high and capture sends max", () => {
    // Production sends "high" (the API default) while capture sends "max"
    // because only "max" reliably elicits a thinking block. Unlike the model
    // sets, which must stay equal, this is an intentional unequal pair; a
    // change to either value trips here.
    expect(ADAPTIVE_THINKING_EFFORT).toBe("high");
    expect(ADAPTIVE_THINKING_CAPTURE_EFFORT).toBe("max");
  });
});
