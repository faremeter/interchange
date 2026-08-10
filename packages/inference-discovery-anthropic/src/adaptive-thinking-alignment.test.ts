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
    // Both layers must agree on which models use the adaptive thinking wire
    // (thinking:{type:"adaptive"} + output_config.effort). If they drift, a
    // captured discovery fixture builds a different request shape than the
    // runtime sends, so the fixture stops proving the production wire. The two
    // sets live in separate packages and are hand-maintained; this pins them
    // equal so a model added to one but not the other fails here.
    expect(new Set(DISCOVERY_ADAPTIVE_MODELS)).toEqual(
      new Set(RUNTIME_ADAPTIVE_MODELS),
    );
  });
});

describe("adaptive-thinking effort pair", () => {
  test("production sends high and capture sends max", () => {
    // The runtime adapter and the discovery capture rig deliberately send
    // different effort on the adaptive-thinking wire: production uses "high"
    // (the Anthropic API default), while the capture rig uses "max" because
    // only "max" reliably elicits a thinking block to capture. Unlike the
    // model sets above, which must stay equal, these two are an intentional
    // unequal pair. Co-locating both values with that rationale means a change
    // to either trips here, so it is made as a conscious decision rather than
    // silent drift.
    expect(ADAPTIVE_THINKING_EFFORT).toBe("high");
    expect(ADAPTIVE_THINKING_CAPTURE_EFFORT).toBe("max");
  });
});
