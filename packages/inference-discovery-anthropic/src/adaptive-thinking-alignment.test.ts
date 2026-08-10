import { describe, test, expect } from "bun:test";
import { ADAPTIVE_THINKING_MODELS as RUNTIME_ADAPTIVE_MODELS } from "@intx/inference/providers";
import { ADAPTIVE_THINKING_MODELS as DISCOVERY_ADAPTIVE_MODELS } from "./request-body";

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
