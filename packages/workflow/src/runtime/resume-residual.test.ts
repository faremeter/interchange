import { describe, expect, test } from "bun:test";

import { resumeResidualOf, type StepKindLookup } from "./dag";

const def: StepKindLookup = {
  steps: {
    hold: { kind: "awaitSignal" },
    think: { kind: "step" },
    nap: { kind: "sleep" },
    items: { kind: "map" },
    bump: { kind: "action" },
  },
};

describe("resumeResidualOf", () => {
  test("continues a signal park and a sleep, and rejects a map park", () => {
    expect(resumeResidualOf(def, "hold", "awaiting-signal")).toBe("continue");
    expect(resumeResidualOf(def, "think", "awaiting-signal")).toBe("continue");
    expect(resumeResidualOf(def, "nap", "awaiting-timer")).toBe("continue");
    expect(resumeResidualOf(def, "items[0]", "awaiting-signal")).toBe(
      "unsupported",
    );
  });

  test("a crashed action or agent step is continuable and not a resume rejection", () => {
    expect(resumeResidualOf(def, "bump", "in-flight")).toBe(
      "crashed-invocation",
    );
    expect(resumeResidualOf(def, "think", "in-flight")).toBe(
      "crashed-invocation",
    );
    expect(resumeResidualOf(def, "items", "in-flight")).toBe("unsupported");
  });
});
