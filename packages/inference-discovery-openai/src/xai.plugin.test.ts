import { describe, test, expect } from "bun:test";
import { INTENTS, SUPPORT_MATRIX } from "@intx/inference-discovery/catalog";
import { createXaiPlugin } from "./index";

const TEST_API_KEY = "test-key";

function makePlugin() {
  return createXaiPlugin({ apiKey: TEST_API_KEY });
}

describe("createXaiPlugin", () => {
  test("declares provider name, model, and redaction lists", () => {
    const plugin = makePlugin();
    expect(plugin.name).toBe("xai");
    expect([...plugin.models]).toEqual([
      "grok-4.20-0309-non-reasoning",
      "grok-4.20-0309-reasoning",
      "grok-4.3",
      "grok-4.5",
      "grok-4.6",
      "grok-build-0.1",
    ]);
    expect(plugin.redactRequestHeaders).toEqual(["authorization"]);
    expect(plugin.redactResponseHeaders).toEqual([
      "set-cookie",
      "x-request-id",
    ]);
  });

  test("every xai model in the support matrix is advertised", () => {
    // XAI_MODELS (advertised) and the support-matrix xai rows are edited
    // separately. A matrix model absent from the advertised roster is one the
    // plugin never surfaces, so this catches that drift between the two lists.
    const advertised = new Set(makePlugin().models);
    const matrixModels = new Set(
      SUPPORT_MATRIX.filter((entry) => entry.provider === "xai").map(
        (entry) => entry.model,
      ),
    );
    const unadvertised = [...matrixModels].filter(
      (model) => !advertised.has(model),
    );
    expect(unadvertised).toEqual([]);
  });

  test("buildAuthHeaders attaches a Bearer token", () => {
    expect(makePlugin().buildAuthHeaders()).toEqual({
      Authorization: "Bearer test-key",
    });
  });

  test("targets the first-party api.x.ai chat completions endpoint", () => {
    const plugin = makePlugin();
    const iter = plugin.iterateCaptureSteps({
      model: "grok-4.5",
      capability: "plain-text",
      intent: INTENTS["plain-text"],
    });
    const first = iter.next();
    if (first.done) throw new Error("expected a capture step");
    expect(first.value.url).toBe("https://api.x.ai/v1/chat/completions");
  });
});
