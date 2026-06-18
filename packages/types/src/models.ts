import { type } from "arktype";

import { Capability } from "./catalog";

export const ModelInfo = type({
  id: "string",
  providerId: "string",
  name: "string",
  "description?": "string | null",
  "capabilities?": Capability.array().describe(
    "Curated platform capability tags advertised for the model (for example vision, tool-use, or long-context).",
  ),
  "pricing?": {
    "input?": type("string").describe(
      "Cost per input (prompt) token, as a decimal string in the provider's billing units.",
    ),
    "output?": type("string").describe(
      "Cost per output (completion) token, as a decimal string in the provider's billing units.",
    ),
    "cacheRead?": type("string").describe(
      "Cost per token read from the provider's prompt cache, as a decimal string. Typically lower than the input rate.",
    ),
    "cacheWrite?": type("string").describe(
      "Cost per token written to the provider's prompt cache, as a decimal string.",
    ),
    "thinking?": type("string").describe(
      "Cost per thinking (reasoning) token, as a decimal string.",
    ),
    "perRequest?": type("string").describe(
      "Flat fee charged per request, as a decimal string.",
    ),
    "perImage?": type("string").describe(
      "Fee charged per image, as a decimal string.",
    ),
    "perAudio?": type("string").describe(
      "Fee charged per audio unit, as a decimal string.",
    ),
  },
  "limits?": {
    "context?": type("number").describe(
      "Maximum combined input plus output tokens the model accepts in a single request (the context window).",
    ),
    "output?": type("number").describe(
      "Maximum number of output tokens the model can produce in a single response.",
    ),
  },
});
