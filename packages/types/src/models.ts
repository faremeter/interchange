import { type } from "arktype";

export const ModelInfo = type({
  id: "string",
  providerId: "string",
  name: "string",
  "description?": "string | null",
  "capabilities?": type("string[]").describe(
    "Capability tags advertised for the model (for example streaming, tool use, or vision support), as reported by the provider.",
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
