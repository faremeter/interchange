import {
  GEMINI_BASE,
  GEMINI_REDACT_HEADERS,
  buildGeminiHeaders,
  runNonStreamingCapture,
} from "../capture.ts";
import type { Capability } from "./index.ts";

const NAME = "google-search-grounding";
const MODEL = "gemini-2.5-flash";
const ENDPOINT = "generateContent";

const REQUEST_BODY = {
  contents: [
    {
      role: "user",
      parts: [
        {
          text: "Who won the 2025 Nobel Prize in Physics, and what was the cited contribution? Cite your sources.",
        },
      ],
    },
  ],
  tools: [{ googleSearch: {} }],
};

export const capability: Capability = {
  name: NAME,
  model: MODEL,
  endpoint: ENDPOINT,
  build: async ({ apiKey, scriptVersion }) => {
    await runNonStreamingCapture({
      capability: NAME,
      model: MODEL,
      endpoint: ENDPOINT,
      url: `${GEMINI_BASE}/${MODEL}:${ENDPOINT}`,
      requestHeaders: buildGeminiHeaders(apiKey),
      redactHeaderNames: GEMINI_REDACT_HEADERS,
      body: REQUEST_BODY,
      scriptVersion,
    });
  },
};
