import {
  GEMINI_BASE,
  GEMINI_REDACT_HEADERS,
  buildGeminiHeaders,
  runNonStreamingCapture,
} from "../capture.ts";
import type { Capability } from "./index.ts";

const NAME = "code-execution";
const MODEL = "gemini-2.5-flash";
const ENDPOINT = "generateContent";

const REQUEST_BODY = {
  contents: [
    {
      role: "user",
      parts: [
        {
          text: "Use Python to compute the 20th Fibonacci number (starting from F(0)=0, F(1)=1) and then state the result.",
        },
      ],
    },
  ],
  tools: [{ codeExecution: {} }],
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
