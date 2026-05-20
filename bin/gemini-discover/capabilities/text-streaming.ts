import { runStreamingCapture } from "../capture.ts";
import type { Capability } from "./index.ts";

const NAME = "text-streaming";
const MODEL = "gemini-2.5-flash";
const ENDPOINT = "streamGenerateContent";

const REQUEST_BODY = {
  contents: [
    {
      role: "user",
      parts: [
        {
          text: "Write a 200-word description of how a sailboat works. Cover the hull, the keel, the sails, and steering. Use clear paragraphs.",
        },
      ],
    },
  ],
  generationConfig: {
    maxOutputTokens: 400,
    thinkingConfig: {
      thinkingBudget: 0,
    },
  },
};

export const capability: Capability = {
  name: NAME,
  model: MODEL,
  endpoint: ENDPOINT,
  build: async ({ apiKey, scriptVersion }) => {
    await runStreamingCapture({
      capability: NAME,
      model: MODEL,
      endpoint: ENDPOINT,
      body: REQUEST_BODY,
      apiKey,
      scriptVersion,
    });
  },
};
