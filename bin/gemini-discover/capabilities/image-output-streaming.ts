import { runStreamingCapture } from "../capture.ts";
import type { Capability } from "./index.ts";

const NAME = "image-output-streaming";
const MODEL = "gemini-2.5-flash-image";
const ENDPOINT = "streamGenerateContent";

const REQUEST_BODY = {
  contents: [
    {
      role: "user",
      parts: [
        {
          text: "Generate a small illustration of a red apple on a white background.",
        },
      ],
    },
  ],
  generationConfig: {
    responseModalities: ["TEXT", "IMAGE"],
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
