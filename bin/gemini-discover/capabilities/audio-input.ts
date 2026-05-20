import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { runNonStreamingCapture } from "../capture.ts";
import type { Capability } from "./index.ts";

const NAME = "audio-input";
const MODEL = "gemini-2.5-flash";
const ENDPOINT = "generateContent";
const MIME_TYPE = "audio/wav";

const ASSET_URL = new URL("../assets/sample.wav", import.meta.url);

export const capability: Capability = {
  name: NAME,
  model: MODEL,
  endpoint: ENDPOINT,
  build: async ({ apiKey, scriptVersion }) => {
    const bytes = await readFile(fileURLToPath(ASSET_URL));
    const data = bytes.toString("base64");

    const body = {
      contents: [
        {
          role: "user",
          parts: [
            {
              text: "Transcribe the spoken words in this audio clip.",
            },
            {
              inlineData: {
                mimeType: MIME_TYPE,
                data,
              },
            },
          ],
        },
      ],
    };

    await runNonStreamingCapture({
      capability: NAME,
      model: MODEL,
      endpoint: ENDPOINT,
      body,
      apiKey,
      scriptVersion,
    });
  },
};
