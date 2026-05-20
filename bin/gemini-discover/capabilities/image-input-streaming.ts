import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { runStreamingCapture } from "../capture.ts";
import type { Capability } from "./index.ts";

const NAME = "image-input-streaming";
const MODEL = "gemini-2.5-flash";
const ENDPOINT = "streamGenerateContent";
const MIME_TYPE = "image/jpeg";

const ASSET_URL = new URL("../assets/sample.jpg", import.meta.url);

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
              text: "Describe the picture in one short sentence.",
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

    await runStreamingCapture({
      capability: NAME,
      model: MODEL,
      endpoint: ENDPOINT,
      body,
      apiKey,
      scriptVersion,
    });
  },
};
