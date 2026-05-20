import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import {
  GEMINI_BASE,
  GEMINI_REDACT_HEADERS,
  buildGeminiHeaders,
  runStreamingCapture,
} from "../capture.ts";
import type { Capability } from "./index.ts";

const NAME = "pdf-input-streaming";
const MODEL = "gemini-2.5-flash";
const ENDPOINT = "streamGenerateContent";
const MIME_TYPE = "application/pdf";

const ASSET_URL = new URL("../assets/sample.pdf", import.meta.url);

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
              text: "Summarize this PDF in one short sentence.",
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
      url: `${GEMINI_BASE}/${MODEL}:${ENDPOINT}?alt=sse`,
      requestHeaders: buildGeminiHeaders(apiKey),
      redactHeaderNames: GEMINI_REDACT_HEADERS,
      body,
      scriptVersion,
    });
  },
};
