import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { runNonStreamingCapture } from "../../gemini-discover/capture.ts";
import {
  OPENCODE_REDACT_HEADERS,
  buildAuthHeaders,
  buildChatCompletionsRequest,
  buildChatCompletionsURL,
} from "../capture.ts";
import { models } from "../models.ts";
import type { Capability } from "./index.ts";

const NAME = "vision-input";
const ENDPOINT = "chat/completions";

const HERE = fileURLToPath(new URL(".", import.meta.url));
const FIXTURE_ROOT = join(
  HERE,
  "..",
  "..",
  "..",
  "packages",
  "inference-testing",
  "wire",
  "opencode-zen",
);

const ASSET_URL = new URL(
  "../../gemini-discover/assets/sample.jpg",
  import.meta.url,
);

const PROMPT = "Describe the picture in one sentence.";

export const capability: Capability = {
  name: NAME,
  endpoint: ENDPOINT,
  build: async ({ apiKey, baseUrl, model, scriptVersion }) => {
    const entry = models.find((m) => m.id === model);
    if (!entry) {
      throw new Error(
        `Model "${model}" not present in opencode-discover registry`,
      );
    }
    if (!entry.capabilities.vision) {
      return;
    }

    const bytes = await readFile(fileURLToPath(ASSET_URL));
    const dataUri = `data:image/jpeg;base64,${bytes.toString("base64")}`;

    const body = buildChatCompletionsRequest({
      model,
      messages: [
        {
          role: "user",
          content: [
            { type: "text", text: PROMPT },
            { type: "image_url", image_url: { url: dataUri } },
          ],
        },
      ],
    });

    await runNonStreamingCapture({
      capability: NAME,
      model,
      endpoint: ENDPOINT,
      url: buildChatCompletionsURL(baseUrl),
      requestHeaders: buildAuthHeaders(apiKey),
      redactHeaderNames: OPENCODE_REDACT_HEADERS,
      body,
      scriptVersion,
      destinationOverride: join(FIXTURE_ROOT, model, NAME),
    });
  },
};
