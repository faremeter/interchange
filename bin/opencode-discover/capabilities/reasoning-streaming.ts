import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { runStreamingCapture } from "../../gemini-discover/capture.ts";
import {
  OPENCODE_REDACT_HEADERS,
  buildAuthHeaders,
  buildChatCompletionsRequest,
  buildChatCompletionsURL,
} from "../capture.ts";
import { models } from "../models.ts";
import { REASONING_PROMPT } from "./reasoning-non-streaming.ts";
import type { Capability } from "./index.ts";

const NAME = "reasoning-streaming";
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
    if (!entry.capabilities.reasoning) {
      return;
    }

    const body = buildChatCompletionsRequest({
      model,
      messages: [{ role: "user", content: REASONING_PROMPT }],
      stream: true,
    });

    await runStreamingCapture({
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
