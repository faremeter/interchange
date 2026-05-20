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

const NAME = "reasoning-non-streaming";
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

export const REASONING_PROMPT =
  "A farmer has 17 sheep. All but 9 die. How many are left? Reason carefully before answering.";

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
