import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { runNonStreamingCapture } from "../../gemini-discover/capture.ts";
import {
  OPENCODE_REDACT_HEADERS,
  buildAuthHeaders,
  buildChatCompletionsRequest,
  buildChatCompletionsURL,
} from "../capture.ts";
import type { Capability } from "./index.ts";
import { TOOL_DEFINITION, USER_PROMPT } from "./function-calling-shared.ts";

const NAME = "function-calling-single";
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
    const body = buildChatCompletionsRequest({
      model,
      messages: [{ role: "user", content: USER_PROMPT }],
      overrides: { tools: [TOOL_DEFINITION] },
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
