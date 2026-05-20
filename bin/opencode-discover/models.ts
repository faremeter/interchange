import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import {
  buildAuthHeaders,
  buildChatCompletionsRequest,
  buildChatCompletionsURL,
  detectFunctionCallingFromJson,
  detectReasoningFromJson,
  detectReasoningFromSseText,
  type ChatMessage,
  type ProbeCapabilityFlags,
  type ProbeResult,
  type ProbeStepRecord,
} from "./capture.ts";

export type OpenCodeModel = {
  id: string;
  vendor: string;
  capabilities: ProbeCapabilityFlags;
};

/**
 * Capability flags are derived empirically by the probe step, which
 * lives in `probeModel` later in this file (invoked from
 * `bin/opencode-discover.ts` when `--probe` is passed). Update this
 * table when re-running probes; the source of truth for downstream
 * consumers is whatever lands in this file at the end of the L1
 * task.
 *
 * Manual overrides on top of the auto-detected flags:
 *
 * - `glm-5.1` vision is set to `false` even though the model's
 *   probe response returned HTTP 200. The endpoint accepts the
 *   `image_url` content part but the model itself refuses the
 *   request ("Please provide an image..."). The auto-detector
 *   cannot judge semantic refusal from a successful HTTP status,
 *   so the flag is overridden to `false` to keep downstream
 *   batches from attempting vision captures that would produce
 *   refusal text rather than real image-description content.
 */
export const models: OpenCodeModel[] = [
  {
    id: "kimi-k2.6",
    vendor: "moonshot",
    capabilities: {
      text: true,
      functionCalling: true,
      reasoning: true,
      vision: true,
    },
  },
  {
    id: "glm-5.1",
    vendor: "zhipuai",
    capabilities: {
      text: true,
      functionCalling: true,
      reasoning: true,
      vision: false,
    },
  },
  {
    id: "deepseek-v4-pro",
    vendor: "deepseek",
    capabilities: {
      text: true,
      functionCalling: true,
      reasoning: true,
      vision: false,
    },
  },
  {
    id: "qwen3.6-plus",
    vendor: "alibaba",
    capabilities: {
      text: true,
      functionCalling: true,
      reasoning: true,
      vision: true,
    },
  },
  {
    id: "mimo-v2-omni",
    vendor: "xiaomi",
    capabilities: {
      text: true,
      functionCalling: true,
      reasoning: true,
      vision: true,
    },
  },
];

const VISION_ASSET_URL = new URL(
  "../gemini-discover/assets/sample.jpg",
  import.meta.url,
);

const VISION_KEYWORDS = [
  "vision",
  "image",
  "images",
  "multimodal",
  "modality",
  "image_url",
];

async function readVisionDataUri(): Promise<string> {
  const bytes = await readFile(fileURLToPath(VISION_ASSET_URL));
  return `data:image/jpeg;base64,${bytes.toString("base64")}`;
}

type ProbeFetchArgs = {
  baseUrl: string;
  apiKey: string;
  body: ReturnType<typeof buildChatCompletionsRequest>;
};

async function fetchNonStreaming(
  args: ProbeFetchArgs,
): Promise<ProbeStepRecord> {
  const url = buildChatCompletionsURL(args.baseUrl);
  const response = await fetch(url, {
    method: "POST",
    headers: buildAuthHeaders(args.apiKey),
    body: JSON.stringify(args.body),
  });
  const text = await response.text();
  let responseBody: unknown;
  let errorMessage: string | undefined;
  try {
    responseBody = JSON.parse(text);
  } catch (cause) {
    responseBody = text;
    errorMessage =
      cause instanceof Error ? cause.message : "unknown JSON parse error";
  }
  const record: ProbeStepRecord = {
    request: args.body,
    httpStatus: response.status,
    httpStatusText: response.statusText,
    responseBody,
  };
  if (errorMessage !== undefined) {
    record.errorMessage = errorMessage;
  }
  return record;
}

async function fetchStreaming(args: ProbeFetchArgs): Promise<ProbeStepRecord> {
  const url = buildChatCompletionsURL(args.baseUrl);
  const response = await fetch(url, {
    method: "POST",
    headers: buildAuthHeaders(args.apiKey),
    body: JSON.stringify(args.body),
  });
  if (!response.ok) {
    const text = await response.text();
    return {
      request: args.body,
      httpStatus: response.status,
      httpStatusText: response.statusText,
      responseBody: text,
    };
  }
  if (!response.body) {
    throw new Error("Streaming probe: response had no body");
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let acc = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) acc += decoder.decode(value, { stream: true });
  }
  acc += decoder.decode();
  return {
    request: args.body,
    httpStatus: response.status,
    httpStatusText: response.statusText,
    responseBody: null,
    responseSseText: acc,
  };
}

const TEXT_PROMPT: ChatMessage[] = [
  { role: "user", content: "Reply with the single word: ready." },
];

const REASONING_PROMPT: ChatMessage[] = [
  {
    role: "user",
    content:
      "A bat and a ball cost $1.10 together. The bat costs $1.00 more than the ball. How much does the ball cost? Explain your reasoning step by step before stating the final answer.",
  },
];

const FUNCTION_CALL_PROMPT: ChatMessage[] = [
  {
    role: "user",
    content:
      "What is the weather in Boston, MA? Use the provided tool to look it up.",
  },
];

const WEATHER_TOOL = {
  type: "function",
  function: {
    name: "getCurrentWeather",
    description: "Get the current weather conditions for a given city.",
    parameters: {
      type: "object",
      properties: {
        location: {
          type: "string",
          description: "The city and optional state, e.g. 'Boston, MA'.",
        },
      },
      required: ["location"],
    },
  },
};

export type ProbeModelArgs = {
  baseUrl: string;
  apiKey: string;
  model: string;
};

export async function probeModel(args: ProbeModelArgs): Promise<ProbeResult> {
  const visionDataUri = await readVisionDataUri();
  const visionMessages: ChatMessage[] = [
    {
      role: "user",
      content: [
        { type: "text", text: "Describe the picture in one short sentence." },
        { type: "image_url", image_url: { url: visionDataUri } },
      ],
    },
  ];

  const textNonStreaming = await fetchNonStreaming({
    baseUrl: args.baseUrl,
    apiKey: args.apiKey,
    body: buildChatCompletionsRequest({
      model: args.model,
      messages: TEXT_PROMPT,
    }),
  });

  const textStreaming = await fetchStreaming({
    baseUrl: args.baseUrl,
    apiKey: args.apiKey,
    body: buildChatCompletionsRequest({
      model: args.model,
      messages: TEXT_PROMPT,
      stream: true,
    }),
  });

  const functionCalling = await fetchNonStreaming({
    baseUrl: args.baseUrl,
    apiKey: args.apiKey,
    body: buildChatCompletionsRequest({
      model: args.model,
      messages: FUNCTION_CALL_PROMPT,
      overrides: { tools: [WEATHER_TOOL] },
    }),
  });

  const reasoningNonStreaming = await fetchNonStreaming({
    baseUrl: args.baseUrl,
    apiKey: args.apiKey,
    body: buildChatCompletionsRequest({
      model: args.model,
      messages: REASONING_PROMPT,
    }),
  });

  const reasoningStreaming = await fetchStreaming({
    baseUrl: args.baseUrl,
    apiKey: args.apiKey,
    body: buildChatCompletionsRequest({
      model: args.model,
      messages: REASONING_PROMPT,
      stream: true,
    }),
  });

  const vision = await fetchNonStreaming({
    baseUrl: args.baseUrl,
    apiKey: args.apiKey,
    body: buildChatCompletionsRequest({
      model: args.model,
      messages: visionMessages,
    }),
  });

  const text =
    textNonStreaming.httpStatus >= 200 && textNonStreaming.httpStatus < 300;
  const fcEvidence = detectFunctionCallingFromJson(
    functionCalling.responseBody,
  );
  const functionCalling2xx =
    functionCalling.httpStatus >= 200 && functionCalling.httpStatus < 300;

  const reasoningJsonEvidence = detectReasoningFromJson(
    reasoningNonStreaming.responseBody,
  );
  const reasoningSseEvidence = reasoningStreaming.responseSseText
    ? detectReasoningFromSseText(reasoningStreaming.responseSseText)
    : null;
  const reasoningEvidence = reasoningJsonEvidence ?? reasoningSseEvidence;

  const visionFlag = determineVisionFlag(vision);

  const flags: ProbeCapabilityFlags = {
    text,
    functionCalling: functionCalling2xx && fcEvidence !== null,
    reasoning: reasoningEvidence !== null,
    vision: visionFlag,
  };

  return {
    model: args.model,
    flags,
    reasoningEvidence,
    textNonStreaming,
    textStreaming,
    functionCalling,
    reasoningNonStreaming,
    reasoningStreaming,
    vision,
  };
}

function determineVisionFlag(record: ProbeStepRecord): boolean {
  if (record.httpStatus >= 200 && record.httpStatus < 300) {
    const content = lookupContent(record.responseBody);
    return typeof content === "string" && content.trim().length > 0;
  }
  if (record.httpStatus >= 400 && record.httpStatus < 500) {
    const bodyText =
      typeof record.responseBody === "string"
        ? record.responseBody
        : JSON.stringify(record.responseBody);
    const lower = bodyText.toLowerCase();
    for (const keyword of VISION_KEYWORDS) {
      if (lower.includes(keyword)) return false;
    }
    return false;
  }
  return false;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function lookupContent(body: unknown): unknown {
  if (!isRecord(body)) return undefined;
  const choices = body.choices;
  if (!Array.isArray(choices) || choices.length === 0) return undefined;
  const first = choices[0];
  if (!isRecord(first)) return undefined;
  const message = first.message;
  if (!isRecord(message)) return undefined;
  return message.content;
}
