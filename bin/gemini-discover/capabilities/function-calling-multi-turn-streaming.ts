import { getLogger } from "@intx/log";
import { type } from "arktype";

import {
  GEMINI_BASE,
  GEMINI_REDACT_HEADERS,
  buildGeminiHeaders,
  runStreamingStepCapture,
  writeMultiStepMetadata,
} from "../capture.ts";
import type { Capability } from "./index.ts";

const logger = getLogger([
  "gemini-discover",
  "function-calling-multi-turn-streaming",
]);

const NAME = "function-calling-multi-turn-streaming";
const MODEL = "gemini-2.5-flash";
const ENDPOINT = "streamGenerateContent";

const FUNCTION_NAME = "getCurrentWeather";

const TOOLS = [
  {
    functionDeclarations: [
      {
        name: FUNCTION_NAME,
        description:
          "Get the current weather conditions for a given city. Use this whenever the user asks about weather.",
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
    ],
  },
];

const USER_TURN = {
  role: "user",
  parts: [
    {
      text: "What is the current weather in Boston, MA? Use the getCurrentWeather tool.",
    },
  ],
};

const FUNCTION_RESULT = {
  location: "Boston, MA",
  temperatureF: 62,
  conditions: "partly cloudy",
  windMph: 8,
};

const FunctionCallPart = type({
  functionCall: type({
    name: "string",
    "args?": "Record<string, unknown>",
  }),
});

const ResponsePart = type("Record<string, unknown>");

const Candidate = type({
  "content?": type({
    "role?": "string",
    "parts?": ResponsePart.array(),
  }),
});

const StreamEvent = type({
  "candidates?": Candidate.array(),
});

type ResponsePart = typeof ResponsePart.infer;

function isFunctionCallPart(
  part: ResponsePart,
): part is typeof FunctionCallPart.infer {
  return !(FunctionCallPart(part) instanceof type.errors);
}

function parseSseEvents(bytes: Uint8Array): unknown[] {
  const text = new TextDecoder("utf-8").decode(bytes);
  const events: unknown[] = [];
  for (const line of text.split(/\r?\n/)) {
    if (!line.startsWith("data: ")) continue;
    const payload = line.slice("data: ".length);
    if (payload.length === 0) continue;
    events.push(JSON.parse(payload));
  }
  return events;
}

function mergeStreamedModelTurn(
  bytes: Uint8Array,
  context: string,
): { role: string; parts: ResponsePart[]; functionCallName: string } {
  const events = parseSseEvents(bytes);
  if (events.length === 0) {
    throw new Error(`${context}: SSE stream had no data events`);
  }

  let role: string | null = null;
  const merged: ResponsePart[] = [];

  for (const event of events) {
    const parsed = StreamEvent(event);
    if (parsed instanceof type.errors) {
      throw new Error(
        `${context}: malformed SSE event: ${parsed.summary}: ${JSON.stringify(event)}`,
      );
    }
    const candidate = parsed.candidates?.[0];
    if (!candidate?.content) continue;
    if (candidate.content.role && role === null) {
      role = candidate.content.role;
    }
    for (const part of candidate.content.parts ?? []) {
      merged.push(part);
    }
  }

  const fnCall = merged.find(isFunctionCallPart);
  if (!fnCall) {
    throw new Error(
      `${context}: streamed turn did not contain a functionCall part; got ${JSON.stringify(merged)}`,
    );
  }

  return {
    role: role ?? "model",
    parts: merged,
    functionCallName: fnCall.functionCall.name,
  };
}

export const capability: Capability = {
  name: NAME,
  model: MODEL,
  endpoint: ENDPOINT,
  build: async ({ apiKey, scriptVersion }) => {
    const turn1Body = {
      contents: [USER_TURN],
      tools: TOOLS,
      toolConfig: {
        functionCallingConfig: {
          mode: "ANY",
          allowedFunctionNames: [FUNCTION_NAME],
        },
      },
      generationConfig: {
        thinkingConfig: { thinkingBudget: 0 },
      },
    };

    const turn1 = await runStreamingStepCapture({
      capability: NAME,
      stepName: "turn-1",
      model: MODEL,
      endpoint: ENDPOINT,
      url: `${GEMINI_BASE}/${MODEL}:${ENDPOINT}?alt=sse`,
      requestHeaders: buildGeminiHeaders(apiKey),
      redactHeaderNames: GEMINI_REDACT_HEADERS,
      body: turn1Body,
    });

    const modelTurn = mergeStreamedModelTurn(turn1.bytes, `${NAME} turn-1`);
    if (modelTurn.functionCallName !== FUNCTION_NAME) {
      throw new Error(
        `${NAME} turn-1: expected functionCall name=${FUNCTION_NAME}, got ${modelTurn.functionCallName}`,
      );
    }
    logger.info`turn-1 streamed functionCall name=${modelTurn.functionCallName}`;

    const turn2Body = {
      contents: [
        USER_TURN,
        {
          role: modelTurn.role,
          parts: modelTurn.parts,
        },
        {
          role: "user",
          parts: [
            {
              functionResponse: {
                name: modelTurn.functionCallName,
                response: FUNCTION_RESULT,
              },
            },
          ],
        },
      ],
      tools: TOOLS,
      generationConfig: {
        thinkingConfig: { thinkingBudget: 0 },
      },
    };

    await runStreamingStepCapture({
      capability: NAME,
      stepName: "turn-2",
      model: MODEL,
      endpoint: ENDPOINT,
      url: `${GEMINI_BASE}/${MODEL}:${ENDPOINT}?alt=sse`,
      requestHeaders: buildGeminiHeaders(apiKey),
      redactHeaderNames: GEMINI_REDACT_HEADERS,
      body: turn2Body,
    });

    await writeMultiStepMetadata({
      capability: NAME,
      model: MODEL,
      endpoint: ENDPOINT,
      scriptVersion,
      sequence: ["turn-1", "turn-2"],
    });
  },
};
