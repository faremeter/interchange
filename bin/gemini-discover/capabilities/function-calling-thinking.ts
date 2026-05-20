import { getLogger } from "@intx/log";

import {
  runNonStreamingStepCapture,
  writeMultiStepMetadata,
} from "../capture.ts";
import type { Capability } from "./index.ts";

const logger = getLogger(["gemini-discover", "function-calling-thinking"]);

const NAME = "function-calling-thinking";
const MODEL = "gemini-2.5-flash";
const ENDPOINT = "generateContent";
const THINKING_BUDGET = 1024;

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
      text: "Think carefully, then use the getCurrentWeather tool to look up the current weather in Boston, MA.",
    },
  ],
};

const FUNCTION_RESULT = {
  location: "Boston, MA",
  temperatureF: 62,
  conditions: "partly cloudy",
  windMph: 8,
};

import { type } from "arktype";

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

const GenerateContentResponse = type({
  "candidates?": Candidate.array(),
});

type ResponsePart = typeof ResponsePart.infer;

function isFunctionCallPart(
  part: ResponsePart,
): part is typeof FunctionCallPart.infer {
  return !(FunctionCallPart(part) instanceof type.errors);
}

function countThoughtSignatures(parts: ResponsePart[]): number {
  let n = 0;
  for (const part of parts) {
    if (typeof part.thoughtSignature === "string") n += 1;
  }
  return n;
}

function extractModelTurn(
  response: unknown,
  context: string,
): { role: string; parts: ResponsePart[]; functionCallName: string } {
  const parsed = GenerateContentResponse(response);
  if (parsed instanceof type.errors) {
    throw new Error(`${context}: malformed response: ${parsed.summary}`);
  }
  const candidate = parsed.candidates?.[0];
  if (!candidate?.content) {
    throw new Error(`${context}: response has no candidates[0].content`);
  }
  const role = candidate.content.role ?? "model";
  const parts = candidate.content.parts ?? [];
  const fnCallPart = parts.find(isFunctionCallPart);
  if (!fnCallPart) {
    throw new Error(
      `${context}: model did not return a functionCall part; got ${JSON.stringify(parts)}`,
    );
  }
  return {
    role,
    parts,
    functionCallName: fnCallPart.functionCall.name,
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
        thinkingConfig: {
          thinkingBudget: THINKING_BUDGET,
          includeThoughts: true,
        },
      },
    };

    const turn1 = await runNonStreamingStepCapture({
      capability: NAME,
      stepName: "turn-1",
      model: MODEL,
      endpoint: ENDPOINT,
      body: turn1Body,
      apiKey,
    });

    const modelTurn = extractModelTurn(turn1.responseJson, `${NAME} turn-1`);
    if (modelTurn.functionCallName !== FUNCTION_NAME) {
      throw new Error(
        `${NAME} turn-1: expected functionCall name=${FUNCTION_NAME}, got ${modelTurn.functionCallName}`,
      );
    }
    const sigCount = countThoughtSignatures(modelTurn.parts);
    if (sigCount === 0) {
      throw new Error(
        `${NAME} turn-1: response contained no thoughtSignature; thinking did not engage`,
      );
    }
    logger.info`turn-1 returned ${String(sigCount)} thoughtSignature(s) and functionCall name=${modelTurn.functionCallName}`;

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
        thinkingConfig: {
          thinkingBudget: THINKING_BUDGET,
          includeThoughts: true,
        },
      },
    };

    await runNonStreamingStepCapture({
      capability: NAME,
      stepName: "turn-2",
      model: MODEL,
      endpoint: ENDPOINT,
      body: turn2Body,
      apiKey,
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
