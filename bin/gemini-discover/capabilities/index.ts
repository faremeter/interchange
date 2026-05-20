import { capability as audioInput } from "./audio-input.ts";
import { capability as audioInputStreaming } from "./audio-input-streaming.ts";
import { capability as codeExecution } from "./code-execution.ts";
import { capability as codeExecutionStreaming } from "./code-execution-streaming.ts";
import { capability as filesApi } from "./files-api.ts";
import { capability as filesApiStreaming } from "./files-api-streaming.ts";
import { capability as functionCallingMultiTurn } from "./function-calling-multi-turn.ts";
import { capability as functionCallingMultiTurnStreaming } from "./function-calling-multi-turn-streaming.ts";
import { capability as functionCallingThinking } from "./function-calling-thinking.ts";
import { capability as functionCallingThinkingStreaming } from "./function-calling-thinking-streaming.ts";
import { capability as googleSearchGrounding } from "./google-search-grounding.ts";
import { capability as googleSearchGroundingStreaming } from "./google-search-grounding-streaming.ts";
import { capability as imageInput } from "./image-input.ts";
import { capability as imageInputStreaming } from "./image-input-streaming.ts";
import { capability as imageOutput } from "./image-output.ts";
import { capability as imageOutputStreaming } from "./image-output-streaming.ts";
import { capability as pdfInput } from "./pdf-input.ts";
import { capability as pdfInputStreaming } from "./pdf-input-streaming.ts";
import { capability as textNonStreaming } from "./text-non-streaming.ts";
import { capability as textStreaming } from "./text-streaming.ts";
import { capability as videoInput } from "./video-input.ts";
import { capability as videoInputStreaming } from "./video-input-streaming.ts";

export type CapabilityBuildArgs = {
  apiKey: string;
  scriptVersion: string;
};

export type Capability = {
  name: string;
  model: string;
  endpoint: string;
  build: (args: CapabilityBuildArgs) => Promise<void>;
};

export const capabilities: Record<string, Capability> = {
  [textNonStreaming.name]: textNonStreaming,
  [textStreaming.name]: textStreaming,
  [functionCallingMultiTurn.name]: functionCallingMultiTurn,
  [functionCallingMultiTurnStreaming.name]: functionCallingMultiTurnStreaming,
  [functionCallingThinking.name]: functionCallingThinking,
  [functionCallingThinkingStreaming.name]: functionCallingThinkingStreaming,
  [imageInput.name]: imageInput,
  [imageInputStreaming.name]: imageInputStreaming,
  [imageOutput.name]: imageOutput,
  [imageOutputStreaming.name]: imageOutputStreaming,
  [audioInput.name]: audioInput,
  [audioInputStreaming.name]: audioInputStreaming,
  [videoInput.name]: videoInput,
  [videoInputStreaming.name]: videoInputStreaming,
  [pdfInput.name]: pdfInput,
  [pdfInputStreaming.name]: pdfInputStreaming,
  [codeExecution.name]: codeExecution,
  [codeExecutionStreaming.name]: codeExecutionStreaming,
  [googleSearchGrounding.name]: googleSearchGrounding,
  [googleSearchGroundingStreaming.name]: googleSearchGroundingStreaming,
  [filesApi.name]: filesApi,
  [filesApiStreaming.name]: filesApiStreaming,
};
