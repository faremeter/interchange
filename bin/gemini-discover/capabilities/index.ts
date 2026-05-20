import { capability as audioInput } from "./audio-input.ts";
import { capability as codeExecution } from "./code-execution.ts";
import { capability as filesApi } from "./files-api.ts";
import { capability as functionCallingMultiTurn } from "./function-calling-multi-turn.ts";
import { capability as functionCallingThinking } from "./function-calling-thinking.ts";
import { capability as googleSearchGrounding } from "./google-search-grounding.ts";
import { capability as imageInput } from "./image-input.ts";
import { capability as imageOutput } from "./image-output.ts";
import { capability as pdfInput } from "./pdf-input.ts";
import { capability as textNonStreaming } from "./text-non-streaming.ts";
import { capability as textStreaming } from "./text-streaming.ts";
import { capability as videoInput } from "./video-input.ts";

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
  [functionCallingThinking.name]: functionCallingThinking,
  [imageInput.name]: imageInput,
  [imageOutput.name]: imageOutput,
  [audioInput.name]: audioInput,
  [videoInput.name]: videoInput,
  [pdfInput.name]: pdfInput,
  [codeExecution.name]: codeExecution,
  [googleSearchGrounding.name]: googleSearchGrounding,
  [filesApi.name]: filesApi,
};
