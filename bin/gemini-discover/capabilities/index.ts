import { capability as functionCallingMultiTurn } from "./function-calling-multi-turn.ts";
import { capability as functionCallingThinking } from "./function-calling-thinking.ts";
import { capability as textNonStreaming } from "./text-non-streaming.ts";
import { capability as textStreaming } from "./text-streaming.ts";

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
};
