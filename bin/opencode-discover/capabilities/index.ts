import { capability as functionCallingMultiTurn } from "./function-calling-multi-turn.ts";
import { capability as functionCallingSingle } from "./function-calling-single.ts";
import { capability as reasoningNonStreaming } from "./reasoning-non-streaming.ts";
import { capability as reasoningStreaming } from "./reasoning-streaming.ts";
import { capability as textNonStreaming } from "./text-non-streaming.ts";
import { capability as textStreaming } from "./text-streaming.ts";
import { capability as visionInput } from "./vision-input.ts";

export type CapabilityBuildArgs = {
  apiKey: string;
  baseUrl: string;
  model: string;
  scriptVersion: string;
};

export type Capability = {
  name: string;
  endpoint: string;
  build: (args: CapabilityBuildArgs) => Promise<void>;
};

export const capabilities: Record<string, Capability> = {
  [textNonStreaming.name]: textNonStreaming,
  [textStreaming.name]: textStreaming,
  [functionCallingSingle.name]: functionCallingSingle,
  [functionCallingMultiTurn.name]: functionCallingMultiTurn,
  [reasoningNonStreaming.name]: reasoningNonStreaming,
  [reasoningStreaming.name]: reasoningStreaming,
  [visionInput.name]: visionInput,
};
