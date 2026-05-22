import type { Capability, CapabilityIntent } from "./catalog";

export interface CaptureStep {
  subdir: string | null;
  url: string;
  body: unknown;
}

export interface CapturedResponse {
  status: number;
  headers: Record<string, string>;
  parsed: unknown | null;
}

export interface IterateCaptureStepsOpts {
  model: string;
  capability: Capability;
  intent: CapabilityIntent;
}

export interface ProviderPlugin {
  name: string;
  models: readonly string[];
  redactRequestHeaders: readonly string[];
  redactResponseHeaders: readonly string[];
  buildAuthHeaders(): Record<string, string>;
  extractReasoningTrace?(parsed: unknown): unknown | null;
  iterateCaptureSteps(
    opts: IterateCaptureStepsOpts,
  ): Generator<CaptureStep, void, CapturedResponse>;
}
