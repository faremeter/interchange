// Provider registry.
//
// The agent accepts an array of pre-configured providers and a defaultModel
// at construction. The provider whose `model` field matches `defaultModel`
// becomes the active config — the same object reference is what the
// reactor's assembly holds and reads lazily at each inference call.
//
// `setProvider` mutates that shared object in place so the next inference
// call observes the new credentials and model. In-flight calls keep using
// the values they read at start-of-call (the reactor does not refetch
// mid-stream); the swap is therefore safe with respect to torn state.

import { type } from "arktype";

import {
  ProviderConfig as ProviderConfigValidator,
  type ProviderConfig,
} from "@interchange/types/runtime";

export class InvalidProviderConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidProviderConfigError";
  }
}

export class ProviderNotFoundError extends Error {
  readonly model: string;

  constructor(model: string) {
    super(`no provider in providers[] has model ${model}`);
    this.name = "ProviderNotFoundError";
    this.model = model;
  }
}

export type ProviderRegistry = {
  /**
   * The mutable active provider config. The same object reference is held
   * by the reactor; mutating it through `setProvider` is what swaps the
   * provider for subsequent inference calls.
   */
  readonly active: ProviderConfig;
  /** Replace the active provider's fields in place. */
  setProvider(config: ProviderConfig): void;
};

export function createProviderRegistry(opts: {
  providers: ProviderConfig[];
  defaultModel: string;
}): ProviderRegistry {
  if (opts.providers.length === 0) {
    throw new InvalidProviderConfigError("providers[] must be non-empty");
  }

  const validated: ProviderConfig[] = [];
  for (const [i, raw] of opts.providers.entries()) {
    const parsed = ProviderConfigValidator(raw);
    if (parsed instanceof type.errors) {
      throw new InvalidProviderConfigError(
        `providers[${String(i)}]: ${parsed.summary}`,
      );
    }
    if (parsed.model === undefined) {
      throw new InvalidProviderConfigError(
        `providers[${String(i)}]: model is required when used in agent providers[]`,
      );
    }
    validated.push(parsed);
  }

  const initial = validated.find((p) => p.model === opts.defaultModel);
  if (initial === undefined) {
    throw new ProviderNotFoundError(opts.defaultModel);
  }

  const active: ProviderConfig = { ...initial };

  function setProvider(config: ProviderConfig): void {
    const parsed = ProviderConfigValidator(config);
    if (parsed instanceof type.errors) {
      throw new InvalidProviderConfigError(parsed.summary);
    }
    active.provider = parsed.provider;
    active.baseURL = parsed.baseURL;
    active.apiKey = parsed.apiKey;
    if (parsed.model !== undefined) {
      active.model = parsed.model;
    } else {
      delete active.model;
    }
  }

  return { active, setProvider };
}
