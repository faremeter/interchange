// Inference source registry.
//
// The agent accepts an array of pre-configured inference sources and a
// `defaultSource` id at construction. The source whose `id` matches
// `defaultSource` becomes the active source — the same object reference
// is what the reactor's assembly holds and reads lazily at each
// inference call.
//
// `setSource` mutates that shared object in place so the next inference
// call observes the new credentials, model, and bound defaults. In-flight
// calls keep using the values they read at start-of-call (the reactor
// does not refetch mid-stream); the swap is therefore safe with respect
// to torn state.

import { type } from "arktype";

import {
  InferenceSource as InferenceSourceValidator,
  applyInferenceSourceFields,
  type InferenceSource,
} from "@intx/types/runtime";

export class InvalidInferenceSourceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidInferenceSourceError";
  }
}

export class SourceNotFoundError extends Error {
  readonly id: string;

  constructor(id: string) {
    super(`no source in sources[] has id ${id}`);
    this.name = "SourceNotFoundError";
    this.id = id;
  }
}

export type SourceRegistry = {
  /**
   * The mutable active source. The same object reference is held by the
   * reactor; mutating it through `setSource` is what swaps the source for
   * subsequent inference calls.
   */
  readonly active: InferenceSource;
  /** Replace the active source's fields in place. */
  setSource(source: InferenceSource): void;
};

export function createSourceRegistry(opts: {
  sources: InferenceSource[];
  defaultSource: string;
}): SourceRegistry {
  if (opts.sources.length === 0) {
    throw new InvalidInferenceSourceError("sources[] must be non-empty");
  }

  const validated: InferenceSource[] = [];
  const seenIds = new Set<string>();
  for (const [i, raw] of opts.sources.entries()) {
    const parsed = InferenceSourceValidator(raw);
    if (parsed instanceof type.errors) {
      throw new InvalidInferenceSourceError(
        `sources[${String(i)}]: ${parsed.summary}`,
      );
    }
    if (seenIds.has(parsed.id)) {
      throw new InvalidInferenceSourceError(
        `sources[${String(i)}]: duplicate id ${parsed.id}`,
      );
    }
    seenIds.add(parsed.id);
    validated.push(parsed);
  }

  const initial = validated.find((s) => s.id === opts.defaultSource);
  if (initial === undefined) {
    throw new SourceNotFoundError(opts.defaultSource);
  }

  const active: InferenceSource = { ...initial };

  function setSource(source: InferenceSource): void {
    const parsed = InferenceSourceValidator(source);
    if (parsed instanceof type.errors) {
      throw new InvalidInferenceSourceError(parsed.summary);
    }
    applyInferenceSourceFields(active, parsed);
  }

  return { active, setSource };
}
