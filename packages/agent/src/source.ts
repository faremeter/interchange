// Inference source registry.
//
// The agent accepts an array of pre-configured sources plus a
// `defaultSource` id; the source whose id matches becomes the active
// source -- the same object reference the reactor reads lazily at each
// inference call. `setSource` mutates that shared object in place so
// the next call observes the new credentials/model; in-flight calls
// keep the values they read at start-of-call.

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
   * The mutable active source, held by reference in the reactor;
   * mutating it is what swaps the source for subsequent calls.
   */
  readonly active: InferenceSource;
  /** Replace the active source's fields in place. */
  setSource(source: InferenceSource): void;
  /**
   * Replace the whole ordered list and activate `defaultSource`.
   */
  setSources(sources: InferenceSource[], defaultSource: string): void;
  /**
   * Fail over to the next source in priority order, in place. Returns
   * false when the active source is already last. Only the registry
   * mutates which source is active, so the single-active-source
   * invariant the reactor relies on holds.
   */
  failOverToNextSource(): boolean;
  /**
   * Reset the active source to the most-preferred one, in place. The
   * reactor calls this at the start of each inference cycle so a
   * failover never permanently demotes the agent.
   */
  resetToPreferredSource(): void;
};

function validateSources(sources: InferenceSource[]): InferenceSource[] {
  if (sources.length === 0) {
    throw new InvalidInferenceSourceError("sources[] must be non-empty");
  }
  const validated: InferenceSource[] = [];
  const seenIds = new Set<string>();
  for (const [i, raw] of sources.entries()) {
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
  return validated;
}

export function createSourceRegistry(opts: {
  sources: InferenceSource[];
  defaultSource: string;
}): SourceRegistry {
  // The ordered list, default index, and active cursor are private to
  // the registry; only it mutates which source is active.
  let list = validateSources(opts.sources);
  let defaultIndex = indexOfDefault(list, opts.defaultSource);
  let activeIndex = defaultIndex;

  const active: InferenceSource = { ...sourceAt(list, activeIndex) };

  function setSource(source: InferenceSource): void {
    const parsed = InferenceSourceValidator(source);
    if (parsed instanceof type.errors) {
      throw new InvalidInferenceSourceError(parsed.summary);
    }
    applyInferenceSourceFields(active, parsed);
    // A hot-swap is an explicit override of the active source, possibly
    // to one not in the list at all. Park the cursor at the default so
    // the next per-cycle resetToPreferredSource is a no-op and the
    // override survives.
    activeIndex = defaultIndex;
  }

  function setSources(sources: InferenceSource[], defaultSource: string): void {
    const validated = validateSources(sources);
    const index = indexOfDefault(validated, defaultSource);
    list = validated;
    defaultIndex = index;
    activeIndex = index;
    applyInferenceSourceFields(active, sourceAt(list, activeIndex));
  }

  function failOverToNextSource(): boolean {
    if (activeIndex >= list.length - 1) return false;
    activeIndex += 1;
    applyInferenceSourceFields(active, sourceAt(list, activeIndex));
    return true;
  }

  function resetToPreferredSource(): void {
    // Only undo a failover that actually moved the cursor; a
    // setSource hot-swap override must survive the per-cycle reset.
    if (activeIndex === defaultIndex) return;
    activeIndex = defaultIndex;
    applyInferenceSourceFields(active, sourceAt(list, activeIndex));
  }

  return {
    active,
    setSource,
    setSources,
    failOverToNextSource,
    resetToPreferredSource,
  };
}

function indexOfDefault(
  list: InferenceSource[],
  defaultSource: string,
): number {
  const match = list.find((s) => s.id === defaultSource);
  if (match === undefined) {
    throw new SourceNotFoundError(defaultSource);
  }
  return list.indexOf(match);
}

function sourceAt(list: InferenceSource[], index: number): InferenceSource {
  const source = list[index];
  if (source === undefined) {
    // Unreachable: callers only pass an in-range index. Satisfies
    // noUncheckedIndexedAccess and fails loud if the invariant breaks.
    throw new InvalidInferenceSourceError(
      `no source at index ${String(index)}`,
    );
  }
  return source;
}
