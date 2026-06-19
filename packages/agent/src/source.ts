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
   * reactor; mutating it (through `setSource`, `setSources`,
   * `failOverToNextSource`, or `resetToPreferredSource`) is what swaps the
   * source for subsequent inference calls.
   */
  readonly active: InferenceSource;
  /** Replace the active source's fields in place. */
  setSource(source: InferenceSource): void;
  /**
   * Replace the whole ordered list and position the active source at
   * `defaultSource`. Used when the control plane pushes a re-resolved
   * source list to a running agent.
   */
  setSources(sources: InferenceSource[], defaultSource: string): void;
  /**
   * Fail over the active source to the next entry in priority order, in
   * place. Returns false when the active source is already the last in the
   * list — there is no further failover target. The ordered list and the
   * cursor are private; only the registry mutates which source is active,
   * so the single-active-source invariant the reactor relies on holds.
   */
  failOverToNextSource(): boolean;
  /**
   * Reset the active source to the most-preferred (highest-priority) one, in
   * place. The reactor calls this at the start of each inference cycle so a
   * failover never permanently demotes the agent off its preferred source.
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
  // The ordered list, the default index, and the active cursor are private
  // to the registry. Only the registry mutates which source is active.
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
    // A hot-swap is an explicit override of the active source, possibly to a
    // source that is not in the list at all. Park the cursor at the default
    // so the next per-cycle resetToPreferredSource is a no-op and the
    // override survives — even when a failover had moved the cursor off the
    // default before the swap.
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
    // Only undo a failover that actually moved the cursor. When the active
    // source is already the preferred one, leave `active` untouched — a
    // caller may have hot-swapped it via setSource (e.g. a director rotating
    // the model), and that override must survive the per-cycle reset.
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
    // Unreachable: callers only ever pass an in-range index. The guard
    // satisfies noUncheckedIndexedAccess without a non-null assertion and
    // fails loud if that invariant is ever broken.
    throw new InvalidInferenceSourceError(
      `no source at index ${String(index)}`,
    );
  }
  return source;
}
