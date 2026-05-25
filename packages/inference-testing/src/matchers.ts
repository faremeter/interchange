// Assertion matchers for `InferenceEvent[]` collected from a harness run.
//
// The matchers are plain functions returning chainable objects rather than
// `expect.extend(...)` registrations, so they work uniformly across bun:test
// and any runner that supports calling functions. Each terminal method
// throws a descriptive `Error` on failure — callers should let those bubble
// up through bun:test's normal failure machinery.

import type { ContentBlock, InferenceEvent } from "@intx/types/runtime";

/**
 * Partial expectation against an `InferenceEvent`. The `type` is required;
 * any other field is a structural sub-match: numbers / strings / booleans
 * compared by `Object.is`, objects compared recursively, arrays compared
 * element-wise.
 *
 * Object matching is partial — fields absent from the partial are ignored
 * on the actual value. Array matching is NOT partial: `partial` and
 * `actual` must have the same length, and elements are compared by
 * position. To assert against a single array element by index, name it
 * with the surrounding object structure rather than supplying a
 * partial-length array.
 */
export type EventPartial = {
  type: InferenceEvent["type"];
} & Partial<Record<string, unknown>>;

/**
 * Structural deep-match used by the matchers. Returns true iff every
 * property in `partial` is satisfied by `actual`.
 *
 * Arrays are compared element-wise: `partial` and `actual` must have the
 * same length, and each pair must match. Objects compare every key in
 * `partial` (extras in `actual` are ignored). Primitives use `Object.is`,
 * which gives NaN-aware equality.
 *
 * The matcher walks unknown shapes, so it tolerates `InferenceEvent`
 * variants without bespoke per-type handling.
 */
function deepMatchPartial(partial: unknown, actual: unknown): boolean {
  if (partial === actual) return true;
  if (typeof partial !== typeof actual) return false;
  if (partial === null || actual === null) return Object.is(partial, actual);
  if (Array.isArray(partial)) {
    if (!Array.isArray(actual)) return false;
    if (partial.length !== actual.length) return false;
    for (let i = 0; i < partial.length; i++) {
      if (!deepMatchPartial(partial[i], actual[i])) return false;
    }
    return true;
  }
  if (isRecord(partial)) {
    if (!isRecord(actual)) return false;
    for (const key of Object.keys(partial)) {
      const p = partial[key];
      if (!(key in actual)) return false;
      if (!deepMatchPartial(p, actual[key])) return false;
    }
    return true;
  }
  return Object.is(partial, actual);
}

/**
 * Format an event for assertion error messages. Strips long `partial`
 * blocks so failure output stays scannable.
 */
function formatEvent(evt: InferenceEvent): string {
  const trimmed: Record<string, unknown> = {
    type: evt.type,
    seq: evt.seq,
  };
  trimmed["data"] = stripPartial(evt.data);
  return JSON.stringify(trimmed);
}

function stripPartial(data: unknown): unknown {
  if (data === null || typeof data !== "object") return data;
  if (Array.isArray(data)) return data.map(stripPartial);
  if (!isRecord(data)) return data;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(data)) {
    if (k === "partial") {
      out[k] = "<partial>";
    } else {
      out[k] = stripPartial(v);
    }
  }
  return out;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Fluent assertion API returned by `expectEvents`. The wrapper carries the
 * events array; chained `to*` methods perform the assertion and return
 * `this` so further assertions can be chained.
 */
export type EventAssertion = {
  /**
   * Assert that the events array contains an ordered subsequence matching
   * every entry in `expected`. Gaps between matches are permitted, so a
   * sequence like `[start, done]` succeeds against an actual run that
   * carried `[start, text.delta, usage, done]`. Each expected entry is a
   * partial: `type` is required, every other field is structurally matched
   * against the actual event.
   *
   * Object fields inside an expected entry are matched partially (missing
   * keys on the partial are ignored). Array fields are matched
   * exact-length, element-by-position — they are NOT partial. To assert
   * against a single element of a longer array, write a partial that
   * names the array's surrounding object structure but skips the array
   * field entirely, or assert against a different event that exposes the
   * element directly.
   *
   * Throws on the first expected entry that cannot be matched at or after
   * the cursor position.
   */
  toMatchSequence(expected: readonly EventPartial[]): EventAssertion;
};

/**
 * Wrap a collected events array in an assertion API. The wrapper is the
 * minimum surface required by the spec: ordered sub-sequence matching with
 * gaps allowed. Additional matchers can be added on a need-to-match basis.
 *
 * The events themselves are immutable from the matcher's perspective; the
 * wrapper does not mutate them.
 */
export function expectEvents(
  events: readonly InferenceEvent[],
): EventAssertion {
  const assertion: EventAssertion = {
    toMatchSequence(expected) {
      let cursor = 0;
      for (let i = 0; i < expected.length; i++) {
        const want = expected[i];
        if (want === undefined) {
          throw new Error(
            `expectEvents.toMatchSequence: expected[${String(i)}] is undefined`,
          );
        }
        let found = -1;
        for (let j = cursor; j < events.length; j++) {
          const evt = events[j];
          if (evt === undefined) continue;
          if (evt.type !== want.type) continue;
          if (deepMatchPartial(want, evt)) {
            found = j;
            break;
          }
        }
        if (found < 0) {
          const seen = events.slice(cursor).map(formatEvent).join("\n  ");
          throw new Error(
            `expectEvents.toMatchSequence: no event matching ${JSON.stringify(want)} at or after index ${String(cursor)}\n  remaining events:\n  ${seen}`,
          );
        }
        cursor = found + 1;
      }
      return assertion;
    },
  };
  return assertion;
}

/**
 * Result entry from `expectToolCalls`. Mirrors the `inference.tool_call.end`
 * data shape so authors can write assertions in the same vocabulary the
 * harness emits.
 */
export type CollectedToolCall = {
  name: string;
  callId: string;
  arguments: Record<string, unknown>;
};

/**
 * Partial expectation against a completed tool call. `name` is required;
 * `arguments` is structurally matched.
 */
export type ToolCallPartial = {
  name: string;
  callId?: string;
  arguments?: Record<string, unknown>;
};

/** Fluent assertion API returned by `expectToolCalls`. */
export type ToolCallsAssertion = {
  /**
   * Assert that at least one collected tool call structurally matches
   * `expected`. Throws with the list of observed tool calls when no match
   * is found.
   */
  toInclude(expected: ToolCallPartial): ToolCallsAssertion;
};

/**
 * Collect every `inference.tool_call.end` event from `events` and wrap it in
 * an assertion API. Use the resulting `toInclude({ name, arguments })` to
 * assert presence; the matcher allows arbitrary other tool calls in the
 * same run.
 */
export function expectToolCalls(
  events: readonly InferenceEvent[],
): ToolCallsAssertion {
  const collected: CollectedToolCall[] = [];
  for (const evt of events) {
    if (evt.type !== "inference.tool_call.end") continue;
    collected.push({
      name: evt.data.name,
      callId: evt.data.callId,
      arguments: evt.data.arguments,
    });
  }

  const assertion: ToolCallsAssertion = {
    toInclude(expected) {
      for (const tc of collected) {
        if (tc.name !== expected.name) continue;
        if (expected.callId !== undefined && tc.callId !== expected.callId) {
          continue;
        }
        if (
          expected.arguments !== undefined &&
          !deepMatchPartial(expected.arguments, tc.arguments)
        ) {
          continue;
        }
        return assertion;
      }
      throw new Error(
        `expectToolCalls.toInclude: no tool call matching ${JSON.stringify(expected)}; observed:\n  ${
          collected.length === 0
            ? "<none>"
            : collected.map((c) => JSON.stringify(c)).join("\n  ")
        }`,
      );
    },
  };
  return assertion;
}

/**
 * Fluent assertion API returned by `expectToolCall(name).from(events)`.
 *
 * The two-step shape lets the caller name the tool of interest up-front and
 * then evaluate properties of its occurrences in the events array.
 */
export type SingleToolCallAssertion = {
  /**
   * Assert that the named tool was called exactly `n` times. Counts
   * `inference.tool_call.end` events matching the captured name.
   */
  toHaveBeenCalledTimes(n: number): SingleToolCallAssertion;
};

/**
 * Build a single-tool assertion bound to the supplied `name`. The returned
 * `from(events)` method materializes the assertion against a collected
 * events array.
 *
 * The two-step shape exists because the matcher needs to count occurrences
 * of a specific name; binding the name first matches the spec's
 * `expectToolCall(name).toHaveBeenCalledTimes(n)` reading and avoids
 * repeating the events argument when chaining multiple assertions.
 */
export function expectToolCall(name: string): {
  from(events: readonly InferenceEvent[]): SingleToolCallAssertion;
} {
  return {
    from(events) {
      const occurrences = events.filter(
        (evt) =>
          evt.type === "inference.tool_call.end" && evt.data.name === name,
      );
      const assertion: SingleToolCallAssertion = {
        toHaveBeenCalledTimes(n) {
          if (occurrences.length !== n) {
            throw new Error(
              `expectToolCall(${JSON.stringify(name)}).toHaveBeenCalledTimes(${String(n)}): observed ${String(occurrences.length)}`,
            );
          }
          return assertion;
        },
      };
      return assertion;
    },
  };
}

// ---------------------------------------------------------------------------
// Media block matchers
//
// Content blocks that carry a MediaSource (image, audio, video, document)
// can hold base64 payloads in the megabyte range — Anthropic's image-output
// captures show ~1MB blobs. A failing assertion that serializes the whole
// block leaves multi-MB of base64 in test logs, which makes debugging the
// failure dramatically worse than the failure itself.
//
// `expectMediaBlock` is the assertion entry point that formats failure
// messages with an elided representation: kind, mime, source discriminant,
// and decoded byte length — never the raw `data` field. Use this matcher
// instead of `expect(block).toEqual(...)` for any media block whose source
// kind might be `"base64"`.
// ---------------------------------------------------------------------------

export type MediaBlock = Extract<
  ContentBlock,
  { type: "image" | "audio" | "video" | "document" }
>;

export type ExpectMediaBlockOpts = {
  /** When supplied, asserts the block's source.kind matches before any chain. */
  source?: "base64" | "file-reference" | "url";
};

export type MediaBlockAssertion = {
  /** Assert the block's mimeType (sources record their own mimeType). */
  toHaveMimeType(expected: string): MediaBlockAssertion;
  /**
   * Assert the decoded payload byte count is at least `min`. Only valid on
   * base64 sources — throws on non-base64 sources (file-reference, url)
   * because their byte length is provider-side and not observable from
   * the block.
   */
  toHaveByteLengthAtLeast(min: number): MediaBlockAssertion;
  /**
   * Exact byte count. Use sparingly — provider re-encodes are common and
   * `toHaveByteLengthAtLeast` is usually the right assertion. Same
   * non-base64 caveat as `toHaveByteLengthAtLeast`.
   */
  toHaveByteLength(exact: number): MediaBlockAssertion;
};

/**
 * Decode the byte length of a base64 string without materializing the
 * decoded bytes. Each four base64 characters encode three bytes, less
 * trailing `=` padding.
 *
 * Validates structural invariants — length divisible by 4 and at most
 * two trailing padding chars — and throws on violation. A silent
 * fallback that returns a meaningless count (negative numbers for
 * stray `=` clusters) would let downstream assertions like
 * `toHaveByteLengthAtLeast(-2)` pass on garbage; surfacing the
 * malformed input loudly forces callers to feed real base64.
 */
function base64ByteLength(b64: string): number {
  if (b64.length % 4 !== 0) {
    throw new Error(
      `base64ByteLength: input length ${String(b64.length)} is not a multiple of 4; not a well-formed base64 string`,
    );
  }
  const padMatch = /=+$/.exec(b64);
  const pad = padMatch === null ? 0 : padMatch[0].length;
  if (pad > 2) {
    throw new Error(
      `base64ByteLength: input carries ${String(pad)} trailing padding chars; base64 permits at most 2`,
    );
  }
  return Math.floor((b64.length * 3) / 4) - pad;
}

function describeMediaBlock(block: MediaBlock): string {
  const src = block.source;
  switch (src.kind) {
    case "base64":
      return `<${block.type} mime=${src.mimeType} source=base64 bytes=${String(
        base64ByteLength(src.data),
      )}>`;
    case "file-reference":
      return `<${block.type} mime=${src.mimeType} source=file-reference reference=${src.reference}>`;
    case "url":
      return `<${block.type} mime=${src.mimeType} source=url url=${src.url}>`;
    default:
      src satisfies never;
      throw new Error(`unreachable: unknown MediaSource kind`);
  }
}

export function expectMediaBlock(
  block: MediaBlock,
  opts: ExpectMediaBlockOpts = {},
): MediaBlockAssertion {
  if (opts.source !== undefined && block.source.kind !== opts.source) {
    throw new Error(
      `expectMediaBlock: expected source=${opts.source}, got ${describeMediaBlock(block)}`,
    );
  }

  const assertion: MediaBlockAssertion = {
    toHaveMimeType(expected) {
      if (block.source.mimeType !== expected) {
        throw new Error(
          `expectMediaBlock.toHaveMimeType(${JSON.stringify(expected)}): got ${describeMediaBlock(block)}`,
        );
      }
      return assertion;
    },

    toHaveByteLengthAtLeast(min) {
      if (block.source.kind !== "base64") {
        throw new Error(
          `expectMediaBlock.toHaveByteLengthAtLeast: byte length is not observable on non-base64 sources; got ${describeMediaBlock(block)}`,
        );
      }
      const actual = base64ByteLength(block.source.data);
      if (actual < min) {
        throw new Error(
          `expectMediaBlock.toHaveByteLengthAtLeast(${String(min)}): got ${describeMediaBlock(block)} (actual ${String(actual)} bytes)`,
        );
      }
      return assertion;
    },

    toHaveByteLength(exact) {
      if (block.source.kind !== "base64") {
        throw new Error(
          `expectMediaBlock.toHaveByteLength: byte length is not observable on non-base64 sources; got ${describeMediaBlock(block)}`,
        );
      }
      const actual = base64ByteLength(block.source.data);
      if (actual !== exact) {
        throw new Error(
          `expectMediaBlock.toHaveByteLength(${String(exact)}): got ${describeMediaBlock(block)} (actual ${String(actual)} bytes)`,
        );
      }
      return assertion;
    },
  };
  return assertion;
}
