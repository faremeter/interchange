// Assertion matchers for `InferenceEvent[]` collected from a harness run.
//
// Plain functions returning chainable objects rather than
// `expect.extend(...)` registrations, so they work across bun:test and any
// runner that supports calling functions. Each terminal method throws a
// descriptive `Error` on failure.

import type { ContentBlock, InferenceEvent } from "@intx/types/runtime";

/**
 * Partial expectation against an `InferenceEvent`. `type` is required; any
 * other field is a structural sub-match: primitives by `Object.is`, objects
 * recursively, arrays element-wise.
 *
 * Objects match partially (fields absent from the partial are ignored);
 * arrays match exactly — same length, compared by position. To assert a
 * single array element, name it via the surrounding object structure.
 */
export type EventPartial = {
  type: InferenceEvent["type"];
} & Partial<Record<string, unknown>>;

/**
 * Structural deep-match used by the matchers. Every property in `partial`
 * must be satisfied by `actual`. Arrays match element-wise and
 * same-length; objects match every key in `partial` (extras in `actual`
 * are ignored); primitives use NaN-aware `Object.is`. Walks unknown
 * shapes, so `InferenceEvent` variants need no bespoke handling.
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
 * Fluent assertion API returned by `expectEvents`. Chained `to*` methods
 * perform the assertion and return `this` for further chaining.
 */
export type EventAssertion = {
  /**
   * Assert the events array contains an ordered subsequence matching
   * every entry in `expected`; gaps between matches are permitted. Each
   * entry is a partial: `type` required, other fields structurally
   * matched (objects partial, arrays exact-length by position). Throws on
   * the first expected entry that cannot be matched at or after the
   * cursor position.
   */
  toMatchSequence(expected: readonly EventPartial[]): EventAssertion;
};

/**
 * Wrap a collected events array in an assertion API: ordered sub-sequence
 * matching with gaps allowed. The events are never mutated.
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
 * data shape.
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
 * Collect every `inference.tool_call.end` event from `events` and wrap it
 * in an assertion API. `toInclude({ name, arguments })` asserts presence;
 * other tool calls in the same run are allowed.
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
 * The two-step shape names the tool of interest up-front, then evaluates
 * properties of its occurrences.
 */
export type SingleToolCallAssertion = {
  /**
   * Assert the named tool was called exactly `n` times (counting
   * `inference.tool_call.end` events for that name).
   */
  toHaveBeenCalledTimes(n: number): SingleToolCallAssertion;
};

/**
 * Build a single-tool assertion bound to `name`; its `from(events)` method
 * materializes the assertion against a collected events array.
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
// can hold base64 payloads in the megabyte range. A failing assertion that
// serializes the whole block leaves multi-MB of base64 in test logs, making
// the failure harder to debug than it needs to be. `expectMediaBlock`
// formats failures with an elided representation: kind, mime, source
// discriminant, decoded byte length — never the raw `data` field. Use it
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
 * decoded bytes: four base64 characters encode three bytes, less trailing
 * `=` padding.
 *
 * Throws on structural violations (length not divisible by 4, more than
 * two padding chars) so malformed input cannot pass assertions like
 * `toHaveByteLengthAtLeast(-2)` silently.
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
