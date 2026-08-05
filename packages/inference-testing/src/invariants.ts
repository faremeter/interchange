// Invariant checks over a computed `InferenceEvent[]` produced by replaying
// a captured fixture through `runInference`. Each invariant is a property
// that must hold for any well-formed call, regardless of provider; the
// session parser regression applies the full list uniformly.
//
// Invariants are ordered foundational-first: schema validity comes before
// everything else, since the higher-level checks build on the assumption
// that events parse cleanly. A failing schema check usually makes the
// downstream invariants noise built on garbage.

import { type } from "arktype";

import { parseInferenceEvent, type InferenceEvent } from "@intx/types/runtime";

/**
 * Per-replay context an invariant may consult. Empty today; expand with
 * additional fields (original request payload, declared tool list, etc.)
 * when an invariant needs information beyond the event stream itself.
 *
 * Adding optional fields is backward compatible; check signatures take it
 * as `context?: ReplayContext` so today's callers don't pass it.
 */
export type ReplayContext = Record<string, never>;

/**
 * A single failure surfaced by an invariant. The `message` is a one-line
 * human-readable summary; `events` lists the indices into the original
 * array that the violation refers to, so a caller can quote them through
 * `formatEventBrief` for richer context without forcing every invariant
 * to format its own.
 */
export type InvariantViolation = {
  invariant: string;
  message: string;
  events: number[];
};

/**
 * A property check over a computed `InferenceEvent[]`. Returns an array
 * of violations (empty when the property holds). Implementations should
 * surface every violation they find, not stop at the first, so a single
 * parser-regression pass reveals everything wrong with a fixture rather than
 * forcing fix-and-rerun cycles.
 */
export type Invariant = {
  name: string;
  check(
    events: readonly InferenceEvent[],
    context?: ReplayContext,
  ): InvariantViolation[];
};

// ---------------------------------------------------------------------------
// Event formatting — elided to prevent payload leaks in violation messages
// ---------------------------------------------------------------------------

const BYTES_FIELD_THRESHOLD = 64;

function abbreviateString(
  value: string,
  limit = BYTES_FIELD_THRESHOLD,
): string {
  if (value.length <= limit) return JSON.stringify(value);
  return `<${String(value.length)} chars>`;
}

/**
 * Format an event for inclusion in a violation message. Elides large
 * string fields (text/thinking tokens, redacted-thinking data blobs,
 * code fragments, image base64) with a `<N chars>` placeholder so an
 * invariant that fires on a megabyte-scale `inference.image_output`
 * doesn't dump the payload into test logs.
 *
 * Output shape: `inference.text.delta[seq=12] { token: "Hello" }`.
 */
export function formatEventBrief(event: InferenceEvent): string {
  const head = `${event.type}[seq=${String(event.seq)}]`;
  switch (event.type) {
    case "inference.text.delta":
    case "inference.thinking.delta":
    case "inference.refusal.delta":
      return `${head} { token: ${abbreviateString(event.data.token)} }`;
    case "inference.block.signature":
      return `${head} { signature: ${abbreviateString(event.data.signature)} }`;
    case "inference.thinking.redacted":
      return `${head} { redactedThinking: <${String(event.data.redactedThinking.data.length)} chars> }`;
    case "inference.code_execution.start":
      return `${head} { request: { id: ${JSON.stringify(event.data.request.id)}, code: ${abbreviateString(event.data.request.code)} } }`;
    case "inference.code_execution.delta":
      return `${head} { requestId: ${JSON.stringify(event.data.requestId)}, codeFragment: ${abbreviateString(event.data.codeFragment)} }`;
    case "inference.code_execution.result":
      return `${head} { result: { requestId: ${JSON.stringify(event.data.result.requestId)}, status: ${JSON.stringify(event.data.result.status)} } }`;
    case "inference.image_output": {
      const src = event.data.image.source;
      // file-reference URIs can be signed URLs or other large opaque
      // handles; abbreviate them with the same threshold the rest of
      // the helper uses for string fields rather than dump them verbatim.
      // The `url` source variant lands in the same shape category as
      // `file-reference` for log-summary purposes -- both are
      // dereferencable references rather than inline bytes.
      let summary: string;
      switch (src.kind) {
        case "base64":
          summary = `base64 mime=${src.mimeType} <${String(src.data.length)} chars>`;
          break;
        case "file-reference":
          summary = `file-reference mime=${src.mimeType} reference=${abbreviateString(src.reference)}`;
          break;
        case "url":
          summary = `url mime=${src.mimeType} url=${abbreviateString(src.url)}`;
          break;
        default:
          src satisfies never;
          throw new Error(`unreachable: unknown MediaSource kind`);
      }
      return `${head} { image: ${summary} }`;
    }
    case "inference.citation":
      return `${head} { citation: { citedText: ${abbreviateString(event.data.citation.citedText)} } }`;
    case "inference.safety_rating":
      return `${head} { safetyRating: { blockReason: ${JSON.stringify(event.data.safetyRating.blockReason)} } }`;
    case "inference.tool_call.start":
      return `${head} { callId: ${JSON.stringify(event.data.callId)}, name: ${JSON.stringify(event.data.name)} }`;
    case "inference.tool_call.delta":
      return `${head} { callId: ${JSON.stringify(event.data.callId)}, argumentFragment: ${abbreviateString(event.data.argumentFragment)} }`;
    case "inference.tool_call.end":
      return `${head} { callId: ${JSON.stringify(event.data.callId)}, name: ${JSON.stringify(event.data.name)} }`;
    default:
      return head;
  }
}

// ---------------------------------------------------------------------------
// blockOpenerIndex — the block-opener status and index of an event
//
// A content block is opened at a wire index by exactly one event kind: a
// text/thinking/refusal/redacted delta, a tool_call.start, an image_output,
// or a code_execution start/result. Each carries the block's index (or none,
// in single-block streams that omit indices). Events that do NOT open a
// block return `{ opener: false }`: block.signature (a zero-width attribution
// marker validated by signature_precedence), tool_call.delta/end (they extend
// an already-open call), and the terminal/usage/citation events.
// ---------------------------------------------------------------------------

type BlockOpener =
  | { opener: true; index: number | undefined }
  | { opener: false };

function blockOpenerIndex(event: InferenceEvent): BlockOpener {
  switch (event.type) {
    case "inference.text.delta":
    case "inference.thinking.delta":
    case "inference.refusal.delta":
    case "inference.thinking.redacted":
    case "inference.tool_call.start":
    case "inference.image_output":
    case "inference.code_execution.start":
    case "inference.code_execution.result":
      return { opener: true, index: event.data.index };
    default:
      return { opener: false };
  }
}

// ---------------------------------------------------------------------------
// Individual invariants
// ---------------------------------------------------------------------------

const schemaValidity: Invariant = {
  name: "schema_validity",
  check(events) {
    const violations: InvariantViolation[] = [];
    events.forEach((event, idx) => {
      const result = parseInferenceEvent(event);
      if (result instanceof type.errors) {
        violations.push({
          invariant: "schema_validity",
          message: `event at index ${String(idx)} (${formatEventBrief(event)}) failed arktype validation: ${result.summary}`,
          events: [idx],
        });
      }
    });
    return violations;
  },
};

const toolCallPairing: Invariant = {
  name: "tool_call_pairing",
  check(events) {
    // Track every start and end per callId rather than a single Map
    // entry. A Map<callId, idx> silently overwrites duplicates, so two
    // starts with the same callId followed by one end would look like a
    // clean pair — but the wire shape (one start + one end per call)
    // makes a duplicate start a real protocol-level bug worth flagging.
    const startsByCallId = new Map<string, number[]>();
    const endsByCallId = new Map<string, number[]>();
    events.forEach((event, idx) => {
      if (event.type === "inference.tool_call.start") {
        const list = startsByCallId.get(event.data.callId) ?? [];
        list.push(idx);
        startsByCallId.set(event.data.callId, list);
      } else if (event.type === "inference.tool_call.end") {
        const list = endsByCallId.get(event.data.callId) ?? [];
        list.push(idx);
        endsByCallId.set(event.data.callId, list);
      }
    });
    const violations: InvariantViolation[] = [];
    for (const [callId, startIdxs] of startsByCallId) {
      if (startIdxs.length > 1) {
        violations.push({
          invariant: "tool_call_pairing",
          message: `tool_call.start for callId=${JSON.stringify(callId)} appears ${String(startIdxs.length)} times; the wire emits one start per call`,
          events: startIdxs,
        });
      }
      if (!endsByCallId.has(callId)) {
        const firstStart = startIdxs[0] ?? -1;
        violations.push({
          invariant: "tool_call_pairing",
          message: `tool_call.start for callId=${JSON.stringify(callId)} has no matching tool_call.end`,
          events: firstStart >= 0 ? [firstStart] : [],
        });
      }
    }
    for (const [callId, endIdxs] of endsByCallId) {
      if (endIdxs.length > 1) {
        violations.push({
          invariant: "tool_call_pairing",
          message: `tool_call.end for callId=${JSON.stringify(callId)} appears ${String(endIdxs.length)} times; the wire emits one end per call`,
          events: endIdxs,
        });
      }
      if (!startsByCallId.has(callId)) {
        const firstEnd = endIdxs[0] ?? -1;
        violations.push({
          invariant: "tool_call_pairing",
          message: `tool_call.end for callId=${JSON.stringify(callId)} has no preceding tool_call.start`,
          events: firstEnd >= 0 ? [firstEnd] : [],
        });
      }
    }
    return violations;
  },
};

const terminalExclusivity: Invariant = {
  name: "terminal_exclusivity",
  check(events) {
    const doneIndices: number[] = [];
    const errorIndices: number[] = [];
    events.forEach((event, idx) => {
      if (event.type === "inference.done") doneIndices.push(idx);
      else if (event.type === "inference.error") errorIndices.push(idx);
    });
    const violations: InvariantViolation[] = [];
    if (doneIndices.length === 0 && errorIndices.length === 0) {
      violations.push({
        invariant: "terminal_exclusivity",
        message:
          "no terminal event: stream lacks both inference.done and inference.error",
        events: [],
      });
    }
    if (doneIndices.length > 0 && errorIndices.length > 0) {
      violations.push({
        invariant: "terminal_exclusivity",
        message: `stream carries both inference.done and inference.error: done@${String(doneIndices[0])}, error@${String(errorIndices[0])}`,
        events: [doneIndices[0] ?? -1, errorIndices[0] ?? -1].filter(
          (n) => n >= 0,
        ),
      });
    }
    if (doneIndices.length > 1) {
      violations.push({
        invariant: "terminal_exclusivity",
        message: `multiple inference.done events: ${String(doneIndices.length)} occurrences`,
        events: doneIndices,
      });
    }
    if (errorIndices.length > 1) {
      violations.push({
        invariant: "terminal_exclusivity",
        message: `multiple inference.error events: ${String(errorIndices.length)} occurrences`,
        events: errorIndices,
      });
    }
    return violations;
  },
};

const usageCoherence: Invariant = {
  // Assumes cumulative usage semantics — both head (early) and tail
  // (terminal) usage events report running totals, not deltas. Providers
  // that emit deltas would fail this check; if such a provider lands,
  // surface the discrepancy and update the invariant rather than silently
  // accommodate.
  name: "usage_coherence_monotonic_non_decreasing",
  check(events) {
    const violations: InvariantViolation[] = [];
    let prevInput: number | undefined;
    let prevOutput: number | undefined;
    let prevCacheRead: number | undefined;
    let prevCacheWrite: number | undefined;

    events.forEach((event, idx) => {
      const usage =
        event.type === "inference.usage"
          ? event.data.usage
          : event.type === "inference.done"
            ? event.data.usage
            : null;
      if (usage === null) return;
      const fields = [
        ["input", usage.input],
        ["output", usage.output],
        ["cacheRead", usage.cacheRead],
        ["cacheWrite", usage.cacheWrite],
        ["thinking", usage.thinking],
      ] as const;
      for (const [field, value] of fields) {
        if (typeof value !== "number") continue;
        if (!Number.isFinite(value) || Number.isNaN(value)) {
          violations.push({
            invariant: "usage_coherence_monotonic_non_decreasing",
            message: `usage.${field} is not a finite number at index ${String(idx)}: ${String(value)}`,
            events: [idx],
          });
        }
        if (value < 0) {
          violations.push({
            invariant: "usage_coherence_monotonic_non_decreasing",
            message: `usage.${field} is negative at index ${String(idx)}: ${String(value)}`,
            events: [idx],
          });
        }
      }
      const checkMonotone = (
        name: string,
        prev: number | undefined,
        next: number,
      ): void => {
        if (prev !== undefined && next < prev) {
          violations.push({
            invariant: "usage_coherence_monotonic_non_decreasing",
            message: `usage.${name} decreased from ${String(prev)} to ${String(next)} at index ${String(idx)} (expected cumulative non-decreasing)`,
            events: [idx],
          });
        }
      };
      checkMonotone("input", prevInput, usage.input);
      checkMonotone("output", prevOutput, usage.output);
      if (usage.cacheRead !== undefined) {
        checkMonotone("cacheRead", prevCacheRead, usage.cacheRead);
        prevCacheRead = usage.cacheRead;
      }
      if (usage.cacheWrite !== undefined) {
        checkMonotone("cacheWrite", prevCacheWrite, usage.cacheWrite);
        prevCacheWrite = usage.cacheWrite;
      }
      prevInput = usage.input;
      prevOutput = usage.output;
    });

    return violations;
  },
};

const recognizedContentBlocks: Invariant = {
  name: "recognized_content_blocks",
  check(events) {
    // Known ContentBlock type discriminants. Keep this list in sync with
    // ContentBlock's union in `packages/types/src/runtime.ts`.
    const known = new Set([
      "text",
      "thinking",
      "redacted_thinking",
      "refusal",
      "image",
      "audio",
      "video",
      "document",
      "citation",
      "safety_rating",
      "code_execution_request",
      "code_execution_result",
      "tool_call",
      "tool_result",
    ]);
    const violations: InvariantViolation[] = [];
    events.forEach((event, idx) => {
      if (event.type !== "inference.done") return;
      event.data.turn.content.forEach((block, bi) => {
        if (!known.has(block.type)) {
          violations.push({
            invariant: "recognized_content_blocks",
            message: `inference.done at index ${String(idx)} carries unrecognized content block type "${block.type}" at content[${String(bi)}]`,
            events: [idx],
          });
        }
      });
    });
    return violations;
  },
};

const toolArgsJson: Invariant = {
  name: "tool_args_parse_as_json",
  check(events) {
    const violations: InvariantViolation[] = [];
    events.forEach((event, idx) => {
      if (event.type !== "inference.tool_call.end") return;
      // The end event carries `arguments` as already-parsed Record. The
      // wire path that produces it goes through JSON.parse on the
      // accumulated argument fragments; if that fails the adapter
      // typically falls back to `{ _raw: <buffer> }` and surfaces the
      // failure here. Treat presence of `_raw` as the failure marker.
      if ("_raw" in event.data.arguments) {
        violations.push({
          invariant: "tool_args_parse_as_json",
          message: `tool_call.end at index ${String(idx)} (callId=${JSON.stringify(event.data.callId)}) carries unparseable arguments wrapped in _raw`,
          events: [idx],
        });
      }
    });
    return violations;
  },
};

const redactedThinkingDataNonEmpty: Invariant = {
  // A redacted_thinking block whose `data` is empty is meaningless on
  // every wire — providers that emit redacted_thinking carry an opaque
  // payload because that payload is what the next turn must echo back
  // verbatim. An empty data field would round-trip as if there were no
  // redacted thinking at all, which silently corrupts the conversation.
  //
  // The signature-presence check on regular thinking blocks is
  // intentionally NOT part of this invariant: it is Anthropic-specific
  // (only Anthropic's extended-thinking surface emits signatures), and
  // applying it to providers like OpenCode-Zen — whose reasoning
  // content arrives via `reasoning_content` without any signature
  // concept — would flag every cross-provider thinking turn. When
  // ReplayContext carries the source provider, a per-provider
  // signature-required check can land alongside.
  name: "redacted_thinking_data_non_empty",
  check(events) {
    const violations: InvariantViolation[] = [];
    events.forEach((event, idx) => {
      if (event.type === "inference.done") {
        event.data.turn.content.forEach((block, bi) => {
          if (block.type === "redacted_thinking" && block.data.length === 0) {
            violations.push({
              invariant: "redacted_thinking_data_non_empty",
              message: `inference.done at index ${String(idx)} content[${String(bi)}] is a redacted_thinking block with an empty data blob`,
              events: [idx],
            });
          }
        });
      } else if (event.type === "inference.thinking.redacted") {
        // Streaming variant: an empty data blob here corrupts downstream
        // consumers that subscribe to the streaming event before the
        // finalized turn arrives.
        if (event.data.redactedThinking.data.length === 0) {
          violations.push({
            invariant: "redacted_thinking_data_non_empty",
            message: `inference.thinking.redacted at index ${String(idx)} carries an empty data blob`,
            events: [idx],
          });
        }
      }
    });
    return violations;
  },
};

const indexDensity: Invariant = {
  // Block openers must be consistently indexed across the whole turn: if
  // any block opener carries an `index`, they all must. Single-block
  // streams that omit indices entirely are legal; a stream that indexes
  // some block openers but not others is a mixed-mode wire violation.
  //
  // Whether the indexed set is gap-free (dense from 0) is deliberately NOT
  // checked here: block indices are provider-assigned, and an adapter that
  // echoes wire content-block indices can legitimately skip indices for
  // block kinds it does not decode, so density-from-0 is not a universal
  // property.
  name: "index_density",
  check(events) {
    const withIndex = new Set<number>();
    const missingIndexAt: number[] = [];
    events.forEach((event, idx) => {
      const opener = blockOpenerIndex(event);
      if (!opener.opener) return;
      if (opener.index === undefined) {
        missingIndexAt.push(idx);
      } else {
        withIndex.add(opener.index);
      }
    });
    const violations: InvariantViolation[] = [];
    if (withIndex.size === 0) return violations; // single-block: no indices
    if (missingIndexAt.length > 0) {
      violations.push({
        invariant: "index_density",
        message: `block openers mix events with and without index (events without index at positions ${missingIndexAt.join(", ")})`,
        events: missingIndexAt,
      });
    }
    return violations;
  },
};

// A signature authenticates the block at its index. The block must
// have been opened earlier in the stream by one of the signable-block
// openers: a thinking/text delta, or one of the atomic starts
// (tool_call, image, code_execution).
function opensSignableBlockAt(
  event: InferenceEvent,
  index: number | undefined,
): boolean {
  switch (event.type) {
    case "inference.thinking.delta":
    case "inference.text.delta":
    case "inference.tool_call.start":
    case "inference.image_output":
    case "inference.code_execution.start":
      return event.data.index === index;
    default:
      return false;
  }
}

const signaturePrecedence: Invariant = {
  name: "signature_precedence",
  check(events) {
    const violations: InvariantViolation[] = [];
    // For each block signature, scan backwards for the event that
    // opened a signable block at the same index (single-block scenarios
    // pair an undefined index against an undefined-index opener).
    events.forEach((event, sigIdx) => {
      if (event.type !== "inference.block.signature") return;
      const sigIndex = event.data.index;
      let foundOpener = false;
      for (let j = sigIdx - 1; j >= 0; j--) {
        const e = events[j];
        if (e === undefined) continue;
        if (opensSignableBlockAt(e, sigIndex)) {
          foundOpener = true;
          break;
        }
      }
      if (!foundOpener) {
        violations.push({
          invariant: "signature_precedence",
          message: `inference.block.signature at position ${String(sigIdx)} (index=${String(sigIndex)}) has no preceding block-opening event at the same index`,
          events: [sigIdx],
        });
      }
    });
    return violations;
  },
};

/**
 * The canonical list applied by the session parser regression. Ordered
 * foundational-first: schema_validity catches structural problems before
 * the higher-level checks build on potentially-garbage events.
 */
export const INVARIANTS: readonly Invariant[] = [
  schemaValidity,
  toolCallPairing,
  terminalExclusivity,
  usageCoherence,
  recognizedContentBlocks,
  toolArgsJson,
  redactedThinkingDataNonEmpty,
  indexDensity,
  signaturePrecedence,
] as const;
