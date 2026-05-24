// Invariant checks over a computed `InferenceEvent[]` produced by replaying
// a captured fixture through `runInference`. Each invariant is a property
// that must hold for any well-formed call, regardless of provider; the
// compat-replay layer applies the full list uniformly.
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
 * compat-replay pass reveals everything wrong with a fixture rather than
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
      return `${head} { token: ${abbreviateString(event.data.token)} }`;
    case "inference.thinking.signature":
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
      const summary =
        src.kind === "base64"
          ? `base64 mime=${src.mimeType} <${String(src.data.length)} chars>`
          : `file-reference mime=${src.mimeType} reference=${abbreviateString(src.reference)}`;
      return `${head} { image: ${summary} }`;
    }
    case "inference.citation":
      return `${head} { citation: { citedText: ${abbreviateString(event.data.citation.citedText)} } }`;
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
// clusterKeyFor — groups events for the index-density check
//
// "Block-type cluster" is the set of events that route to the same logical
// content block. Different content kinds get separate clusters; tool calls
// are correlated by `callId` since the wire emits one block per call but
// each call's deltas should land contiguously.
// ---------------------------------------------------------------------------

function clusterKeyFor(event: InferenceEvent): string | null {
  switch (event.type) {
    case "inference.text.delta":
      return "text";
    case "inference.thinking.delta":
    case "inference.thinking.signature":
    case "inference.thinking.redacted":
      return "thinking";
    case "inference.tool_call.start":
    case "inference.tool_call.delta":
    case "inference.tool_call.end":
      return `tool_call:${event.data.callId}`;
    default:
      return null;
  }
}

function eventIndex(event: InferenceEvent): number | undefined {
  switch (event.type) {
    case "inference.text.delta":
    case "inference.thinking.delta":
    case "inference.thinking.signature":
    case "inference.thinking.redacted":
    case "inference.tool_call.start":
    case "inference.tool_call.delta":
    case "inference.tool_call.end":
      return event.data.index;
    default:
      return undefined;
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
      "image",
      "audio",
      "video",
      "document",
      "citation",
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
  // For each block-type cluster, if any event in the cluster carries an
  // `index`, all events in that cluster must carry one and the index set
  // must be dense from 0 (no gaps). Streams that omit `index` entirely
  // are legal (single-block scenarios) and skip the check for that
  // cluster.
  name: "index_density",
  check(events) {
    const clusters = new Map<
      string,
      { withIndex: Set<number>; missingIndexAt: number[] }
    >();
    events.forEach((event, idx) => {
      const cluster = clusterKeyFor(event);
      if (cluster === null) return;
      const eIdx = eventIndex(event);
      const entry = clusters.get(cluster) ?? {
        withIndex: new Set<number>(),
        missingIndexAt: [],
      };
      if (eIdx === undefined) {
        entry.missingIndexAt.push(idx);
      } else {
        entry.withIndex.add(eIdx);
      }
      clusters.set(cluster, entry);
    });
    const violations: InvariantViolation[] = [];
    for (const [cluster, entry] of clusters) {
      if (entry.withIndex.size === 0) continue; // no indices used in this cluster
      // Some events carry index, others don't — mixed-mode failure.
      if (entry.missingIndexAt.length > 0) {
        violations.push({
          invariant: "index_density",
          message: `cluster "${cluster}" mixes events with and without index (events without index at indices ${entry.missingIndexAt.join(", ")})`,
          events: entry.missingIndexAt,
        });
        continue;
      }
      // All events have index — check density.
      const sorted = [...entry.withIndex].sort((a, b) => a - b);
      for (let i = 0; i < sorted.length; i++) {
        if (sorted[i] !== i) {
          violations.push({
            invariant: "index_density",
            message: `cluster "${cluster}" indices are not dense from 0: observed ${sorted.join(", ")}`,
            events: [],
          });
          break;
        }
      }
    }
    return violations;
  },
};

const signaturePrecedence: Invariant = {
  name: "signature_precedence",
  check(events) {
    const violations: InvariantViolation[] = [];
    // For each thinking signature, scan backwards for a thinking.delta
    // with the same index (or undefined index, single-block scenarios).
    events.forEach((event, sigIdx) => {
      if (event.type !== "inference.thinking.signature") return;
      const sigIndex = event.data.index;
      let foundDelta = false;
      for (let j = sigIdx - 1; j >= 0; j--) {
        const e = events[j];
        if (e === undefined) continue;
        if (e.type !== "inference.thinking.delta") continue;
        if (e.data.index === sigIndex) {
          foundDelta = true;
          break;
        }
      }
      if (!foundDelta) {
        violations.push({
          invariant: "signature_precedence",
          message: `inference.thinking.signature at index ${String(sigIdx)} (index=${String(sigIndex)}) has no preceding inference.thinking.delta with the same index`,
          events: [sigIdx],
        });
      }
    });
    return violations;
  },
};

/**
 * The canonical list applied by the compat-replay layer. Ordered
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
