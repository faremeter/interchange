// Semantic classification of a captured session by replaying its responses
// through the production runtime decoder. A capture that returned HTTP 200 is
// not proof the capability works: the body may be an empty shell, a soft
// decline, or an error payload the wire happened to deliver with a 200. The
// authoritative test of "genuine" is that the production `runInference` decodes
// the captured response into real content events with no invariant violation.
//
// This maps a replayed event stream to the support-matrix outcome vocabulary.
// Only the three outcomes a decode can distinguish are produced here:
//
//   captured — real content: text or a tool call.
//   refused  — the model declined (a refusal, no content).
//   misled   — a 200 that decodes to nothing genuine: an error event, an
//              invariant violation, an empty stream, or a decode that throws.
//
// The HTTP-outcome axis (`http-error`, `unsupported`) is not derivable from a
// committed fixture — the capture format does not persist the status line — so
// it belongs to the probe, not to this offline replay.

import type { InferenceEvent } from "@intx/types/runtime";
import { replayResponsesForParsing } from "./session-replay";
import type { ParserReplayResult } from "./session-replay";
import { INVARIANTS } from "./invariants";
import type { Invariant } from "./invariants";

export type ReplayOutcome = "captured" | "refused" | "misled";

export interface ReplayClassification {
  outcome: ReplayOutcome;
  reason: string;
}

function isContent(event: InferenceEvent): boolean {
  return (
    event.type === "inference.text.delta" ||
    event.type.startsWith("inference.tool_call")
  );
}

// Classify a single session's replayed exchanges. A session may span several
// exchanges (a multi-turn tool flow, a Files-API generate step); the outcome is
// aggregated across all of them, because a decode error or a violation anywhere
// discredits the whole capture, and content in any exchange proves the flow
// produced something genuine.
export function classifyReplay(
  results: readonly ParserReplayResult[],
): ReplayClassification {
  const replayed = results.filter((r) => r.kind === "replayed");
  if (replayed.length === 0) {
    return {
      outcome: "misled",
      reason: "no replayable exchange (every exchange was skipped)",
    };
  }

  for (const result of replayed) {
    const [first] = result.violations;
    if (first !== undefined) {
      return {
        outcome: "misled",
        reason: `invariant '${first.invariant}' violated: ${first.message}`,
      };
    }
    const errorEvent = result.events.find(
      (event) => event.type === "inference.error",
    );
    if (errorEvent !== undefined) {
      return {
        outcome: "misled",
        reason: "decoded to an inference.error event",
      };
    }
  }

  let hasContent = false;
  let hasRefusal = false;
  for (const result of replayed) {
    for (const event of result.events) {
      if (isContent(event)) hasContent = true;
      if (event.type === "inference.refusal.delta") hasRefusal = true;
    }
  }

  if (hasContent) {
    return {
      outcome: "captured",
      reason: "decoded to text or tool-call events",
    };
  }
  if (hasRefusal) {
    return {
      outcome: "refused",
      reason: "decoded to a refusal with no content",
    };
  }
  return {
    outcome: "misled",
    reason: "decoded to no content, tool call, or refusal",
  };
}

// Replay a session directory and classify it. A replay that throws (an adapter
// that cannot decode the captured bytes) is its own signal, not a crash: the
// response was undecodable, so the cell is misled. Callers batching many
// sessions rely on this never propagating a per-session decode failure.
export async function classifySession(
  sessionDir: string,
  opts?: { invariants?: readonly Invariant[] },
): Promise<ReplayClassification> {
  let results: ParserReplayResult[];
  try {
    results = await replayResponsesForParsing({
      sessionDir,
      invariants: opts?.invariants ?? INVARIANTS,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      outcome: "misled",
      reason: `replay decode threw: ${message}`,
    };
  }
  return classifyReplay(results);
}
