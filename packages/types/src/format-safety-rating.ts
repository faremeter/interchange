import type { SafetyRatingBlock } from "./runtime";

/**
 * Human-readable rendering of a SafetyRatingBlock for reply text,
 * timeline summaries, and request-history rewrites when a provider
 * has no input wire shape for safety_rating. Single owner of the
 * display string so reply / history / transform stay in lockstep.
 */
export function formatSafetyRatingText(block: SafetyRatingBlock): string {
  return `Request blocked: ${block.blockReason}`;
}
