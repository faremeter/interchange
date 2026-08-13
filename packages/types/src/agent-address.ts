// Run addresses are "<runId>@<domain>" where runId is the local part: the
// `run_`-prefixed identifier that names the run. These helpers are the single
// source of truth for that format.
//
// The shape of the right-hand side of the "@" is not validated beyond the
// requirement that it be non-empty: tightening the contract (DNS-ish
// validation, normalisation, etc.) is a separate follow-up.
//
// `@intx/hub-sessions`'s `parseAgentId` is the canonical throwing wrapper
// over `parseRunAddress` — call it when a `null` return would
// propagate as a silent bug, and keep this parser's `null` return
// reserved for callers that already have a structured fallback.

const RUN_PREFIX = "run_";

export function formatRunAddress(runId: string, domain: string): string {
  return `${runId}@${domain}`;
}

export function parseRunAddress(
  address: string,
): { runId: string; domain: string } | null {
  const atIdx = address.indexOf("@");
  if (atIdx <= 0) return null;
  const runId = address.slice(0, atIdx);
  const domain = address.slice(atIdx + 1);
  if (!runId.startsWith(RUN_PREFIX)) return null;
  if (domain.length === 0) return null;
  return { runId, domain };
}

export function isRunAddress(address: string): boolean {
  return parseRunAddress(address) !== null;
}
