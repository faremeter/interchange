// Agent instance addresses are "<instanceId>@<domain>" where instanceId is
// the `ins_`-prefixed identifier produced by generateId("instance"). These
// helpers are the single source of truth for that format.
//
// The shape of the right-hand side of the "@" is not validated beyond the
// requirement that it be non-empty: tightening the contract (DNS-ish
// validation, normalisation, etc.) is a separate follow-up.
//
// `@intx/hub-sessions`'s `parseAgentId` is the canonical throwing wrapper
// over `parseAgentAddress` — call it when a `null` return would
// propagate as a silent bug, and keep this parser's `null` return
// reserved for callers that already have a structured fallback.

const INSTANCE_PREFIX = "ins_";

export function formatAgentAddress(instanceId: string, domain: string): string {
  return `${instanceId}@${domain}`;
}

export function parseAgentAddress(
  address: string,
): { instanceId: string; domain: string } | null {
  const atIdx = address.indexOf("@");
  if (atIdx <= 0) return null;
  const instanceId = address.slice(0, atIdx);
  const domain = address.slice(atIdx + 1);
  if (!instanceId.startsWith(INSTANCE_PREFIX)) return null;
  if (domain.length === 0) return null;
  return { instanceId, domain };
}

export function isAgentAddress(address: string): boolean {
  return parseAgentAddress(address) !== null;
}
