/**
 * Match a target string against a glob pattern.
 *
 * Supports `*` as a wildcard that matches any sequence of characters.
 * No other glob syntax is supported (no `?`, `**`, or character classes).
 *
 * Examples:
 *   matchPattern("*", "agent:agt_abc")          => true
 *   matchPattern("agent:*", "agent:agt_abc")    => true
 *   matchPattern("agent:agt_abc", "agent:agt_abc") => true
 *   matchPattern("agent:agt_abc", "agent:agt_xyz") => false
 *   matchPattern("wallet:wal_*", "wallet:wal_123") => true
 *   matchPattern("wallet:wal_*", "wallet:xyz")     => false
 */
export function matchPattern(pattern: string, target: string): boolean {
  if (pattern === "*") return true;
  if (pattern === target) return true;

  if (!pattern.includes("*")) return false;

  const parts = pattern.split("*");
  let pos = 0;

  for (let i = 0; i < parts.length; i++) {
    const segment = parts[i] ?? "";
    if (segment.length === 0) continue;

    const idx = target.indexOf(segment, pos);
    if (idx === -1) return false;

    // First segment must anchor to the start
    if (i === 0 && idx !== 0) return false;

    pos = idx + segment.length;
  }

  // Last segment must anchor to the end
  const lastSegment = parts[parts.length - 1] ?? "";
  if (lastSegment.length > 0 && !target.endsWith(lastSegment)) return false;

  return true;
}
