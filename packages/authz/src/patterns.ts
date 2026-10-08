/**
 * Match a target string against a glob pattern. Only `*` is a wildcard
 * (any sequence, including empty); no `?`, `**`, or character classes.
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
