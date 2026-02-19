/**
 * Compute a specificity score for a pattern.
 *
 * More specific patterns get higher scores:
 *   "*"              => 0  (matches everything)
 *   "agent:*"        => 6  (type-level wildcard, 6 literal chars)
 *   "wallet:wal_*"   => 11 (prefix match, 11 literal chars)
 *   "agent:agt_abc"  => 13 (exact match, 13 literal chars, no wildcards)
 *
 * The score is the count of non-wildcard characters, plus a bonus
 * for patterns with no wildcards at all (exact matches).
 */
export function patternSpecificity(pattern: string): number {
  if (pattern === "*") return 0;

  const literalLength = pattern.replace(/\*/g, "").length;
  const hasWildcard = pattern.includes("*");

  // Exact matches get a bonus to ensure they always beat prefix globs
  // of similar length
  return hasWildcard ? literalLength : literalLength + 1000;
}

/**
 * Combined specificity of a resource + action pair.
 */
export function grantSpecificity(resource: string, action: string): number {
  return patternSpecificity(resource) + patternSpecificity(action);
}
