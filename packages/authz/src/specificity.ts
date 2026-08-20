/**
 * Compute a specificity score for a pattern.
 *
 * More specific patterns get higher scores:
 *   "*"                    => 0    (matches everything)
 *   "wallet:wal_*"         => 11   (prefix match, 11 literal chars)
 *   "workflow-run:*"       => 13   (type-level wildcard, 13 literal chars)
 *   "workflow-run:run_abc" => 1020 (exact match, 20 literal chars, no wildcards)
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
