/**
 * Specificity of a pattern: count of non-wildcard characters, plus
 * 1000 when the pattern has no wildcard (exact matches always win).
 */
export function patternSpecificity(pattern: string): number {
  if (pattern === "*") return 0;

  const literalLength = pattern.replace(/\*/g, "").length;
  const hasWildcard = pattern.includes("*");

  return hasWildcard ? literalLength : literalLength + 1000;
}

/**
 * Combined specificity of a resource + action pair.
 */
export function grantSpecificity(resource: string, action: string): number {
  return patternSpecificity(resource) + patternSpecificity(action);
}
