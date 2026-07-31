// Boundary parse for the required environment variables the bin scripts read.
// Kept apart from each config resolver so a missing or malformed value fails
// with a diagnostic naming the offending variable, rather than surfacing as an
// opaque error further in.

/**
 * Return `env[name]`, or throw when it is unset or empty. An empty string is
 * treated as absent: a blank connection variable is a misconfiguration, not an
 * intentional value.
 */
export function requireEnvVar(
  env: Record<string, string | undefined>,
  name: string,
): string {
  const value = env[name];
  if (value === undefined || value === "") {
    throw new Error(`${name} is required`);
  }
  return value;
}

/**
 * Return `env[name]` parsed as a positive integer, or throw. Requires the
 * variable (via `requireEnvVar`) and rejects a value that is not a whole number
 * greater than zero, so a bad port fails at the boundary.
 */
export function requireIntVar(
  env: Record<string, string | undefined>,
  name: string,
): number {
  const raw = requireEnvVar(env, name);
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(
      `${name} must be a positive integer; got ${JSON.stringify(raw)}`,
    );
  }
  return parsed;
}
