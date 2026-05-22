export function requireEnv(
  name: string,
  env: NodeJS.ProcessEnv = process.env,
): string {
  const value = env[name];
  if (value === undefined || value === "") {
    throw new Error(
      `Required environment variable ${name} is not set. ` +
        `Export it before running discovery.`,
    );
  }
  return value;
}

export function requireEnvSet(
  names: readonly string[],
  env: NodeJS.ProcessEnv = process.env,
): Record<string, string> {
  const result: Record<string, string> = {};
  const missing: string[] = [];
  for (const name of names) {
    const value = env[name];
    if (value === undefined || value === "") {
      missing.push(name);
      continue;
    }
    result[name] = value;
  }
  if (missing.length > 0) {
    throw new Error(
      `Required environment variables are not set: ${missing.join(", ")}. ` +
        `Export them before running discovery.`,
    );
  }
  return result;
}
