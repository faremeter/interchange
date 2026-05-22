export function assertNotCI(env: NodeJS.ProcessEnv = process.env): void {
  const value = env.CI;
  if (value !== undefined && value !== "") {
    throw new Error(
      "Discovery runs make live network calls and must not run in CI. " +
        "Unset the CI environment variable to proceed.",
    );
  }
}
