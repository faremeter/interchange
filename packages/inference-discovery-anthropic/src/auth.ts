// API version pin: new versions opt in to wire-shape changes, so pinning
// keeps captures stable across upstream rollouts.
export const ANTHROPIC_VERSION = "2023-06-01";

export const API_KEY_HEADER = "x-api-key";
export const VERSION_HEADER = "anthropic-version";

export function buildAuthHeaders(apiKey: string): Record<string, string> {
  if (apiKey.length === 0) {
    throw new Error("anthropic: apiKey must be a non-empty string");
  }
  return {
    [API_KEY_HEADER]: apiKey,
    [VERSION_HEADER]: ANTHROPIC_VERSION,
  };
}
