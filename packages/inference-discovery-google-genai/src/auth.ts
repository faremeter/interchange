export const AUTH_HEADER = "x-goog-api-key";

export function buildAuthHeaders(apiKey: string): Record<string, string> {
  if (apiKey.length === 0) {
    throw new Error("google-genai: apiKey must be a non-empty string");
  }
  return { [AUTH_HEADER]: apiKey };
}
