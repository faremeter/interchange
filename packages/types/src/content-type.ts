export type ResponseKind = "sse" | "json";

export function detectResponseKind(headers: Headers): ResponseKind {
  const raw = headers.get("content-type");
  if (raw === null) {
    throw new Error(
      "Cannot detect response kind: response has no Content-Type header",
    );
  }
  const normalized = raw.trim().toLowerCase();
  if (normalized.startsWith("text/event-stream")) {
    return "sse";
  }
  if (normalized.startsWith("application/json")) {
    return "json";
  }
  throw new Error(
    `Unsupported response Content-Type: ${raw}. Expected text/event-stream or application/json.`,
  );
}
