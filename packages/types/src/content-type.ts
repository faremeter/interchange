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

/**
 * Names the protocol of a response body when the header alone cannot. A
 * recognisable Content-Type wins; otherwise a body that parses as JSON is
 * JSON and anything else is taken as SSE. This is a labelling heuristic
 * for capture and recording tools that must persist whatever a backend
 * sends, not a substitute for the harness's strict detection.
 */
export function sniffResponseKind(
  headers: Headers,
  body: Uint8Array,
): ResponseKind {
  try {
    return detectResponseKind(headers);
  } catch {
    try {
      JSON.parse(new TextDecoder().decode(body));
      return "json";
    } catch {
      return "sse";
    }
  }
}
