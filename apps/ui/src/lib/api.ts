export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export async function api<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const res = await fetch(path, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!res.ok) {
    const data = (await res.json().catch(() => null)) as {
      error?: { code?: string; message?: string };
    } | null;
    throw new ApiError(
      res.status,
      data?.error?.code ?? "unknown",
      data?.error?.message ?? `HTTP ${res.status}`,
    );
  }

  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

// Note: EventSource error events carry no HTTP status or message — `e` is an
// opaque Event object. Do not expect to extract error details from it.
export function openStream(
  path: string,
  onEvent: (type: string, data: unknown) => void,
  onError?: (e: Event) => void,
): () => void {
  const es = new EventSource(path);
  es.onmessage = (e) => {
    try {
      const { type, data } = JSON.parse(e.data) as {
        type: string;
        data: unknown;
      };
      onEvent(type, data);
    } catch {
      // malformed event, ignore
    }
  };
  if (onError) es.onerror = onError;
  return () => es.close();
}
