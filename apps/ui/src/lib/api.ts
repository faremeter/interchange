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
  onEvent: (event: unknown) => void,
  opts?: { eventName?: string; onError?: (e: Event) => void },
): () => void {
  const es = new EventSource(path);
  const handler = (e: MessageEvent) => {
    try {
      onEvent(JSON.parse(e.data));
    } catch {
      // malformed event, ignore
    }
  };
  if (opts?.eventName) {
    es.addEventListener(opts.eventName, handler);
  } else {
    es.onmessage = handler;
  }
  if (opts?.onError) es.onerror = opts.onError;
  return () => es.close();
}
