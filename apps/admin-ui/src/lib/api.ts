import { type } from "arktype";

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

const ErrorBody = type({
  "error?": {
    "code?": "string",
    "message?": "string",
  },
});

export async function api<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const res = await fetch(path, {
    method,
    ...(body
      ? {
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        }
      : {}),
  });

  if (!res.ok) {
    const raw = await res.json().catch(() => null);
    const data = raw !== null ? ErrorBody(raw) : null;
    const errorBody = data instanceof type.errors ? null : data;
    throw new ApiError(
      res.status,
      errorBody?.error?.code ?? "unknown",
      errorBody?.error?.message ?? `HTTP ${res.status}`,
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- T is a generic parameter; runtime validation is the caller's responsibility
  if (res.status === 204) return undefined as T;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- T is a generic parameter; runtime validation is the caller's responsibility
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
