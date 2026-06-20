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

export interface Transport {
  fetch<T>(method: string, path: string, body?: unknown): Promise<T>;
  subscribe(
    path: string,
    onEvent: (event: unknown) => void,
    opts?: { eventName?: string },
  ): () => void;
}

export function createBrowserTransport(): Transport {
  return {
    async fetch<T>(method: string, path: string, body?: unknown): Promise<T> {
      const init: RequestInit = { method };
      if (body) {
        init.headers = { "Content-Type": "application/json" };
        init.body = JSON.stringify(body);
      }
      const res = await globalThis.fetch(path, init);

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

      // 204 (No Content) and 202 (Accepted) carry no response body, so
      // there is nothing to JSON-parse. Calling res.json() on the empty
      // body would throw "Unexpected end of JSON input".
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- T is a generic parameter; runtime validation is the caller's responsibility
      if (res.status === 204 || res.status === 202) return undefined as T;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- T is a generic parameter; runtime validation is the caller's responsibility
      return (await res.json()) as T;
    },

    subscribe(
      path: string,
      onEvent: (event: unknown) => void,
      opts?: { eventName?: string },
    ): () => void {
      const es = new EventSource(path);
      const handler = (e: MessageEvent) => {
        onEvent(JSON.parse(e.data));
      };
      if (opts?.eventName) {
        es.addEventListener(opts.eventName, handler);
      } else {
        es.onmessage = handler;
      }
      return () => es.close();
    },
  };
}
