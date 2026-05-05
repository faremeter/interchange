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
