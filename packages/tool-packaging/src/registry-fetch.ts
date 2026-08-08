// Registry HTTP fetch helpers for the tool-package loader: building the
// npm-registry-fetch options for a configured registry, deriving a default
// tarball URL, and reading a response body under a byte cap. Extracted from
// `loader.ts` so the fetch concern is isolated from author-code loading and
// store layout.

import { ToolLoaderError } from "./loader-internal";
import type { RegistryConfig } from "./resolver";

export function buildRegistryFetchOpts(
  registry: RegistryConfig,
): Record<string, unknown> {
  const opts: Record<string, unknown> = { registry: registry.url };
  if (registry.auth?.token !== undefined) {
    opts.token = registry.auth.token;
  }
  if (registry.auth?.basic !== undefined) {
    const { user, pass } = registry.auth.basic;
    // `npm-registry-fetch` builds the `Authorization: Basic` header by
    // base64-encoding `<username>:<password>` itself. Pre-encoding
    // `pass` would double-encode the password component (the registry
    // would see `base64(plaintext)` as the password, not `plaintext`).
    opts.forceAuth = { username: user, password: pass };
  }
  return opts;
}

export function defaultTarballUrl(
  registryUrl: string,
  name: string,
  version: string,
): string {
  const base = registryUrl.endsWith("/") ? registryUrl : `${registryUrl}/`;
  // Match npm's canonical tarball URL: {registry}/{name}/-/{basename}-{version}.tgz
  const basename = name.startsWith("@") ? name.split("/")[1] : name;
  if (basename === undefined) {
    throw new Error(`internal: cannot derive tarball basename for ${name}`);
  }
  return `${base}${name}/-/${basename}-${version}.tgz`;
}

/**
 * True when `body` is a web `ReadableStream`-shaped value the reader loop
 * can pull through `getReader()`. Test seams that build a real `Response`
 * hit this path; the production `npm-registry-fetch` body does not.
 */
function hasWebReadableBody(
  body: unknown,
): body is { getReader: () => ReadableStreamDefaultReader<Uint8Array> } {
  return (
    typeof body === "object" &&
    body !== null &&
    "getReader" in body &&
    typeof body.getReader === "function"
  );
}

/**
 * True when `body` is a Node-style byte stream: async-iterable, with an
 * optional `destroy` the abort path uses to tear down a stalled read.
 * This is the shape `npm-registry-fetch`'s Minipass response body has —
 * chunks are validated as `Uint8Array` per-iteration, so the element type
 * is left `unknown` here rather than asserted.
 */
function isByteStreamAsyncIterable(
  body: unknown,
): body is AsyncIterable<unknown> & { destroy?: (err?: Error) => void } {
  return (
    typeof body === "object" &&
    body !== null &&
    Symbol.asyncIterator in body &&
    typeof body[Symbol.asyncIterator] === "function"
  );
}

/**
 * Read an HTTP-registry tarball response into a Uint8Array while enforcing
 * `maxBytes`. Two guards:
 *
 *   1. If the upstream sent a `Content-Length` header, parse it (digit-
 *      only, per RFC 9110 §8.6) and reject up front when the declared
 *      length exceeds the cap. A header that fails the digit shape is
 *      also rejected so a header like `1e9` cannot read as 1e9 against
 *      `Number()` while a digit-only cap check would pass.
 *   2. Stream the body chunk-by-chunk, tallying byte length, and abort
 *      the read when the running total crosses the cap. This catches
 *      the missing-or-lying header case.
 *
 * The body is read whether it is a web `ReadableStream` (the `getReader`
 * shape a real `Response` exposes, exercised by the test seams) or a
 * Node/Minipass async-iterable of `Buffer` chunks (what
 * `npm-registry-fetch` actually returns in production). Both branches
 * enforce the same running `maxBytes` cap while streaming, so the byte
 * guard is never bypassed by buffering the whole body up front
 * (`arrayBuffer()`).
 *
 * An optional `signal` adds a time guard: when it aborts (the caller's
 * fetch deadline), the in-flight read is cancelled — the web reader via
 * `cancel()`, the Node stream via `destroy()` — and the call rejects, so
 * a registry that streams the body slowly or stalls mid-stream cannot
 * outlast the deadline while staying under the byte cap.
 *
 * All rejections surface as `registry.fetch.failed` so the apply layer
 * routes them the same as any other registry-side fetch defect.
 *
 * Exported for direct unit testing.
 */
export async function readResponseWithLimit(
  res: Response,
  maxBytes: number,
  ctx: {
    readonly registry: string;
    readonly name: string;
    readonly version: string;
  },
  signal?: AbortSignal,
): Promise<Uint8Array> {
  const declaredLengthRaw = res.headers.get("content-length");
  if (declaredLengthRaw !== null) {
    if (!/^\d+$/.test(declaredLengthRaw)) {
      throw new ToolLoaderError({
        category: "registry.fetch.failed",
        message: `registry "${ctx.registry}" returned non-digit Content-Length ${JSON.stringify(declaredLengthRaw)} for ${ctx.name}@${ctx.version}`,
        package: { name: ctx.name, version: ctx.version },
      });
    }
    const declaredLength = Number(declaredLengthRaw);
    if (!Number.isFinite(declaredLength) || declaredLength > maxBytes) {
      throw new ToolLoaderError({
        category: "registry.fetch.failed",
        message: `tarball for ${ctx.name}@${ctx.version} declares Content-Length ${declaredLengthRaw} which exceeds the ${String(maxBytes)}-byte cap`,
        package: { name: ctx.name, version: ctx.version },
      });
    }
  }

  const timeoutError = (): ToolLoaderError =>
    new ToolLoaderError({
      category: "registry.fetch.failed",
      message: `tarball read for ${ctx.name}@${ctx.version} exceeded the registry fetch timeout`,
      package: { name: ctx.name, version: ctx.version },
    });
  const capOverflowError = (): ToolLoaderError =>
    new ToolLoaderError({
      category: "registry.fetch.failed",
      message: `tarball for ${ctx.name}@${ctx.version} streamed past the ${String(maxBytes)}-byte cap`,
      package: { name: ctx.name, version: ctx.version },
    });

  // `res.body`'s declared web-`ReadableStream` type is a lie on the
  // production path: `npm-registry-fetch` returns a Minipass (Node)
  // stream that has no `getReader`, only async iteration. Treat the body
  // as an unvalidated boundary value and dispatch on its actual runtime
  // shape rather than trusting the declared type.
  const body: unknown = res.body;
  if (body === null || body === undefined) {
    // No body and the upstream returned 2xx: treat as a zero-byte
    // tarball. The cache and tar-extract layers will reject the
    // resulting bytes as non-tar content, but the fetch itself didn't
    // fail — keep this path simple rather than over-rejecting.
    return new Uint8Array(0);
  }

  const chunks: Uint8Array[] = [];
  let total = 0;

  if (hasWebReadableBody(body)) {
    const reader = body.getReader();
    // Cancelling the reader settles any pending read() as done, so the
    // post-read check below surfaces the timeout even when the underlying
    // body stream does not itself observe the abort signal.
    let timedOut = false;
    const onAbort = () => {
      timedOut = true;
      void reader.cancel();
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted === true) onAbort();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (timedOut) throw timeoutError();
        if (done) break;
        if (value === undefined) continue;
        total += value.byteLength;
        if (total > maxBytes) {
          // Stop reading; we already have enough evidence the upstream
          // is over the cap. The reader.cancel() call requests
          // cancellation upstream; the runtime decides whether to drop
          // the in-flight TCP frames or just unsubscribe our reader.
          await reader.cancel();
          throw capOverflowError();
        }
        chunks.push(value);
      }
    } finally {
      signal?.removeEventListener("abort", onAbort);
      reader.releaseLock();
    }
  } else if (isByteStreamAsyncIterable(body)) {
    // A `for await` parked awaiting the next chunk cannot observe the
    // abort flag until a chunk (or the stream's end) arrives, so unlike
    // the web reader's cancel() the flag alone cannot unblock a stalled
    // read. destroy() forces the iteration to settle — it rejects with a
    // stream-teardown error, which the catch below rewrites to the
    // deadline error when the teardown was ours.
    let timedOut = false;
    const onAbort = () => {
      timedOut = true;
      if (typeof body.destroy === "function") body.destroy();
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted === true) onAbort();
    try {
      for await (const chunk of body) {
        if (timedOut) throw timeoutError();
        if (!(chunk instanceof Uint8Array)) {
          throw new ToolLoaderError({
            category: "registry.fetch.failed",
            message: `registry "${ctx.registry}" streamed a non-binary chunk fetching ${ctx.name}@${ctx.version}`,
            package: { name: ctx.name, version: ctx.version },
          });
        }
        total += chunk.byteLength;
        if (total > maxBytes) throw capOverflowError();
        chunks.push(chunk);
      }
      // An abort that lands after the final chunk destroys the stream
      // without rejecting the iteration; surface the timeout here so that
      // race does not slip through as a successful read.
      if (timedOut) throw timeoutError();
    } catch (err) {
      if (timedOut && !(err instanceof ToolLoaderError)) throw timeoutError();
      throw err;
    } finally {
      signal?.removeEventListener("abort", onAbort);
    }
  } else {
    throw new ToolLoaderError({
      category: "registry.fetch.failed",
      message: `registry "${ctx.registry}" returned a response body of an unreadable shape for ${ctx.name}@${ctx.version}`,
      package: { name: ctx.name, version: ctx.version },
    });
  }

  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}
