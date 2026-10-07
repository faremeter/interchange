// Registry HTTP fetch helpers for the tool-package loader: building the
// npm-registry-fetch options for a configured registry, deriving a default
// tarball URL, and reading a response body under a byte cap. Extracted from
// `loader.ts` so the fetch concern stays isolated.

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
    // `npm-registry-fetch` base64-encodes `<user>:<pass>` itself;
    // pre-encoding `pass` would double-encode the password component.
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
 * True when `body` is a web `ReadableStream`-shaped value readable via
 * `getReader()`. Test seams that build a real `Response` hit this path;
 * the production `npm-registry-fetch` body does not.
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
 * This is the shape `npm-registry-fetch`'s Minipass body has.
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
 * Read an HTTP-registry tarball response into a Uint8Array while
 * enforcing `maxBytes` with two guards:
 *
 *   1. A digit-only `Content-Length` (RFC 9110 §8.6) is rejected up
 *      front when it exceeds the cap; a non-digit header (e.g. `1e9`)
 *      is rejected too so it cannot smuggle past a Number() check.
 *   2. The body is streamed chunk-by-chunk and the read aborts when
 *      the running total crosses the cap, catching missing or lying
 *      headers.
 *
 * Handles both body shapes — web `ReadableStream` (`getReader`) and
 * Node/Minipass async-iterable — so the byte guard is never bypassed
 * by buffering the whole body (`arrayBuffer()`).
 *
 * An optional `signal` adds a time guard: on abort the in-flight read
 * is cancelled (web `cancel()`, Node `destroy()`) and the call rejects,
 * so a slow or stalled registry cannot outlast the deadline while
 * staying under the byte cap.
 *
 * All rejections surface as `registry.fetch.failed`.
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
    // No body: treat as a zero-byte tarball. The cache and tar-extract
    // layers reject the resulting bytes as non-tar content.
    return new Uint8Array(0);
  }

  const chunks: Uint8Array[] = [];
  let total = 0;

  if (hasWebReadableBody(body)) {
    const reader = body.getReader();
    // cancel() settles a pending read(), so the post-read check
    // surfaces the timeout even when the stream does not observe the
    // abort signal itself.
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
          // Stop reading; we already know the upstream is over the cap.
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
    // A `for await` parked on the next chunk cannot observe the abort
    // flag until a chunk arrives; destroy() forces the iteration to
    // settle, and the catch below rewrites its teardown error to the
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
      // An abort landing after the final chunk destroys the stream
      // without rejecting the iteration; surface the timeout here.
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
