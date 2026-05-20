import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { getLogger } from "@intx/log";

const logger = getLogger(["gemini-discover"]);

const HERE = fileURLToPath(new URL(".", import.meta.url));
export const FIXTURE_ROOT = join(
  HERE,
  "..",
  "..",
  "packages",
  "inference-testing",
  "wire",
  "gemini",
);

const REDACTED = "<redacted>";
const REDACTED_HEADER_NAMES = new Set(["x-goog-api-key"]);

export type HeadersMap = Record<string, string>;

export function headersToMap(headers: Headers): HeadersMap {
  const map: HeadersMap = {};
  headers.forEach((value, key) => {
    map[key] = value;
  });
  return map;
}

export function redactRequestHeaders(headers: HeadersMap): HeadersMap {
  const redacted: HeadersMap = {};
  for (const [key, value] of Object.entries(headers)) {
    if (REDACTED_HEADER_NAMES.has(key.toLowerCase())) {
      redacted[key] = REDACTED;
    } else {
      redacted[key] = value;
    }
  }
  return redacted;
}

export function fixtureDirectoryFor(capability: string): string {
  return join(FIXTURE_ROOT, capability);
}

export type Metadata = {
  capability: string;
  model: string;
  endpoint: string;
  capturedAt: string;
  scriptVersion: string;
};

export function buildMetadata(opts: {
  capability: string;
  model: string;
  endpoint: string;
  scriptVersion: string;
  now?: Date;
}): Metadata {
  const now = opts.now ?? new Date();
  return {
    capability: opts.capability,
    model: opts.model,
    endpoint: opts.endpoint,
    capturedAt: now.toISOString(),
    scriptVersion: opts.scriptVersion,
  };
}

export type WriteFixtureArgs = {
  capability: string;
  model: string;
  endpoint: string;
  scriptVersion: string;
  requestBody: unknown;
  requestHeaders: HeadersMap;
  responseHeaders: HeadersMap;
  responseBytes?: Uint8Array;
  responseJson?: unknown;
  now?: Date;
  /**
   * Override the destination directory. Tests use this to write into a
   * temporary directory instead of the canonical fixture tree. Production
   * callers must not set this; the canonical layout is enforced by
   * fixtureDirectoryFor.
   */
  destinationOverride?: string;
};

export async function writeFixture(args: WriteFixtureArgs): Promise<string> {
  if (args.responseBytes === undefined && args.responseJson === undefined) {
    throw new Error(
      `writeFixture for capability ${args.capability} requires either responseBytes or responseJson`,
    );
  }
  if (args.responseBytes !== undefined && args.responseJson !== undefined) {
    throw new Error(
      `writeFixture for capability ${args.capability} received both responseBytes and responseJson`,
    );
  }

  const dir = args.destinationOverride ?? fixtureDirectoryFor(args.capability);
  await mkdir(dir, { recursive: true });

  const requestHeadersRedacted = redactRequestHeaders(args.requestHeaders);

  await writeFile(
    join(dir, "request.json"),
    JSON.stringify(args.requestBody, null, 2) + "\n",
  );
  await writeFile(
    join(dir, "request-headers.json"),
    JSON.stringify(requestHeadersRedacted, null, 2) + "\n",
  );
  await writeFile(
    join(dir, "response-headers.json"),
    JSON.stringify(args.responseHeaders, null, 2) + "\n",
  );

  if (args.responseBytes !== undefined) {
    await writeFile(join(dir, "response.sse"), args.responseBytes);
  } else {
    await writeFile(
      join(dir, "response.json"),
      JSON.stringify(args.responseJson, null, 2) + "\n",
    );
  }

  const metadata = buildMetadata({
    capability: args.capability,
    model: args.model,
    endpoint: args.endpoint,
    scriptVersion: args.scriptVersion,
    ...(args.now ? { now: args.now } : {}),
  });
  await writeFile(
    join(dir, "metadata.json"),
    JSON.stringify(metadata, null, 2) + "\n",
  );

  return dir;
}

const GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta/models";

function buildHeaders(apiKey: string): HeadersMap {
  return {
    "content-type": "application/json",
    "x-goog-api-key": apiKey,
  };
}

export type NonStreamingCaptureArgs = {
  capability: string;
  model: string;
  endpoint: string;
  body: unknown;
  apiKey: string;
  scriptVersion: string;
};

export async function runNonStreamingCapture(
  args: NonStreamingCaptureArgs,
): Promise<{ fixtureDir: string; responseJson: unknown }> {
  const url = `${GEMINI_BASE}/${args.model}:${args.endpoint}`;
  const requestHeaders = buildHeaders(args.apiKey);

  logger.info`POST ${url}`;

  const response = await fetch(url, {
    method: "POST",
    headers: requestHeaders,
    body: JSON.stringify(args.body),
  });

  const responseHeaders = headersToMap(response.headers);
  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Gemini non-streaming request failed: ${String(response.status)} ${response.statusText}: ${text}`,
    );
  }

  let responseJson: unknown;
  try {
    responseJson = JSON.parse(text);
  } catch (cause) {
    throw new Error(
      `Failed to parse non-streaming response as JSON for ${args.capability}`,
      { cause },
    );
  }

  const fixtureDir = await writeFixture({
    capability: args.capability,
    model: args.model,
    endpoint: args.endpoint,
    scriptVersion: args.scriptVersion,
    requestBody: args.body,
    requestHeaders,
    responseHeaders,
    responseJson,
  });

  return { fixtureDir, responseJson };
}

export type StreamingCaptureArgs = {
  capability: string;
  model: string;
  endpoint: string;
  body: unknown;
  apiKey: string;
  scriptVersion: string;
  source?: {
    stream: ReadableStream<Uint8Array>;
    responseHeaders: HeadersMap;
  };
  destinationOverride?: string;
};

export async function runStreamingCapture(
  args: StreamingCaptureArgs,
): Promise<{ fixtureDir: string; bytes: Uint8Array }> {
  const requestHeaders = buildHeaders(args.apiKey);

  let stream: ReadableStream<Uint8Array>;
  let responseHeaders: HeadersMap;

  if (args.source) {
    stream = args.source.stream;
    responseHeaders = args.source.responseHeaders;
  } else {
    const url = `${GEMINI_BASE}/${args.model}:${args.endpoint}?alt=sse`;
    logger.info`POST (stream) ${url}`;
    const response = await fetch(url, {
      method: "POST",
      headers: requestHeaders,
      body: JSON.stringify(args.body),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(
        `Gemini streaming request failed: ${String(response.status)} ${response.statusText}: ${text}`,
      );
    }
    if (!response.body) {
      throw new Error(
        `Gemini streaming response for ${args.capability} had no body`,
      );
    }
    stream = response.body;
    responseHeaders = headersToMap(response.headers);
  }

  const bytes = await teeStreamToBytes(stream, (chunk) => {
    const text = new TextDecoder("utf-8", { fatal: false }).decode(chunk);
    for (const line of text.split(/\r?\n/)) {
      if (line.length === 0) continue;
      logger.debug`sse< ${line}`;
    }
  });

  const fixtureDir = await writeFixture({
    capability: args.capability,
    model: args.model,
    endpoint: args.endpoint,
    scriptVersion: args.scriptVersion,
    requestBody: args.body,
    requestHeaders,
    responseHeaders,
    responseBytes: bytes,
    ...(args.destinationOverride
      ? { destinationOverride: args.destinationOverride }
      : {}),
  });

  return { fixtureDir, bytes };
}

export async function teeStreamToBytes(
  source: ReadableStream<Uint8Array>,
  onChunk?: (chunk: Uint8Array) => void,
): Promise<Uint8Array> {
  const reader = source.getReader();
  const parts: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (!value) continue;
    parts.push(value);
    total += value.byteLength;
    if (onChunk) onChunk(value);
  }
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    merged.set(part, offset);
    offset += part.byteLength;
  }
  return merged;
}
