import path from "node:path";
import fs from "node:fs/promises";
import type { Capability, CapabilityIntent } from "./catalog";
import { detectResponseKind } from "./content-type";
import { buildManifest } from "./manifest";
import type { CaptureStep, CapturedResponse, ProviderPlugin } from "./plugin";
import {
  writeCapture,
  type RequestBody,
  type ResponseBody,
  type WriteCaptureInput,
} from "./write-capture";

export type FetchLike = (
  input: string,
  init: {
    method: string;
    headers: Record<string, string>;
    body: string | Uint8Array;
  },
) => Promise<Response>;

export interface RunCaptureOpts {
  plugin: ProviderPlugin;
  model: string;
  capability: Capability;
  intent: CapabilityIntent;
  outDir: string;
  now?: () => Date;
  fetch?: FetchLike;
}

const REASONING_TRACE_CAPABILITY_PREFIXES = [
  "reasoning-content",
  "redacted-thinking",
] as const;

function shouldEmitReasoningTrace(capability: Capability): boolean {
  for (const prefix of REASONING_TRACE_CAPABILITY_PREFIXES) {
    if (capability.startsWith(prefix)) return true;
  }
  return false;
}

function headersToObject(headers: Headers): Record<string, string> {
  const out: Record<string, string> = {};
  headers.forEach((value, key) => {
    out[key] = value;
  });
  return out;
}

const defaultFetch: FetchLike = (input, init) =>
  fetch(input, {
    method: init.method,
    headers: init.headers,
    body: init.body,
  });

function mergeHeaders(
  defaults: Record<string, string>,
  stepHeaders: Record<string, string>,
  authHeaders: Record<string, string>,
): Record<string, string> {
  const authKeys = new Set(
    Object.keys(authHeaders).map((k) => k.toLowerCase()),
  );
  for (const key of Object.keys(stepHeaders)) {
    if (authKeys.has(key.toLowerCase())) {
      throw new Error(
        `capture step attempted to override plug-in auth header '${key}'; ` +
          `auth headers are plug-in-wide and cannot be overridden per step`,
      );
    }
  }
  // Default → step overrides default → auth wins over everything.
  return { ...defaults, ...stepHeaders, ...authHeaders };
}

function buildRequestForStep(
  step: CaptureStep,
  authHeaders: Record<string, string>,
): {
  method: string;
  headers: Record<string, string>;
  body: string | Uint8Array;
  request: RequestBody;
} {
  const method = step.method ?? "POST";
  const stepHeaders = step.headers ?? {};
  if (step.kind === "raw") {
    const headers = mergeHeaders(
      { "Content-Type": step.contentType },
      stepHeaders,
      authHeaders,
    );
    return {
      method,
      headers,
      body: step.body,
      request: { kind: "raw", bytes: step.body, contentType: step.contentType },
    };
  }
  const headers = mergeHeaders(
    { "Content-Type": "application/json" },
    stepHeaders,
    authHeaders,
  );
  return {
    method,
    headers,
    body: JSON.stringify(step.body),
    request: { kind: "json", body: step.body },
  };
}

async function captureStep(args: {
  step: CaptureStep;
  outDir: string;
  plugin: ProviderPlugin;
  capability: Capability;
  doFetch: FetchLike;
}): Promise<CapturedResponse> {
  const { step, outDir, plugin, capability, doFetch } = args;

  const stepDir =
    step.subdir === null ? outDir : path.join(outDir, step.subdir);

  const authHeaders = plugin.buildAuthHeaders();
  const {
    method,
    headers: requestHeaders,
    body,
    request,
  } = buildRequestForStep(step, authHeaders);

  const response = await doFetch(step.url, {
    method,
    headers: requestHeaders,
    body,
  });

  const responseHeaders = headersToObject(response.headers);
  const kind = detectResponseKind(response.headers);

  let captured: ResponseBody;
  let parsedForGenerator: unknown | null;
  let bytesForGenerator: Uint8Array | null;
  if (kind === "sse") {
    const buf = await response.arrayBuffer();
    const bytes = new Uint8Array(buf);
    captured = { kind: "sse", bytes };
    parsedForGenerator = null;
    bytesForGenerator = bytes;
  } else {
    const text = await response.text();
    const parsed: unknown = JSON.parse(text);
    captured = { kind: "json", body: parsed };
    parsedForGenerator = parsed;
    bytesForGenerator = null;
  }

  const captureInput: WriteCaptureInput = {
    request,
    requestHeaders,
    redactRequestHeaders: plugin.redactRequestHeaders,
    response: captured,
    responseHeaders,
    redactResponseHeaders: plugin.redactResponseHeaders,
  };

  await writeCapture(stepDir, captureInput);

  if (
    plugin.extractReasoningTrace !== undefined &&
    shouldEmitReasoningTrace(capability) &&
    parsedForGenerator !== null
  ) {
    const trace = plugin.extractReasoningTrace(parsedForGenerator);
    if (trace !== null) {
      await fs.writeFile(
        path.join(stepDir, "reasoning-trace.json"),
        `${JSON.stringify(trace, null, 2)}\n`,
      );
    }
  }

  return {
    status: response.status,
    headers: responseHeaders,
    parsed: parsedForGenerator,
    bytes: bytesForGenerator,
  };
}

export async function runCapture(opts: RunCaptureOpts): Promise<void> {
  const { plugin, model, capability, intent, outDir } = opts;
  const doFetch = opts.fetch ?? defaultFetch;

  const iterator = plugin.iterateCaptureSteps({ model, capability, intent });

  let stepsExecuted = 0;
  let iterResult = iterator.next();
  while (!iterResult.done) {
    const captured = await captureStep({
      step: iterResult.value,
      outDir,
      plugin,
      capability,
      doFetch,
    });
    stepsExecuted += 1;
    iterResult = iterator.next(captured);
  }

  if (stepsExecuted === 0) {
    throw new Error(
      `plug-in ${plugin.name} produced no capture steps for ${model}/${capability}`,
    );
  }

  const manifestOpts: Parameters<typeof buildManifest>[0] = {
    provider: plugin.name,
    model,
    capability,
  };
  if (opts.now !== undefined) {
    manifestOpts.now = opts.now;
  }
  const manifest = buildManifest(manifestOpts);

  await fs.mkdir(outDir, { recursive: true });
  await fs.writeFile(
    path.join(outDir, "manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
}
