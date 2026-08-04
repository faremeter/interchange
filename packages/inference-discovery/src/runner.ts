import path from "node:path";
import { detectResponseKind } from "@intx/types/content-type";
import {
  adapterForCatalogProvider,
  baseURLForCatalogProvider,
  writeCaptureManifest,
  type Capability,
  type CapabilityIntent,
  type CaptureManifest,
} from "./catalog";
import {
  extractDispatches,
  writeDispatches,
  type ReconstructedDispatch,
} from "./dispatch-reconstruction";
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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

const defaultNow = (): Date => new Date();

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
  exchangeDir: string;
  plugin: ProviderPlugin;
  doFetch: FetchLike;
}): Promise<CapturedResponse> {
  const { step, exchangeDir, plugin, doFetch } = args;

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
  // Read the body as raw bytes regardless of kind so the capture writes them
  // verbatim. For a JSON response we also parse the body: a multi-turn
  // iterator consumes the parsed turn's response (via CapturedResponse.parsed)
  // to build the next turn's request. The bytes written to disk are the
  // original network bytes, not a re-serialised parsed value.
  const buf = await response.arrayBuffer();
  const bytes = new Uint8Array(buf);
  if (kind === "sse") {
    captured = { kind: "sse", bytes };
    parsedForGenerator = null;
    bytesForGenerator = bytes;
  } else {
    const text = new TextDecoder().decode(bytes);
    const parsed: unknown = JSON.parse(text);
    captured = { kind: "json", bytes, parsed };
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

  await writeCapture(exchangeDir, captureInput);

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
  const now = opts.now ?? defaultNow;

  const adapterProvider = adapterForCatalogProvider(plugin.name);
  if (adapterProvider === undefined) {
    throw new Error(
      `runCapture: no adapter mapping for provider ${JSON.stringify(plugin.name)}`,
    );
  }
  const baseURL = baseURLForCatalogProvider(plugin.name);
  if (baseURL === undefined) {
    throw new Error(
      `runCapture: no base URL configured for provider ${JSON.stringify(plugin.name)}`,
    );
  }

  const iterator = plugin.iterateCaptureSteps({ model, capability, intent });

  // Exchanges are numbered by execution order. A capability is single-turn,
  // multi-turn, or files-api, and its iterator yields steps in transcript
  // order, so this counter reproduces the same exchange indices the
  // wire-to-session converter derives from leaf ordering. The final JSON
  // request holds the whole transcript, so its body is where tool dispatches
  // are reconstructed from.
  let exchangeIndex = 0;
  let firstStepURL: string | undefined;
  let finalRequestBody: unknown;
  let iterResult = iterator.next();
  while (!iterResult.done) {
    const step = iterResult.value;
    if (firstStepURL === undefined) firstStepURL = step.url;
    if (step.kind === "json") finalRequestBody = step.body;
    const captured = await captureStep({
      step,
      exchangeDir: path.join(outDir, "exchanges", String(exchangeIndex)),
      plugin,
      doFetch,
    });
    exchangeIndex += 1;
    iterResult = iterator.next(captured);
  }

  if (exchangeIndex === 0 || firstStepURL === undefined) {
    throw new Error(
      `plug-in ${plugin.name} produced no capture steps for ${model}/${capability}`,
    );
  }

  // The recorded base URL must be the endpoint the rig actually dialed. The
  // per-brand map supplies the canonical form (with any path prefix); assert
  // its origin matches the dialed URL so a stale map or a moved endpoint fails
  // loudly rather than recording a base URL that was never hit.
  const dialedOrigin = new URL(firstStepURL).origin;
  const recordedOrigin = new URL(baseURL).origin;
  if (dialedOrigin !== recordedOrigin) {
    throw new Error(
      `runCapture: provider ${JSON.stringify(plugin.name)} dialed ${dialedOrigin} ` +
        `but its recorded base URL origin is ${recordedOrigin}`,
    );
  }

  let dispatches: ReconstructedDispatch[] = [];
  if (finalRequestBody !== undefined) {
    if (!isRecord(finalRequestBody)) {
      throw new Error(
        `runCapture: the final request body for ${plugin.name} ${model}/${capability} is not a request object`,
      );
    }
    dispatches = extractDispatches(adapterProvider, finalRequestBody);
  }
  await writeDispatches(outDir, dispatches);

  const manifest: CaptureManifest = {
    schemaVersion: "2",
    source: { provider: adapterProvider, model, baseURL },
    origin: "live",
    capturedAt: now().toISOString(),
    capability,
  };
  await writeCaptureManifest(outDir, manifest);
}
