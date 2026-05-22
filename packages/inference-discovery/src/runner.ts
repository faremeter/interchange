import path from "node:path";
import fs from "node:fs/promises";
import type { Capability, CapabilityIntent } from "./catalog";
import { detectResponseKind } from "./content-type";
import { buildManifest } from "./manifest";
import type { CaptureStep, CapturedResponse, ProviderPlugin } from "./plugin";
import {
  writeCapture,
  type ResponseBody,
  type WriteCaptureInput,
} from "./write-capture";

export type FetchLike = (
  input: string,
  init: { method: string; headers: Record<string, string>; body: string },
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
  const serializedBody = JSON.stringify(step.body);

  const authHeaders = plugin.buildAuthHeaders();
  const requestHeaders: Record<string, string> = {
    "Content-Type": "application/json",
    ...authHeaders,
  };

  const response = await doFetch(step.url, {
    method: "POST",
    headers: requestHeaders,
    body: serializedBody,
  });

  const responseHeaders = headersToObject(response.headers);
  const kind = detectResponseKind(response.headers);

  let captured: ResponseBody;
  let parsedForGenerator: unknown | null;
  if (kind === "sse") {
    const buf = await response.arrayBuffer();
    captured = { kind: "sse", bytes: new Uint8Array(buf) };
    parsedForGenerator = null;
  } else {
    const text = await response.text();
    const parsed: unknown = JSON.parse(text);
    captured = { kind: "json", body: parsed };
    parsedForGenerator = parsed;
  }

  const captureInput: WriteCaptureInput = {
    request: step.body,
    requestHeaders,
    redactRequestHeaders: plugin.redactRequestHeaders,
    response: captured,
    responseHeaders,
    redactResponseHeaders: plugin.redactResponseHeaders,
  };

  await writeCapture(stepDir, captureInput);

  if (
    plugin.extractReasoningTrace !== undefined &&
    capability.startsWith("reasoning-content") &&
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
