import type { Capability, CapabilityIntent } from "./catalog";

export interface CapturedResponse {
  status: number;
  headers: Record<string, string>;
  // Populated for application/json responses; null for SSE.
  parsed: unknown | null;
  // Populated for text/event-stream responses; null otherwise. Iterators that
  // build a turn-2 body parse these bytes themselves — the runner does not
  // interpret SSE.
  bytes: Uint8Array | null;
}

export type Turn1Reconstructor = (bytes: Uint8Array) => unknown;

// Resolves turn-1's assistant response for a multi-turn capture: a parsed JSON
// body, or SSE bytes the provider reconstructs into the shape its turn-2
// builder expects. Enforces the parsed-XOR-bytes invariant — a response
// carrying neither is malformed and throws.
export function resolveTurn1Response(
  turn1: CapturedResponse,
  reconstruct: Turn1Reconstructor,
): unknown {
  if (turn1.parsed !== null) return turn1.parsed;
  if (turn1.bytes === null) {
    throw new Error(
      "resolveTurn1Response: CapturedResponse had neither a parsed body nor SSE bytes (violates the parsed-XOR-bytes invariant)",
    );
  }
  return reconstruct(turn1.bytes);
}

export interface IterateCaptureStepsOpts {
  model: string;
  capability: Capability;
  intent: CapabilityIntent;
}

interface CaptureStepBase {
  url: string;
  // Defaults to "POST" when omitted.
  method?: "POST" | "PUT" | "PATCH";
  // Extra headers on top of the runner's content-type default (may override
  // it, e.g. a multipart upload). MUST NOT collide with the plug-in's auth
  // headers — the runner detects that and throws; auth is plug-in-wide, and
  // capability-specific overrides belong on the step.
  headers?: Record<string, string>;
}

// A step whose body is a JSON-serializable value; the runner writes it to
// `request.json` and sends it with the default content-type unless overridden.
export interface JsonCaptureStep extends CaptureStepBase {
  kind: "json";
  body: unknown;
}

// A step whose body is raw bytes (e.g. a multipart upload envelope). The
// runner writes them to `request.bin` and sends them verbatim with the
// supplied `contentType`; the plug-in owns content-type because there is no
// sensible default for non-JSON bodies.
export interface RawCaptureStep extends CaptureStepBase {
  kind: "raw";
  contentType: string;
  body: Uint8Array;
}

export type CaptureStep = JsonCaptureStep | RawCaptureStep;

export interface ProviderPlugin {
  name: string;
  models: readonly string[];
  redactRequestHeaders: readonly string[];
  redactResponseHeaders: readonly string[];
  // Plug-in-wide credentials only. Capability-specific headers (beta
  // flags, upload-protocol markers) belong on the step's `headers` map.
  buildAuthHeaders(): Record<string, string>;
  iterateCaptureSteps(
    opts: IterateCaptureStepsOpts,
  ): Generator<CaptureStep, void, CapturedResponse>;
}
