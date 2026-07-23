import type { Capability, CapabilityIntent } from "./catalog";

export interface CapturedResponse {
  status: number;
  headers: Record<string, string>;
  // Populated for application/json responses; null for SSE.
  parsed: unknown | null;
  // Populated for text/event-stream responses; null otherwise. Iterators
  // that consume streaming turn-1 responses to build a turn-2 body parse
  // these bytes themselves — the runner does not interpret SSE.
  bytes: Uint8Array | null;
}

export type Turn1Reconstructor = (bytes: Uint8Array) => unknown;

// Resolves turn-1's assistant response for a multi-turn capture. A non-streaming
// turn-1 arrives as a parsed JSON body; a streaming turn-1 arrives as SSE bytes
// that the provider reconstructs into the response shape its turn-2 builder
// expects. Enforces CapturedResponse's parsed-XOR-bytes invariant: exactly one
// of parsed/bytes is non-null, so a response carrying neither is malformed and
// throws.
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
  // Subdirectory under the capture root for this step's artifacts.
  // null places the step's files directly under the capture root; a string
  // segregates them (e.g. "turn-1", "turn-2", "upload", "generate").
  subdir: string | null;
  url: string;
  // Defaults to "POST" when omitted.
  method?: "POST" | "PUT" | "PATCH";
  // Headers the step contributes on top of the runner's content-type default.
  // Per-step headers may override the default content-type (for example,
  // a multipart upload). They MUST NOT collide with the plug-in's auth
  // headers — the runner detects that collision and throws, on the
  // principle that auth is a plug-in-wide invariant and capability-
  // specific overrides belong on the step.
  headers?: Record<string, string>;
}

// A step whose body is a JSON-serializable value. The runner writes it to
// `request.json` after JSON.stringify and sends it with the default
// `Content-Type: application/json` unless overridden via `headers`.
export interface JsonCaptureStep extends CaptureStepBase {
  kind: "json";
  body: unknown;
}

// A step whose body is raw bytes (e.g. a multipart upload envelope, a
// single-part octet-stream). The runner writes the bytes to `request.bin`
// and sends them verbatim with the supplied `contentType`. The plug-in
// owns content-type because there is no sensible default for non-JSON
// bodies.
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
  extractReasoningTrace?(parsed: unknown): unknown | null;
  iterateCaptureSteps(
    opts: IterateCaptureStepsOpts,
  ): Generator<CaptureStep, void, CapturedResponse>;
}
