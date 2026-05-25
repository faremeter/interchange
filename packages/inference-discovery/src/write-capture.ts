import fs from "node:fs/promises";
import path from "node:path";

/**
 * Captured response body. The on-disk file (response.json or
 * response.sse) is written from `bytes` verbatim so the recording
 * is byte-identical to what the server sent — pretty-printing
 * a parsed JSON body would lose original key order, trailing
 * whitespace, and any content-length / signature semantics. The
 * optional `parsed` field is kept for the discovery rig's
 * extractReasoningTrace path, which needs the JSON-decoded value
 * to pull provider-specific metadata; writeCapture itself never
 * reads it.
 */
export type ResponseBody =
  | { kind: "json"; bytes: Uint8Array; parsed: unknown }
  | { kind: "sse"; bytes: Uint8Array };

export type RequestBody =
  | { kind: "json"; body: unknown }
  | { kind: "raw"; bytes: Uint8Array; contentType: string };

export interface WriteCaptureInput {
  request: RequestBody;
  requestHeaders: Record<string, string>;
  redactRequestHeaders: readonly string[];
  response: ResponseBody;
  responseHeaders: Record<string, string>;
  redactResponseHeaders: readonly string[];
}

const REDACTED = "<REDACTED>";

function applyRedaction(
  headers: Record<string, string>,
  redactNames: readonly string[],
): Record<string, string> {
  const lowered = new Set(redactNames.map((name) => name.toLowerCase()));
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    result[key] = lowered.has(key.toLowerCase()) ? REDACTED : value;
  }
  return result;
}

export async function writeCapture(
  dir: string,
  input: WriteCaptureInput,
): Promise<void> {
  await fs.mkdir(dir, { recursive: true });

  const redactedRequest = applyRedaction(
    input.requestHeaders,
    input.redactRequestHeaders,
  );
  const redactedResponse = applyRedaction(
    input.responseHeaders,
    input.redactResponseHeaders,
  );

  if (input.request.kind === "json") {
    await fs.writeFile(
      path.join(dir, "request.json"),
      `${JSON.stringify(input.request.body, null, 2)}\n`,
    );
  } else {
    await fs.writeFile(path.join(dir, "request.bin"), input.request.bytes);
  }
  await fs.writeFile(
    path.join(dir, "request-headers.json"),
    `${JSON.stringify(redactedRequest, null, 2)}\n`,
  );
  await fs.writeFile(
    path.join(dir, "response-headers.json"),
    `${JSON.stringify(redactedResponse, null, 2)}\n`,
  );

  if (input.response.kind === "json") {
    await fs.writeFile(path.join(dir, "response.json"), input.response.bytes);
  } else {
    await fs.writeFile(path.join(dir, "response.sse"), input.response.bytes);
  }
}
