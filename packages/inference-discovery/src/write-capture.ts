import fs from "node:fs/promises";
import path from "node:path";

export type ResponseBody =
  | { kind: "json"; body: unknown }
  | { kind: "sse"; bytes: Uint8Array };

export interface WriteCaptureInput {
  request: unknown;
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

  await fs.writeFile(
    path.join(dir, "request.json"),
    `${JSON.stringify(input.request, null, 2)}\n`,
  );
  await fs.writeFile(
    path.join(dir, "request-headers.json"),
    `${JSON.stringify(redactedRequest, null, 2)}\n`,
  );
  await fs.writeFile(
    path.join(dir, "response-headers.json"),
    `${JSON.stringify(redactedResponse, null, 2)}\n`,
  );

  if (input.response.kind === "json") {
    await fs.writeFile(
      path.join(dir, "response.json"),
      `${JSON.stringify(input.response.body, null, 2)}\n`,
    );
  } else {
    await fs.writeFile(path.join(dir, "response.sse"), input.response.bytes);
  }
}
