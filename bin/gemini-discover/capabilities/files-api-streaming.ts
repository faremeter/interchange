import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { type } from "arktype";

import { getLogger } from "@intx/log";

import {
  GEMINI_BASE,
  GEMINI_REDACT_HEADERS,
  buildGeminiHeaders,
  fixtureDirectoryFor,
  headersToMap,
  redactRequestHeaders,
  runStreamingStepCapture,
} from "../capture.ts";
import type { Capability } from "./index.ts";

const logger = getLogger(["gemini-discover", "files-api-streaming"]);

const NAME = "files-api-streaming";
const MODEL = "gemini-2.5-flash";
const ENDPOINT = "streamGenerateContent";
const MIME_TYPE = "application/pdf";

const ASSET_RELATIVE_PATH = "bin/gemini-discover/assets/sample.pdf";
const ASSET_URL = new URL("../assets/sample.pdf", import.meta.url);

const UPLOAD_URL =
  "https://generativelanguage.googleapis.com/upload/v1beta/files";
const UPLOAD_DISPLAY_NAME = "sample.pdf";

const URI_CONTRACT = "documentary";
const URI_TTL_HOURS = 48;

const UploadedFile = type({
  file: type({
    name: "string",
    uri: "string",
    mimeType: "string",
    "expirationTime?": "string",
  }),
});

async function captureUploadStep(args: {
  apiKey: string;
  capDir: string;
}): Promise<{ fileUri: string; expirationTime: string | undefined }> {
  const bytes = await readFile(fileURLToPath(ASSET_URL));

  const requestHeaders: Record<string, string> = {
    "x-goog-api-key": args.apiKey,
    "x-goog-upload-protocol": "raw",
    "x-goog-upload-file-name": UPLOAD_DISPLAY_NAME,
    "content-type": MIME_TYPE,
    "content-length": String(bytes.byteLength),
  };

  logger.info`POST ${UPLOAD_URL} (step=upload, bytes=${String(bytes.byteLength)})`;

  const response = await fetch(UPLOAD_URL, {
    method: "POST",
    headers: requestHeaders,
    body: new Uint8Array(bytes),
  });

  const responseHeaders = headersToMap(response.headers);
  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Gemini files upload failed: ${String(response.status)} ${response.statusText}: ${text}`,
    );
  }

  let responseJson: unknown;
  try {
    responseJson = JSON.parse(text);
  } catch (cause) {
    throw new Error(`Failed to parse files.upload response as JSON`, { cause });
  }

  const parsed = UploadedFile(responseJson);
  if (parsed instanceof type.errors) {
    throw new Error(
      `files.upload response did not match expected shape: ${parsed.summary}`,
    );
  }

  const requestDescriptor = {
    method: "POST",
    url: UPLOAD_URL,
    mimeType: MIME_TYPE,
    contentLength: bytes.byteLength,
    displayName: UPLOAD_DISPLAY_NAME,
    assetPath: ASSET_RELATIVE_PATH,
  };

  const stepDir = join(args.capDir, "upload");
  await mkdir(stepDir, { recursive: true });
  await writeFile(
    join(stepDir, "request.json"),
    JSON.stringify(requestDescriptor, null, 2) + "\n",
  );
  await writeFile(
    join(stepDir, "request-headers.json"),
    JSON.stringify(
      redactRequestHeaders(requestHeaders, GEMINI_REDACT_HEADERS),
      null,
      2,
    ) + "\n",
  );
  await writeFile(
    join(stepDir, "response.json"),
    JSON.stringify(responseJson, null, 2) + "\n",
  );
  await writeFile(
    join(stepDir, "response-headers.json"),
    JSON.stringify(responseHeaders, null, 2) + "\n",
  );

  return {
    fileUri: parsed.file.uri,
    expirationTime: parsed.file.expirationTime,
  };
}

export const capability: Capability = {
  name: NAME,
  model: MODEL,
  endpoint: ENDPOINT,
  build: async ({ apiKey, scriptVersion }) => {
    const capDir = fixtureDirectoryFor(NAME);
    await mkdir(capDir, { recursive: true });

    const { fileUri, expirationTime } = await captureUploadStep({
      apiKey,
      capDir,
    });
    logger.info`upload returned fileUri=${fileUri}`;

    const generateBody = {
      contents: [
        {
          role: "user",
          parts: [
            { text: "Summarize this PDF in one short sentence." },
            {
              fileData: {
                mimeType: MIME_TYPE,
                fileUri,
              },
            },
          ],
        },
      ],
    };

    await runStreamingStepCapture({
      capability: NAME,
      stepName: "generate",
      model: MODEL,
      endpoint: ENDPOINT,
      url: `${GEMINI_BASE}/${MODEL}:${ENDPOINT}?alt=sse`,
      requestHeaders: buildGeminiHeaders(apiKey),
      redactHeaderNames: GEMINI_REDACT_HEADERS,
      body: generateBody,
    });

    const metadata = {
      capability: NAME,
      model: MODEL,
      endpoint: ENDPOINT,
      capturedAt: new Date().toISOString(),
      scriptVersion,
      sequence: ["upload", "generate"],
      uriContract: URI_CONTRACT,
      uriTtlHours: URI_TTL_HOURS,
      uploadedFileUri: fileUri,
      uploadedFileExpiresAt: expirationTime ?? null,
      assetPath: ASSET_RELATIVE_PATH,
    };

    await writeFile(
      join(capDir, "metadata.json"),
      JSON.stringify(metadata, null, 2) + "\n",
    );
  },
};
