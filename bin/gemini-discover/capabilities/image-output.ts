import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { getLogger } from "@intx/log";

import {
  buildMetadata,
  fixtureDirectoryFor,
  headersToMap,
  redactRequestHeaders,
  writeFixture,
} from "../capture.ts";
import type { Capability } from "./index.ts";

const logger = getLogger(["gemini-discover", "image-output"]);

const NAME = "image-output";
const MODEL = "gemini-2.5-flash-image";
const ENDPOINT = "generateContent";

const GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta/models";

const REQUEST_BODY = {
  contents: [
    {
      role: "user",
      parts: [
        {
          text: "Generate a small illustration of a red apple on a white background.",
        },
      ],
    },
  ],
  generationConfig: {
    responseModalities: ["TEXT", "IMAGE"],
  },
};

export const capability: Capability = {
  name: NAME,
  model: MODEL,
  endpoint: ENDPOINT,
  build: async ({ apiKey, scriptVersion }) => {
    const url = `${GEMINI_BASE}/${MODEL}:${ENDPOINT}`;
    const requestHeaders = {
      "content-type": "application/json",
      "x-goog-api-key": apiKey,
    };

    logger.info`POST ${url}`;

    const response = await fetch(url, {
      method: "POST",
      headers: requestHeaders,
      body: JSON.stringify(REQUEST_BODY),
    });

    const responseHeaders = headersToMap(response.headers);
    const text = await response.text();

    let parsedBody: unknown;
    let parseError: string | null = null;
    try {
      parsedBody = JSON.parse(text);
    } catch (cause) {
      parsedBody = undefined;
      parseError =
        cause instanceof Error ? cause.message : "unknown JSON parse error";
    }

    if (response.ok) {
      if (parsedBody === undefined) {
        throw new Error(
          `image-output: 2xx response did not parse as JSON: ${parseError ?? "no parsed body"}: ${text}`,
        );
      }
      await writeFixture({
        capability: NAME,
        model: MODEL,
        endpoint: ENDPOINT,
        scriptVersion,
        requestBody: REQUEST_BODY,
        requestHeaders,
        responseHeaders,
        responseJson: parsedBody,
      });
      return;
    }

    logger.warn`image-output non-2xx (${String(response.status)} ${response.statusText}); capturing failure fixture verbatim`;

    const dir = fixtureDirectoryFor(NAME);
    await mkdir(dir, { recursive: true });

    const requestHeadersRedacted = redactRequestHeaders(requestHeaders);

    await writeFile(
      join(dir, "request.json"),
      JSON.stringify(REQUEST_BODY, null, 2) + "\n",
    );
    await writeFile(
      join(dir, "request-headers.json"),
      JSON.stringify(requestHeadersRedacted, null, 2) + "\n",
    );
    await writeFile(
      join(dir, "response-headers.json"),
      JSON.stringify(responseHeaders, null, 2) + "\n",
    );

    if (parsedBody !== undefined) {
      await writeFile(
        join(dir, "response.json"),
        JSON.stringify(parsedBody, null, 2) + "\n",
      );
    } else {
      await writeFile(join(dir, "response.body.txt"), text);
    }

    const baseMetadata = buildMetadata({
      capability: NAME,
      model: MODEL,
      endpoint: ENDPOINT,
      scriptVersion,
    });
    const metadata = {
      ...baseMetadata,
      error: {
        httpStatus: response.status,
        httpStatusText: response.statusText,
        ...(parseError ? { jsonParseError: parseError } : {}),
        bodySnippet: text.slice(0, 500),
      },
    };
    await writeFile(
      join(dir, "metadata.json"),
      JSON.stringify(metadata, null, 2) + "\n",
    );
  },
};
