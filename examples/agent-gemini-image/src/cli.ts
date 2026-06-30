// agent-gemini-image: end-to-end Gemini image-generation example.
//
// Demonstrates the smallest runnable path that pulls an
// `inference.image_output` event off the Gemini adapter's event
// stream and writes the generated image bytes to disk. Reading
// this file should answer:
//
//   - How is `runInference` wired up against `gemini-2.5-flash-image`?
//   - What event type carries the image bytes (`inference.image_output`)?
//   - How do the streaming-text deltas interleave with the atomic
//     image part on the wire?
//   - What does the final `inference.done` turn look like when its
//     `content[]` carries an `ImageBlock`?
//
// The example uses `runInference` directly rather than the higher-
// level `@intx/agent` surface because `image_output` is a streaming
// event whose payload (typically a base64 image megabyte or so)
// belongs in a streaming consumer, not behind a single
// `agent.send()` await. A future `@intx/agent` capability could
// fan image events out as a side channel; this example shows what
// to consume in the meantime.

import { writeFileSync } from "node:fs";
import { join } from "node:path";

import { runInference } from "@intx/inference";
import { createDefaultDependencies } from "@intx/inference/providers";
import { base64Decode } from "@intx/types";
import type { InferenceEvent, InferenceSource } from "@intx/types/runtime";

export interface MainOptions {
  outputDir?: string;
  // Optional override for an injected fetch -- mostly useful for
  // integration tests that replay a captured fixture rather than
  // hitting the live endpoint.
  fetch?: typeof fetch;
}

export async function main(
  argv: string[],
  env: NodeJS.ProcessEnv,
  opts: MainOptions = {},
): Promise<number> {
  const prompt = argv.join(" ").trim();
  if (prompt === "") {
    process.stderr.write(
      "usage: bun run start <prompt for image generation>\n" +
        "  example: bun run start 'a small illustration of a red apple'\n",
    );
    return 1;
  }

  const apiKey = env.GEMINI_API_KEY;
  if (apiKey === undefined || apiKey === "") {
    process.stderr.write(
      "agent-gemini-image: GEMINI_API_KEY environment variable is not " +
        "set. Export it before running this example.\n",
    );
    return 1;
  }

  const outputDir = opts.outputDir ?? process.cwd();

  const source: InferenceSource = {
    id: "google-genai:gemini-2.5-flash-image",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    apiKey,
    model: "gemini-2.5-flash-image",
  };

  // `responseModalities: ["text", "image"]` is what tells the
  // image-capable model to emit `inlineData` parts alongside its
  // text deltas. Without it, Gemini returns only text and the
  // adapter never emits `inference.image_output`.
  const deps =
    opts.fetch !== undefined
      ? { ...createDefaultDependencies(), fetch: opts.fetch }
      : createDefaultDependencies();

  let seq = 0;
  let savedCount = 0;
  let imageEventCount = 0;

  for await (const ev of runInference({
    turns: [
      {
        role: "user",
        content: [{ type: "text", text: prompt }],
        timestamp: Date.now(),
      },
    ],
    source,
    nextSeq: () => seq++,
    deps,
    inferenceOptions: {
      responseModalities: ["text", "image"],
    },
  })) {
    handleEvent(
      ev,
      outputDir,
      () => {
        savedCount++;
      },
      () => {
        imageEventCount++;
      },
    );
  }

  if (imageEventCount === 0) {
    process.stderr.write(
      "agent-gemini-image: response carried no image. The model may " +
        "have refused or the responseModalities request was not honored.\n",
    );
    return 1;
  }

  process.stdout.write(
    `\nagent-gemini-image: saved ${String(savedCount)} image(s) to ` +
      `${outputDir}\n`,
  );
  return 0;
}

function handleEvent(
  ev: InferenceEvent,
  outputDir: string,
  onSaved: () => void,
  onImageEvent: () => void,
): void {
  switch (ev.type) {
    case "inference.text.delta": {
      // Stream the running text reply to stdout so the prompt's
      // narrative ("Here you go:") prints alongside the image
      // save. The model typically interleaves a short caption
      // with the image.
      process.stdout.write(ev.data.token);
      return;
    }
    case "inference.image_output": {
      onImageEvent();
      const src = ev.data.image.source;
      if (src.kind !== "base64") {
        // Gemini's image-output endpoint emits base64 inline
        // bytes; a file-reference or url variant would point at
        // a different model or a future endpoint shape this
        // example does not handle. Surface rather than guess.
        process.stderr.write(
          `agent-gemini-image: unsupported image source kind ` +
            `${JSON.stringify(src.kind)} (this example only writes ` +
            `base64 payloads to disk).\n`,
        );
        return;
      }
      // `index` is optional on the event schema but the Gemini
      // adapter always populates it (every image-output is at a
      // freshly-allocated block index). An absent index here is a
      // wire shape this example was not designed to handle --
      // falling back to a default would silently collide multiple
      // images on the same filename, so surface instead.
      const index = ev.data.index;
      if (index === undefined) {
        throw new Error(
          "agent-gemini-image: inference.image_output arrived without " +
            "an `index`; the Gemini adapter always populates this " +
            "field, so this example does not know how to anchor the " +
            "output filename.",
        );
      }
      const ext = mimeToExtension(src.mimeType);
      const filename = `gemini-image-${String(Date.now())}-${String(index)}${ext}`;
      const path = join(outputDir, filename);
      const bytes = base64Decode(src.data);
      writeFileSync(path, bytes);
      process.stdout.write(
        `\nagent-gemini-image: wrote ${String(bytes.length)} bytes ` +
          `to ${path}\n`,
      );
      onSaved();
      return;
    }
    case "inference.usage": {
      // Surface the token tally on stderr so it stays out of the
      // streaming text on stdout. Operators tuning prompts care
      // about this; pipeline consumers do not.
      process.stderr.write(
        `\nagent-gemini-image: usage input=${String(ev.data.usage.input)} ` +
          `output=${String(ev.data.usage.output)} ` +
          `thinking=${String(ev.data.usage.thinking)}\n`,
      );
      return;
    }
    case "inference.error": {
      process.stderr.write(
        `\nagent-gemini-image: error: ${ev.data.error.category}: ` +
          `${ev.data.error.message}\n`,
      );
      return;
    }
    // All other event kinds are noise for this example -- we
    // intentionally don't react to thinking, tool_call, or
    // citation events.
    default:
      return;
  }
}

function mimeToExtension(mime: string): string {
  switch (mime) {
    case "image/png":
      return ".png";
    case "image/jpeg":
      return ".jpg";
    case "image/webp":
      return ".webp";
    default: {
      // Strip the `image/` prefix and use whatever subtype is
      // left as the extension. A new image MIME the catalog has
      // not added yet still produces a usable filename. An empty
      // or prefixless mime would land us with a bare-dot
      // filename; reject loudly instead.
      const slash = mime.indexOf("/");
      if (slash < 0 || slash === mime.length - 1) {
        throw new Error(
          `agent-gemini-image: cannot derive a filename extension ` +
            `from mimeType ${JSON.stringify(mime)} (expected ` +
            `"image/<subtype>").`,
        );
      }
      return `.${mime.slice(slash + 1)}`;
    }
  }
}

if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
