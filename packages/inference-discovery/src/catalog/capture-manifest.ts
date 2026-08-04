// Capture-level manifest: the single manifest file at a capture directory's
// root.
//
// Carries only facts that describe the capture as a whole — never a catalog
// of its contents. Everything else (which exchanges exist, what tools
// dispatched, in what order) is discoverable from the directory layout. A
// catalog at the root would go stale the moment someone added or removed a
// file; the filesystem walk cannot lie.

import fs from "node:fs/promises";
import path from "node:path";
import { type } from "arktype";

export const CaptureManifest = type({
  schemaVersion: "'1'",
  source: type({
    provider: "string",
    model: "string",
    baseURL: "string",
  }),
  capturedAt: "string",
});
export type CaptureManifest = typeof CaptureManifest.infer;

const MANIFEST_FILENAME = "session.json";

export async function loadCaptureManifest(
  captureDir: string,
): Promise<CaptureManifest> {
  const manifestPath = path.join(captureDir, MANIFEST_FILENAME);
  const text = await fs.readFile(manifestPath, "utf-8");
  const parsed: unknown = JSON.parse(text);
  const validated = CaptureManifest(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `Invalid capture manifest at ${manifestPath}: ${validated.summary}`,
    );
  }
  return validated;
}

export async function writeCaptureManifest(
  captureDir: string,
  manifest: CaptureManifest,
): Promise<void> {
  const validated = CaptureManifest(manifest);
  if (validated instanceof type.errors) {
    throw new Error(
      `Refusing to write invalid capture manifest: ${validated.summary}`,
    );
  }
  await fs.mkdir(captureDir, { recursive: true });
  await fs.writeFile(
    path.join(captureDir, MANIFEST_FILENAME),
    `${JSON.stringify(validated, null, 2)}\n`,
  );
}
