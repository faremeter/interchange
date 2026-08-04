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

import { Capability } from "./capability";

export const CaptureManifest = type({
  schemaVersion: "'2'",
  source: type({
    provider: "string",
    model: "string",
    baseURL: "string",
  }),
  // Distinguishes a capture recorded against a real provider endpoint
  // ("live") from one produced through the recording harness's synthetic wire
  // DSL fetch seam ("synthetic"). Derived at write time from whether a fetch
  // override was supplied — never accepted as a separate input, so it cannot
  // contradict the seam that actually produced the bytes.
  origin: "'live' | 'synthetic'",
  // Present for discovery-derived captures (each is a catalog cell); absent
  // for orchestration recordings, which are not catalog cells. Kept optional
  // deliberately: the two populations are separated by directory location,
  // not by this field, so the catalog — the layer that walks the discovery
  // corpus — is where a manifest missing its capability must be rejected, not
  // this shared type.
  "capability?": Capability,
  // The provider-reported model version observed at capture time, when the
  // provider surfaces one distinct from the requested `source.model`.
  "observedModelVersion?": "string | null",
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
