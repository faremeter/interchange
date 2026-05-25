// Session-level manifest: the single file at a session capture's root.
//
// Carries only facts that describe the session as a whole — never a
// catalog of its contents. Everything else (which exchanges exist, what
// tools dispatched, in what order) is discoverable from the directory
// layout. A catalog at the session root would go stale the moment
// someone added or removed a file; the filesystem walk cannot lie.
//
// The session schema version is independent of the per-exchange capture
// format from @intx/inference-discovery so the two evolve separately —
// a v2 session layout doesn't force a discovery format change and vice
// versa.

import fs from "node:fs/promises";
import path from "node:path";
import { type } from "arktype";

export const SessionManifest = type({
  sessionSchemaVersion: "'1'",
  source: type({
    provider: "string",
    model: "string",
    baseURL: "string",
  }),
  capturedAt: "string",
});
export type SessionManifest = typeof SessionManifest.infer;

const MANIFEST_FILENAME = "session.json";

export async function loadSessionManifest(
  sessionDir: string,
): Promise<SessionManifest> {
  const manifestPath = path.join(sessionDir, MANIFEST_FILENAME);
  const text = await fs.readFile(manifestPath, "utf-8");
  const parsed: unknown = JSON.parse(text);
  const validated = SessionManifest(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `Invalid session manifest at ${manifestPath}: ${validated.summary}`,
    );
  }
  return validated;
}

export async function writeSessionManifest(
  sessionDir: string,
  manifest: SessionManifest,
): Promise<void> {
  const validated = SessionManifest(manifest);
  if (validated instanceof type.errors) {
    throw new Error(
      `Refusing to write invalid session manifest: ${validated.summary}`,
    );
  }
  await fs.mkdir(sessionDir, { recursive: true });
  await fs.writeFile(
    path.join(sessionDir, MANIFEST_FILENAME),
    `${JSON.stringify(validated, null, 2)}\n`,
  );
}
