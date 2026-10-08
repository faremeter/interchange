// eslint-disable-next-line @typescript-eslint/triple-slash-reference -- npm-team packages ship no types; declarations.d.ts must be visible to downstream typecheckers that import from this package's source.
/// <reference path="./declarations.d.ts" />
// Single source of truth for "open an npm-style tarball, find the
// package/package.json entry, parse it as JSON, hand the parsed value
// back". Used by the hub-side resolver and the hub-sessions
// `package-registry` kind handler, which used to ship near-identical
// streaming parsers that drifted independently. Returns a
// discriminated outcome rather than throwing so each caller can map
// the failure classes onto its own domain error type.
//
// Node-bound: streams through node:stream and tar, not portable to
// environments without those APIs.

import { Readable } from "node:stream";

import { type } from "arktype";
import { Parser as TarParser } from "tar/parse";
import type { ReadEntry } from "tar/read-entry";

import { concatBytes } from "@intx/types";
import { PackageJSON } from "@intx/types/package-json";

/**
 * Outcome of extracting the top-level `package.json` entry from an
 * npm-style tarball: validated against `PackageJSON` on `"ok"`;
 * `"shape-invalid"` (parsed but schema-mismatched) vs `"json-error"`
 * (malformed JSON).
 */
export type ExtractPackageJSONOutcome =
  | { kind: "ok"; parsed: PackageJSON; raw: unknown }
  | { kind: "missing-entry" }
  | { kind: "multiple-entries"; paths: string[] }
  | { kind: "parse-error"; message: string }
  | { kind: "json-error"; message: string }
  | { kind: "shape-invalid"; message: string; raw: unknown };

/**
 * Stream the tarball bytes through a tar parser, drain only the top-
 * level `package.json` member, JSON.parse it, validate against
 * `PackageJSON`, and return a discriminated outcome. Accepts tarball or
 * gzipped bytes (`tar.Parser` auto-detects gzip).
 *
 * The entry is matched by its two-segment tail (`<segment>/package.json`)
 * rather than the literal `package/` path because the sidecar's
 * extractor uses `strip:1` and accepts any first segment; matching by
 * tail keeps the hub's validation aligned with the sidecar's contract.
 *
 * On a tar parser failure the upstream readable is destroyed so a
 * malformed archive does not leave a half-drained source buffered in
 * the parser.
 *
 * The raw parsed JSON is surfaced alongside the validated descriptor so
 * callers can read fields outside `PackageJSON`'s minimum schema
 * without re-extracting.
 */
export async function extractTarballPackageJSON(
  bytes: Uint8Array,
): Promise<ExtractPackageJSONOutcome> {
  return new Promise<ExtractPackageJSONOutcome>((resolve) => {
    let resolved = false;
    let pkgJsonBuf: Uint8Array | null = null;
    const collectChunks: Uint8Array[] = [];
    // Capture every top-level `<seg>/package.json` path so the kind
    // handler can reject ambiguous archives. This helper captures the
    // first occurrence, but the sidecar's `tar.extract` with `strip:1`
    // overwrites on each subsequent same-named path — so a tarball with
    // multiple top-level package directories would validate against the
    // first entry but load the last; the validation boundary refuses
    // the upload instead.
    const topLevelPackageJSONPaths: string[] = [];

    const source = Readable.from([bytes]);

    const finalize = (outcome: ExtractPackageJSONOutcome): void => {
      if (resolved) return;
      resolved = true;
      resolve(outcome);
    };

    const parser = new TarParser();
    parser.on("entry", (entry: ReadEntry) => {
      const segments = entry.path.split("/");
      const isTopLevelPackageJSON =
        segments.length === 2 && segments[1] === "package.json";
      if (isTopLevelPackageJSON) {
        topLevelPackageJSONPaths.push(entry.path);
      }
      if (isTopLevelPackageJSON && pkgJsonBuf === null) {
        entry.on("data", (chunk: Uint8Array) => {
          collectChunks.push(chunk);
        });
        entry.on("end", () => {
          pkgJsonBuf = concatBytes(collectChunks);
        });
      } else {
        entry.resume();
      }
    });
    parser.on("error", (err: Error) => {
      source.destroy();
      finalize({ kind: "parse-error", message: err.message });
    });
    parser.on("end", () => {
      if (pkgJsonBuf === null) {
        finalize({ kind: "missing-entry" });
        return;
      }
      if (topLevelPackageJSONPaths.length > 1) {
        finalize({
          kind: "multiple-entries",
          paths: topLevelPackageJSONPaths,
        });
        return;
      }
      let raw: unknown;
      try {
        raw = JSON.parse(new TextDecoder().decode(pkgJsonBuf));
      } catch (cause) {
        finalize({
          kind: "json-error",
          message: cause instanceof Error ? cause.message : String(cause),
        });
        return;
      }
      const validated = PackageJSON(raw);
      if (validated instanceof type.errors) {
        finalize({ kind: "shape-invalid", message: validated.summary, raw });
        return;
      }
      finalize({ kind: "ok", parsed: validated, raw });
    });

    source.pipe(parser);
  });
}
