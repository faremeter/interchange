// eslint-disable-next-line @typescript-eslint/triple-slash-reference -- npm-team packages ship no types; declarations.d.ts must be visible to downstream typecheckers that import from this package's source.
/// <reference path="./declarations.d.ts" />
// Single source of truth for "open an npm-style tarball, find the
// package/package.json entry, parse it as JSON, hand the parsed value
// back". Used by the hub-side resolver (`AssetRegistrySource` builds
// packuments from asset-stored tarballs) and the hub-sessions package
// (the `package-registry` kind handler validates uploads before the
// commit is accepted). Both call sites used to ship near-identical
// streaming parsers; the duplication drifted independently.
//
// The helper returns a discriminated outcome rather than throwing so
// each caller can construct its own domain-shaped error
// (ManifestInvalidError on the resolver side, ValidatePushResult
// reason string on the kind-handler side) without losing the
// failure-class distinction.

// This module is Node-bound: it streams through node:stream and the tar
// library, which are not portable to environments without those APIs.

import { Readable } from "node:stream";

import { type } from "arktype";
import { Parser as TarParser } from "tar/parse";
import type { ReadEntry } from "tar/read-entry";

import { concatBytes } from "@intx/types";
import { PackageJSON } from "@intx/types/package-json";

/**
 * Outcome of extracting the top-level `package.json` entry from an
 * npm-style tarball.
 *
 * `kind: "ok"` carries a value already validated against `PackageJSON`;
 * `kind: "shape-invalid"` distinguishes "JSON parsed cleanly but did
 * not match the expected schema" from "the JSON itself was malformed"
 * (`kind: "json-error"`). Callers that ship their own domain error type
 * can map the rejected outcomes onto whatever shape they raise.
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
 * level `package.json` member, JSON.parse the collected bytes, run them
 * through `PackageJSON`, and return a discriminated outcome. The bytes
 * argument may be any Uint8Array (tarball or gzipped tarball —
 * `tar.Parser` auto-detects gzip on the input stream).
 *
 * The tar entry is matched by its tail (`<segment>/package.json` with
 * exactly two segments) rather than the literal `package/package.json`
 * path because the sidecar's tarball extractor uses `strip:1` and
 * therefore accepts any first segment. Matching by tail here keeps the
 * hub's validation aligned with the sidecar's runtime contract — a
 * tarball whose top-level directory is not literally `package/` will
 * load identically in both places.
 *
 * On a tar parser failure the upstream readable is destroyed so a
 * malformed archive does not leave a half-drained source buffered in
 * the parser's internal state.
 *
 * The raw parsed JSON is surfaced alongside the validated descriptor on
 * success so callers that need fields outside `PackageJSON`'s minimum
 * schema (the resolver's `readDependencyFields` consumes `dependencies`,
 * `optionalDependencies`, etc.) can read them without re-extracting.
 */
export async function extractTarballPackageJSON(
  bytes: Uint8Array,
): Promise<ExtractPackageJSONOutcome> {
  return new Promise<ExtractPackageJSONOutcome>((resolve) => {
    let resolved = false;
    let pkgJsonBuf: Uint8Array | null = null;
    const collectChunks: Uint8Array[] = [];
    // Capture every top-level `<seg>/package.json` path we see during
    // the walk so the kind handler can reject ambiguous archives. The
    // hub's resolver and the sidecar's tarball extractor resolve
    // collisions differently — the resolver via this helper captures
    // the first occurrence, while the sidecar's `tar.extract` with
    // `strip:1` overwrites on every subsequent path with the same
    // stripped name. A tarball carrying multiple top-level package
    // directories therefore validates against the first entry on the
    // hub but loads the last entry on the sidecar; the divergence is
    // resolved at the validation boundary by refusing the upload.
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
