// Compat-replay: drive a captured response through a registered provider
// adapter and apply invariants over the resulting `InferenceEvent[]`.
// Captured wire bytes are the fixed input; the event stream is recomputed
// fresh on every run, so the only way a compat-replay test fails is a
// code-side change.
//
// Scope today: single-turn streaming SSE captures only. The helper skips
// non-streaming captures and providers without a registered adapter.
// Multi-turn captures, raw-bytes uploads, and the full SUPPORT_MATRIX
// iteration are extensions for which the necessary plumbing is not yet
// in place.
//
// Replay fidelity caveats:
//   - Real SSE arrives in many small TCP chunks; the stub fetch returns
//     the captured bytes as a single chunk. Parser chunk-boundary bugs
//     that depend on byte-level fragmentation will not be exercised here.
//   - Status code is hard-coded to 200. When the manifest carries the
//     captured status, the stub should switch to the replayed value so
//     error-path parsers are exercised.

import { promises as fs } from "node:fs";
import * as path from "node:path";

import {
  createDefaultDependencies,
  runInference,
  type Dependencies,
} from "@intx/inference";
import {
  SUPPORT_MATRIX,
  getFixtureDir,
  type SupportEntry,
} from "@intx/inference-discovery/catalog";
import type { InferenceEvent, InferenceSource } from "@intx/types/runtime";

import {
  INVARIANTS,
  type Invariant,
  type InvariantViolation,
} from "./invariants";

// Catalog provider name → registered adapter name. The catalog ("provider"
// in SUPPORT_MATRIX) and the adapter registry ("provider" in
// InferenceSource) use overlapping vocabularies for different concepts.
// This map is the one place that translates from the former to the latter.
const CATALOG_TO_ADAPTER: Record<string, string> = {
  "opencode-zen": "openai-compatible",
  anthropic: "anthropic",
};

export type CompatReplaySkipReason =
  | "no_adapter_registered"
  | "non_streaming_capture"
  | "raw_bytes_upload"
  | "non_captured_outcome";

export type CompatReplayResult =
  | {
      kind: "replayed";
      events: InferenceEvent[];
      violations: InvariantViolation[];
    }
  | { kind: "skipped"; reason: CompatReplaySkipReason };

export type CompatReplayOpts = {
  /**
   * Absolute path to a fixture directory containing `request.json`,
   * `response.sse` (or `response.json`), `response-headers.json`,
   * `manifest.json`. Catalog-relative paths from `getFixtureDir` must
   * be resolved by the caller via `path.resolve(workspaceRoot, …)`.
   */
  fixtureDir: string;
  /** Catalog provider name (matches `SupportEntry.provider`). */
  provider: string;
  /** Model id (matches `SupportEntry.model`). */
  model: string;
  /** Invariants to apply. Defaults to the canonical `INVARIANTS` list. */
  invariants?: readonly Invariant[];
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function fileExists(p: string): Promise<boolean> {
  try {
    await fs.access(p);
    return true;
  } catch (cause) {
    // Only `ENOENT` means "the file is not there." Anything else
    // (permission denied, IO error, EBUSY) is a real failure that the
    // caller should see — silently treating EACCES as "missing"
    // produces a misleading "fixture appears malformed" diagnostic when
    // the real problem is a chmod.
    const code =
      cause !== null && typeof cause === "object" && "code" in cause
        ? cause.code
        : undefined;
    if (code === "ENOENT") return false;
    throw cause;
  }
}

function headersFromRecord(record: Record<string, unknown>): Headers {
  const headers = new Headers();
  for (const [key, value] of Object.entries(record)) {
    if (typeof value === "string") {
      headers.set(key, value);
    } else if (Array.isArray(value)) {
      for (const v of value) {
        if (typeof v === "string") headers.append(key, v);
      }
    }
  }
  return headers;
}

export async function runCompatReplay(
  opts: CompatReplayOpts,
): Promise<CompatReplayResult> {
  // 1. Adapter lookup.
  const adapterName = CATALOG_TO_ADAPTER[opts.provider];
  if (adapterName === undefined) {
    return { kind: "skipped", reason: "no_adapter_registered" };
  }

  // 2. Locate response files. SSE only is in-scope today; a fixture
  //    that has neither is malformed and surfaces as a thrown error
  //    rather than a silent skip (skip is for "we don't handle this
  //    capability shape yet", not "the fixture is broken").
  const ssePath = path.join(opts.fixtureDir, "response.sse");
  const jsonPath = path.join(opts.fixtureDir, "response.json");
  const sseExists = await fileExists(ssePath);
  const jsonExists = await fileExists(jsonPath);
  if (!sseExists && !jsonExists) {
    throw new Error(
      `compat-replay: ${opts.fixtureDir} has neither response.sse nor response.json — fixture appears malformed`,
    );
  }
  if (!sseExists) {
    return { kind: "skipped", reason: "non_streaming_capture" };
  }

  // 3. Load the SSE bytes and the captured response headers. A fixture
  //    with a `response.sse` but no `response-headers.json` is the same
  //    class of malformed-fixture problem as the missing-response check
  //    above; surface symmetrically.
  const sseBytes = await fs.readFile(ssePath);
  const headersPath = path.join(opts.fixtureDir, "response-headers.json");
  if (!(await fileExists(headersPath))) {
    throw new Error(
      `compat-replay: ${opts.fixtureDir} carries response.sse but no response-headers.json — fixture appears malformed`,
    );
  }
  const headersJson = await fs.readFile(headersPath, "utf-8");
  const parsedHeaders: unknown = JSON.parse(headersJson);
  if (!isRecord(parsedHeaders)) {
    throw new Error(
      `compat-replay: ${headersPath} did not parse into a flat object`,
    );
  }
  const headers = headersFromRecord(parsedHeaders);
  // Force content-type to text/event-stream for SSE replay. Whatever the
  // capture serialized — including buggy captures that wrote
  // `application/json` against an SSE body — would otherwise let the
  // adapter mis-classify the response. The file-existence gate above
  // pinned this as a streaming capture; the header must match.
  headers.set("content-type", "text/event-stream");

  // 4. Synthetic source. `source.provider` is the lookup key the harness
  //    uses against the adapter registry — that's the ADAPTER NAME, not
  //    the catalog provider name.
  const source: InferenceSource = {
    id: `${opts.provider}:${opts.model}`,
    provider: adapterName,
    baseURL: "https://compat-replay.invalid",
    apiKey: "compat-replay-stub",
    model: opts.model,
  };

  // 5. Stub fetch — counts calls so a never-called or multi-called
  //    adapter surfaces as a loud failure rather than silently passing.
  let fetchCallCount = 0;
  const defaults = createDefaultDependencies();
  const deps: Dependencies = {
    ...defaults,
    fetch: (_input, _init) => {
      fetchCallCount++;
      return Promise.resolve(
        new Response(sseBytes, {
          status: 200,
          headers,
        }),
      );
    },
  };

  // 6. Run inference, collect events.
  let seq = 0;
  const events: InferenceEvent[] = [];
  for await (const event of runInference({
    turns: [
      {
        role: "user",
        content: [{ type: "text", text: "x" }],
        timestamp: 0,
      },
    ],
    source,
    nextSeq: () => ++seq,
    deps,
  })) {
    events.push(event);
  }

  if (fetchCallCount === 0) {
    throw new Error(
      `compat-replay: adapter for ${opts.provider} did not call fetch; the replay window was never opened`,
    );
  }
  if (fetchCallCount > 1) {
    throw new Error(
      `compat-replay: adapter for ${opts.provider} called fetch ${String(fetchCallCount)} times; single-turn replay only supports one call`,
    );
  }

  // 7. Apply invariants.
  const checks = opts.invariants ?? INVARIANTS;
  const violations: InvariantViolation[] = [];
  for (const invariant of checks) {
    violations.push(...invariant.check(events));
  }

  return { kind: "replayed", events, violations };
}

// ---------------------------------------------------------------------------
// Full-corpus iteration
//
// Walk every captured fixture in the discovery `SUPPORT_MATRIX` through
// the same replay pipeline. Multi-turn captures live under
// `<capability>/turn-1/`, `turn-2/`, …; files-api captures live under
// `<capability>/upload/`, `generate/`, where the upload directory
// carries `request.bin` (raw bytes) instead of `request.json` — those
// are out of scope for the inference layer and skip with
// `raw_bytes_upload`. Single-turn captures place `request.json`
// directly in the capability directory.
// ---------------------------------------------------------------------------

/**
 * A single replayable leaf within a capability's fixture tree.
 * `fixtureDir` is the leaf directory carrying `request.json` +
 * `response.sse` (or `response.json`); `subPath` is the relative
 * path from the capability root, used in result reporting to
 * distinguish multi-turn replays from each other.
 */
export type CompatReplayCorpusLeaf = {
  entry: SupportEntry;
  fixtureDir: string;
  /** Relative path from the capability root ("" for single-turn). */
  subPath: string;
};

/**
 * Result for a single corpus leaf: either the replay outcome or a
 * shaped reason for skipping. Aggregated across the full corpus by
 * `runCompatReplayCorpus`.
 */
export type CompatReplayCorpusResult = CompatReplayCorpusLeaf &
  (
    | {
        kind: "replayed";
        events: InferenceEvent[];
        violations: InvariantViolation[];
      }
    | { kind: "skipped"; reason: CompatReplaySkipReason }
  );

export type CompatReplayCorpusOpts = {
  /**
   * Absolute path of the workspace root. The catalog's
   * `getFixtureDir` returns workspace-relative paths; this option
   * resolves them.
   */
  workspaceRoot: string;
  /** Invariants to apply to each replay. Defaults to canonical INVARIANTS. */
  invariants?: readonly Invariant[];
  /**
   * Optional filter on support entries. Receives a candidate entry
   * before any fixture-walking; return `false` to skip. Useful for
   * narrowing the suite during development.
   */
  filter?: (entry: SupportEntry) => boolean;
};

async function findCapturedRequestDirs(
  capabilityRoot: string,
): Promise<{ subPath: string; rawBytesUpload: boolean }[]> {
  // Walk the capability directory looking for leaves with either
  // `request.json` or `request.bin`. Returns a sorted list (by
  // subPath) so iteration is deterministic.
  const leaves: { subPath: string; rawBytesUpload: boolean }[] = [];
  async function walk(dir: string, subPath: string): Promise<void> {
    let entries: string[];
    try {
      entries = await fs.readdir(dir);
    } catch (cause) {
      const code =
        cause !== null && typeof cause === "object" && "code" in cause
          ? cause.code
          : undefined;
      if (code === "ENOENT") return;
      throw cause;
    }
    // Check if this directory is itself a leaf (contains a request
    // file). If so, record it and don't descend further — leaves
    // don't nest.
    const hasRequestJson = entries.includes("request.json");
    const hasRequestBin = entries.includes("request.bin");
    if (hasRequestJson || hasRequestBin) {
      leaves.push({
        subPath,
        rawBytesUpload: hasRequestBin && !hasRequestJson,
      });
      return;
    }
    // Otherwise, descend into subdirectories in sorted order for
    // deterministic walk. Use stat to discriminate dirs from files.
    const subdirs: string[] = [];
    for (const name of entries.sort()) {
      const stat = await fs.stat(path.join(dir, name));
      if (stat.isDirectory()) subdirs.push(name);
    }
    for (const name of subdirs) {
      const nextSubPath = subPath === "" ? name : `${subPath}/${name}`;
      await walk(path.join(dir, name), nextSubPath);
    }
  }
  await walk(capabilityRoot, "");
  return leaves;
}

/**
 * Iterate every captured leaf in the discovery SUPPORT_MATRIX
 * through `runCompatReplay`. Returns a flat array of per-leaf
 * results — each carrying either a replay outcome (events,
 * violations) or a structured skip reason. Non-captured outcomes
 * (`misled`, `refused`, etc.) are skipped with `non_captured_outcome`;
 * providers without a registered adapter (currently `google-genai`)
 * skip with `no_adapter_registered`; raw-bytes upload leaves
 * (files-api `upload/`) skip with `raw_bytes_upload`; non-streaming
 * captures (response.json only) skip with `non_streaming_capture`.
 */
export async function runCompatReplayCorpus(
  opts: CompatReplayCorpusOpts,
): Promise<CompatReplayCorpusResult[]> {
  const results: CompatReplayCorpusResult[] = [];
  for (const entry of SUPPORT_MATRIX) {
    if (opts.filter && !opts.filter(entry)) continue;

    if (entry.outcome !== "captured") {
      results.push({
        entry,
        fixtureDir: "",
        subPath: "",
        kind: "skipped",
        reason: "non_captured_outcome",
      });
      continue;
    }

    const relDir = getFixtureDir(entry);
    if (relDir === null) {
      // Outcome was "captured" so getFixtureDir should return a path;
      // a null return here would mean the catalog and outcome model
      // disagree. Surface as a thrown error so the catalog discrepancy
      // is visible, not silently masked as a skip.
      throw new Error(
        `compat-replay corpus: captured entry has null fixture dir: ${entry.provider}/${entry.model}/${entry.capability}`,
      );
    }
    const capabilityRoot = path.resolve(opts.workspaceRoot, relDir);
    const leaves = await findCapturedRequestDirs(capabilityRoot);

    for (const leaf of leaves) {
      const fixtureDir = path.join(capabilityRoot, leaf.subPath);
      if (leaf.rawBytesUpload) {
        results.push({
          entry,
          fixtureDir,
          subPath: leaf.subPath,
          kind: "skipped",
          reason: "raw_bytes_upload",
        });
        continue;
      }
      const runOpts: CompatReplayOpts = {
        fixtureDir,
        provider: entry.provider,
        model: entry.model,
        ...(opts.invariants !== undefined
          ? { invariants: opts.invariants }
          : {}),
      };
      const result = await runCompatReplay(runOpts);
      if (result.kind === "skipped") {
        results.push({
          entry,
          fixtureDir,
          subPath: leaf.subPath,
          kind: "skipped",
          reason: result.reason,
        });
      } else {
        results.push({
          entry,
          fixtureDir,
          subPath: leaf.subPath,
          kind: "replayed",
          events: result.events,
          violations: result.violations,
        });
      }
    }
  }
  return results;
}
