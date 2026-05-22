// Shared `MainOptions` shape for the agent-* example CLIs.
//
// Every example accepts the same four test seams (stdout/stderr
// sinks, inference Dependencies, contextDir override) plus
// example-specific extras. Centralising the base shape here lets
// each example express only what is unique about its `main()`
// surface.

import type { Dependencies } from "@intx/inference";
import type { InferenceSource } from "@intx/types/runtime";

/**
 * Test-seam options every agent-* example's `main()` accepts:
 * stdout/stderr write sinks (defaulted to the real process streams
 * via `resolveStdio`), the inference Dependencies hook tests use to
 * swap fetch, and an optional contextDir override.
 */
export type CommonMainOptions = {
  stdout?: (chunk: string) => void;
  stderr?: (chunk: string) => void;
  /** Inject inference deps (used by the harness-driven tests). */
  deps?: Dependencies;
  /** Override the default `tmp/<example-name>/context/` location. */
  contextDir?: string;
};

/**
 * `CommonMainOptions` plus the `sourceOverride` test seam. Seven
 * of the eight agent-* examples wire a single inference source and use
 * this shape directly. The multi-provider example extends
 * `CommonMainOptions` with its own primary/fallback overrides.
 */
export type SingleSourceMainOptions = CommonMainOptions & {
  /** Skip env resolution and use this source directly. */
  sourceOverride?: InferenceSource;
};

/**
 * Resolve the stdout/stderr write functions an example's `main()`
 * should use. Production paths default to `process.stdout.write` /
 * `process.stderr.write`; tests pass their own collectors.
 */
export function resolveStdio(opts: CommonMainOptions): {
  stdout: (chunk: string) => void;
  stderr: (chunk: string) => void;
} {
  return {
    stdout: opts.stdout ?? ((s) => void process.stdout.write(s)),
    stderr: opts.stderr ?? ((s) => void process.stderr.write(s)),
  };
}
