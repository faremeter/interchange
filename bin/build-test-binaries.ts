#!/usr/bin/env bun
/* eslint-disable no-console */

// Bundle the sidecar and its two child binaries for the test lanes.
//
// The workflow-deploy pass spawns three fresh bun processes per test
// file: the sidecar subprocess, a one-shot workflow-probe child per
// deploy, and a workflow-process child per deploy. Each boots the
// whole `@intx/*` module graph, transpiling every TypeScript module at
// start. A pre-bundled single-file binary skips that per-module
// transpile, cutting the probe child's boot from ~0.6s to ~0.2s and
// the sidecar/child boots by ~0.1s each.
//
// The bundles are written to `apps/sidecar/dist-test/` (gitignored).
// The spawners (`workflow-child-spawner.ts`, `workflow-probe-spawner.ts`)
// and the test harness (`tests/hub-agent/lib/deploy-flow-env.ts`) prefer
// a bundle when present and fall back to the source binaries otherwise,
// so a worktree that never runs this step keeps the old behavior.
//
// `make build-test-binaries` runs this script; `make test-workflow`
// depends on it, so `make test` and `make all` always use the bundles.

import { cp, mkdir, rename, rm } from "node:fs/promises";
import path from "node:path";

const repoRoot = path.resolve(import.meta.dir, "..");
const sidecarDir = path.join(repoRoot, "apps", "sidecar");
const binDir = path.join(sidecarDir, "bin");
const srcDir = path.join(sidecarDir, "src");
const outDir = path.join(sidecarDir, "dist-test");

// The children are extensionless shebang binaries; bun build needs a
// resolvable entry name, so each is copied to a `.ts` temp inside the
// bin dir (the binaries import `../src/...` relative to `bin/`), built,
// then deleted. The temp names must not collide with the checked-in
// binaries.
const ENTRY_TEMPS = {
  "workflow-child": "workflow-child-bundle-entry.ts",
  "workflow-probe-child": "workflow-probe-child-bundle-entry.ts",
} as const;

async function buildEntry(entry: string, outputName: string): Promise<void> {
  const built = await Bun.build({
    entrypoints: [entry],
    target: "bun",
    format: "esm",
    conditions: ["intx-src"],
    outdir: outDir,
  });
  const artifact = built.outputs[0];
  if (artifact === undefined) {
    throw new Error(`build-test-binaries: ${entry} produced no output`);
  }
  // The output keeps the entry basename; rename it to the stable name
  // the spawners resolve.
  const produced = path.join(outDir, path.basename(artifact.path));
  const target = path.join(outDir, outputName);
  if (produced !== target) {
    await rename(produced, target);
  }
}

await rm(outDir, { recursive: true, force: true });
await mkdir(outDir, { recursive: true });

try {
  for (const [binary, tempName] of Object.entries(ENTRY_TEMPS)) {
    await cp(path.join(binDir, binary), path.join(binDir, tempName));
  }
  await Promise.all([
    buildEntry(path.join(srcDir, "index.ts"), "index.js"),
    buildEntry(
      path.join(binDir, ENTRY_TEMPS["workflow-child"]),
      "workflow-child.js",
    ),
    buildEntry(
      path.join(binDir, ENTRY_TEMPS["workflow-probe-child"]),
      "workflow-probe-child.js",
    ),
  ]);
} finally {
  for (const tempName of Object.values(ENTRY_TEMPS)) {
    await rm(path.join(binDir, tempName), { force: true });
  }
}

console.log(
  `build-test-binaries: wrote ${outDir} (sidecar, workflow-child, workflow-probe-child)`,
);
