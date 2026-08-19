// Bundle a workflow source entry module into one self-contained `.mjs`, with
// every `@intx/*` import rewritten to its on-disk source. The sidecar-
// materialized closure evaluates the bundle with no bare `@intx/` import left
// to resolve, so a code-sourced deploy needs no published workspace packages.
//
// Promoted out of `tests/workflow-deploy/source-workflow.e2e.test.ts` so every
// code-sourced deploy test and the shared `deployWorkflowSourceForTest` helper
// share one bundler.

import { promises as fs } from "node:fs";
import path from "node:path";
import { dirname } from "node:path";

// This module lives at `tests/hub-agent/lib/`, so the repo root is three
// directories up. The bundler plugin uses it as the fallback resolve base and
// to detect an importer that already sits inside the repo tree.
const repoRoot = path.resolve(import.meta.dir, "..", "..", "..");

export async function bundleWorkflowEntry(
  scratchDir: string,
  entrySource: string,
): Promise<string> {
  const entrySrcPath = path.join(scratchDir, "source-workflow-entry-src.ts");
  await fs.writeFile(entrySrcPath, entrySource);

  const built = await Bun.build({
    entrypoints: [entrySrcPath],
    target: "bun",
    format: "esm",
    throw: true,
    plugins: [
      {
        name: "resolve-intx-to-source",
        setup(build) {
          build.onResolve({ filter: /^@intx\// }, (args) => {
            const fromDir = args.importer.startsWith(`${repoRoot}${path.sep}`)
              ? dirname(args.importer)
              : repoRoot;
            return { path: Bun.resolveSync(args.path, fromDir) };
          });
        },
      },
    ],
  });

  const artifact = built.outputs[0];
  if (artifact === undefined) {
    throw new Error("bundleWorkflowEntry: Bun.build produced no output");
  }
  const code = await artifact.text();
  if (code.includes("@intx/")) {
    throw new Error(
      "bundleWorkflowEntry: bundle still carries a bare @intx import",
    );
  }
  return code;
}
