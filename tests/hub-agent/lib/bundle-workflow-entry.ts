// Bundle a workflow source entry module into one self-contained `.mjs`, with
// every `@intx/*` import rewritten to its on-disk source, so a code-sourced
// deploy needs no published workspace packages.

import { promises as fs } from "node:fs";
import path from "node:path";
import { dirname } from "node:path";

// This module lives at `tests/hub-agent/lib/`, so the repo root is three
// directories up; the plugin uses it as the fallback resolve base.
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
  // A workflow's own source legitimately carries `@intx/` string values (a
  // tool bundle id, a package name), so a substring check would false-positive.
  // Detect an unresolved bare specifier instead: an `import`/`export ... from`,
  // a dynamic `import(...)`, or a `require(...)` still starting with `@intx/`.
  const BARE_INTX_SPECIFIER = /(?:from|import|require)\s*\(?\s*["'`]@intx\//;
  if (BARE_INTX_SPECIFIER.test(code)) {
    throw new Error(
      "bundleWorkflowEntry: bundle still carries a bare @intx import",
    );
  }
  return code;
}
