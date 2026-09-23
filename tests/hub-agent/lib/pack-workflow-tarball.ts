// Pack a single-package workflow definition into an npm-shaped tarball: a
// `package/` root holding a `package.json` that declares the
// `interchange.workflow` entry, plus the bundled entry module. The bytes are
// what a `package-registry` asset publishes under `tarballs/`, and the
// filename follows npm's `<basename>-<version>.tgz` convention.

import { promises as fs } from "node:fs";
import path from "node:path";

import * as tar from "tar";

export type WorkflowTarball = {
  bytes: Uint8Array;
  tarballFilename: string;
};

export async function packWorkflowTarball(opts: {
  scratchDir: string;
  name: string;
  version: string;
  entry: string;
  workflowJs: string;
}): Promise<WorkflowTarball> {
  const packageDir = path.join(opts.scratchDir, "package");
  await fs.mkdir(packageDir, { recursive: true });
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: opts.name,
      version: opts.version,
      interchange: { workflow: opts.entry },
    }),
  );
  await fs.writeFile(
    path.join(packageDir, path.basename(opts.entry)),
    opts.workflowJs,
  );
  const tarballPath = path.join(opts.scratchDir, "out.tgz");
  await tar.create({ cwd: opts.scratchDir, gzip: true, file: tarballPath }, [
    "package",
  ]);
  const basename = opts.name.replace(/^@[^/]+\//, "");
  return {
    bytes: await fs.readFile(tarballPath),
    tarballFilename: `${basename}-${opts.version}.tgz`,
  };
}
