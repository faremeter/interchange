// Pack a single-package npm tarball: a `package/` root holding package.json
// plus one module. Stages under an internal mkdtemp so two packs cannot
// clobber a shared scratch dir. The filename follows npm's
// `<basename>-<version>.tgz` convention.

import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import * as tar from "tar";

export type SyntheticNpmPackageTarball = {
  bytes: Uint8Array;
  tarballFilename: string;
};

export async function buildSyntheticNpmPackageTarball(opts: {
  packageName: string;
  version: string;
  moduleFilename: string;
  moduleSource: string;
  manifest?: Record<string, unknown>;
}): Promise<SyntheticNpmPackageTarball> {
  const stagingDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "npm-pkg-fixture-"),
  );
  try {
    const packageDir = path.join(stagingDir, "package");
    await fs.mkdir(packageDir, { recursive: true });
    await fs.writeFile(
      path.join(packageDir, "package.json"),
      JSON.stringify({
        name: opts.packageName,
        version: opts.version,
        ...opts.manifest,
      }),
    );
    await fs.writeFile(
      path.join(packageDir, opts.moduleFilename),
      opts.moduleSource,
    );
    const tarballPath = path.join(stagingDir, "out.tgz");
    await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
      "package",
    ]);
    const basename = opts.packageName.replace(/^@[^/]+\//, "");
    return {
      bytes: new Uint8Array(await fs.readFile(tarballPath)),
      tarballFilename: `${basename}-${opts.version}.tgz`,
    };
  } finally {
    await fs.rm(stagingDir, { recursive: true, force: true });
  }
}
