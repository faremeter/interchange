// Determinism gate for `bin/build-builtins.ts`.
//
// The packer's contract is bit-identical output across runs and
// machines: same workspace inputs produce the same tarball bytes (and
// therefore the same SRI integrity) so the seed's pin set can refer to
// an exact version without coordinating with the build step. The
// gate boils down to three invariants:
//
//   - `listFilesSorted` walks the staging tree in lexical order so
//     `tar.create` writes a stable entry sequence regardless of
//     `fs.readdir` ordering.
//   - `normalizeStagingModes` rewrites mode bits to canonical values
//     before tar reads the entry stats, so two builders with different
//     umasks emit identical headers.
//   - `tar.create` is called with `{ mtime: epoch, portable: true,
//     noDirRecurse: true }`, zeroing per-entry mtimes and stripping
//     uid/gid/uname/gname.
//
// `packStaging` and `normalizeStagingModes` mirror the production
// helpers in `bin/build-builtins.ts` step-for-step. Keeping the test
// self-contained avoids dragging `bin/`'s tsconfig project into the
// test project graph, at the cost of having to keep the two copies in
// lockstep — if a future change rewrites the production packer, this
// test must follow.
//
// The umask test is the load-bearing piece: without
// `normalizeStagingModes`, two builders with different umasks produce
// distinct headers and the SRI diverges, but the same-tree tests
// above would still pass.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import ssri from "ssri";
import * as tar from "tar";

let scratch: string;

beforeEach(async () => {
  scratch = await fs.mkdtemp(path.join(os.tmpdir(), "build-det-"));
});

afterEach(async () => {
  await fs.rm(scratch, { recursive: true, force: true });
});

async function listFilesSorted(cwd: string, root: string): Promise<string[]> {
  const acc: string[] = [];
  async function walk(rel: string): Promise<void> {
    const abs = path.join(cwd, rel);
    const entries = await fs.readdir(abs, { withFileTypes: true });
    entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    for (const entry of entries) {
      const childRel = path.join(rel, entry.name);
      acc.push(childRel);
      if (entry.isDirectory()) {
        await walk(childRel);
      }
    }
  }
  acc.push(root);
  await walk(root);
  return acc;
}

async function normalizeStagingModes(
  cwd: string,
  entries: string[],
): Promise<void> {
  for (const rel of entries) {
    const abs = path.join(cwd, rel);
    const stat = await fs.lstat(abs);
    if (stat.isSymbolicLink()) continue;
    if (stat.isDirectory()) {
      await fs.chmod(abs, 0o755);
    } else if (stat.isFile()) {
      await fs.chmod(abs, 0o644);
    }
  }
}

async function packStaging(
  stagingDir: string,
  outPath: string,
): Promise<string> {
  const entries = await listFilesSorted(stagingDir, "package");
  await normalizeStagingModes(stagingDir, entries);
  const opts: tar.TarOptionsWithAliasesAsyncFile = {
    cwd: stagingDir,
    gzip: true,
    file: outPath,
    portable: true,
    mtime: new Date(0),
    noDirRecurse: true,
  };
  await tar.create(opts, entries);
  const bytes = await fs.readFile(outPath);
  return ssri.fromData(bytes, { algorithms: ["sha512"] }).toString();
}

async function writeMinimalPackage(stagingDir: string): Promise<void> {
  const pkgDir = path.join(stagingDir, "package");
  const subDir = path.join(pkgDir, "dist");
  await fs.mkdir(subDir, { recursive: true });
  await fs.writeFile(
    path.join(pkgDir, "package.json"),
    JSON.stringify(
      {
        name: "tools-determinism",
        version: "1.0.0",
        interchange: { tools: "./dist/main.js" },
      },
      null,
      2,
    ),
  );
  await fs.writeFile(
    path.join(subDir, "main.js"),
    "export const main = () => ({});\n",
  );
}

describe("build-builtins determinism", () => {
  test("packing the same staging tree twice yields identical SRI", async () => {
    const stagingDir = path.join(scratch, "pkg");
    await writeMinimalPackage(stagingDir);

    const outA = path.join(scratch, "first.tgz");
    const outB = path.join(scratch, "second.tgz");
    const integrityA = await packStaging(stagingDir, outA);
    const integrityB = await packStaging(stagingDir, outB);

    expect(integrityA).toBe(integrityB);

    const bytesA = await fs.readFile(outA);
    const bytesB = await fs.readFile(outB);
    expect(Buffer.compare(bytesA, bytesB)).toBe(0);
  });

  test("packing two staging trees with different mtimes still yields identical SRI", async () => {
    // The packer zeroes mtimes via `{ mtime: epoch }`; the source
    // files' mtimes must not leak into the tarball. Create the same
    // tree twice with deliberately different filesystem mtimes and
    // assert the integrity is unchanged.
    async function buildTree(dir: string, mtimeMs: number): Promise<void> {
      const pkgDir = path.join(dir, "package");
      await fs.mkdir(pkgDir, { recursive: true });
      const pkgJsonPath = path.join(pkgDir, "package.json");
      await fs.writeFile(
        pkgJsonPath,
        JSON.stringify(
          {
            name: "tools-determinism",
            version: "1.0.0",
            interchange: { tools: "./main.js" },
          },
          null,
          2,
        ),
      );
      const mainPath = path.join(pkgDir, "main.js");
      await fs.writeFile(mainPath, "export const main = () => ({});\n");
      const mtime = new Date(mtimeMs);
      await fs.utimes(pkgJsonPath, mtime, mtime);
      await fs.utimes(mainPath, mtime, mtime);
    }

    const stagingA = path.join(scratch, "a");
    const stagingB = path.join(scratch, "b");
    await buildTree(stagingA, 1_000_000_000_000);
    await buildTree(stagingB, 1_700_000_000_000);

    const outA = path.join(scratch, "a.tgz");
    const outB = path.join(scratch, "b.tgz");
    const integrityA = await packStaging(stagingA, outA);
    const integrityB = await packStaging(stagingB, outB);

    expect(integrityA).toBe(integrityB);
  });

  test("packing two staging trees with different mode bits still yields identical SRI", async () => {
    // `normalizeStagingModes` runs before `tar.create` reads each
    // entry's stat, so two builders whose umasks landed different mode
    // bits on disk emit identical headers. Without that step (or if a
    // future refactor reordered it after the tar walk), this test
    // would fail because tar would record the as-on-disk modes.
    const stagingA = path.join(scratch, "modes-a");
    const stagingB = path.join(scratch, "modes-b");
    await writeMinimalPackage(stagingA);
    await writeMinimalPackage(stagingB);

    // Force divergent mode bits on the two staging trees, both for a
    // directory and a regular file, so the normalization step has
    // work to do on both shapes.
    await fs.chmod(path.join(stagingA, "package"), 0o700);
    await fs.chmod(path.join(stagingA, "package", "package.json"), 0o600);
    await fs.chmod(path.join(stagingA, "package", "dist"), 0o711);
    await fs.chmod(path.join(stagingA, "package", "dist", "main.js"), 0o755);

    await fs.chmod(path.join(stagingB, "package"), 0o777);
    await fs.chmod(path.join(stagingB, "package", "package.json"), 0o664);
    await fs.chmod(path.join(stagingB, "package", "dist"), 0o775);
    await fs.chmod(path.join(stagingB, "package", "dist", "main.js"), 0o600);

    const outA = path.join(scratch, "modes-a.tgz");
    const outB = path.join(scratch, "modes-b.tgz");
    const integrityA = await packStaging(stagingA, outA);
    const integrityB = await packStaging(stagingB, outB);

    expect(integrityA).toBe(integrityB);
  });
});
