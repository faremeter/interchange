import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import os from "node:os";
import path from "node:path";
import nodeFs, { promises as fsp } from "node:fs";
import * as tar from "tar";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto";
import { collectReachableObjects } from "@intx/storage-isogit/node";
import type { KeyPair } from "@intx/types/runtime";

import {
  packageRegistryKindHandler,
  asTarballEntry,
  validateTarballPackageJSON,
} from "./package-registry-kind";
import { createRepoStore } from "./repo-store";
import type {
  KindHandler,
  RepoId,
  Principal,
  ValidatePushResult,
} from "./repo-store";
import { validatePushArgs } from "./repo-store/validate-push.test-helpers";

const REF = "refs/heads/main";

const HUB_PRINCIPAL: Principal = { kind: "hub" };
function uniqueRepoId(): RepoId {
  return {
    kind: "package-registry",
    id: `pkr-${Math.random().toString(36).slice(2, 10)}`,
  };
}

const tempDirs: string[] = [];

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fsp.rm(d, { recursive: true, force: true }).catch(() => undefined);
  }
});

async function makeTarball(pkg: Record<string, unknown>): Promise<Uint8Array> {
  const stagingRoot = await fsp.mkdtemp(path.join(os.tmpdir(), "pkr-test-"));
  tempDirs.push(stagingRoot);
  const pkgDir = path.join(stagingRoot, "package");
  await fsp.mkdir(pkgDir, { recursive: true });
  await fsp.writeFile(
    path.join(pkgDir, "package.json"),
    JSON.stringify(pkg, null, 2),
    "utf-8",
  );
  await fsp.writeFile(path.join(pkgDir, "index.js"), "module.exports = {};\n");
  const out = path.join(stagingRoot, "out.tgz");
  await tar.create(
    {
      cwd: stagingRoot,
      gzip: true,
      file: out,
      portable: true,
    },
    ["package"],
  );
  return new Uint8Array(await fsp.readFile(out));
}

/**
 * Build a tarball that carries more than one top-level `<seg>/package.json`
 * entry. The hub validates the first emitted entry, but the sidecar's
 * `tar.extract({ strip: 1 })` overwrites on every subsequent stripped
 * path and loads the last one; the kind handler must reject the
 * ambiguous archive at the validation boundary.
 */
async function makeMultiPackageTarball(
  entries: { dir: string; pkg: Record<string, unknown> }[],
): Promise<Uint8Array> {
  const stagingRoot = await fsp.mkdtemp(
    path.join(os.tmpdir(), "pkr-test-multi-"),
  );
  tempDirs.push(stagingRoot);
  const childDirs: string[] = [];
  for (const entry of entries) {
    const childDir = path.join(stagingRoot, entry.dir);
    await fsp.mkdir(childDir, { recursive: true });
    await fsp.writeFile(
      path.join(childDir, "package.json"),
      JSON.stringify(entry.pkg, null, 2),
      "utf-8",
    );
    childDirs.push(entry.dir);
  }
  const out = path.join(stagingRoot, "out.tgz");
  await tar.create(
    { cwd: stagingRoot, gzip: true, file: out, portable: true },
    childDirs,
  );
  return new Uint8Array(await fsp.readFile(out));
}

type FileMap = Record<string, Uint8Array>;

describe("asTarballEntry", () => {
  test("accepts a safe filename", () => {
    expect(asTarballEntry("tarballs/foo-1.0.0.tgz")).toBe("foo-1.0.0.tgz");
  });
  test("rejects nested directories", () => {
    expect(asTarballEntry("tarballs/sub/foo-1.0.0.tgz")).toBeNull();
  });
  test("rejects non-tgz extension", () => {
    expect(asTarballEntry("tarballs/foo-1.0.0.tar")).toBeNull();
  });
  test("rejects entries outside the prefix", () => {
    expect(asTarballEntry("other/foo-1.0.0.tgz")).toBeNull();
  });
  test("rejects path traversal in the filename", () => {
    expect(asTarballEntry("tarballs/../escape.tgz")).toBeNull();
  });
  test("rejects a leading-dot filename ('..tgz')", () => {
    expect(asTarballEntry("tarballs/..tgz")).toBeNull();
  });
  test("rejects a hidden-style filename ('.hidden.tgz')", () => {
    expect(asTarballEntry("tarballs/.hidden.tgz")).toBeNull();
  });
  test("accepts a scoped-style filename", () => {
    expect(asTarballEntry("tarballs/@intx-tools-mail-0.1.2.tgz")).toBe(
      "@intx-tools-mail-0.1.2.tgz",
    );
  });
});

describe("validateTarballPackageJSON", () => {
  test("accepts a tarball whose package.json has name + version", async () => {
    const bytes = await makeTarball({ name: "my-pkg", version: "1.0.0" });
    const outcome = await validateTarballPackageJSON("my-pkg-1.0.0.tgz", bytes);
    expect(outcome.ok).toBe(true);
  });

  test("rejects a tarball missing package/package.json", async () => {
    const stagingRoot = await fsp.mkdtemp(
      path.join(os.tmpdir(), "pkr-test-bad-"),
    );
    tempDirs.push(stagingRoot);
    const noPkg = path.join(stagingRoot, "no-pkg");
    await fsp.mkdir(noPkg);
    await fsp.writeFile(path.join(noPkg, "stray"), "data");
    const out = path.join(stagingRoot, "out.tgz");
    await tar.create(
      { cwd: stagingRoot, gzip: true, file: out, portable: true },
      ["no-pkg"],
    );
    const bytes = new Uint8Array(await fsp.readFile(out));
    const outcome = await validateTarballPackageJSON("bad-1.0.0.tgz", bytes);
    expect(outcome.ok).toBe(false);
  });

  test("rejects a tarball whose package.json fails arktype validation", async () => {
    const bytes = await makeTarball({ version: "1.0.0" });
    const outcome = await validateTarballPackageJSON("nameless.tgz", bytes);
    expect(outcome.ok).toBe(false);
  });

  test("rejects a tarball that carries multiple top-level package.json entries", async () => {
    // The hub-side validator captures the first `<seg>/package.json`
    // the tar walk emits, but the sidecar's `tar.extract({ strip: 1 })`
    // overwrites on every collision and loads the last entry. A
    // tarball with two top-level package directories therefore would
    // validate against one descriptor on the hub and execute against
    // another on the sidecar — exactly the kind of TOCTOU gap a single
    // signed integrity hash cannot close. Refuse the upload at the
    // validation boundary.
    const bytes = await makeMultiPackageTarball([
      { dir: "package", pkg: { name: "first", version: "1.0.0" } },
      { dir: "evil", pkg: { name: "second", version: "2.0.0" } },
    ]);
    const outcome = await validateTarballPackageJSON("ambiguous.tgz", bytes);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("expected reject");
    expect(outcome.reason).toContain("multiple top-level package.json entries");
    expect(outcome.reason).toContain("package/package.json");
    expect(outcome.reason).toContain("evil/package.json");
  });
});

describe("packageRegistryKindHandler.validatePush", () => {
  test("accepts an empty repo with only .gitignore", async () => {
    const files: FileMap = {
      ".gitignore": new TextEncoder().encode("node_modules\n"),
    };
    const result = await packageRegistryKindHandler.validatePush(
      validatePushArgs({
        repoId: uniqueRepoId(),
        ref: REF,
        principal: HUB_PRINCIPAL,
        files,
      }),
    );
    expect(result).toEqual({ ok: true });
  });

  test("rejects an unexpected top-level entry", async () => {
    const files: FileMap = { "stray.json": new TextEncoder().encode("{}") };
    const result = await packageRegistryKindHandler.validatePush(
      validatePushArgs({
        repoId: uniqueRepoId(),
        ref: REF,
        principal: HUB_PRINCIPAL,
        files,
      }),
    );
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected reject");
    expect(result.reason).toContain("unexpected top-level entry");
  });

  test("accepts a tarballs/ entry with a valid tarball", async () => {
    const bytes = await makeTarball({ name: "tool-a", version: "2.0.0" });
    const files: FileMap = {
      "tarballs/tool-a-2.0.0.tgz": bytes,
    };
    const result = await packageRegistryKindHandler.validatePush(
      validatePushArgs({
        repoId: uniqueRepoId(),
        ref: REF,
        principal: HUB_PRINCIPAL,
        files,
      }),
    );
    expect(result).toEqual({ ok: true });
  });

  test("rejects a tarballs/ entry whose filename is unsafe", async () => {
    const bytes = await makeTarball({ name: "x", version: "0.0.1" });
    const files: FileMap = {
      "tarballs/has spaces.tgz": bytes,
    };
    const result = await packageRegistryKindHandler.validatePush(
      validatePushArgs({
        repoId: uniqueRepoId(),
        ref: REF,
        principal: HUB_PRINCIPAL,
        files,
      }),
    );
    expect(result.ok).toBe(false);
  });

  test("rejects two tarballs publishing the same name@version", async () => {
    // The AssetRegistrySource builds packuments by `fs.readdir` order
    // and overwrites duplicates; resolving an unambiguous closure
    // requires the substrate to guarantee one tarball per name@version.
    const aBytes = await makeTarball({ name: "dup", version: "1.0.0" });
    const bBytes = await makeTarball({ name: "dup", version: "1.0.0" });
    const files: FileMap = {
      "tarballs/dup-1.0.0-a.tgz": aBytes,
      "tarballs/dup-1.0.0-b.tgz": bBytes,
    };
    const result = await packageRegistryKindHandler.validatePush(
      validatePushArgs({
        repoId: uniqueRepoId(),
        ref: REF,
        principal: HUB_PRINCIPAL,
        files,
      }),
    );
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected reject");
    expect(result.reason).toContain("multiple tarballs publishing dup@1.0.0");
  });

  test("rejects a tarballs/ entry whose package.json fails validation", async () => {
    const bytes = await makeTarball({ version: "1.0.0" });
    const files: FileMap = {
      "tarballs/bad-1.0.0.tgz": bytes,
    };
    const result = await packageRegistryKindHandler.validatePush(
      validatePushArgs({
        repoId: uniqueRepoId(),
        ref: REF,
        principal: HUB_PRINCIPAL,
        files,
      }),
    );
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected reject");
    expect(result.reason).toContain("package.json failed validation");
  });

  test("rejects a tarballs/ entry whose tarball carries multiple top-level package.json paths", async () => {
    // End-to-end: an ambiguous tarball reaches the kind handler's
    // validatePush, which translates `multiple-entries` into a
    // ValidatePushResult rejection. Without this, the hub would commit
    // an upload whose hub-side validation captured one descriptor and
    // whose sidecar-side runtime would load another.
    const bytes = await makeMultiPackageTarball([
      { dir: "package", pkg: { name: "front", version: "1.0.0" } },
      { dir: "shadow", pkg: { name: "back", version: "1.0.0" } },
    ]);
    const files: FileMap = {
      "tarballs/ambiguous-1.0.0.tgz": bytes,
    };
    const result = await packageRegistryKindHandler.validatePush(
      validatePushArgs({
        repoId: uniqueRepoId(),
        ref: REF,
        principal: HUB_PRINCIPAL,
        files,
      }),
    );
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected reject");
    expect(result.reason).toContain("multiple top-level package.json entries");
  });

  test("surfaces a non-ENOENT listDir failure as a rejection rather than treating tarballs/ as empty", async () => {
    // A transient EACCES / EIO / malformed-tree error from listDir
    // is NOT "the tarballs subtree is absent" — collapsing both into
    // `tarballChildren = []` would let a push that legitimately
    // includes tarballs through validation without examining any of
    // them. Only the not-found case should fall through to "no
    // tarballs to validate"; every other listDir error must surface
    // so the operator sees the failure rather than a silent accept.
    const bytes = await makeTarball({ name: "tool-a", version: "1.0.0" });
    const files: FileMap = {
      "tarballs/tool-a-1.0.0.tgz": bytes,
    };
    const args = validatePushArgs({
      repoId: uniqueRepoId(),
      ref: REF,
      principal: HUB_PRINCIPAL,
      files,
    });
    const result = await packageRegistryKindHandler.validatePush({
      ...args,
      listDir: async (treePath) => {
        if (treePath === "tarballs") {
          const err = new Error("induced EACCES on tarballs subtree");
          Object.assign(err, { code: "EACCES" });
          throw err;
        }
        return args.listDir(treePath);
      },
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected reject");
    expect(result.reason).toContain("tarballs");
  });
});

// The substrate's `receivePack` walks every new commit in the pack and
// invokes the kind handler's `validatePush` once per commit, so a tree
// that violates the package-registry allowlist on an intermediate
// commit must reject the pack even when the tip is valid. The
// package-registry handler does not consult prior closures — each
// commit's tree is judged on its own top-level paths and per-tarball
// shape. This regression pins that behaviour: the per-commit walk
// catches an intermediate-state violation at the offending commit,
// not by accidentally being lenient at the tip.
describe("package-registry per-commit pack walk", () => {
  const tempDirsForStore: string[] = [];
  let signingKey: KeyPair;

  beforeAll(async () => {
    signingKey = await generateKeyPair();
  });

  afterAll(async () => {
    for (const d of tempDirsForStore.splice(0)) {
      await fsp.rm(d, { recursive: true, force: true }).catch(() => undefined);
    }
  });

  async function makeTempDir(prefix: string): Promise<string> {
    const d = await fsp.mkdtemp(path.join(os.tmpdir(), prefix));
    tempDirsForStore.push(d);
    return d;
  }

  const permissiveHandler: KindHandler = {
    kind: "package-registry",
    directoryPrefix: "assets/package-registry",
    validatePush(): ValidatePushResult {
      return { ok: true };
    },
    onRefUpdated() {
      /* no-op */
    },
  };

  const PRINCIPAL: Principal = { kind: "hub" };
  // The handler does not gate validation on ref name; pick a non-`main`
  // ref so the source's `initRepo` genesis (on `refs/heads/main`) does
  // not collide with the test's pack target.
  const REGISTRY_REF = "refs/heads/deploy";

  test("rejects a multi-commit pack whose second commit adds a disallowed top-level path", async () => {
    const sourceDataDir = await makeTempDir("pkg-reg-percommit-src-");
    const sourceStore = createRepoStore({
      dataDir: sourceDataDir,
      signingKey,
      handlers: { "package-registry": permissiveHandler },
      authorize: () => ({ allowed: true }),
    });
    const repoId: RepoId = {
      kind: "package-registry",
      id: `pkr-${Math.random().toString(36).slice(2, 10)}`,
    };
    await sourceStore.initRepo(repoId);

    // Seed both commits with a valid tarball under `tarballs/` so the
    // first commit is a realistic registry that the per-commit walk
    // accepts, and only the second commit's `stray.json` can cause the
    // rejection. The seed is defensive: the handler skips
    // `listDir("tarballs")` for a tree without `tarballs`, and the
    // pack-path `listDir` returns `[]` for a missing directory, so a
    // first commit without the seed also validates.
    const seedTarball = await makeTarball({
      name: "seed-pkg",
      version: "1.0.0",
    });
    const { commitSha: firstSha } = await sourceStore.writeTree(
      PRINCIPAL,
      repoId,
      REGISTRY_REF,
      {
        files: {
          ".gitignore": "node_modules\n",
          "tarballs/seed-pkg-1.0.0.tgz": seedTarball,
        },
        message: "valid single-tarball registry",
      },
    );
    const { commitSha: secondSha } = await sourceStore.writeTree(
      PRINCIPAL,
      repoId,
      REGISTRY_REF,
      {
        files: {
          ".gitignore": "node_modules\n",
          "tarballs/seed-pkg-1.0.0.tgz": seedTarball,
          "stray.json": "{}",
        },
        message: "intermediate violation: stray top-level path",
      },
    );

    const sourceDir = sourceStore.getRepoDir(repoId);
    const firstObjects = await collectReachableObjects(sourceDir, firstSha);
    const secondObjects = await collectReachableObjects(sourceDir, secondSha);
    const oids = Array.from(new Set([...firstObjects, ...secondObjects]));
    const packResult = await git.packObjects({
      fs: nodeFs,
      dir: sourceDir,
      oids,
      write: false,
    });
    if (packResult.packfile === undefined) {
      throw new Error("git.packObjects returned no packfile");
    }
    const pack = packResult.packfile;

    const targetDataDir = await makeTempDir("pkg-reg-percommit-tgt-");
    const targetStore = createRepoStore({
      dataDir: targetDataDir,
      signingKey,
      handlers: { "package-registry": packageRegistryKindHandler },
      authorize: () => ({ allowed: true }),
    });
    await targetStore.initRepo(repoId);

    await expect(
      targetStore.receivePack(
        PRINCIPAL,
        repoId,
        REGISTRY_REF,
        pack,
        secondSha,
        null,
      ),
    ).rejects.toThrow(
      /path_violation:.*unexpected top-level entry "stray\.json"/,
    );

    const resolvedAfter = await targetStore.resolveRef(
      PRINCIPAL,
      repoId,
      REGISTRY_REF,
    );
    expect(resolvedAfter).toBeNull();
  });

  test("the same violation at the tip of a single-commit pack also rejects", async () => {
    const sourceDataDir = await makeTempDir("pkg-reg-tiponly-src-");
    const sourceStore = createRepoStore({
      dataDir: sourceDataDir,
      signingKey,
      handlers: { "package-registry": permissiveHandler },
      authorize: () => ({ allowed: true }),
    });
    const repoId: RepoId = {
      kind: "package-registry",
      id: `pkr-${Math.random().toString(36).slice(2, 10)}`,
    };
    await sourceStore.initRepo(repoId);
    // Pair the violating top-level entry with a valid tarballs subtree
    // so `stray.json` is the only fault in the tree. The seed is
    // defensive: the handler checks the top-level allowlist before it
    // reads `tarballs`, and the pack-path `listDir` returns `[]` for a
    // missing directory, so a tree without the seed also rejects on
    // `stray.json`.
    const seedTarball = await makeTarball({
      name: "seed-pkg",
      version: "1.0.0",
    });
    const { commitSha } = await sourceStore.writeTree(
      PRINCIPAL,
      repoId,
      REGISTRY_REF,
      {
        files: {
          "tarballs/seed-pkg-1.0.0.tgz": seedTarball,
          "stray.json": "{}",
        },
        message: "tip violates the package-registry allowlist",
      },
    );
    const { pack } = await sourceStore.createPack(
      PRINCIPAL,
      repoId,
      REGISTRY_REF,
    );

    const targetDataDir = await makeTempDir("pkg-reg-tiponly-tgt-");
    const targetStore = createRepoStore({
      dataDir: targetDataDir,
      signingKey,
      handlers: { "package-registry": packageRegistryKindHandler },
      authorize: () => ({ allowed: true }),
    });
    await targetStore.initRepo(repoId);
    await expect(
      targetStore.receivePack(
        PRINCIPAL,
        repoId,
        REGISTRY_REF,
        pack,
        commitSha,
        null,
      ),
    ).rejects.toThrow(
      /path_violation:.*unexpected top-level entry "stray\.json"/,
    );
  });

  test("accepts a pack whose tree omits the tarballs subtree entirely", async () => {
    // A tree without a `tarballs/` subtree (e.g. the genesis tree,
    // which only writes `.gitignore`) must validate cleanly through the
    // pack path. The handler gates `listDir("tarballs")` on
    // `topLevelTreePaths.includes("tarballs")`, so a throw from that
    // call means a real fault. The pack-path `listDir` returns `[]` for
    // a missing directory, so the push also accepts without the gate;
    // the test guards the accept, not the gate.
    const sourceDataDir = await makeTempDir("pkg-reg-notarballs-src-");
    const sourceStore = createRepoStore({
      dataDir: sourceDataDir,
      signingKey,
      handlers: { "package-registry": permissiveHandler },
      authorize: () => ({ allowed: true }),
    });
    const repoId: RepoId = {
      kind: "package-registry",
      id: `pkr-${Math.random().toString(36).slice(2, 10)}`,
    };
    await sourceStore.initRepo(repoId);
    const { commitSha } = await sourceStore.writeTree(
      PRINCIPAL,
      repoId,
      REGISTRY_REF,
      {
        files: {
          ".gitignore": "node_modules\n",
        },
        message: "registry without tarballs subtree",
      },
    );
    const { pack } = await sourceStore.createPack(
      PRINCIPAL,
      repoId,
      REGISTRY_REF,
    );

    const targetDataDir = await makeTempDir("pkg-reg-notarballs-tgt-");
    const targetStore = createRepoStore({
      dataDir: targetDataDir,
      signingKey,
      handlers: { "package-registry": packageRegistryKindHandler },
      authorize: () => ({ allowed: true }),
    });
    await targetStore.initRepo(repoId);
    await targetStore.receivePack(
      PRINCIPAL,
      repoId,
      REGISTRY_REF,
      pack,
      commitSha,
      null,
    );
    const resolvedAfter = await targetStore.resolveRef(
      PRINCIPAL,
      repoId,
      REGISTRY_REF,
    );
    expect(resolvedAfter).toBe(commitSha);
  });
});
