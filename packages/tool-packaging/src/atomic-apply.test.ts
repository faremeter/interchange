import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs, type PathLike } from "node:fs";
import os from "node:os";
import path from "node:path";

import type { DeployApplyErrorCategory } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import { applyAtomic } from "./atomic-apply";
import {
  type LoadedToolFactory,
  type LoadedToolPackage,
  type ToolLoader,
  ToolLoaderError,
} from "./loader";

let scratch: string;
let instanceDir: string;
let assetRoot: string;

beforeEach(async () => {
  scratch = await fs.mkdtemp(path.join(os.tmpdir(), "tool-packaging-atomic-"));
  instanceDir = path.join(scratch, "instance");
  assetRoot = path.join(scratch, "asset");
  await fs.mkdir(instanceDir, { recursive: true });
  await fs.mkdir(assetRoot, { recursive: true });
});

afterEach(async () => {
  await fs.rm(scratch, { recursive: true, force: true });
});

function fakeFactory(id: string): LoadedToolFactory {
  const fn = () => ({
    definitions: [],
    run: async () => ({ callId: "stub", content: "ok" }),
  });
  return Object.assign(fn, { id, requires: [] as readonly string[] });
}

function makeStubLoader(
  impl: (args: { instanceScratchDir: string }) => Promise<LoadedToolPackage[]>,
): ToolLoader {
  return {
    loadManifest: async (args) => {
      // Write a sentinel file into the scratch dir so the test can
      // observe what staged before the loader returned.
      await fs.writeFile(
        path.join(args.instanceScratchDir, "loader-sentinel"),
        "ran",
      );
      return impl({ instanceScratchDir: args.instanceScratchDir });
    },
  };
}

const minimalManifest: ToolPackageManifest = {
  schemaVersion: "1",
  topLevel: [],
  entries: [],
};

async function dirExists(p: string): Promise<boolean> {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

describe("applyAtomic success", () => {
  test("swaps pending to active and returns the new deploy id", async () => {
    const loader = makeStubLoader(async () => [
      {
        name: "foo",
        version: "1.0.0",
        plugins: [],
        factories: [fakeFactory("@foo/a")],
        directors: [],
      },
    ]);
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_1",
      previousDeployId: "dpl_0",
      newDeployId: "dpl_1",
    });
    expect(result.status).toBe("ok");
    if (result.status !== "ok") return;
    expect(result.activeDeployId).toBe("dpl_1");
    expect(result.loaded).toHaveLength(1);
    expect(await dirExists(path.join(instanceDir, "active"))).toBe(true);
    expect(await dirExists(path.join(instanceDir, "pending"))).toBe(false);
    // First apply has no previous active to back up.
    expect(await dirExists(path.join(instanceDir, "previous"))).toBe(false);
  });

  test("on second apply, prior active becomes previous", async () => {
    const loader = makeStubLoader(async () => []);
    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_1",
      previousDeployId: "dpl_0",
      newDeployId: "dpl_1",
    });
    // Marker so we can confirm the original active tree was preserved.
    await fs.writeFile(path.join(instanceDir, "active", "marker"), "first");

    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_2",
      previousDeployId: "dpl_1",
      newDeployId: "dpl_2",
    });
    expect(await dirExists(path.join(instanceDir, "active"))).toBe(true);
    expect(await dirExists(path.join(instanceDir, "previous"))).toBe(true);
    // The marker should now live under previous/.
    expect(
      await fs.readFile(path.join(instanceDir, "previous", "marker"), "utf8"),
    ).toBe("first");
  });
});

describe("applyAtomic failure: every loader error category", () => {
  const categories: DeployApplyErrorCategory[] = [
    "tarball.missing",
    "integrity.mismatch",
    "registry.fetch.failed",
    "registry.unknown",
    "registry.auth.failed",
    "tarball.extract.failed",
    "manifest.invalid",
    "package.entry.missing",
    "package.entry.invalid",
    "factory.construct.failed",
  ];

  for (const category of categories) {
    test(`category ${category}: pending discarded, active untouched, error carries previousDeployId`, async () => {
      // Seed an "active" tree to confirm it is preserved.
      const activeMarker = path.join(instanceDir, "active", "marker");
      await fs.mkdir(path.dirname(activeMarker), { recursive: true });
      await fs.writeFile(activeMarker, "untouched");

      const loader: ToolLoader = {
        loadManifest: async () => {
          throw new ToolLoaderError({
            category,
            message: `induced failure: ${category}`,
            package: { name: "p", version: "1.0.0" },
          });
        },
      };
      const result = await applyAtomic({
        manifest: minimalManifest,
        loader,
        instanceDir,
        assetRoot,
        assetMounts: new Map(),
        attemptId: "atp_X",
        previousDeployId: "dpl_prior",
        newDeployId: "dpl_attempted",
      });
      expect(result.status).toBe("failed");
      if (result.status !== "failed") return;
      expect(result.category).toBe(category);
      expect(result.previousDeployId).toBe("dpl_prior");
      expect(result.attemptId).toBe("atp_X");
      expect(result.package).toEqual({ name: "p", version: "1.0.0" });

      // Atomicity invariant: active is unchanged.
      expect(await fs.readFile(activeMarker, "utf8")).toBe("untouched");
      // Pending has been removed.
      expect(await dirExists(path.join(instanceDir, "pending"))).toBe(false);
    });
  }
});

describe("applyAtomic failure: tool.name.duplicate", () => {
  test("two factories with the same id across packages → tool.name.duplicate", async () => {
    const loader = makeStubLoader(async () => [
      {
        name: "a",
        version: "1.0.0",
        plugins: [],
        factories: [fakeFactory("@dup/x")],
        directors: [],
      },
      {
        name: "b",
        version: "1.0.0",
        plugins: [],
        factories: [fakeFactory("@dup/x")],
        directors: [],
      },
    ]);
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_D",
      previousDeployId: "dpl_prior",
      newDeployId: "dpl_attempted",
    });
    expect(result.status).toBe("failed");
    if (result.status !== "failed") return;
    expect(result.category).toBe("tool.name.duplicate");
    expect(result.previousDeployId).toBe("dpl_prior");
    expect(await dirExists(path.join(instanceDir, "active"))).toBe(false);
    expect(await dirExists(path.join(instanceDir, "pending"))).toBe(false);
  });
});

describe("applyAtomic failure: unexpected error shape", () => {
  test("loader throwing a plain Error becomes factory.construct.failed", async () => {
    const loader: ToolLoader = {
      loadManifest: async () => {
        throw new Error("something exploded");
      },
    };
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_U",
      previousDeployId: "dpl_prior",
      newDeployId: "dpl_attempted",
    });
    expect(result.status).toBe("failed");
    if (result.status !== "failed") return;
    expect(result.category).toBe("factory.construct.failed");
    expect(result.message).toContain("something exploded");
  });
});

describe("applyAtomic failure: pending→active swap", () => {
  // Fault-injection helper: wrap fs.promises.rename to fail on calls
  // whose destination matches a predicate. Returns a restore fn. Used
  // to exercise the swap-failure branches that the success path
  // (everything above) cannot reach without an actual fs failure.
  function patchRename(failIf: (dst: string) => string | null): () => void {
    const real = fs.rename.bind(fs);
    const wrapped = async (src: PathLike, dst: PathLike): Promise<void> => {
      const reason = failIf(String(dst));
      if (reason !== null) {
        const e = new Error(reason);
        Object.assign(e, { code: "EIO" });
        throw e;
      }
      await real(src, dst);
    };
    fs.rename = wrapped;
    return () => {
      fs.rename = real;
    };
  }

  test("single rename failure (rollback ok) → apply.swap.failed structured failure", async () => {
    // Seed a prior active so step-2 has something to roll back.
    const activeMarker = path.join(instanceDir, "active", "marker");
    await fs.mkdir(path.dirname(activeMarker), { recursive: true });
    await fs.writeFile(activeMarker, "prior-active");

    const loader = makeStubLoader(async () => []);
    const activeDir = path.join(instanceDir, "active");
    // Fail only the FIRST rename targeting activeDir (the
    // pending→active swap). The rollback's previous→active rename is
    // the second such call and is allowed through, exercising the
    // rollback-succeeded branch.
    let activeRenameSeen = 0;
    const restore = patchRename((dst) => {
      if (dst !== activeDir) return null;
      activeRenameSeen += 1;
      return activeRenameSeen === 1 ? "induced step-3 rename failure" : null;
    });
    try {
      const result = await applyAtomic({
        manifest: minimalManifest,
        loader,
        instanceDir,
        assetRoot,
        assetMounts: new Map(),
        attemptId: "atp_S1",
        previousDeployId: "dpl_prior",
        newDeployId: "dpl_attempted",
      });
      expect(result.status).toBe("failed");
      if (result.status !== "failed") return;
      expect(result.category).toBe("apply.swap.failed");
      expect(result.message).toContain("pending→active swap failed");
      expect(result.message).toContain("induced step-3 rename failure");
      expect(result.previousDeployId).toBe("dpl_prior");
      expect(result.attemptId).toBe("atp_S1");
      // Rollback restored the prior active tree byte-for-byte.
      expect(await fs.readFile(activeMarker, "utf8")).toBe("prior-active");
      // Aborted apply does not leave a staged pending tree behind —
      // matches the sibling failure branches' cleanup behavior.
      expect(await dirExists(path.join(instanceDir, "pending"))).toBe(false);
    } finally {
      restore();
    }
  });

  test("active→staged rename failure sweeps the pending dir before returning apply.swap.failed", async () => {
    // Seed a prior active so the swap prelude attempts the
    // active→staged rename. Fail that rename, then assert the
    // structured failure category fires and no `pending/` tree
    // survives.
    const activeMarker = path.join(instanceDir, "active", "marker");
    await fs.mkdir(path.dirname(activeMarker), { recursive: true });
    await fs.writeFile(activeMarker, "prior-active");

    const stagedDir = path.join(instanceDir, "previous.staged");
    const loader = makeStubLoader(async () => []);
    const restore = patchRename((dst) =>
      dst === stagedDir ? "induced active→staged rename failure" : null,
    );
    try {
      const result = await applyAtomic({
        manifest: minimalManifest,
        loader,
        instanceDir,
        assetRoot,
        assetMounts: new Map(),
        attemptId: "atp_AS",
        previousDeployId: "dpl_prior",
        newDeployId: "dpl_attempted",
      });
      expect(result.status).toBe("failed");
      if (result.status !== "failed") return;
      expect(result.category).toBe("apply.swap.failed");
      expect(result.message).toContain("active→staged swap failed");
      expect(await dirExists(path.join(instanceDir, "pending"))).toBe(false);
    } finally {
      restore();
    }
  });

  test("swap failure preserves the prior-previous safety net", async () => {
    // Seed the on-disk shape that exists on a second apply: an
    // existing `active/` (prior deploy) AND an existing `previous/`
    // (the prior-previous safety net the docstring at the top of
    // `atomic-apply.ts` promises to retain). Then induce a swap
    // failure; `previous/` must still be present and untouched
    // afterwards.
    const activeMarker = path.join(instanceDir, "active", "marker");
    await fs.mkdir(path.dirname(activeMarker), { recursive: true });
    await fs.writeFile(activeMarker, "prior-active");
    const previousMarker = path.join(instanceDir, "previous", "marker");
    await fs.mkdir(path.dirname(previousMarker), { recursive: true });
    await fs.writeFile(previousMarker, "prior-previous");

    const loader = makeStubLoader(async () => []);
    const activeDir = path.join(instanceDir, "active");
    let activeRenameSeen = 0;
    const restore = patchRename((dst) => {
      if (dst !== activeDir) return null;
      activeRenameSeen += 1;
      return activeRenameSeen === 1 ? "induced step-3 rename failure" : null;
    });
    try {
      const result = await applyAtomic({
        manifest: minimalManifest,
        loader,
        instanceDir,
        assetRoot,
        assetMounts: new Map(),
        attemptId: "atp_SP",
        previousDeployId: "dpl_prior",
        newDeployId: "dpl_attempted",
      });
      expect(result.status).toBe("failed");
      // Both safety nets must survive: rollback restored prior-active,
      // and prior-previous was never touched.
      expect(await fs.readFile(activeMarker, "utf8")).toBe("prior-active");
      expect(await fs.readFile(previousMarker, "utf8")).toBe("prior-previous");
    } finally {
      restore();
    }
  });

  test("double rename failure (rollback also fails) throws with diverged-disk message", async () => {
    const activeMarker = path.join(instanceDir, "active", "marker");
    await fs.mkdir(path.dirname(activeMarker), { recursive: true });
    await fs.writeFile(activeMarker, "prior-active");

    const loader = makeStubLoader(async () => []);
    const activeDir = path.join(instanceDir, "active");
    // Fail every rename whose destination is activeDir — that is
    // both step-3 (pending → active) and the rollback (previous →
    // active). The active → previous step (step 2) renames TO
    // previous and is allowed through.
    const restore = patchRename((dst) =>
      dst === activeDir ? "induced rename failure" : null,
    );
    try {
      let caught: unknown;
      try {
        await applyAtomic({
          manifest: minimalManifest,
          loader,
          instanceDir,
          assetRoot,
          assetMounts: new Map(),
          attemptId: "atp_S2",
          previousDeployId: "dpl_prior",
          newDeployId: "dpl_attempted",
        });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(Error);
      const msg = String(caught);
      expect(msg).toContain("atomic apply diverged on disk");
      expect(msg).toContain("atp_S2");
      expect(msg).toContain("pending→active rename failed");
      expect(msg).toContain("staged→active rollback also failed");
      expect(msg).toContain("harness must abort");
      // cause chain points at the original step-3 rename error.
      if (caught instanceof Error) {
        expect(caught.cause).toBeInstanceOf(Error);
      }
    } finally {
      restore();
    }
  });
});

describe("applyAtomic failure: post-swap previous-dir rotation", () => {
  // Fault-injection helper. Returns a restore function. The predicate
  // sees both the source and destination so a test can fail one
  // rename in a sequence without also failing a sibling rename whose
  // destination matches but whose source does not (e.g. failing
  // `stagedDir → previousDir` while permitting the `reapDir →
  // previousDir` rollback that follows).
  function patchRename(
    failIf: (src: string, dst: string) => string | null,
  ): () => void {
    const real = fs.rename.bind(fs);
    fs.rename = (async (src: PathLike, dst: PathLike): Promise<void> => {
      const reason = failIf(String(src), String(dst));
      if (reason !== null) {
        const e = new Error(reason);
        Object.assign(e, { code: "EBUSY" });
        throw e;
      }
      await real(src, dst);
    }) as typeof fs.rename;
    return () => {
      fs.rename = real;
    };
  }

  test("fs.rename failure moving prior-previous aside surfaces as apply.previous-rotation.failed and preserves the safety net", async () => {
    // Seed two prior applies so the third one exercises the
    // post-swap rotation branch (active exists and previous exists).
    const loader = makeStubLoader(async () => []);
    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_1",
      previousDeployId: "dpl_0",
      newDeployId: "dpl_1",
    });
    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_2",
      previousDeployId: "dpl_1",
      newDeployId: "dpl_2",
    });

    // Drop a marker into the prior-previous tree so we can confirm
    // the rotation never destroyed it — the docstring at the top of
    // atomic-apply.ts promises the previous-deploy safety net survives
    // a failed rotation.
    const previousDir = path.join(instanceDir, "previous");
    const reapDir = path.join(instanceDir, "previous.reap");
    const previousMarker = path.join(previousDir, "safety-net-marker");
    await fs.writeFile(previousMarker, "prior-previous");

    // Fail the previous→reap rename specifically (the move-aside that
    // displaces the prior-previous before the staged→previous rename
    // promotes the prior-active).
    const restore = patchRename((_src, dst) =>
      dst === reapDir ? "induced EBUSY on previous→reap rename" : null,
    );
    try {
      const result = await applyAtomic({
        manifest: minimalManifest,
        loader,
        instanceDir,
        assetRoot,
        assetMounts: new Map(),
        attemptId: "atp_3",
        previousDeployId: "dpl_2",
        newDeployId: "dpl_3",
      });
      expect(result.status).toBe("failed");
      if (result.status !== "failed") return;
      expect(result.category).toBe("apply.previous-rotation.failed");
      expect(result.message).toContain(
        "post-swap previous-dir rotation failed",
      );
      // The new deploy is live on disk; previousDeployId carries the
      // new id rather than the pre-apply one.
      expect(result.previousDeployId).toBe("dpl_3");
      expect(result.attemptId).toBe("atp_3");
      // Active is the new deploy.
      expect(await dirExists(path.join(instanceDir, "active"))).toBe(true);
      // Safety net retained: the marker file written into the
      // prior-previous tree is still readable at previousDir.
      expect(await fs.readFile(previousMarker, "utf8")).toBe("prior-previous");
      // The pending dir was renamed to active before the rotation
      // failed; no leftover pending tree survives this category
      // either, matching the swap-failure sibling branches.
      expect(await dirExists(path.join(instanceDir, "pending"))).toBe(false);
    } finally {
      restore();
    }
  });

  test("fs.rename failure on the staged→previous step surfaces as apply.previous-rotation.failed and rolls back the safety net", async () => {
    const loader = makeStubLoader(async () => []);
    // Seed two prior applies so the third one hits the rotation path
    // with a prior-previous tree to roll back from after the staged→
    // previous rename fails.
    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_1",
      previousDeployId: "dpl_0",
      newDeployId: "dpl_1",
    });
    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_2",
      previousDeployId: "dpl_1",
      newDeployId: "dpl_2",
    });

    // Seed a marker into the prior-previous tree so the rollback
    // assertion can confirm the safety net is restored bit-for-bit.
    const previousDir = path.join(instanceDir, "previous");
    const previousMarker = path.join(previousDir, "safety-net-marker");
    await fs.writeFile(previousMarker, "prior-previous");

    // Allow the previous→reap and pending→active renames through;
    // fail the staged→previous rename in the post-swap rotation.
    // Discriminate on src as well as dst so the reapDir→previousDir
    // rollback that runs after the staged→previous failure is
    // permitted to succeed (the safety-net-restored assertion below
    // depends on the rollback landing).
    const stagedDir = path.join(instanceDir, "previous.staged");
    const restore = patchRename((src, dst) =>
      src === stagedDir && dst === previousDir
        ? "induced EBUSY on staged→previous rename"
        : null,
    );
    try {
      const result = await applyAtomic({
        manifest: minimalManifest,
        loader,
        instanceDir,
        assetRoot,
        assetMounts: new Map(),
        attemptId: "atp_3",
        previousDeployId: "dpl_2",
        newDeployId: "dpl_3",
      });
      expect(result.status).toBe("failed");
      if (result.status !== "failed") return;
      expect(result.category).toBe("apply.previous-rotation.failed");
      expect(result.message).toContain(
        "post-swap previous-dir rotation failed",
      );
      expect(result.previousDeployId).toBe("dpl_3");
      expect(result.attemptId).toBe("atp_3");
      expect(await dirExists(path.join(instanceDir, "active"))).toBe(true);
      // Safety net restored: the rollback put the prior-previous tree
      // back at previousDir after the staged→previous rename failed.
      expect(await fs.readFile(previousMarker, "utf8")).toBe("prior-previous");
      // Same as the move-aside-failure sibling: pending is gone by
      // the time the rotation step runs.
      expect(await dirExists(path.join(instanceDir, "pending"))).toBe(false);
    } finally {
      restore();
    }
  });

  test("staged→previous rename failure surfaces apply.previous-rotation.failed even when no prior-previous existed", async () => {
    // The rotation runs whenever the prior apply produced an active
    // tree, regardless of whether a prior-previous slot was occupied.
    // First apply: no rotation (active was absent). Second apply:
    // rotation fires with `activeExists === true` but `previousDir`
    // does not exist yet — `fs.access(previousDir)` ENOENTs and
    // `priorPreviousMovedAside` stays false. If the staged→previous
    // rename then fails, the rollback branch must NOT fire (there's
    // nothing in `reapDir` to restore from) but the structured
    // failure must still surface so the caller routes it through
    // the apply-error pipeline rather than letting the exception
    // bubble past.
    const loader = makeStubLoader(async () => []);
    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_1",
      previousDeployId: "dpl_0",
      newDeployId: "dpl_1",
    });

    // Confirm the setup: active exists, previous does not.
    expect(await dirExists(path.join(instanceDir, "active"))).toBe(true);
    expect(await dirExists(path.join(instanceDir, "previous"))).toBe(false);

    // Allow previous→reap to no-op (no previous to move aside) and
    // pending→active rename to succeed; fail the staged→previous
    // rename in the post-swap rotation.
    const previousDir = path.join(instanceDir, "previous");
    const stagedDir = path.join(instanceDir, "previous.staged");
    const restore = patchRename((src, dst) =>
      src === stagedDir && dst === previousDir
        ? "induced EBUSY on staged→previous rename"
        : null,
    );
    try {
      const result = await applyAtomic({
        manifest: minimalManifest,
        loader,
        instanceDir,
        assetRoot,
        assetMounts: new Map(),
        attemptId: "atp_2",
        previousDeployId: "dpl_1",
        newDeployId: "dpl_2",
      });
      expect(result.status).toBe("failed");
      if (result.status !== "failed") return;
      expect(result.category).toBe("apply.previous-rotation.failed");
      expect(result.message).toContain(
        "post-swap previous-dir rotation failed",
      );
      // The swap committed before the rotation step failed, so the
      // new deploy is live and previousDeployId carries it.
      expect(result.previousDeployId).toBe("dpl_2");
      expect(result.attemptId).toBe("atp_2");
      expect(await dirExists(path.join(instanceDir, "active"))).toBe(true);
      expect(await dirExists(path.join(instanceDir, "pending"))).toBe(false);
    } finally {
      restore();
    }
  });

  test("staged→previous and reap→previous both failing still surfaces apply.previous-rotation.failed (cascade)", async () => {
    // The rollback path can itself fail: staged→previous fails first,
    // then the reapDir→previousDir rollback fails too. The safety-net
    // tree is genuinely lost on disk in that branch, but the apply
    // pipeline still owes the caller a structured failure — the swap
    // already committed, so an exception bubble-up would leave
    // active-deploy-id un-bumped while the on-disk tree advanced.
    // Verify the structured failure still surfaces and carries the
    // newDeployId (the swap is live on disk in this branch).
    const loader = makeStubLoader(async () => []);
    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_1",
      previousDeployId: "dpl_0",
      newDeployId: "dpl_1",
    });
    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_2",
      previousDeployId: "dpl_1",
      newDeployId: "dpl_2",
    });

    // Fail every rename whose destination is `previousDir` — that
    // matches both staged→previous (the primary rotation step) and
    // reap→previous (the rollback). The previous→reap move-aside is
    // permitted because its destination is reapDir.
    const previousDir = path.join(instanceDir, "previous");
    const restore = patchRename((_src, dst) =>
      dst === previousDir
        ? "induced EBUSY on every previous-dir destination"
        : null,
    );
    try {
      const result = await applyAtomic({
        manifest: minimalManifest,
        loader,
        instanceDir,
        assetRoot,
        assetMounts: new Map(),
        attemptId: "atp_3",
        previousDeployId: "dpl_2",
        newDeployId: "dpl_3",
      });
      expect(result.status).toBe("failed");
      if (result.status !== "failed") return;
      expect(result.category).toBe("apply.previous-rotation.failed");
      expect(result.message).toContain(
        "post-swap previous-dir rotation failed",
      );
      // newDeployId because the swap committed before the rotation
      // step failed — the on-disk active tree is now the new deploy.
      expect(result.previousDeployId).toBe("dpl_3");
      expect(result.attemptId).toBe("atp_3");
      expect(await dirExists(path.join(instanceDir, "active"))).toBe(true);
    } finally {
      restore();
    }
  });
});

describe("applyAtomic clears a stale pending dir before staging", () => {
  test("a leftover pending directory does not block the next apply", async () => {
    await fs.mkdir(path.join(instanceDir, "pending"), { recursive: true });
    await fs.writeFile(path.join(instanceDir, "pending", "stale"), "old");
    const loader = makeStubLoader(async () => []);
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_S",
      previousDeployId: "dpl_prior",
      newDeployId: "dpl_new",
    });
    expect(result.status).toBe("ok");
    // The stale file should not be present in the resulting active dir.
    expect(await dirExists(path.join(instanceDir, "active", "stale"))).toBe(
      false,
    );
  });

  test("a leftover previous.staged directory is swept on the next apply", async () => {
    // apply.previous-rotation.failed leaves `previous.staged` in
    // place (the comment chain in atomic-apply.ts says the next
    // apply sweeps it). Seed a fake stale staged dir and verify the
    // next apply removes it as part of its swap prelude.
    const stagedDir = path.join(instanceDir, "previous.staged");
    await fs.mkdir(stagedDir, { recursive: true });
    await fs.writeFile(path.join(stagedDir, "leftover"), "stale");
    const loader = makeStubLoader(async () => []);
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_PS",
      previousDeployId: "dpl_prior",
      newDeployId: "dpl_new",
    });
    expect(result.status).toBe("ok");
    expect(await dirExists(stagedDir)).toBe(false);
  });
});
