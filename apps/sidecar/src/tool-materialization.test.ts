import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  clearDirtyMarker,
  materializeToolPackages,
  parseActiveDeployId,
  persistActiveDeployIdWithFallback,
} from "./tool-materialization";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "default-harness-test-"),
  );
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

describe("materializeToolPackages — manifest.invalid gate", () => {
  test("returns empty factories when no manifest bytes supplied", async () => {
    const storeDir = await tempDir();
    const result = await materializeToolPackages({
      assetMounts: new Map(),
      rawManifestBytes: undefined,
      storeDir,
      agentAddress: "agent-no-manifest",
      cacheRoot: "/unused-by-manifest-invalid-gate",
      cacheMaxBytes: 1024 * 1024,
      registryMaxTarballBytes: 10 * 1024 * 1024,
    });
    expect(result.factories).toEqual([]);
    expect(result.pluginFactories).toEqual([]);
  });

  test("JSON.parse failure throws and persists the raw bytes verbatim", async () => {
    const storeDir = await tempDir();
    const corrupt = "{this is not valid json";
    let caught: unknown;
    try {
      await materializeToolPackages({
        assetMounts: new Map(),
        rawManifestBytes: corrupt,
        storeDir,
        agentAddress: "agent-corrupt",
        cacheRoot: "/unused-by-manifest-invalid-gate",
        cacheMaxBytes: 1024 * 1024,
        registryMaxTarballBytes: 10 * 1024 * 1024,
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(String(caught)).toMatch(/manifest\.invalid/);
    expect(String(caught)).toMatch(/JSON\.parse failed/);

    const auditRoot = path.join(storeDir, "audit", "rejected-applies");
    const attemptDirs = await fs.promises.readdir(auditRoot);
    expect(attemptDirs).toHaveLength(1);
    const attemptDirName = attemptDirs[0];
    if (attemptDirName === undefined) {
      throw new Error("no attempt dir was created");
    }
    const attemptDir = path.join(auditRoot, attemptDirName);

    // The manifest file must contain the exact corrupt bytes, not a
    // JSON-encoded string literal wrapping them — the audit-trail
    // writer must not push the bytes through JSON.stringify, which
    // would double-encode the string and erase the original-input
    // evidence the audit file exists to preserve.
    const persisted = await fs.promises.readFile(
      path.join(attemptDir, "manifest.json"),
      "utf-8",
    );
    expect(persisted).toBe(corrupt);

    const errorJson = JSON.parse(
      await fs.promises.readFile(path.join(attemptDir, "error.json"), "utf-8"),
    );
    expect(errorJson.category).toBe("manifest.invalid");
    expect(errorJson.message).toMatch(/JSON\.parse failed/);
    expect(errorJson.previousDeployId).toBe("none");
    expect(typeof errorJson.attemptId).toBe("string");
    expect(errorJson.attemptId.length).toBeGreaterThan(0);
  });

  test("schema failure throws and persists the raw bytes verbatim", async () => {
    const storeDir = await tempDir();
    // Valid JSON but wrong shape — arktype rejects it.
    const wrongShape = JSON.stringify({ schemaVersion: 99 });
    let caught: unknown;
    try {
      await materializeToolPackages({
        assetMounts: new Map(),
        rawManifestBytes: wrongShape,
        storeDir,
        agentAddress: "agent-schema",
        cacheRoot: "/unused-by-manifest-invalid-gate",
        cacheMaxBytes: 1024 * 1024,
        registryMaxTarballBytes: 10 * 1024 * 1024,
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(String(caught)).toMatch(/manifest\.invalid/);
    expect(String(caught)).toMatch(/schema validation failed/);

    const auditRoot = path.join(storeDir, "audit", "rejected-applies");
    const attemptDirs = await fs.promises.readdir(auditRoot);
    expect(attemptDirs).toHaveLength(1);
    const attemptDirName = attemptDirs[0];
    if (attemptDirName === undefined) {
      throw new Error("no attempt dir was created");
    }
    const attemptDir = path.join(auditRoot, attemptDirName);

    // The audit trail records the bytes-as-supplied for both
    // failure modes, so a future investigator can replay the
    // same input against a newer validator.
    const persisted = await fs.promises.readFile(
      path.join(attemptDir, "manifest.json"),
      "utf-8",
    );
    expect(persisted).toBe(wrongShape);

    const errorJson = JSON.parse(
      await fs.promises.readFile(path.join(attemptDir, "error.json"), "utf-8"),
    );
    expect(errorJson.category).toBe("manifest.invalid");
  });

  describe("parseActiveDeployId", () => {
    test("accepts a v1-prefixed id", () => {
      expect(parseActiveDeployId("v1:abc-123", "/dummy")).toBe("abc-123");
    });

    test("accepts a bare id for backward compatibility with pre-versioned files", () => {
      expect(parseActiveDeployId("legacy-deploy-id", "/dummy")).toBe(
        "legacy-deploy-id",
      );
    });

    test("rejects an unknown version prefix loudly", () => {
      expect(() => parseActiveDeployId("v2:abc", "/dummy")).toThrow(
        /unknown version prefix/,
      );
    });

    test("rejects a v1-prefixed but empty id", () => {
      expect(() => parseActiveDeployId("v1:", "/dummy")).toThrow(
        /carries the v1 prefix but no id/,
      );
    });

    test("rejects an empty file", () => {
      expect(() => parseActiveDeployId("", "/dummy")).toThrow(/is empty/);
    });
  });

  describe("persistActiveDeployIdWithFallback", () => {
    test("returns degraded=false and removes a stale dirty marker on full success", async () => {
      const instanceDir = await tempDir();
      const activeIdFile = path.join(instanceDir, "active-deploy-id");
      const dirtyFile = `${activeIdFile}.dirty`;
      await fs.promises.writeFile(dirtyFile, "v1:stale-id");

      const outcome = await persistActiveDeployIdWithFallback(
        instanceDir,
        activeIdFile,
        "fresh-id",
      );
      expect(outcome.degraded).toBe(false);
      const recorded = await fs.promises.readFile(activeIdFile, "utf-8");
      expect(recorded).toBe("v1:fresh-id");
      // The dirty marker must be cleared so the next boot reads the
      // freshly-recorded id directly, not the stale marker that
      // pre-dated the successful persist.
      let dirtyExists = false;
      try {
        await fs.promises.access(dirtyFile);
        dirtyExists = true;
      } catch {
        dirtyExists = false;
      }
      expect(dirtyExists).toBe(false);
    });

    test("the degraded-persist code path clears a stale dirty marker via the shared helper", async () => {
      // The fsync'd primary persist and the no-fsync fallback persist
      // both delegate to `clearDirtyMarker` after writing the recorded
      // id. The fallback branch is hard to drive end-to-end without
      // injecting an `fs` failure mode that distinguishes
      // `fs.promises.open(path, "w")` from `fs.promises.writeFile(path)`
      // — they share flags and permissions on every supported FS — so
      // the contract is pinned at the helper level: when called against
      // an instance dir that holds a stale marker, the marker is gone
      // afterward. The wrapper's no-fsync branch routes through this
      // helper, which gives the boot reader the freshly-recorded id on
      // the next boot.
      const instanceDir = await tempDir();
      const activeIdFile = path.join(instanceDir, "active-deploy-id");
      const dirtyFile = `${activeIdFile}.dirty`;
      await fs.promises.writeFile(dirtyFile, "v1:stale-from-prior-apply");
      await clearDirtyMarker(activeIdFile, "test");
      let dirtyExists = false;
      try {
        await fs.promises.access(dirtyFile);
        dirtyExists = true;
      } catch {
        dirtyExists = false;
      }
      expect(dirtyExists).toBe(false);
    });

    test("clearing a missing dirty marker is a no-op", async () => {
      // ENOENT is the normal case when no prior apply wrote a marker.
      // The helper must not throw or log when there is nothing to
      // remove; the wrapper relies on the helper being safe to call
      // unconditionally on every successful persist.
      const instanceDir = await tempDir();
      const activeIdFile = path.join(instanceDir, "active-deploy-id");
      await clearDirtyMarker(activeIdFile, "test");
    });

    test("writes the dirty marker when the primary persist cannot open the file", async () => {
      // Simulate a primary persist failure by making activeIdFile itself
      // a directory. Both the fsync'd open() and the no-fsync
      // writeFile() will then fail with EISDIR, exercising the dirty-
      // marker path.
      const instanceDir = await tempDir();
      const activeIdFile = path.join(instanceDir, "active-deploy-id");
      await fs.promises.mkdir(activeIdFile);

      const outcome = await persistActiveDeployIdWithFallback(
        instanceDir,
        activeIdFile,
        "new-id-42",
      );
      expect(outcome.degraded).toBe(true);
      expect(outcome.error).toBeInstanceOf(Error);

      const dirtyContents = await fs.promises.readFile(
        `${activeIdFile}.dirty`,
        "utf-8",
      );
      expect(dirtyContents).toBe("v1:new-id-42");
    });
  });

  test("boot reconciliation records the dirty marker as previousDeployId", async () => {
    // Seed an instance where the prior apply could not durably record
    // the committed deploy id and a dirty marker carries the truth.
    // The next apply (any apply — here, a manifest-invalid one) must
    // surface the marker's id as `previousDeployId` in the audit
    // record, not the stale recorded id and not "none".
    const storeDir = await tempDir();
    const instanceDir = path.join(storeDir, "tool-packages");
    await fs.promises.mkdir(instanceDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(instanceDir, "active-deploy-id"),
      "v1:stale-recorded",
    );
    await fs.promises.writeFile(
      path.join(instanceDir, "active-deploy-id.dirty"),
      "v1:truth-from-marker",
    );

    let caught: unknown;
    try {
      await materializeToolPackages({
        assetMounts: new Map(),
        rawManifestBytes: "{ not json",
        storeDir,
        agentAddress: "agent-dirty-marker",
        cacheRoot: "/unused-by-manifest-invalid-gate",
        cacheMaxBytes: 1024 * 1024,
        registryMaxTarballBytes: 10 * 1024 * 1024,
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);

    const auditRoot = path.join(storeDir, "audit", "rejected-applies");
    const attemptDirs = await fs.promises.readdir(auditRoot);
    expect(attemptDirs).toHaveLength(1);
    const attemptDirName = attemptDirs[0];
    if (attemptDirName === undefined) {
      throw new Error("no attempt dir was created");
    }
    const errorJson = JSON.parse(
      await fs.promises.readFile(
        path.join(auditRoot, attemptDirName, "error.json"),
        "utf-8",
      ),
    );
    expect(errorJson.previousDeployId).toBe("truth-from-marker");
  });

  test("manifest.invalid throws and writes an audit entry", async () => {
    const storeDir = await tempDir();
    let caught: unknown;
    try {
      await materializeToolPackages({
        assetMounts: new Map(),
        rawManifestBytes: "{ not json",
        storeDir,
        agentAddress: "agent-throws",
        cacheRoot: "/unused-by-manifest-invalid-gate",
        cacheMaxBytes: 1024 * 1024,
        registryMaxTarballBytes: 10 * 1024 * 1024,
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(String(caught)).toMatch(/manifest\.invalid/);

    const auditRoot = path.join(storeDir, "audit", "rejected-applies");
    const attemptDirs = await fs.promises.readdir(auditRoot);
    expect(attemptDirs).toHaveLength(1);
  });
});
