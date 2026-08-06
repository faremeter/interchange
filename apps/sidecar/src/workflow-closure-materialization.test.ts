import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";

import type { RegistryConfig, TarballFetcher } from "@intx/tool-packaging";
import type { WorkflowProbeRequestFrame } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import { createWorkflowClosureMaterializer } from "./workflow-closure-materialization";

const REGISTRY_NAME = "test-registry";
const WORKFLOW_PACKAGE_NAME = "@fixture/wf-probe";
const WORKFLOW_PACKAGE_VERSION = "1.0.0";
const DEP_PACKAGE_NAME = "@fixture/dep";
const DEP_PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "index.js";

// The pinned workflow entry writes a sentinel file as an import-time side
// effect. The materializer lays the closure out WITHOUT importing any author
// code, so the sentinel must never appear -- its absence proves no module was
// evaluated on the host.
const SENTINEL_ENV = "PROBE_MATERIALIZER_SENTINEL";
const WORKFLOW_ENTRY_SOURCE = `
import { writeFileSync } from "node:fs";
const sentinel = process.env[${JSON.stringify(SENTINEL_ENV)}];
if (sentinel !== undefined) writeFileSync(sentinel, "imported");
export default {
  id: "probe-materializer-fixture",
  triggers: [],
  steps: { done: { kind: "sleep", id: "done", durationMs: 1 } },
  stepOrder: ["done"],
};
`;

let scratchRoot: string;
let cacheRoot: string;
let materializerScratch: string;
let fixtureSourceRoot: string;

beforeEach(async () => {
  scratchRoot = await fs.mkdtemp(
    path.join(os.tmpdir(), "sidecar-wf-probe-materialize-"),
  );
  cacheRoot = path.join(scratchRoot, "cache");
  materializerScratch = path.join(scratchRoot, "probe-closures");
  fixtureSourceRoot = path.join(scratchRoot, "fixture-source");
  await fs.mkdir(cacheRoot, { recursive: true });
  await fs.mkdir(materializerScratch, { recursive: true });
  await fs.mkdir(fixtureSourceRoot, { recursive: true });
});

afterEach(async () => {
  await fs.rm(scratchRoot, { recursive: true, force: true });
});

/**
 * Pack a package directory into an npm-style tarball (with the `package/`
 * prefix the loader strips) and return its bytes plus the SRI integrity the
 * frozen manifest pins.
 */
async function packFixture(
  pkgJson: Record<string, unknown>,
  files: Record<string, string>,
): Promise<{ bytes: Uint8Array; integrity: string }> {
  const stagingDir = path.join(
    fixtureSourceRoot,
    `${String(pkgJson.name).replace("/", "_")}-${String(pkgJson.version)}`,
  );
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify(pkgJson),
  );
  for (const [rel, contents] of Object.entries(files)) {
    const dest = path.join(packageDir, rel);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    await fs.writeFile(dest, contents);
  }

  const tarballPath = path.join(stagingDir, "out.tgz");
  const proc = Bun.spawnSync([
    "tar",
    "-czf",
    tarballPath,
    "-C",
    stagingDir,
    "package",
  ]);
  if (!proc.success) {
    throw new Error(
      `tar failed to pack the fixture: ${new TextDecoder().decode(proc.stderr)}`,
    );
  }
  const bytes = await fs.readFile(tarballPath);
  const integrity = `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
  return { bytes, integrity };
}

function registries(): ReadonlyMap<string, RegistryConfig> {
  return new Map([[REGISTRY_NAME, { url: "https://registry.invalid" }]]);
}

function materializerConfig(fetchTarball?: TarballFetcher) {
  return {
    cacheRoot,
    cacheMaxBytes: 10_000_000,
    registryMaxTarballBytes: 10_000_000,
    registries: registries(),
    scratchRoot: materializerScratch,
    ...(fetchTarball !== undefined ? { fetchTarball } : {}),
  };
}

function probeFrame(
  closure: ToolPackageManifest,
  entry: string,
): WorkflowProbeRequestFrame {
  return {
    type: "workflow.probe.request",
    requestId: "req-1",
    source: { kind: "registry", registry: REGISTRY_NAME },
    closure,
    entry,
  };
}

describe("createWorkflowClosureMaterializer", () => {
  test("lays out the frozen closure, resolves node_modules, and imports no author code", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
        dependencies: { [DEP_PACKAGE_NAME]: DEP_PACKAGE_VERSION },
      },
      { [WORKFLOW_ENTRY]: WORKFLOW_ENTRY_SOURCE },
    );
    const dep = await packFixture(
      {
        name: DEP_PACKAGE_NAME,
        version: DEP_PACKAGE_VERSION,
        type: "module",
      },
      { "index.js": "export const x = 1;\n" },
    );

    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [
        {
          name: WORKFLOW_PACKAGE_NAME,
          version: WORKFLOW_PACKAGE_VERSION,
          integrity: workflow.integrity,
          source: { kind: "registry", registry: REGISTRY_NAME },
        },
        {
          name: DEP_PACKAGE_NAME,
          version: DEP_PACKAGE_VERSION,
          integrity: dep.integrity,
          source: { kind: "registry", registry: REGISTRY_NAME },
        },
      ],
    };

    const fetched: string[] = [];
    const fetchTarball: TarballFetcher = async (entry) => {
      fetched.push(`${entry.name}@${entry.version}`);
      if (entry.name === WORKFLOW_PACKAGE_NAME) return workflow.bytes;
      if (entry.name === DEP_PACKAGE_NAME) return dep.bytes;
      throw new Error(`unexpected fetch for ${entry.name}@${entry.version}`);
    };

    const sentinelPath = path.join(scratchRoot, "import-sentinel");
    process.env[SENTINEL_ENV] = sentinelPath;
    try {
      const materialize = createWorkflowClosureMaterializer(
        materializerConfig(fetchTarball),
      );
      const materialized = await materialize(
        probeFrame(closure, WORKFLOW_ENTRY),
      );

      // The workflow package's own package.json is present in the laid-out
      // store directory.
      const pkgJson = JSON.parse(
        await fs.readFile(
          path.join(materialized.packageDir, "package.json"),
          "utf8",
        ),
      );
      expect(pkgJson.name).toBe(WORKFLOW_PACKAGE_NAME);
      expect(pkgJson.version).toBe(WORKFLOW_PACKAGE_VERSION);

      // The dependency resolves through the laid-out node_modules graph.
      const depPkgJson = JSON.parse(
        await fs.readFile(
          path.join(
            materialized.packageDir,
            "node_modules",
            "@fixture",
            "dep",
            "package.json",
          ),
          "utf8",
        ),
      );
      expect(depPkgJson.name).toBe(DEP_PACKAGE_NAME);

      // Both frozen entries were fetched exactly once, at their pinned
      // concrete versions.
      expect(fetched.sort()).toEqual([
        `${DEP_PACKAGE_NAME}@${DEP_PACKAGE_VERSION}`,
        `${WORKFLOW_PACKAGE_NAME}@${WORKFLOW_PACKAGE_VERSION}`,
      ]);

      // No author code ran: the entry module's import-time sentinel was
      // never written.
      await expect(fs.stat(sentinelPath)).rejects.toThrow();

      // Cleanup removes the ephemeral scratch tree.
      await materialized.cleanup();
      await expect(fs.stat(materialized.packageDir)).rejects.toThrow();
    } finally {
      Reflect.deleteProperty(process.env, SENTINEL_ENV);
    }
  });

  test("fails loud when the closure pins no top-level package", async () => {
    const materialize = createWorkflowClosureMaterializer(materializerConfig());
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    };
    await expect(
      materialize(probeFrame(closure, WORKFLOW_ENTRY)),
    ).rejects.toThrow(/exactly one top-level package/);
  });

  test("fails loud when the closure pins more than one top-level package", async () => {
    const materialize = createWorkflowClosureMaterializer(materializerConfig());
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
        { name: DEP_PACKAGE_NAME, version: DEP_PACKAGE_VERSION },
      ],
      entries: [],
    };
    await expect(
      materialize(probeFrame(closure, WORKFLOW_ENTRY)),
    ).rejects.toThrow(/exactly one top-level package/);
  });

  test("fails loud when the frame entry disagrees with interchange.workflow", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
      },
      { [WORKFLOW_ENTRY]: WORKFLOW_ENTRY_SOURCE },
    );
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [
        {
          name: WORKFLOW_PACKAGE_NAME,
          version: WORKFLOW_PACKAGE_VERSION,
          integrity: workflow.integrity,
          source: { kind: "registry", registry: REGISTRY_NAME },
        },
      ],
    };
    const fetchTarball: TarballFetcher = async () => workflow.bytes;

    const materialize = createWorkflowClosureMaterializer(
      materializerConfig(fetchTarball),
    );
    await expect(
      materialize(probeFrame(closure, "./does-not-match.js")),
    ).rejects.toThrow(
      /does not match the materialized package's interchange.workflow/,
    );
  });
});
