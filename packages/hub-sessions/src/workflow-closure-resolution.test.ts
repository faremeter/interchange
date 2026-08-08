// Hub-side workflow closure resolution over an in-process fixture registry.
//
// Mirrors the fixture-registry harness the tool-packaging end-to-end suite
// uses (`buildFixture` / `makePackumentFromFixtures` / the `PackumentFetcher`
// seam) but drives it through `resolveWorkflowClosure`, the workflow-source
// entry point. No sidecar and no network: the resolver reads synthetic
// packuments only. The test asserts the frozen closure carries concrete
// versions and integrity SRIs for the top-level package and its transitive
// dependency.
//
// Integrity is computed with `node:crypto` rather than `ssri` so the test
// depends only on packages already declared by `hub-sessions`; the SRI shape
// (`sha512-<base64 digest>`) is identical to what the real registry serves.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import * as tar from "tar";

import type { Packument, PackumentFetcher } from "@intx/tool-packaging";
import type { WorkflowDefinitionRegistrySource } from "@intx/types/workflow-sources";

import { resolveWorkflowClosure } from "./workflow-closure-resolution";

let scratch: string;
let fixtureRoot: string;

beforeEach(async () => {
  scratch = await fs.mkdtemp(path.join(os.tmpdir(), "wf-closure-"));
  fixtureRoot = path.join(scratch, "fixtures");
  await fs.mkdir(fixtureRoot, { recursive: true });
});

afterEach(async () => {
  await fs.rm(scratch, { recursive: true, force: true });
});

interface Fixture {
  name: string;
  version: string;
  integrity: string;
  tarballUrl: string;
  dependencies: Record<string, string>;
}

function sriFromBytes(bytes: Uint8Array): string {
  return `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
}

async function buildFixture(spec: {
  name: string;
  version: string;
  dependencies?: Record<string, string>;
}): Promise<Fixture> {
  const stagingDir = path.join(
    fixtureRoot,
    `${spec.name.replace("/", "_")}-${spec.version}`,
  );
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });

  const dependencies = spec.dependencies ?? {};
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: spec.name,
      version: spec.version,
      dependencies,
      interchange: { tools: "./tools.js" },
    }),
  );
  await fs.writeFile(
    path.join(packageDir, "tools.js"),
    "// stub; hub-side resolution never executes this\n",
  );

  const tarballPath = path.join(stagingDir, "out.tgz");
  await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
    "package",
  ]);
  const bytes = await fs.readFile(tarballPath);
  const tarballUrl = `https://registry.test/${spec.name}/-/${
    spec.name.startsWith("@") ? spec.name.split("/")[1] : spec.name
  }-${spec.version}.tgz`;
  return {
    name: spec.name,
    version: spec.version,
    integrity: sriFromBytes(bytes),
    tarballUrl,
    dependencies,
  };
}

function makePackumentFromFixtures(fixtures: readonly Fixture[]): Packument[] {
  return fixtures.map((f) => ({
    name: f.name,
    versions: {
      [f.version]: {
        name: f.name,
        version: f.version,
        dist: {
          tarball: f.tarballUrl,
          integrity: f.integrity,
        },
        dependencies: f.dependencies,
      },
    },
  }));
}

describe("resolveWorkflowClosure", () => {
  test("resolves a registry-sourced workflow to concrete versions + SRIs", async () => {
    const dep = await buildFixture({
      name: "wf-fixture-dep",
      version: "1.2.0",
    });
    const top = await buildFixture({
      name: "wf-fixture-top",
      version: "1.0.0",
      dependencies: { [dep.name]: "^1.0.0" },
    });

    const packuments = makePackumentFromFixtures([top, dep]);
    const fetchPackument: PackumentFetcher = async (name) => {
      const p = packuments.find((q) => q.name === name);
      if (p === undefined) throw new Error(`no packument: ${name}`);
      return p;
    };

    const source: WorkflowDefinitionRegistrySource = {
      kind: "registry",
      registry: "fixture-registry",
    };

    const manifest = await resolveWorkflowClosure({
      source,
      pin: `${top.name}@${top.version}`,
      registryConfig: { url: "https://registry.test" },
      fetchPackument,
    });

    expect(manifest.topLevel).toEqual([
      { name: top.name, version: top.version },
    ]);

    const byName = new Map(manifest.entries.map((e) => [e.name, e]));
    expect(byName.size).toBe(2);

    const topEntry = byName.get(top.name);
    if (topEntry === undefined) throw new Error("missing top-level entry");
    expect(topEntry.version).toBe(top.version);
    expect(topEntry.integrity).toBe(top.integrity);
    expect(topEntry.source).toEqual({
      kind: "registry",
      registry: source.registry,
    });

    const depEntry = byName.get(dep.name);
    if (depEntry === undefined) throw new Error("missing transitive entry");
    expect(depEntry.version).toBe(dep.version);
    expect(depEntry.integrity).toBe(dep.integrity);
    expect(depEntry.source).toEqual({
      kind: "registry",
      registry: source.registry,
    });
  });

  test("reuses parsePin to accept a range spec for the top-level pin", async () => {
    const only = await buildFixture({
      name: "wf-ranged",
      version: "2.3.4",
    });
    const packuments = makePackumentFromFixtures([only]);
    const fetchPackument: PackumentFetcher = async (name) => {
      const p = packuments.find((q) => q.name === name);
      if (p === undefined) throw new Error(`no packument: ${name}`);
      return p;
    };

    const manifest = await resolveWorkflowClosure({
      source: { kind: "registry", registry: "fixture-registry" },
      pin: `${only.name}@^2.0.0`,
      registryConfig: { url: "https://registry.test" },
      fetchPackument,
    });

    // A ranged pin freezes to the concrete version the registry advertises.
    expect(manifest.topLevel).toEqual([
      { name: only.name, version: only.version },
    ]);
    expect(manifest.entries).toHaveLength(1);
    expect(manifest.entries[0]?.version).toBe(only.version);
    expect(manifest.entries[0]?.integrity).toBe(only.integrity);
  });
});
