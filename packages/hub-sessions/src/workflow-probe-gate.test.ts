// Mutation tests for the security-load-bearing probe gate + freeze.
//
// The gate is the single point where a code-sourced workflow's inert probe
// answer becomes an approved, frozen definition. These tests pin the three
// invariants a mutation must not be able to break:
//
//   1. Tamper-evidence: a probe whose SHIPPED hash disagrees with the hub
//      recompute over the RECEIVED projection is rejected, and nothing is
//      frozen.
//   2. Freeze fidelity: on approval the RECOMPUTED hash (not the shipped one)
//      and exactly the workflow's advertised grant set (not the operator's
//      wider approval set) are frozen.
//   3. Subset invariant: deploy grants materialize as a subset of the frozen
//      approved set, so a naive live re-walk that surfaces MORE grants than
//      were approved cannot grant beyond the freeze.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import * as tar from "tar";

import { base64Decode } from "@intx/types";
import type { DBExecutor } from "@intx/db";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import type { ApprovalSet } from "@intx/workflow-deploy";

import {
  gateAndFreezeProbeResult,
  installAndApproveWorkflowDefinition,
  type FrozenApproval,
  type PersistFrozenApprovalFn,
} from "./workflow-probe-gate";
import type { SendProbeArgs, WorkflowProbeResult } from "./ws/sidecar-handler";

const PROJECTION: WorkflowProjectionDefinition = {
  id: "wf-under-test",
  triggers: [],
  stepOrder: [],
  steps: {},
};

// A probe result whose shipped hash matches the hub recompute over its
// projection. `shippedWireHash` overrides the shipped value to model a tampered
// answer; `grants` overrides the advisory set.
async function makeProbeResult(overrides?: {
  shippedWireHash?: string;
  grants?: string[];
  projection?: WorkflowProjectionDefinition;
}): Promise<WorkflowProbeResult> {
  const projection = overrides?.projection ?? PROJECTION;
  const wireHash =
    overrides?.shippedWireHash ?? (await computeWireDefinitionHash(projection));
  return {
    projection,
    grants: overrides?.grants ?? ["tool:fetch", "effect:log"],
    // The gate operates over the flattened `grants`; the un-flattened walk
    // snapshot rides through untouched, so an empty snapshot suffices here.
    grantWalkSnapshot: { perStep: [], grantRequirements: [] },
    wireHash,
  };
}

// A persist double that records every freeze it is asked to write and hands
// back a fixed definition id.
function recordingPersist(definitionId: string): {
  persist: PersistFrozenApprovalFn;
  calls: FrozenApproval[];
} {
  const calls: FrozenApproval[] = [];
  const persist: PersistFrozenApprovalFn = async (approval) => {
    calls.push(approval);
    return { definitionId };
  };
  return { persist, calls };
}

// A persist double that fails the test if the gate ever tries to freeze.
const persistMustNotRun: PersistFrozenApprovalFn = async () => {
  throw new Error("freeze must not run when the gate rejects");
};

describe("gateAndFreezeProbeResult", () => {
  test("rejects a probe whose shipped hash differs from the hub recompute", async () => {
    const probeResult = await makeProbeResult({
      shippedWireHash: "sha256:not-the-real-hash",
    });
    const approvals: ApprovalSet = new Set(probeResult.grants);

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist: persistMustNotRun,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    expect(result.reason).toBe("wire_hash_mismatch");
    if (result.reason !== "wire_hash_mismatch") throw new Error("wrong reason");
    expect(result.shippedWireHash).toBe("sha256:not-the-real-hash");
    expect(result.recomputedWireHash).toBe(
      await computeWireDefinitionHash(probeResult.projection),
    );
    expect(result.recomputedWireHash).not.toBe(result.shippedWireHash);
  });

  test("freezes the recomputed hash and the advertised grant set on approval", async () => {
    const probeResult = await makeProbeResult({
      grants: ["tool:fetch", "effect:log"],
    });
    // The operator approved MORE than the workflow asked for. The freeze must
    // capture the workflow's advertised set, not the wider approval set.
    const approvals: ApprovalSet = new Set([
      "tool:fetch",
      "effect:log",
      "tool:unused-extra",
    ]);
    const { persist, calls } = recordingPersist("def-123");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    const expectedHash = await computeWireDefinitionHash(
      probeResult.projection,
    );

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("expected approval");
    expect(result.definitionId).toBe("def-123");
    expect(result.approvedWireHash).toBe(expectedHash);
    expect([...result.approvedGrants].sort()).toEqual([
      "effect:log",
      "tool:fetch",
    ]);

    // The freeze was written exactly once, carrying the recomputed hash and the
    // advertised set -- not the shipped hash blindly and not the wider approval
    // set.
    expect(calls).toHaveLength(1);
    const frozen = calls[0];
    if (frozen === undefined) throw new Error("no freeze recorded");
    expect(frozen.assetId).toBe("asset-1");
    expect(frozen.approvedWireHash).toBe(expectedHash);
    expect([...frozen.approvedGrants].sort()).toEqual([
      "effect:log",
      "tool:fetch",
    ]);
    expect(frozen.approvedGrants).not.toContain("tool:unused-extra");
  });

  test("surfaces the inert projection on the ok-arm so the deploy hand-off carries the hashed content", async () => {
    const probeResult = await makeProbeResult({
      grants: ["tool:fetch", "effect:log"],
    });
    const approvals: ApprovalSet = new Set(probeResult.grants);
    const { persist } = recordingPersist("def-projection");

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist,
    });

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("expected approval");
    // The freeze hashed exactly this projection; the ok-arm carries it verbatim
    // so the deploy frame binds to the hashed content rather than a re-probe.
    expect(result.projection).toBe(probeResult.projection);
    expect(await computeWireDefinitionHash(result.projection)).toBe(
      result.approvedWireHash,
    );
  });

  test("rejects and freezes nothing when an advisory grant is unapproved", async () => {
    const probeResult = await makeProbeResult({
      grants: ["tool:fetch", "effect:log", "tool:escalate"],
    });
    // The operator did not approve `tool:escalate`.
    const approvals: ApprovalSet = new Set(["tool:fetch", "effect:log"]);

    const result = await gateAndFreezeProbeResult({
      assetId: "asset-1",
      probeResult,
      approvals,
      persist: persistMustNotRun,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected rejection");
    expect(result.reason).toBe("grants_not_approved");
    if (result.reason !== "grants_not_approved")
      throw new Error("wrong reason");
    expect(result.unapprovedGrants).toEqual(["tool:escalate"]);
  });
});

let installScratch: string;

/**
 * Pack a minimal workflow definition package into an npm tarball, returning the
 * bytes and the filename under which a synthetic asset would hold it.
 */
async function buildWorkflowTarball(spec: {
  name: string;
  version: string;
}): Promise<{ bytes: Uint8Array; tarballFile: string }> {
  const stagingDir = path.join(
    installScratch,
    `${spec.name.replace("/", "_")}-${spec.version}`,
  );
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: spec.name,
      version: spec.version,
      interchange: { workflow: "./index.js" },
    }),
  );
  await fs.writeFile(
    path.join(packageDir, "index.js"),
    "export const workflow = {};\n",
  );
  const tarballPath = path.join(stagingDir, "out.tgz");
  await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
    "package",
  ]);
  const bytes = await fs.readFile(tarballPath);
  return {
    bytes,
    tarballFile: `${spec.name.replace("@", "").replace("/", "-")}-${spec.version}.tgz`,
  };
}

function makeAssetReaders(tarballs: Record<string, Uint8Array>): {
  readBlob: (path: string) => Promise<Uint8Array>;
  listBlobs: (dir: string) => Promise<string[]>;
} {
  return {
    readBlob: async (blobPath) => {
      const filename = blobPath.replace(/^tarballs\//, "");
      const bytes = tarballs[filename];
      if (bytes === undefined) throw new Error(`no blob at ${blobPath}`);
      return bytes;
    },
    listBlobs: async (dir) => (dir === "tarballs" ? Object.keys(tarballs) : []),
  };
}

describe("installAndApproveWorkflowDefinition", () => {
  beforeEach(async () => {
    installScratch = await fs.mkdtemp(path.join(os.tmpdir(), "wf-install-"));
  });
  afterEach(async () => {
    await fs.rm(installScratch, { recursive: true, force: true });
  });

  test("delivers the source asset inline in the probe frame", async () => {
    const wf = await buildWorkflowTarball({
      name: "@fixture/wf",
      version: "1.0.0",
    });
    const { readBlob, listBlobs } = makeAssetReaders({
      [wf.tarballFile]: wf.bytes,
    });
    const assetId = "asset_wf";
    const commitSha = "0123456789abcdef0123456789abcdef01234567";

    let captured: SendProbeArgs | undefined;
    const router = {
      sendProbe: async (frame: SendProbeArgs): Promise<WorkflowProbeResult> => {
        captured = frame;
        // Capture the built frame and stop: the gate/freeze is out of scope for
        // this producer test.
        throw new Error("captured-probe-frame");
      },
    };
    const resolveAttachment = async (id: string) => ({
      pack: new TextEncoder().encode(`PACK:${id}`),
      ref: "refs/heads/main",
      commitSha,
    });

    await expect(
      installAndApproveWorkflowDefinition({
        source: { kind: "asset", assetId, package: { format: "tarball" } },
        pin: "@fixture/wf@1.0.0",
        entry: "./index.js",
        assetId,
        approvals: new Set<string>() satisfies ApprovalSet,
        router,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- sendProbe throws before db is read
        db: {} as unknown as DBExecutor,
        readBlob,
        listBlobs,
        resolveAttachment,
      }),
    ).rejects.toThrow(/captured-probe-frame/);

    expect(captured?.source).toEqual({
      kind: "asset",
      assetId,
      package: { format: "tarball" },
    });
    expect(captured?.entry).toBe("./index.js");
    expect(captured?.closure.topLevel).toEqual([
      { name: "@fixture/wf", version: "1.0.0" },
    ]);
    expect(captured?.assets).toHaveLength(1);
    const mount = captured?.assets?.[0];
    if (mount === undefined) throw new Error("no delivered asset mount");
    // The materializer requires the source asset among the delivered mounts.
    expect(mount.assetId).toBe(assetId);
    expect(mount.mountPath).toBe(`source-assets/${assetId}/`);
    expect(mount.ref).toBe("refs/heads/main");
    expect(mount.commitSha).toBe(commitSha);
    expect(new TextDecoder().decode(base64Decode(mount.pack))).toBe(
      `PACK:${assetId}`,
    );
  });
});
