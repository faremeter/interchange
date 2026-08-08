import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import {
  WorkflowDeploymentRecord,
  writeWorkflowDeploymentRecord,
  deleteWorkflowDeploymentRecord,
  scanWorkflowDeploymentRecords,
} from "./workflow-deployment-record";

async function makeDataDir(): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), "wdr-"));
}

function recordPath(dataDir: string, deploymentId: string): string {
  return path.join(dataDir, "workflow-runs", deploymentId, "deployment.json");
}

async function fileExists(p: string): Promise<boolean> {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

const SINGLE_STEP: WorkflowDeploymentRecord = {
  version: 1,
  agentAddress: "ins_abc123@tenant.example",
  definitionId: "wf_abc123",
  sources: {
    "step-1": [
      {
        id: "anthropic:mock",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        apiKey: "sk-x",
        model: "claude-mock",
      },
    ],
  },
  sessionId: "ses_1",
  hubPublicKey: "deadbeef",
  // Per-body approved hashes persisted for the onTrigger-body re-verify to
  // survive a restart; round-tripped here so a schema change to the map shape
  // is caught.
  referencedDefinitionHashes: {
    "body-a": "a".repeat(64),
    "body-b": "b".repeat(64),
  },
  lineage: "live-authored",
};

// A multi-step deployment records no head hub key and may carry no session
// id -- both optional fields absent.
const MULTI_STEP: WorkflowDeploymentRecord = {
  version: 1,
  agentAddress: "ins_dep_xyz@tenant.example",
  definitionId: "wf_xyz",
  sources: {
    plan: [
      {
        id: "anthropic:mock",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        apiKey: "sk-y",
        model: "claude-mock",
      },
    ],
    execute: [
      {
        id: "openai:mock",
        provider: "openai",
        baseURL: "https://api.example/openai",
        apiKey: "sk-z",
        model: "gpt-mock",
      },
    ],
  },
};

// A source-ref deployment: the record schema's discriminated union REQUIRES
// a sourceRef pin (source + closure) + approvedWireHash for lineage
// "source-ref".
const SOURCE_REF: WorkflowDeploymentRecord = {
  version: 1,
  agentAddress: "ins_dep_src@tenant.example",
  definitionId: "wf_src",
  sources: {
    "step-1": [
      {
        id: "anthropic:mock",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        apiKey: "sk-s",
        model: "claude-mock",
      },
    ],
  },
  approvedWireHash: "c".repeat(64),
  lineage: "source-ref",
  sourceRef: {
    source: { kind: "registry", registry: "npm" },
    closure: {
      schemaVersion: "1",
      // The workflow-definition package is the single top-level pin the sidecar
      // re-materializes; its transitive deps would ride `entries`.
      topLevel: [{ name: "@x/wf", version: "1.0.0" }],
      entries: [],
    },
  },
};

describe("workflow deployment record store", () => {
  test("round-trips a source-ref record (source + closure + approvedWireHash)", async () => {
    const dataDir = await makeDataDir();
    const deploymentId = "src-tenant-example";
    await writeWorkflowDeploymentRecord(dataDir, deploymentId, SOURCE_REF);

    const raw = await fs.readFile(recordPath(dataDir, deploymentId), "utf8");
    const parsed = WorkflowDeploymentRecord(JSON.parse(raw));
    if (parsed instanceof type.errors) {
      throw new Error(`record failed validation: ${parsed.summary}`);
    }
    expect(parsed).toEqual(SOURCE_REF);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("rejects a source-ref record missing sourceRef / a pin half / approvedWireHash", () => {
    // The whole safety argument for deleting the restore loop's hand-rolled
    // source-ref guard is that the union rejects EVERY malformed source-ref
    // record at the scan boundary -- so check each required piece on its own,
    // not just the all-missing shape (a future combinator swap could keep
    // all-missing rejecting while silently admitting a partial record). The
    // `SourceRefPin` co-requires its `source` + `closure`, so a half-populated
    // pin must be rejected too, not just an absent one.
    const rejects = (r: unknown): boolean =>
      WorkflowDeploymentRecord(r) instanceof type.errors;

    const base = {
      version: 1,
      agentAddress: "ins_dep_bad@tenant.example",
      definitionId: "wf_bad",
      sources: SOURCE_REF.sources,
      lineage: "source-ref",
    };
    const source = { kind: "registry", registry: "npm" };
    const closure = { schemaVersion: "1", topLevel: [], entries: [] };
    const sourceRef = { source, closure };
    const approvedWireHash = "c".repeat(64);

    // Both required pieces (sourceRef, approvedWireHash) missing.
    expect(rejects(base)).toBe(true);
    // The pin present but no hash, and the hash present but no pin -- each is
    // individually insufficient for a valid source-ref record.
    expect(rejects({ ...base, sourceRef })).toBe(true);
    expect(rejects({ ...base, approvedWireHash })).toBe(true);
    // A half-populated pin (only one of source/closure) is rejected by the
    // pin's own co-requirement, even alongside a valid hash.
    expect(rejects({ ...base, sourceRef: { source }, approvedWireHash })).toBe(
      true,
    );
    expect(rejects({ ...base, sourceRef: { closure }, approvedWireHash })).toBe(
      true,
    );
    // Full pin + hash -> accepted.
    expect(rejects({ ...base, sourceRef, approvedWireHash })).toBe(false);
  });

  test("round-trips a schema-valid record through disk (single-step)", async () => {
    const dataDir = await makeDataDir();
    const deploymentId = "abc123-tenant-example";
    await writeWorkflowDeploymentRecord(dataDir, deploymentId, SINGLE_STEP);

    // The record embeds source apiKeys, so it must not be group/world
    // readable on a shared host.
    const stat = await fs.stat(recordPath(dataDir, deploymentId));
    expect(stat.mode & 0o077).toBe(0);

    const raw = await fs.readFile(recordPath(dataDir, deploymentId), "utf8");
    const parsed = WorkflowDeploymentRecord(JSON.parse(raw));
    if (parsed instanceof type.errors) {
      throw new Error(`record failed validation: ${parsed.summary}`);
    }
    expect(parsed).toEqual(SINGLE_STEP);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("round-trips a record with the optional fields absent (multi-step)", async () => {
    const dataDir = await makeDataDir();
    const deploymentId = "dep_xyz-tenant-example";
    await writeWorkflowDeploymentRecord(dataDir, deploymentId, MULTI_STEP);

    const raw = await fs.readFile(recordPath(dataDir, deploymentId), "utf8");
    const parsed = WorkflowDeploymentRecord(JSON.parse(raw));
    if (parsed instanceof type.errors) {
      throw new Error(`record failed validation: ${parsed.summary}`);
    }
    expect(parsed).toEqual(MULTI_STEP);
    expect("hubPublicKey" in parsed).toBe(false);
    expect("sessionId" in parsed).toBe(false);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("overwriting a record leaves the new one and no temp orphan", async () => {
    const dataDir = await makeDataDir();
    const deploymentId = "rotated-1";

    // A source rotation overwrites the existing record in place. The
    // atomic write must replace it cleanly, leaving only the record and
    // no `.tmp` staging file behind.
    await writeWorkflowDeploymentRecord(dataDir, deploymentId, SINGLE_STEP);
    await writeWorkflowDeploymentRecord(dataDir, deploymentId, MULTI_STEP);

    const dir = path.join(dataDir, "workflow-runs", deploymentId);
    expect(await fs.readdir(dir)).toEqual(["deployment.json"]);

    const scanned = await scanWorkflowDeploymentRecords(dataDir);
    expect(scanned.map((s) => s.record)).toEqual([MULTI_STEP]);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("delete removes the record and is a no-op when absent", async () => {
    const dataDir = await makeDataDir();
    const deploymentId = "gone-1";

    // No-op when the record was never written.
    await deleteWorkflowDeploymentRecord(dataDir, deploymentId);

    await writeWorkflowDeploymentRecord(dataDir, deploymentId, SINGLE_STEP);
    expect(await fileExists(recordPath(dataDir, deploymentId))).toBe(true);

    await deleteWorkflowDeploymentRecord(dataDir, deploymentId);
    expect(await fileExists(recordPath(dataDir, deploymentId))).toBe(false);

    await fs.rm(dataDir, { recursive: true, force: true });
  });
});

describe("scanWorkflowDeploymentRecords", () => {
  test("returns an empty list when the workflow-runs directory is absent", async () => {
    const dataDir = await makeDataDir();
    // First boot: nothing has been deployed, so `workflow-runs/` does not
    // exist. That is the legitimate empty case, not an error.
    expect(await scanWorkflowDeploymentRecords(dataDir)).toEqual([]);
    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("returns every schema-valid record keyed by its directory name", async () => {
    const dataDir = await makeDataDir();
    await writeWorkflowDeploymentRecord(dataDir, "dep-a", SINGLE_STEP);
    await writeWorkflowDeploymentRecord(dataDir, "dep-b", MULTI_STEP);

    const scanned = await scanWorkflowDeploymentRecords(dataDir);
    const byId = new Map(scanned.map((s) => [s.deploymentId, s.record]));
    expect(byId.size).toBe(2);
    expect(byId.get("dep-a")).toEqual(SINGLE_STEP);
    expect(byId.get("dep-b")).toEqual(MULTI_STEP);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("soft-fails a corrupt or schema-invalid record while returning the valid ones", async () => {
    const dataDir = await makeDataDir();
    await writeWorkflowDeploymentRecord(dataDir, "dep-valid", SINGLE_STEP);

    // A directory whose record is not valid JSON.
    const corruptDir = path.join(dataDir, "workflow-runs", "dep-corrupt");
    await fs.mkdir(corruptDir, { recursive: true });
    await fs.writeFile(path.join(corruptDir, "deployment.json"), "{ not json");

    // A directory whose record parses but fails the schema (missing fields).
    const invalidDir = path.join(dataDir, "workflow-runs", "dep-invalid");
    await fs.mkdir(invalidDir, { recursive: true });
    await fs.writeFile(
      path.join(invalidDir, "deployment.json"),
      JSON.stringify({ version: 1 }),
    );

    // A bare run directory with no record at all.
    await fs.mkdir(path.join(dataDir, "workflow-runs", "dep-empty"), {
      recursive: true,
    });

    const scanned = await scanWorkflowDeploymentRecords(dataDir);
    expect(scanned.map((s) => s.deploymentId)).toEqual(["dep-valid"]);
    expect(scanned[0]?.record).toEqual(SINGLE_STEP);

    await fs.rm(dataDir, { recursive: true, force: true });
  });
});
