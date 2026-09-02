import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";
import { createEnvKeyCredentialCipher } from "@intx/crypto";
import type { CredentialDelivery } from "@intx/types/sidecar";

import {
  WorkflowRunRecord,
  writeWorkflowRunRecord,
  deleteWorkflowRunRecord,
  scanWorkflowRunRecords,
} from "./workflow-run-record";

// Real ciphers (not a noop) so the tests exercise actual seal/unseal. A second
// cipher under a different key drives the decrypt-failure path (a rotated
// sidecar key).
const CIPHER = createEnvKeyCredentialCipher(new Uint8Array(32).fill(1));
const OTHER_KEY_CIPHER = createEnvKeyCredentialCipher(
  new Uint8Array(32).fill(2),
);

async function makeDataDir(): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), "wdr-"));
}

function recordPath(dataDir: string, anchorRunId: string): string {
  return path.join(dataDir, "workflow-runs", anchorRunId, "deployment.json");
}

async function fileExists(p: string): Promise<boolean> {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

/** Read the raw on-disk record (still sealed) and validate its schema shape. */
async function readRawRecord(
  dataDir: string,
  anchorRunId: string,
): Promise<WorkflowRunRecord> {
  const raw = await fs.readFile(recordPath(dataDir, anchorRunId), "utf8");
  const parsed = WorkflowRunRecord(JSON.parse(raw));
  if (parsed instanceof type.errors) {
    throw new Error(`record failed validation: ${parsed.summary}`);
  }
  return parsed;
}

/** Build a credential-material cell (no tool bindings) from id -> secret. */
function deliveryOf(materials: Record<string, string>): CredentialDelivery {
  return {
    bindings: [],
    materials: Object.entries(materials).map(([credentialId, secret]) => ({
      credentialId,
      providerKey: "anthropic",
      origin: "https://api.example/anthropic",
      secret,
    })),
  };
}

// Fixtures are the unified format: `sources`/`bodySources` are non-secret config
// carrying a `credentialId` per source, and every secret lives in `credentials`
// (the material cell), sealed to ciphertext on disk by `writeWorkflowRunRecord`
// and unsealed on read by `scanWorkflowRunRecords`.
const SINGLE_STEP: WorkflowRunRecord = {
  version: 2,
  agentAddress: "run_abc123@tenant.example",
  definitionId: "wf_abc123",
  sources: {
    "step-1": [
      {
        id: "anthropic:mock",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        credentialId: "cred-x",
        model: "claude-mock",
      },
    ],
  },
  credentials: deliveryOf({ "cred-x": "sk-x" }),
  sessionId: "ses_1",
  hubPublicKey: "deadbeef",
  // Source-ref is the only lineage: every record carries the pin + approved
  // hash the restore re-verifies the re-materialized closure against.
  approvedWireHash: "d".repeat(64),
  lineage: "source-ref",
  sourceRef: {
    source: { kind: "registry", registry: "npm" },
    closure: {
      schemaVersion: "1",
      topLevel: [{ name: "@x/wf", version: "1.0.0" }],
      entries: [],
    },
  },
};

// A multi-step deployment records no head hub key and may carry no session
// id -- both optional fields absent.
const MULTI_STEP: WorkflowRunRecord = {
  version: 2,
  agentAddress: "run_xyz@tenant.example",
  definitionId: "wf_xyz",
  sources: {
    plan: [
      {
        id: "anthropic:mock",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        credentialId: "cred-y",
        model: "claude-mock",
      },
    ],
    execute: [
      {
        id: "openai:mock",
        provider: "openai",
        baseURL: "https://api.example/openai",
        credentialId: "cred-z",
        model: "gpt-mock",
      },
    ],
  },
  credentials: deliveryOf({ "cred-y": "sk-y", "cred-z": "sk-z" }),
  approvedWireHash: "e".repeat(64),
  lineage: "source-ref",
  sourceRef: {
    source: { kind: "registry", registry: "npm" },
    closure: {
      schemaVersion: "1",
      topLevel: [{ name: "@x/wf-multi", version: "2.0.0" }],
      entries: [],
    },
  },
};

// A source-ref deployment: the record schema's discriminated union REQUIRES
// a sourceRef pin (source + closure) + approvedWireHash for lineage
// "source-ref".
const SOURCE_REF: WorkflowRunRecord = {
  version: 2,
  agentAddress: "ins_dep_src@tenant.example",
  definitionId: "wf_src",
  sources: {
    "step-1": [
      {
        id: "anthropic:mock",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        credentialId: "cred-s",
        model: "claude-mock",
      },
    ],
  },
  credentials: deliveryOf({ "cred-s": "sk-s" }),
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

// A deployment that spawns bodies (an onTrigger section and a childWorkflow
// child): the top-level `sources` plus a `bodySources` table keyed by each
// body's definition id, and one `credentials` cell backing every source's
// `credentialId` -- top-level and per-body alike.
const WITH_BODIES: WorkflowRunRecord = {
  version: 2,
  agentAddress: "ins_dep_bodies@tenant.example",
  definitionId: "wf_bodies",
  sources: {
    "step-1": [
      {
        id: "anthropic:mock",
        provider: "anthropic",
        baseURL: "https://api.example/anthropic",
        credentialId: "cred-top",
        model: "claude-mock",
      },
    ],
  },
  bodySources: {
    "wf_bodies:onTrigger:0": {
      "body-step": [
        {
          id: "anthropic:mock",
          provider: "anthropic",
          baseURL: "https://api.example/anthropic",
          credentialId: "cred-body-a",
          model: "claude-mock",
        },
      ],
    },
    "wf_bodies:child:handler": {
      plan: [
        {
          id: "openai:mock",
          provider: "openai",
          baseURL: "https://api.example/openai",
          credentialId: "cred-body-b",
          model: "gpt-mock",
        },
      ],
    },
  },
  credentials: deliveryOf({
    "cred-top": "sk-top",
    "cred-body-a": "sk-body-a",
    "cred-body-b": "sk-body-b",
  }),
  approvedWireHash: "f".repeat(64),
  lineage: "source-ref",
  sourceRef: {
    source: { kind: "registry", registry: "npm" },
    closure: {
      schemaVersion: "1",
      topLevel: [{ name: "@x/wf", version: "1.0.0" }],
      entries: [],
    },
  },
};

describe("workflow run record store", () => {
  test("seal/unseal round-trips a source-ref record through disk", async () => {
    const dataDir = await makeDataDir();
    const anchorRunId = "src-tenant-example";
    await writeWorkflowRunRecord(dataDir, anchorRunId, SOURCE_REF, CIPHER);

    // The scan unseals the credential cell, so the restored record matches the
    // original.
    const scanned = await scanWorkflowRunRecords(dataDir, CIPHER);
    expect(scanned.map((s) => s.record)).toEqual([SOURCE_REF]);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("seals each credential secret at rest and unseals it on scan", async () => {
    const dataDir = await makeDataDir();
    const anchorRunId = "abc123-tenant-example";
    await writeWorkflowRunRecord(dataDir, anchorRunId, SINGLE_STEP, CIPHER);

    // The record embeds credential secrets (sealed) plus the deployment's
    // identity, so it must not be group/world readable on a shared host.
    const stat = await fs.stat(recordPath(dataDir, anchorRunId));
    expect(stat.mode & 0o077).toBe(0);

    // On disk the record is version 2 and each material's secret is ciphertext
    // -- NOT the plaintext the caller handed in. The source config carries only
    // the non-secret `credentialId`.
    const onDisk = await readRawRecord(dataDir, anchorRunId);
    expect(onDisk.version).toBe(2);
    expect(onDisk.sources["step-1"]?.[0]?.credentialId).toBe("cred-x");
    const sealedSecret = onDisk.credentials?.materials[0]?.secret;
    expect(typeof sealedSecret).toBe("string");
    expect(sealedSecret).not.toBe("sk-x");

    // The scan unseals it back to the plaintext for the restored run.
    const scanned = await scanWorkflowRunRecords(dataDir, CIPHER);
    expect(scanned[0]?.record.credentials?.materials[0]?.secret).toBe("sk-x");

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("seals every credential in the cell, top-level and per-body alike", async () => {
    const dataDir = await makeDataDir();
    const anchorRunId = "bodies-tenant-example";
    await writeWorkflowRunRecord(dataDir, anchorRunId, WITH_BODIES, CIPHER);

    // On disk every material secret is ciphertext, distinct from its plaintext,
    // and the AAD is namespaced by credential id so two materials never seal to
    // the same ciphertext even were their plaintext to match.
    const onDisk = await readRawRecord(dataDir, anchorRunId);
    const sealed = new Map(
      (onDisk.credentials?.materials ?? []).map((m) => [
        m.credentialId,
        m.secret,
      ]),
    );
    expect(sealed.get("cred-top")).not.toBe("sk-top");
    expect(sealed.get("cred-body-a")).not.toBe("sk-body-a");
    expect(sealed.get("cred-body-b")).not.toBe("sk-body-b");
    expect(new Set(sealed.values()).size).toBe(3);

    // The scan unseals the whole cell back to the plaintext the caller handed in.
    const scanned = await scanWorkflowRunRecords(dataDir, CIPHER);
    expect(scanned).toEqual([{ runId: anchorRunId, record: WITH_BODIES }]);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("soft-skips a credential decrypt failure, dropping the whole run", async () => {
    const dataDir = await makeDataDir();
    const anchorRunId = "bodies-corrupt-example";
    await writeWorkflowRunRecord(dataDir, anchorRunId, WITH_BODIES, CIPHER);

    // Corrupt ONE material's sealed secret in place under the correct key. The
    // other materials are intact, so this isolates a single-credential decrypt
    // failure: the whole run is dropped rather than half-restored.
    const onDisk = await readRawRecord(dataDir, anchorRunId);
    const material = onDisk.credentials?.materials[0];
    if (material === undefined) {
      throw new Error("fixture is missing a material to corrupt");
    }
    material.secret = "not-a-valid-ciphertext";
    await fs.writeFile(
      recordPath(dataDir, anchorRunId),
      JSON.stringify(onDisk, null, 2),
      "utf8",
    );

    const scanned = await scanWorkflowRunRecords(dataDir, CIPHER);
    expect(scanned).toEqual([]);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("round-trips a record with the optional fields absent (multi-step)", async () => {
    const dataDir = await makeDataDir();
    const anchorRunId = "run_xyz-tenant-example";
    await writeWorkflowRunRecord(dataDir, anchorRunId, MULTI_STEP, CIPHER);

    const scanned = await scanWorkflowRunRecords(dataDir, CIPHER);
    const restored = scanned[0]?.record;
    expect(restored).toEqual(MULTI_STEP);
    expect(restored !== undefined && "hubPublicKey" in restored).toBe(false);
    expect(restored !== undefined && "sessionId" in restored).toBe(false);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("round-trips a record that binds no credentials", async () => {
    const dataDir = await makeDataDir();
    const anchorRunId = "no-creds-1";
    // A deployment whose sources need no secret carries no `credentials` cell;
    // the write seals nothing and the scan unseals nothing.
    const { credentials: _omit, ...noCreds } = SOURCE_REF;
    void _omit;
    await writeWorkflowRunRecord(dataDir, anchorRunId, noCreds, CIPHER);

    const scanned = await scanWorkflowRunRecords(dataDir, CIPHER);
    const restored = scanned[0]?.record;
    expect(restored).toEqual(noCreds);
    expect(restored !== undefined && "credentials" in restored).toBe(false);

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
      WorkflowRunRecord(r) instanceof type.errors;

    const base = {
      version: 2,
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

  test("overwriting a record leaves the new one and no temp orphan", async () => {
    const dataDir = await makeDataDir();
    const anchorRunId = "rotated-1";

    // A source rotation overwrites the existing record in place. The
    // atomic write must replace it cleanly, leaving only the record and
    // no `.tmp` staging file behind.
    await writeWorkflowRunRecord(dataDir, anchorRunId, SINGLE_STEP, CIPHER);
    await writeWorkflowRunRecord(dataDir, anchorRunId, MULTI_STEP, CIPHER);

    const dir = path.join(dataDir, "workflow-runs", anchorRunId);
    expect(await fs.readdir(dir)).toEqual(["deployment.json"]);

    const scanned = await scanWorkflowRunRecords(dataDir, CIPHER);
    expect(scanned.map((s) => s.record)).toEqual([MULTI_STEP]);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("delete removes the record and is a no-op when absent", async () => {
    const dataDir = await makeDataDir();
    const anchorRunId = "gone-1";

    // No-op when the record was never written.
    await deleteWorkflowRunRecord(dataDir, anchorRunId);

    await writeWorkflowRunRecord(dataDir, anchorRunId, SINGLE_STEP, CIPHER);
    expect(await fileExists(recordPath(dataDir, anchorRunId))).toBe(true);

    await deleteWorkflowRunRecord(dataDir, anchorRunId);
    expect(await fileExists(recordPath(dataDir, anchorRunId))).toBe(false);

    await fs.rm(dataDir, { recursive: true, force: true });
  });
});

describe("scanWorkflowRunRecords", () => {
  test("returns an empty list when the workflow-runs directory is absent", async () => {
    const dataDir = await makeDataDir();
    // First boot: nothing has been deployed, so `workflow-runs/` does not
    // exist. That is the legitimate empty case, not an error.
    expect(await scanWorkflowRunRecords(dataDir, CIPHER)).toEqual([]);
    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("returns every schema-valid record keyed by its directory name", async () => {
    const dataDir = await makeDataDir();
    await writeWorkflowRunRecord(dataDir, "dep-a", SINGLE_STEP, CIPHER);
    await writeWorkflowRunRecord(dataDir, "dep-b", MULTI_STEP, CIPHER);

    const scanned = await scanWorkflowRunRecords(dataDir, CIPHER);
    const byId = new Map(scanned.map((s) => [s.runId, s.record]));
    expect(byId.size).toBe(2);
    expect(byId.get("dep-a")).toEqual(SINGLE_STEP);
    expect(byId.get("dep-b")).toEqual(MULTI_STEP);

    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("soft-skips a record sealed under a different key", async () => {
    const dataDir = await makeDataDir();
    // A record whose credential cell was sealed under one key cannot be
    // unsealed under a rotated/wrong key: the whole run is dropped as
    // corruption, not half-restored.
    await writeWorkflowRunRecord(dataDir, "wrong-key", SINGLE_STEP, CIPHER);
    expect(await scanWorkflowRunRecords(dataDir, OTHER_KEY_CIPHER)).toEqual([]);
    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("soft-skips a record whose JSON is corrupt", async () => {
    const dataDir = await makeDataDir();
    const dir = path.join(dataDir, "workflow-runs", "dep-corrupt");
    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(path.join(dir, "deployment.json"), "{ not json", "utf8");
    expect(await scanWorkflowRunRecords(dataDir, CIPHER)).toEqual([]);
    await fs.rm(dataDir, { recursive: true, force: true });
  });

  test("soft-skips a record that fails schema validation", async () => {
    const dataDir = await makeDataDir();
    const dir = path.join(dataDir, "workflow-runs", "dep-invalid");
    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(
      path.join(dir, "deployment.json"),
      JSON.stringify({ version: 2 }),
      "utf8",
    );
    expect(await scanWorkflowRunRecords(dataDir, CIPHER)).toEqual([]);
    await fs.rm(dataDir, { recursive: true, force: true });
  });
});
