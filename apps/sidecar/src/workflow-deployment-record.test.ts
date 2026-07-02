import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import {
  WorkflowDeploymentRecord,
  writeWorkflowDeploymentRecord,
  deleteWorkflowDeploymentRecord,
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
    "step-1": {
      id: "anthropic:mock",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      apiKey: "sk-x",
      model: "claude-mock",
    },
  },
  sessionId: "ses_1",
  hubPublicKey: "deadbeef",
};

// A multi-step deployment records no head hub key and may carry no session
// id -- both optional fields absent.
const MULTI_STEP: WorkflowDeploymentRecord = {
  version: 1,
  agentAddress: "ins_dep_xyz@tenant.example",
  definitionId: "wf_xyz",
  sources: {
    plan: {
      id: "anthropic:mock",
      provider: "anthropic",
      baseURL: "https://api.example/anthropic",
      apiKey: "sk-y",
      model: "claude-mock",
    },
    execute: {
      id: "openai:mock",
      provider: "openai",
      baseURL: "https://api.example/openai",
      apiKey: "sk-z",
      model: "gpt-mock",
    },
  },
};

describe("workflow deployment record store", () => {
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
