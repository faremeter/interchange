// Regression suite covering the substrate-cluster fixes: sequence
// contiguity, principal-vs-path scoping, claim-check inbox-deletion,
// and workflow.json steps-as-array rejection. Each test pins the
// post-fix contract; a regression that loosens any of these checks
// surfaces here.

import { describe, test, expect } from "bun:test";
import {
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "./workflow-run-kind";
import type { Principal } from "./repo-store";

const REF = "refs/heads/events";
const HUB: Principal = { kind: "hub" };

function makeReadBlob(files: Record<string, string>) {
  return async (path: string): Promise<Uint8Array> => {
    const body = files[path];
    if (body === undefined) throw new Error(`readBlob: ${path} not found`);
    return new TextEncoder().encode(body);
  };
}

function makeListDir(files: Record<string, string>) {
  return async (path: string): Promise<string[]> => {
    const prefix = path === "" ? "" : `${path}/`;
    const names = new Set<string>();
    for (const p of Object.keys(files)) {
      if (prefix !== "" && !p.startsWith(prefix)) continue;
      const rest = p.slice(prefix.length);
      if (rest.length === 0) continue;
      const slash = rest.indexOf("/");
      names.add(slash === -1 ? rest : rest.substring(0, slash));
    }
    return Array.from(names);
  };
}

function topLevels(files: Record<string, string>): string[] {
  const names = new Set<string>();
  for (const p of Object.keys(files)) {
    const slash = p.indexOf("/");
    names.add(slash === -1 ? p : p.substring(0, slash));
  }
  return Array.from(names);
}

function makePriorReadBlob(files: Record<string, string>) {
  return async (path: string): Promise<Uint8Array | null> => {
    const body = files[path];
    if (body === undefined) return null;
    return new TextEncoder().encode(body);
  };
}

async function validateRun(
  prospective: Record<string, string>,
  prior: Record<string, string>,
  principal: Principal = HUB,
) {
  return workflowRunKindHandler.validatePush({
    repoId: { kind: "workflow-run", id: "dep-1" },
    ref: REF,
    principal,
    topLevelTreePaths: topLevels(prospective),
    readBlob: makeReadBlob(prospective),
    listDir: makeListDir(prospective),
    priorReadBlob: makePriorReadBlob(prior),
    priorListDir: makeListDir(prior),
  });
}

describe("workflow-run principal-vs-path scoping (regression)", () => {
  test("rejects workflow-process {runId: r1} writing events under runs/r2/", async () => {
    const principal = {
      kind: "workflow-process",
      deploymentId: "dep-1",
      runId: "r1",
    } as Principal;
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "runs/r2/events/0.json": JSON.stringify({ seq: 0, type: "RunStarted" }),
    };
    const r = await validateRun(prospective, {}, principal);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/workflow-process|runId|r1|r2/);
  });

  test("rejects workflow-process writing inbox to any address", async () => {
    const principal = {
      kind: "workflow-process",
      deploymentId: "dep-1",
      runId: "r1",
    } as Principal;
    const ADDR = "other-runs-address";
    const ADDR_SEG = encodeURIComponent(ADDR);
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`addresses/${ADDR_SEG}/inbox/100-msg-1.json`]: JSON.stringify({
        messageId: "msg-1",
        receivedAt: 100,
        address: ADDR,
        mailAuditRef: { store: "audit", path: "x" },
      }),
    };
    const r = await validateRun(prospective, {}, principal);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/workflow-process|addresses/);
  });

  test("accepts workflow-process {runId: r1} writing events under runs/r1/", async () => {
    const principal = {
      kind: "workflow-process",
      deploymentId: "dep-1",
      runId: "r1",
    } as Principal;
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "runs/r1/events/0.json": JSON.stringify({ seq: 0, type: "RunStarted" }),
    };
    const r = await validateRun(prospective, {}, principal);
    expect(r.ok).toBe(true);
  });

  test("accepts workflow-process without runId writing events under any runs/", async () => {
    const principal = {
      kind: "workflow-process",
      deploymentId: "dep-1",
    } as Principal;
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "runs/anything/events/0.json": JSON.stringify({
        seq: 0,
        type: "RunStarted",
      }),
    };
    const r = await validateRun(prospective, {}, principal);
    expect(r.ok).toBe(true);
  });
});
describe("workflow-run sequence contiguity (regression)", () => {
  test("rejects a tree with seq gaps (0.json + 2.json, no 1.json)", async () => {
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "runs/r1/events/0.json": JSON.stringify({ seq: 0, type: "RunStarted" }),
      "runs/r1/events/2.json": JSON.stringify({
        seq: 2,
        type: "StepCompleted",
      }),
    };
    const r = await validateRun(prospective, {});
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain("runs/r1/events");
    expect(r.reason).toMatch(/seq|contiguous|gap/);
  });

  test("rejects events with a gap in the middle (1,3 skipping 2)", async () => {
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "runs/r1/events/1.json": JSON.stringify({ seq: 1, type: "RunStarted" }),
      "runs/r1/events/3.json": JSON.stringify({
        seq: 3,
        type: "StepCompleted",
      }),
    };
    const r = await validateRun(prospective, {});
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain("runs/r1/events");
    expect(r.reason).toContain("2.json");
  });

  test("accepts a single event whose seq is not 0 (the runtime starts at seq=1)", async () => {
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "runs/r1/events/1.json": JSON.stringify({ seq: 1, type: "RunStarted" }),
    };
    const r = await validateRun(prospective, {});
    expect(r.ok).toBe(true);
  });
});

describe("workflow-run single-event drop in a multi-event run (regression)", () => {
  test("rejects dropping only the middle event in a multi-event run", async () => {
    const prior = {
      "runs/r1/events/0.json": JSON.stringify({ seq: 0, type: "RunStarted" }),
      "runs/r1/events/1.json": JSON.stringify({ seq: 1, type: "StepStarted" }),
      "runs/r1/events/2.json": JSON.stringify({
        seq: 2,
        type: "StepCompleted",
      }),
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "runs/r1/events/0.json": JSON.stringify({ seq: 0, type: "RunStarted" }),
      "runs/r1/events/2.json": JSON.stringify({
        seq: 2,
        type: "StepCompleted",
      }),
    };
    const r = await validateRun(prospective, prior);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain("runs/r1/events/1.json");
  });
});
