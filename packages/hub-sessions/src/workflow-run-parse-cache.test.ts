// Probes that the workflow-run parse cache, shared through one
// validationScope, never lets a cached result stand in for a check that
// depends on the path, the bytes, or the principal.
import { describe, test, expect } from "bun:test";
import {
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
  WORKFLOW_RUN_RUNS_PREFIX,
} from "./workflow-run-kind";
import { validatePushArgs } from "./repo-store/validate-push.test-helpers";
import type { Principal } from "./repo-store";

const REF = "refs/heads/events";
const HUB: Principal = { kind: "hub" };
const SUPERVISOR: Principal = { kind: "supervisor" };
const EVENTS = `${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events`;

function eventBody(
  seq: number,
  type: string,
  extras: Record<string, unknown> = {},
): string {
  return JSON.stringify({ seq, type, ...extras });
}

async function validateInScope(
  scope: object,
  files: Record<string, string>,
  priorFiles: Record<string, string> | undefined,
  principal: Principal = HUB,
) {
  const args = validatePushArgs({
    repoId: { kind: "workflow-run", id: "scope-probe" },
    ref: REF,
    principal,
    files,
    priorFiles,
  });
  return workflowRunKindHandler.validatePush({
    ...args,
    validationScope: scope,
  });
}

const base = {
  [WORKFLOW_RUN_GITIGNORE_PATH]: "",
  [`${EVENTS}/0.json`]: eventBody(0, "RunStarted"),
};

describe("workflow-run parse cache within one validation scope", () => {
  test("a cached event's bytes at a second path are parsed again", async () => {
    const scope = {};
    const first = {
      ...base,
      [`${EVENTS}/1.json`]: eventBody(1, "StepCompleted"),
    };
    expect((await validateInScope(scope, first, base)).ok).toBe(true);
    // `2.json` carries the bytes of `1.json`: the same blob OID at
    // another path, whose filename seq does not match the body.
    const second = {
      ...first,
      [`${EVENTS}/2.json`]: eventBody(1, "StepCompleted"),
    };
    const r = await validateInScope(scope, second, first);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain("does not match filename seq");
  });

  test("a changed blob at a cached path is rejected as diverging", async () => {
    const scope = {};
    expect((await validateInScope(scope, base, undefined)).ok).toBe(true);
    const changed = {
      ...base,
      [`${EVENTS}/0.json`]: eventBody(0, "StepCompleted"),
    };
    const r = await validateInScope(scope, changed, base);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /event runs\/run-x\/events\/0\.json bytes diverge from the prior tree \(blob OID [0-9a-f]{40} vs blob OID [0-9a-f]{40}\)/,
    );
  });

  test("a cached CancelRequested still gets the principal check when newly added", async () => {
    const scope = {};
    const cancel = eventBody(1, "CancelRequested", {
      origin: "hub-admin",
      reason: "operator",
    });
    const withCancel = { ...base, [`${EVENTS}/1.json`]: cancel };
    expect((await validateInScope(scope, withCancel, base, HUB)).ok).toBe(true);
    // Same blob at the same path: a cache hit. It is newly added again,
    // under a principal that may not author a hub-admin cancel.
    const cached = await validateInScope(scope, withCancel, base, SUPERVISOR);
    const fresh = await validateInScope({}, withCancel, base, SUPERVISOR);
    expect(fresh.ok).toBe(false);
    expect(cached).toEqual(fresh);
  });
});
