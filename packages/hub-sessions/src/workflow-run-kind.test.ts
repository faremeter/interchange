import { describe, test, expect } from "bun:test";
import {
  workflowRunKindHandler,
  workflowRunAuthorize,
  WORKFLOW_RUN_GITIGNORE_PATH,
  WORKFLOW_RUN_RUNS_PREFIX,
} from "./workflow-run-kind";
import type { Principal, RepoId } from "./repo-store";

const REF = "refs/heads/events";

function makeReadBlob(
  files: Record<string, string>,
): (path: string) => Promise<Uint8Array> {
  return async (path) => {
    const body = files[path];
    if (body === undefined) {
      throw new Error(`readBlob: ${path} not found`);
    }
    return new TextEncoder().encode(body);
  };
}

function makeListDir(
  files: Record<string, string>,
): (path: string) => Promise<string[]> {
  return async (path) => {
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

function uniqueRepoId(prefix: string): RepoId {
  const id = `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
  return { kind: "workflow-run", id };
}

function eventBody(
  seq: number,
  type: string,
  extras: Record<string, unknown> = {},
): string {
  return JSON.stringify({ seq, type, ...extras });
}

const HUB_PRINCIPAL: Principal = { kind: "hub" };
const SUPERVISOR_PRINCIPAL: Principal = { kind: "supervisor" };
const WORKFLOW_PROCESS_PRINCIPAL: Principal = { kind: "workflow-process" };
const noPriorBlob = async (): Promise<Uint8Array | null> => null;
const noPriorDir = async (): Promise<string[]> => [];

type ValidateOpts = {
  ref?: string;
  principal?: Principal;
  priorFiles?: Record<string, string>;
};

async function validate(
  files: Record<string, string>,
  opts: ValidateOpts = {},
) {
  const repoId = uniqueRepoId("wfr");
  const ref = opts.ref ?? REF;
  const principal = opts.principal ?? HUB_PRINCIPAL;
  const priorReadBlob =
    opts.priorFiles === undefined
      ? noPriorBlob
      : makePriorReadBlob(opts.priorFiles);
  const priorListDir =
    opts.priorFiles === undefined ? noPriorDir : makeListDir(opts.priorFiles);
  return workflowRunKindHandler.validatePush({
    repoId,
    ref,
    principal,
    topLevelTreePaths: topLevels(files),
    readBlob: makeReadBlob(files),
    listDir: makeListDir(files),
    priorReadBlob,
    priorListDir,
  });
}

function makePriorReadBlob(
  files: Record<string, string>,
): (path: string) => Promise<Uint8Array | null> {
  return async (path) => {
    const body = files[path];
    if (body === undefined) return null;
    return new TextEncoder().encode(body);
  };
}

describe("workflowRunKindHandler metadata", () => {
  test("declares the workflow-run kind and workflow-runs directory prefix", () => {
    expect(workflowRunKindHandler.kind).toBe("workflow-run");
    expect(workflowRunKindHandler.directoryPrefix).toBe("workflow-runs");
  });
});

describe("workflowRunKindHandler.validatePush — accepts", () => {
  test("accepts a .gitignore-only genesis tree", async () => {
    const r = await validate({ [WORKFLOW_RUN_GITIGNORE_PATH]: "" });
    expect(r.ok).toBe(true);
  });

  test("accepts a tree with a single run and a single non-terminal event", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    });
    expect(r.ok).toBe(true);
  });

  test("accepts a tree with multiple runs and many ordered events", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "StepStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/2.json`]: eventBody(
        2,
        "StepCompleted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    });
    expect(r.ok).toBe(true);
  });

  test("accepts a terminal event as the last event in the run", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCompleted",
      ),
    });
    expect(r.ok).toBe(true);
  });

  test("accepts a CancelRequested event whose origin matches the signing principal kind", async () => {
    const matrix: { origin: string; principal: Principal }[] = [
      { origin: "self", principal: SUPERVISOR_PRINCIPAL },
      { origin: "supervisor-drain", principal: SUPERVISOR_PRINCIPAL },
      { origin: "supervisor-operator", principal: SUPERVISOR_PRINCIPAL },
      { origin: "hub-admin", principal: HUB_PRINCIPAL },
    ];
    for (const { origin, principal } of matrix) {
      const r = await validate(
        {
          [WORKFLOW_RUN_GITIGNORE_PATH]: "",
          [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
            0,
            "RunStarted",
          ),
          [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
            1,
            "CancelRequested",
            { origin, reason: "operator pressed stop" },
          ),
        },
        { principal },
      );
      expect(r.ok).toBe(true);
    }
  });
});

describe("workflowRunKindHandler.validatePush — rejects top-level shape", () => {
  test("rejects any path under addresses/ (deferred subtree)", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "addresses/user%40example.com/inbox/123.json": "{}",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/deferred .*addresses\//);
  });

  test("rejects any path under control/ (unsupported subtree)", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "control/policy.json": "{}",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/unsupported .*control\//);
  });

  test("rejects an arbitrary disallowed top-level entry", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "stray.txt": "nope",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/unexpected top-level entry/);
  });
});

describe("workflowRunKindHandler.validatePush — rejects event shape", () => {
  test("rejects when a run directory has no events subdirectory", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/notes.txt`]: "stray",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/contains unexpected entry/);
  });

  test("rejects an event file whose name is not <seq>.json", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/oops.json`]: eventBody(
        0,
        "RunStarted",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/does not match <seq>\.json/);
  });

  test("rejects an event whose body is not valid JSON", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: "{not-json",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/is not valid JSON/);
  });

  test("rejects an event missing the required seq and type envelope", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: JSON.stringify({
        data: "no envelope",
      }),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/envelope invalid/);
  });

  test("rejects an event whose body.seq does not match its filename seq", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        7,
        "RunStarted",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/body\.seq .* does not match filename seq/);
  });
});

describe("workflowRunKindHandler.validatePush — terminal-phase lock", () => {
  test("rejects an event whose seq is strictly greater than a prior terminal event's", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCompleted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/2.json`]: eventBody(
        2,
        "StepStarted",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/after terminal RunCompleted/);
  });

  test("rejects events after a RunFailed terminal event", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunFailed",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/2.json`]: eventBody(
        2,
        "StepStarted",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/after terminal RunFailed/);
  });

  test("rejects events after a RunCancelled terminal event", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCancelled",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/2.json`]: eventBody(
        2,
        "StepStarted",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/after terminal RunCancelled/);
  });

  test("treats terminal lock per-run: another run is unaffected", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCompleted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/1.json`]: eventBody(
        1,
        "StepStarted",
      ),
    });
    expect(r.ok).toBe(true);
  });
});

describe("workflowRunKindHandler.validatePush — CancelRequested origin", () => {
  test("rejects a CancelRequested whose origin is not a CancelOrigin", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "CancelRequested",
        { origin: "rogue-actor", reason: "spoof" },
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/origin .* is not a recognised CancelOrigin/);
  });

  test("rejects a CancelRequested missing the origin field", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "CancelRequested",
        { reason: "no origin field" },
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/CancelRequested payload invalid/);
  });

  test("rejects a CancelRequested with an empty reason", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "CancelRequested",
        { origin: "self", reason: "" },
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/CancelRequested payload invalid/);
  });
});

describe("workflowRunKindHandler.validatePush — CancelRequested principal-vs-origin", () => {
  // Only a `hub` principal may mint a `hub-admin` origin; only a
  // `supervisor` principal may mint `self`, `supervisor-drain`, or
  // `supervisor-operator`. The handler enforces the principal-vs-
  // origin pairing at `validatePush`; these cases pin the boundary.

  function cancelTree(origin: string): Record<string, string> {
    return {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "CancelRequested",
        { origin, reason: "cancel" },
      ),
    };
  }

  test("rejects CancelRequested{origin:hub-admin} signed by a workflow-process principal", async () => {
    const r = await validate(cancelTree("hub-admin"), {
      principal: WORKFLOW_PROCESS_PRINCIPAL,
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /CancelRequested origin "hub-admin" requires principal\.kind="hub"/,
    );
    expect(r.reason).toMatch(/principal\.kind="workflow-process"/);
  });

  test("rejects CancelRequested{origin:hub-admin} signed by a supervisor principal", async () => {
    const r = await validate(cancelTree("hub-admin"), {
      principal: SUPERVISOR_PRINCIPAL,
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/requires principal\.kind="hub"/);
    expect(r.reason).toMatch(/principal\.kind="supervisor"/);
  });

  test("rejects CancelRequested{origin:self} signed by a hub principal", async () => {
    const r = await validate(cancelTree("self"), { principal: HUB_PRINCIPAL });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/requires principal\.kind="supervisor"/);
    expect(r.reason).toMatch(/principal\.kind="hub"/);
  });

  test("rejects CancelRequested{origin:supervisor-drain} signed by a hub principal", async () => {
    const r = await validate(cancelTree("supervisor-drain"), {
      principal: HUB_PRINCIPAL,
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/requires principal\.kind="supervisor"/);
  });

  test("rejects CancelRequested{origin:supervisor-operator} signed by a workflow-process principal", async () => {
    const r = await validate(cancelTree("supervisor-operator"), {
      principal: WORKFLOW_PROCESS_PRINCIPAL,
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/requires principal\.kind="supervisor"/);
  });

  test("accepts CancelRequested{origin:hub-admin} signed by a hub principal", async () => {
    const r = await validate(cancelTree("hub-admin"), {
      principal: HUB_PRINCIPAL,
    });
    expect(r.ok).toBe(true);
  });

  test("accepts CancelRequested{origin:self} signed by a supervisor principal", async () => {
    const r = await validate(cancelTree("self"), {
      principal: SUPERVISOR_PRINCIPAL,
    });
    expect(r.ok).toBe(true);
  });
});

describe("workflowRunKindHandler.validatePush — append-only via prior-tree", () => {
  // The handler reads the parent commit's tree via `priorReadBlob`
  // and rejects any event path whose prospective bytes diverge from
  // the prior bytes. These cases pin that append-only boundary at
  // `validatePush`.

  test("rejects a prospective tree that mutates the bytes of an event present in the prior tree", async () => {
    const prior = {
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: eventBody(
        0,
        "RunStarted",
        { original: true },
      ),
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: eventBody(
        0,
        "RunStarted",
        { original: false },
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/1.json`]: eventBody(
        1,
        "StepStarted",
      ),
    };
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /event runs\/run-x\/events\/0\.json bytes diverge from the prior tree/,
    );
  });

  test("accepts a prospective tree that appends a new event while preserving prior bytes", async () => {
    const seq0 = eventBody(0, "RunStarted");
    const prior = {
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: seq0,
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: seq0,
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/1.json`]: eventBody(
        1,
        "StepStarted",
      ),
    };
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(true);
  });

  test("accepts the first push (no prior tree) as inherently append-only", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    });
    expect(r.ok).toBe(true);
  });

  test("rejects a prospective tree that truncates an existing event blob", async () => {
    const prior = {
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: eventBody(
        0,
        "RunStarted",
        { padding: "x".repeat(64) },
      ),
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    };
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/bytes diverge from the prior tree/);
  });
});

describe("workflowRunAuthorize — repoId guard", () => {
  test("rejects calls when repoId.kind is not workflow-run", () => {
    const r = workflowRunAuthorize(
      { kind: "hub" } as Principal,
      { kind: "agent-state", id: "dep-1" } as RepoId,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/non-workflow-run repo/);
  });
});

describe("workflowRunAuthorize — hub principal", () => {
  test("allowed for every action", () => {
    const repo: RepoId = { kind: "workflow-run", id: "dep-1" };
    for (const action of [
      "init",
      "writeTree",
      "receivePack",
      "createPack",
      "resolveRef",
    ] as const) {
      const r = workflowRunAuthorize(
        { kind: "hub" } as Principal,
        repo,
        REF,
        action,
      );
      expect(r.allowed).toBe(true);
    }
  });
});

describe("workflowRunAuthorize — workflow-process principal", () => {
  const REPO: RepoId = { kind: "workflow-run", id: "dep-1" };

  test("allowed for full read+write on its own deployment", () => {
    const principal = {
      kind: "workflow-process",
      deploymentId: "dep-1",
      runId: "run-a",
    } as Principal;
    for (const action of [
      "init",
      "writeTree",
      "receivePack",
      "createPack",
      "resolveRef",
    ] as const) {
      const r = workflowRunAuthorize(principal, REPO, REF, action);
      expect(r.allowed).toBe(true);
    }
  });

  test("allowed without runId field (the per-call runId is optional)", () => {
    const principal = {
      kind: "workflow-process",
      deploymentId: "dep-1",
    } as Principal;
    const r = workflowRunAuthorize(principal, REPO, REF, "writeTree");
    expect(r.allowed).toBe(true);
  });

  test("denied when targeting another deployment's repo", () => {
    const principal = {
      kind: "workflow-process",
      deploymentId: "dep-other",
    } as Principal;
    const r = workflowRunAuthorize(principal, REPO, REF, "writeTree");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/cannot access workflow-run/);
  });

  test("malformed workflow-process principal is denied", () => {
    const principal = { kind: "workflow-process" } as Principal;
    const r = workflowRunAuthorize(principal, REPO, REF, "writeTree");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/workflow-process principal is malformed/);
  });
});

describe("workflowRunAuthorize — supervisor principal", () => {
  const REPO: RepoId = { kind: "workflow-run", id: "dep-1" };

  test("allowed for full read+write on its own deployment", () => {
    const principal = {
      kind: "supervisor",
      deploymentId: "dep-1",
    } as Principal;
    for (const action of [
      "init",
      "writeTree",
      "receivePack",
      "createPack",
      "resolveRef",
    ] as const) {
      const r = workflowRunAuthorize(principal, REPO, REF, action);
      expect(r.allowed).toBe(true);
    }
  });

  test("denied when targeting another deployment's repo", () => {
    const principal = {
      kind: "supervisor",
      deploymentId: "dep-other",
    } as Principal;
    const r = workflowRunAuthorize(principal, REPO, REF, "writeTree");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/cannot access workflow-run/);
  });

  test("malformed supervisor principal is denied", () => {
    const principal = { kind: "supervisor" } as Principal;
    const r = workflowRunAuthorize(principal, REPO, REF, "writeTree");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/supervisor principal is malformed/);
  });
});

describe("workflowRunAuthorize — sidecar principal", () => {
  const REPO: RepoId = { kind: "workflow-run", id: "dep-1" };

  test("createPack / resolveRef allowed", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    expect(workflowRunAuthorize(sidecar, REPO, REF, "createPack").allowed).toBe(
      true,
    );
    expect(workflowRunAuthorize(sidecar, REPO, REF, "resolveRef").allowed).toBe(
      true,
    );
  });

  test("writeTree / receivePack / init denied", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    for (const action of ["init", "writeTree", "receivePack"] as const) {
      const r = workflowRunAuthorize(sidecar, REPO, REF, action);
      expect(r.allowed).toBe(false);
      if (r.allowed) throw new Error("unreachable");
      expect(r.reason).toMatch(/sidecars may only read workflow-run/);
    }
  });

  test("malformed sidecar principal is denied", () => {
    const malformed = { kind: "sidecar" } as Principal;
    const r = workflowRunAuthorize(malformed, REPO, REF, "createPack");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/sidecar principal is malformed/);
  });
});

describe("workflowRunAuthorize — user principal", () => {
  const REPO: RepoId = { kind: "workflow-run", id: "dep-1" };

  function farFuture(): number {
    return Date.now() + 60_000;
  }

  function userPrincipal(
    overrides: {
      effect?: "allow" | "deny";
      resource?: string;
      grantVerb?: string;
      refPattern?: string;
      actions?: string[];
      expiresAt?: number;
    } = {},
  ): Principal {
    return {
      kind: "user",
      principalId: "user-1",
      tenantId: "tenant-1",
      authz: {
        effect: overrides.effect ?? "allow",
        resource: overrides.resource ?? "workflow-run:dep-1",
        grantVerb: overrides.grantVerb ?? "read",
      },
      tokenClaims: {
        refPattern: overrides.refPattern ?? "refs/heads/**",
        actions: overrides.actions ?? ["createPack", "resolveRef"],
        expiresAt: overrides.expiresAt ?? farFuture(),
      },
    } as Principal;
  }

  test("allowed when claims and verdict agree", () => {
    const r = workflowRunAuthorize(userPrincipal(), REPO, REF, "createPack");
    expect(r.allowed).toBe(true);
  });

  test("bulk read uses '*' ref and bypasses refPattern check", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ refPattern: "refs/heads/release-*" }),
      REPO,
      "*",
      "resolveRef",
    );
    expect(r.allowed).toBe(true);
  });

  test("malformed user principal is denied", () => {
    const badPrincipal = { kind: "user", principalId: "u" } as Principal;
    const r = workflowRunAuthorize(badPrincipal, REPO, REF, "createPack");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/user principal is malformed/);
  });

  test("denied when tokenClaims.actions does not include the requested action", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ actions: ["resolveRef"] }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/token does not grant action createPack/);
  });

  test("denied when refPattern does not match the requested ref", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ refPattern: "refs/heads/release-*" }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/refPattern .* does not match/);
  });

  test("denied when the token is expired", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ expiresAt: Date.now() - 1 }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/token expired/);
  });

  test("denied when verdict.resource targets a different workflow-run id", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ resource: "workflow-run:dep-other" }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict resource .* does not match/);
  });

  test("denied when verdict.resource has the wrong kind prefix", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ resource: "workflow:dep-1" }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict resource .* does not match/);
  });

  test("denied when verdict.grantVerb does not match the action's verb", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ grantVerb: "write" }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict grantVerb .* does not match/);
  });

  test("denied when verdict effect is deny even though all sanity checks pass", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ effect: "deny" }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict denied/);
  });

  test("write action requires write grantVerb and matching claims", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ actions: ["receivePack"], grantVerb: "write" }),
      REPO,
      REF,
      "receivePack",
    );
    expect(r.allowed).toBe(true);
  });
});

describe("workflowRunAuthorize — unknown principal", () => {
  test("denied with a generic reason", () => {
    const r = workflowRunAuthorize(
      { kind: "robot" } as Principal,
      { kind: "workflow-run", id: "dep-1" },
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/unknown principal kind/);
  });
});
