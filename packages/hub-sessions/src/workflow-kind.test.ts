import { describe, test, expect } from "bun:test";
import {
  workflowKindHandler,
  workflowAuthorize,
  WORKFLOW_JSON_PATH,
  CAPABILITY_DECLARATIONS_JSON_PATH,
  WORKFLOW_GITIGNORE_PATH,
} from "./workflow-kind";
import type { Principal, RepoId } from "./repo-store";

const REF = "refs/heads/main";

const HUB_PRINCIPAL: Principal = { kind: "hub" };
const noPriorBlob = async (): Promise<Uint8Array | null> => null;
const noPriorDir = async (): Promise<string[]> => [];

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

function uniqueRepoId(prefix: string): RepoId {
  const id = `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
  return { kind: "workflow", id };
}

function validWorkflowJSON(): string {
  return JSON.stringify({
    id: "my-workflow",
    triggers: [{ type: "manual" }],
    steps: {
      first: { kind: "step", id: "first" },
    },
    stepOrder: ["first"],
  });
}

describe("workflowKindHandler.validatePush", () => {
  test("accepts a tree with workflow.json, capability-declarations.json, and .gitignore", async () => {
    const repoId = uniqueRepoId("complete");
    const files = {
      [WORKFLOW_JSON_PATH]: validWorkflowJSON(),
      [CAPABILITY_DECLARATIONS_JSON_PATH]: JSON.stringify({ declarations: [] }),
      [WORKFLOW_GITIGNORE_PATH]: "",
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [
        WORKFLOW_JSON_PATH,
        CAPABILITY_DECLARATIONS_JSON_PATH,
        WORKFLOW_GITIGNORE_PATH,
      ],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(true);
  });

  test("accepts a tree with only workflow.json", async () => {
    const repoId = uniqueRepoId("minimal");
    const files = {
      [WORKFLOW_JSON_PATH]: validWorkflowJSON(),
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [WORKFLOW_JSON_PATH],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(true);
  });

  test("rejects when workflow.json is missing", async () => {
    const repoId = uniqueRepoId("nodef");
    const files = {
      [WORKFLOW_GITIGNORE_PATH]: "",
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [WORKFLOW_GITIGNORE_PATH],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/missing required workflow\.json/);
  });

  test("rejects when a disallowed top-level entry is present", async () => {
    const repoId = uniqueRepoId("extra");
    const files = {
      [WORKFLOW_JSON_PATH]: validWorkflowJSON(),
      "stray-file.txt": "not allowed here",
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [WORKFLOW_JSON_PATH, "stray-file.txt"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/unexpected top-level entry/);
  });

  test("rejects when a disallowed top-level directory is present", async () => {
    const repoId = uniqueRepoId("subdir");
    const files = {
      [WORKFLOW_JSON_PATH]: validWorkflowJSON(),
      "extras/notes.md": "stray",
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [WORKFLOW_JSON_PATH, "extras"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/unexpected top-level entry/);
  });

  test("rejects when workflow.json is not valid JSON", async () => {
    const repoId = uniqueRepoId("badjson");
    const files = {
      [WORKFLOW_JSON_PATH]: "{not-json",
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [WORKFLOW_JSON_PATH],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/workflow\.json is not valid JSON/);
  });

  test("rejects when workflow.json is missing required fields", async () => {
    const repoId = uniqueRepoId("incomplete");
    const files = {
      [WORKFLOW_JSON_PATH]: JSON.stringify({ id: "x" }),
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [WORKFLOW_JSON_PATH],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/workflow\.json failed validation/);
  });

  test("rejects when workflow.json id is empty", async () => {
    const repoId = uniqueRepoId("emptyid");
    const files = {
      [WORKFLOW_JSON_PATH]: JSON.stringify({
        id: "",
        triggers: [],
        steps: {},
        stepOrder: [],
      }),
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [WORKFLOW_JSON_PATH],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/workflow\.json failed validation/);
  });

  test("rejects when workflow.json is a JSON primitive instead of an object", async () => {
    const repoId = uniqueRepoId("notobj");
    const files = {
      [WORKFLOW_JSON_PATH]: "42",
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [WORKFLOW_JSON_PATH],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/workflow\.json failed validation/);
  });

  test("rejects when capability-declarations.json is not valid JSON", async () => {
    const repoId = uniqueRepoId("badcap");
    const files = {
      [WORKFLOW_JSON_PATH]: validWorkflowJSON(),
      [CAPABILITY_DECLARATIONS_JSON_PATH]: "not-json",
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [
        WORKFLOW_JSON_PATH,
        CAPABILITY_DECLARATIONS_JSON_PATH,
      ],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(
      /capability-declarations\.json is not valid JSON/,
    );
  });

  test("rejects when capability-declarations.json is a JSON array instead of an object", async () => {
    const repoId = uniqueRepoId("caparr");
    const files = {
      [WORKFLOW_JSON_PATH]: validWorkflowJSON(),
      [CAPABILITY_DECLARATIONS_JSON_PATH]: "[]",
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [
        WORKFLOW_JSON_PATH,
        CAPABILITY_DECLARATIONS_JSON_PATH,
      ],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(
      /capability-declarations\.json must be a JSON object/,
    );
  });

  test("accepts capability-declarations.json with an arbitrary object body (deferred to walk task)", async () => {
    const repoId = uniqueRepoId("freeform");
    const files = {
      [WORKFLOW_JSON_PATH]: validWorkflowJSON(),
      [CAPABILITY_DECLARATIONS_JSON_PATH]: JSON.stringify({
        anything: "goes",
        nested: { values: [1, 2, 3] },
      }),
    };
    const result = await workflowKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [
        WORKFLOW_JSON_PATH,
        CAPABILITY_DECLARATIONS_JSON_PATH,
      ],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(true);
  });
});

describe("workflowKindHandler metadata", () => {
  test("declares the workflow kind and assets/workflow directory prefix", () => {
    expect(workflowKindHandler.kind).toBe("workflow");
    expect(workflowKindHandler.directoryPrefix).toBe("assets/workflow");
  });
});

describe("workflowAuthorize", () => {
  const WORKFLOW_REPO: RepoId = { kind: "workflow", id: "wf-123" };
  const AGENT_STATE_REPO: RepoId = { kind: "agent-state", id: "wf-123" };

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
        resource: overrides.resource ?? "workflow:wf-123",
        grantVerb: overrides.grantVerb ?? "read",
      },
      tokenClaims: {
        refPattern: overrides.refPattern ?? "refs/heads/**",
        actions: overrides.actions ?? ["createPack", "resolveRef"],
        expiresAt: overrides.expiresAt ?? farFuture(),
      },
    } as Principal;
  }

  test("rejects calls when repoId.kind is not workflow", () => {
    const r = workflowAuthorize(
      { kind: "hub" } as Principal,
      AGENT_STATE_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/non-workflow repo/);
  });

  test("hub principal: allowed for every action", () => {
    for (const action of [
      "init",
      "writeTree",
      "receivePack",
      "createPack",
      "resolveRef",
    ] as const) {
      const r = workflowAuthorize(
        { kind: "hub" } as Principal,
        WORKFLOW_REPO,
        REF,
        action,
      );
      expect(r.allowed).toBe(true);
    }
  });

  test("sidecar principal: createPack / resolveRef allowed", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    expect(
      workflowAuthorize(sidecar, WORKFLOW_REPO, REF, "createPack").allowed,
    ).toBe(true);
    expect(
      workflowAuthorize(sidecar, WORKFLOW_REPO, REF, "resolveRef").allowed,
    ).toBe(true);
  });

  test("sidecar principal: writeTree / receivePack / init denied", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    for (const action of ["init", "writeTree", "receivePack"] as const) {
      const r = workflowAuthorize(sidecar, WORKFLOW_REPO, REF, action);
      expect(r.allowed).toBe(false);
      if (r.allowed) throw new Error("unreachable");
      expect(r.reason).toMatch(/sidecars may only read workflow assets/);
    }
  });

  test("sidecar principal: malformed principal is denied", () => {
    const malformed = { kind: "sidecar" } as Principal;
    const r = workflowAuthorize(malformed, WORKFLOW_REPO, REF, "createPack");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/sidecar principal is malformed/);
  });

  test("user principal: allowed when claims and verdict agree", () => {
    const r = workflowAuthorize(
      userPrincipal(),
      WORKFLOW_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(true);
  });

  test("user principal: bulk read uses '*' ref and bypasses refPattern check", () => {
    const r = workflowAuthorize(
      userPrincipal({ refPattern: "refs/heads/release-*" }),
      WORKFLOW_REPO,
      "*",
      "resolveRef",
    );
    expect(r.allowed).toBe(true);
  });

  test("user principal: malformed principal is denied with structural reason", () => {
    const badPrincipal = {
      kind: "user",
      principalId: "user-1",
    } as Principal;
    const r = workflowAuthorize(badPrincipal, WORKFLOW_REPO, REF, "createPack");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/user principal is malformed/);
  });

  test("user principal: denied when tokenClaims.actions does not include the requested action", () => {
    const r = workflowAuthorize(
      userPrincipal({ actions: ["resolveRef"] }),
      WORKFLOW_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/token does not grant action createPack/);
  });

  test("user principal: denied when refPattern does not match the requested ref", () => {
    const r = workflowAuthorize(
      userPrincipal({ refPattern: "refs/heads/release-*" }),
      WORKFLOW_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/refPattern .* does not match/);
  });

  test("user principal: denied when the token is expired", () => {
    const r = workflowAuthorize(
      userPrincipal({ expiresAt: Date.now() - 1 }),
      WORKFLOW_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/token expired/);
  });

  test("user principal: denied when verdict.resource targets a different workflow id", () => {
    const r = workflowAuthorize(
      userPrincipal({ resource: "workflow:other-wf" }),
      WORKFLOW_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict resource .* does not match/);
  });

  test("user principal: denied when verdict.resource has the wrong kind prefix", () => {
    const r = workflowAuthorize(
      userPrincipal({ resource: "asset:wf-123" }),
      WORKFLOW_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict resource .* does not match/);
  });

  test("user principal: denied when verdict.grantVerb does not match the action's verb", () => {
    const r = workflowAuthorize(
      userPrincipal({ grantVerb: "write" }),
      WORKFLOW_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict grantVerb .* does not match/);
  });

  test("user principal: denied when verdict effect is deny even though all sanity checks pass", () => {
    const r = workflowAuthorize(
      userPrincipal({ effect: "deny" }),
      WORKFLOW_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict denied/);
  });

  test("user principal: write action requires write grantVerb and matching claims", () => {
    const r = workflowAuthorize(
      userPrincipal({
        actions: ["receivePack"],
        grantVerb: "write",
      }),
      WORKFLOW_REPO,
      REF,
      "receivePack",
    );
    expect(r.allowed).toBe(true);
  });

  test("unknown principal kind is denied with a generic reason", () => {
    const r = workflowAuthorize(
      { kind: "robot" } as Principal,
      WORKFLOW_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/unknown principal kind/);
  });
});
