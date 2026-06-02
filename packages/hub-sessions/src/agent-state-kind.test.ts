import { describe, test, expect } from "bun:test";
import {
  agentStateAuthorize,
  agentStateKindHandler,
  AGENT_STATE_DEPLOY_REF,
} from "./agent-state-kind";
import type { Principal, RepoId } from "./repo-store";

describe("agentStateKindHandler metadata", () => {
  test("declares the agent-state kind and agents directory prefix", () => {
    expect(agentStateKindHandler.kind).toBe("agent-state");
    expect(agentStateKindHandler.directoryPrefix).toBe("agents");
  });
});

describe("agentStateAuthorize", () => {
  const REPO: RepoId = { kind: "agent-state", id: "agent-1" };
  const OTHER_REPO: RepoId = { kind: "agent-state", id: "agent-2" };
  const STATE_REF = "refs/heads/state";

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
        resource: overrides.resource ?? "agent-state:agent-1",
        grantVerb: overrides.grantVerb ?? "read",
      },
      tokenClaims: {
        refPattern: overrides.refPattern ?? "refs/heads/**",
        actions: overrides.actions ?? ["createPack", "resolveRef"],
        expiresAt: overrides.expiresAt ?? farFuture(),
      },
    } as Principal;
  }

  test("hub principal: allowed for any action (regression)", () => {
    for (const action of [
      "init",
      "writeTree",
      "receivePack",
      "createPack",
      "resolveRef",
    ] as const) {
      const r = agentStateAuthorize(
        { kind: "hub" } as Principal,
        REPO,
        STATE_REF,
        action,
      );
      expect(r.allowed).toBe(true);
    }
  });

  test("sidecar principal: receivePack allowed on its own repo (regression)", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    const r = agentStateAuthorize(sidecar, REPO, STATE_REF, "receivePack");
    expect(r.allowed).toBe(true);
  });

  test("sidecar principal: createPack / resolveRef on deploy ref allowed (regression)", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    expect(
      agentStateAuthorize(sidecar, REPO, AGENT_STATE_DEPLOY_REF, "createPack")
        .allowed,
    ).toBe(true);
    expect(
      agentStateAuthorize(sidecar, REPO, AGENT_STATE_DEPLOY_REF, "resolveRef")
        .allowed,
    ).toBe(true);
  });

  test("sidecar principal: createPack on non-deploy ref denied (regression)", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    const r = agentStateAuthorize(sidecar, REPO, STATE_REF, "createPack");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/sidecar may only fetch/);
  });

  test("sidecar principal: cannot access another sidecar's repo (regression)", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    const r = agentStateAuthorize(
      sidecar,
      OTHER_REPO,
      STATE_REF,
      "receivePack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/cannot access/);
  });

  test("sidecar principal: writeTree denied (regression)", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    const r = agentStateAuthorize(sidecar, REPO, STATE_REF, "writeTree");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/hub-only/);
  });

  test("user principal: allowed when claims and verdict agree", () => {
    const r = agentStateAuthorize(
      userPrincipal(),
      REPO,
      STATE_REF,
      "createPack",
    );
    expect(r.allowed).toBe(true);
  });

  test("user principal: denied for non-agent-state repo", () => {
    const r = agentStateAuthorize(
      userPrincipal({ resource: "asset:asset-1" }),
      { kind: "skill", id: "asset-1" },
      STATE_REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/non-agent-state repo/);
  });

  test("user principal: malformed principal is denied with structural reason", () => {
    const badPrincipal = {
      kind: "user",
      principalId: "user-1",
      // missing tenantId, authz, tokenClaims
    } as Principal;
    const r = agentStateAuthorize(badPrincipal, REPO, STATE_REF, "createPack");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/user principal is malformed/);
  });

  test("user principal: denied when tokenClaims.actions does not include the requested action", () => {
    const r = agentStateAuthorize(
      userPrincipal({ actions: ["resolveRef"] }),
      REPO,
      STATE_REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/token does not grant action createPack/);
  });

  test("user principal: denied when refPattern does not match the requested ref", () => {
    const r = agentStateAuthorize(
      userPrincipal({ refPattern: "refs/heads/release-*" }),
      REPO,
      STATE_REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/refPattern .* does not match/);
  });

  test("user principal: denied when the token is expired", () => {
    const r = agentStateAuthorize(
      userPrincipal({ expiresAt: Date.now() - 1 }),
      REPO,
      STATE_REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/token expired/);
  });

  test("user principal: denied when verdict.resource does not target this repo (sanity drift)", () => {
    const r = agentStateAuthorize(
      userPrincipal({ resource: "agent-state:agent-other" }),
      REPO,
      STATE_REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict resource .* does not match/);
  });

  test("user principal: denied when verdict.resource has the wrong kind prefix (sanity drift)", () => {
    const r = agentStateAuthorize(
      userPrincipal({ resource: "asset:agent-1" }),
      REPO,
      STATE_REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict resource .* does not match/);
  });

  test("user principal: denied when verdict.grantVerb does not match the action's verb (sanity drift)", () => {
    const r = agentStateAuthorize(
      userPrincipal({ grantVerb: "write" }),
      REPO,
      STATE_REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict grantVerb .* does not match/);
  });

  test("user principal: denied when verdict effect is deny even though all sanity checks pass", () => {
    const r = agentStateAuthorize(
      userPrincipal({ effect: "deny" }),
      REPO,
      STATE_REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict denied/);
  });

  test("user principal: write action requires write grantVerb and matching claims", () => {
    const r = agentStateAuthorize(
      userPrincipal({
        actions: ["receivePack"],
        grantVerb: "write",
      }),
      REPO,
      STATE_REF,
      "receivePack",
    );
    expect(r.allowed).toBe(true);
  });
});
