import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto-node";
import { collectReachableObjects } from "@intx/storage-isogit";
import type { KeyPair } from "@intx/types/runtime";
import {
  agentStateAuthorize,
  agentStateKindHandler,
  AGENT_STATE_DEPLOY_REF,
} from "./agent-state-kind";
import { createRepoStore } from "./repo-store";
import type {
  KindHandler,
  Principal,
  RepoId,
  ValidatePushResult,
} from "./repo-store";

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

// The substrate's `receivePack` walks every new commit in the pack and
// invokes the kind handler's `validatePush` once per commit, so a tree
// that violates the kind's allowlist on an intermediate commit must
// reject the pack even when the tip's tree happens to be valid. The
// agent-state handler does not consult prior closures — every commit's
// tree is judged on its own top-level paths. This regression pins that
// behaviour: the per-commit walk catches an intermediate-state
// violation at the offending commit, not by accidentally being lenient
// at the tip.
describe("agent-state per-commit pack walk", () => {
  const tempDirs: string[] = [];
  let signingKey: KeyPair;

  beforeAll(async () => {
    signingKey = await generateKeyPair();
  });

  afterAll(async () => {
    for (const d of tempDirs.splice(0)) {
      await fs.promises
        .rm(d, { recursive: true, force: true })
        .catch(() => undefined);
    }
  });

  async function makeTempDir(prefix: string): Promise<string> {
    const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
    tempDirs.push(d);
    return d;
  }

  const permissiveHandler: KindHandler = {
    kind: "agent-state",
    directoryPrefix: "agents",
    validatePush(): ValidatePushResult {
      return { ok: true };
    },
    onRefUpdated() {
      /* no-op */
    },
  };

  const PRINCIPAL: Principal = { kind: "hub" };
  const STATE_REF = "refs/heads/state";

  test("rejects a multi-commit pack whose second commit adds a disallowed top-level path", async () => {
    const sourceDataDir = await makeTempDir("agent-state-percommit-src-");
    const sourceStore = createRepoStore({
      dataDir: sourceDataDir,
      signingKey,
      handlers: { "agent-state": permissiveHandler },
      authorize: () => ({ allowed: true }),
    });
    const repoId: RepoId = {
      kind: "agent-state",
      id: `agent-${Math.random().toString(36).slice(2, 10)}`,
    };
    await sourceStore.initRepo(repoId);

    const { commitSha: firstSha } = await sourceStore.writeTree(
      PRINCIPAL,
      repoId,
      STATE_REF,
      {
        files: { "manifest.jsonl": '{"turn":0}\n' },
        message: "valid state-bearing tree",
      },
    );
    const { commitSha: secondSha } = await sourceStore.writeTree(
      PRINCIPAL,
      repoId,
      STATE_REF,
      {
        files: { "forbidden.txt": "not in the agent-state allowlist" },
        message: "intermediate violation: stray top-level path",
      },
    );

    const sourceDir = sourceStore.getRepoDir(repoId);
    const firstObjects = await collectReachableObjects(sourceDir, firstSha);
    const secondObjects = await collectReachableObjects(sourceDir, secondSha);
    const oids = Array.from(new Set([...firstObjects, ...secondObjects]));
    const packResult = await git.packObjects({
      fs,
      dir: sourceDir,
      oids,
      write: false,
    });
    if (packResult.packfile === undefined) {
      throw new Error("git.packObjects returned no packfile");
    }
    const pack = packResult.packfile;

    const targetDataDir = await makeTempDir("agent-state-percommit-tgt-");
    const targetStore = createRepoStore({
      dataDir: targetDataDir,
      signingKey,
      handlers: { "agent-state": agentStateKindHandler },
      authorize: () => ({ allowed: true }),
    });
    await targetStore.initRepo(repoId);

    await expect(
      targetStore.receivePack(
        PRINCIPAL,
        repoId,
        STATE_REF,
        pack,
        secondSha,
        null,
      ),
    ).rejects.toThrow(
      /path_violation:.*tree contains disallowed top-level path: forbidden\.txt/,
    );

    // The rejected pack must leave the ref unset; a future legitimate
    // push must not observe a half-applied state.
    const resolvedAfter = await targetStore.resolveRef(
      PRINCIPAL,
      repoId,
      STATE_REF,
    );
    expect(resolvedAfter).toBeNull();
  });

  test("the same violation at the tip of a single-commit pack also rejects", async () => {
    // Sanity check: the per-commit walk catches the same kind-handler
    // verdict the tip-only walk would have caught when the violation
    // lives at the tip. Without this the multi-commit test above
    // could in principle pass via some pack-walk-specific path that
    // never reached the kind handler at all.
    const sourceDataDir = await makeTempDir("agent-state-tiponly-src-");
    const sourceStore = createRepoStore({
      dataDir: sourceDataDir,
      signingKey,
      handlers: { "agent-state": permissiveHandler },
      authorize: () => ({ allowed: true }),
    });
    const repoId: RepoId = {
      kind: "agent-state",
      id: `agent-${Math.random().toString(36).slice(2, 10)}`,
    };
    await sourceStore.initRepo(repoId);
    const { commitSha } = await sourceStore.writeTree(
      PRINCIPAL,
      repoId,
      STATE_REF,
      {
        files: { "forbidden.txt": "tip-only violation" },
        message: "tip violates the agent-state allowlist",
      },
    );
    const { pack } = await sourceStore.createPack(PRINCIPAL, repoId, STATE_REF);

    const targetDataDir = await makeTempDir("agent-state-tiponly-tgt-");
    const targetStore = createRepoStore({
      dataDir: targetDataDir,
      signingKey,
      handlers: { "agent-state": agentStateKindHandler },
      authorize: () => ({ allowed: true }),
    });
    await targetStore.initRepo(repoId);
    await expect(
      targetStore.receivePack(
        PRINCIPAL,
        repoId,
        STATE_REF,
        pack,
        commitSha,
        null,
      ),
    ).rejects.toThrow(
      /path_violation:.*tree contains disallowed top-level path: forbidden\.txt/,
    );
  });
});
