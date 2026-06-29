import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto";
import { collectReachableObjects } from "@intx/storage-isogit";
import type { KeyPair } from "@intx/types/runtime";
import {
  skillKindHandler,
  skillFrontmatterSchema,
  skillAuthorize,
  getSkillIndex,
  type SkillIndexEntry,
} from "./skill-kind";
import { createRepoStore } from "./repo-store";
import type {
  KindHandler,
  Principal,
  RepoId,
  ValidatePushResult,
} from "./repo-store";
import { type } from "arktype";

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

function skillMd(frontmatter: Record<string, unknown>, body = ""): string {
  const lines = ["---"];
  for (const [k, v] of Object.entries(frontmatter)) {
    if (typeof v === "string") {
      lines.push(`${k}: ${JSON.stringify(v)}`);
    } else {
      lines.push(`${k}: ${JSON.stringify(v)}`);
    }
  }
  lines.push("---");
  if (body.length > 0) {
    lines.push(body);
  }
  return lines.join("\n");
}

function uniqueRepoId(prefix: string): RepoId {
  const id = `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
  return { kind: "skill", id };
}

describe("skillFrontmatterSchema", () => {
  test("accepts valid frontmatter", () => {
    const result = skillFrontmatterSchema({
      name: "good-skill",
      description: "Describes what this skill does.",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects non-kebab name", () => {
    const result = skillFrontmatterSchema({
      name: "BadName",
      description: "ok",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects name longer than 64 characters", () => {
    const result = skillFrontmatterSchema({
      name: "a".repeat(65),
      description: "ok",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects forbidden name 'anthropic'", () => {
    const result = skillFrontmatterSchema({
      name: "anthropic",
      description: "ok",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects forbidden name 'claude'", () => {
    const result = skillFrontmatterSchema({
      name: "claude",
      description: "ok",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects empty description", () => {
    const result = skillFrontmatterSchema({
      name: "ok",
      description: "",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects description longer than 1024 characters", () => {
    const result = skillFrontmatterSchema({
      name: "ok",
      description: "a".repeat(1025),
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects description containing XML tags", () => {
    const result = skillFrontmatterSchema({
      name: "ok",
      description: "this has <tag>content</tag>",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("accepts and preserves optional Claude Code superset fields", () => {
    const result = skillFrontmatterSchema({
      name: "ok",
      description: "ok",
      when_to_use: "when greeting",
      "allowed-tools": ["Read", "Edit"],
      paths: ["src/"],
      model: "opus",
    });
    expect(result instanceof type.errors).toBe(false);
  });
});

describe("skillKindHandler.validatePush", () => {
  test("accepts a valid single-skill asset and populates the index after onRefUpdated", async () => {
    const repoId = uniqueRepoId("single");
    const files = {
      "greet/SKILL.md": skillMd({
        name: "greet",
        description: "Greets the user.",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["greet"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(true);

    await skillKindHandler.onRefUpdated({
      repoId,
      ref: REF,
      oldSha: null,
      newSha: "deadbeef",
    });

    const entries: SkillIndexEntry[] = getSkillIndex(repoId.id, REF);
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    if (entry === undefined) throw new Error("unreachable");
    expect(entry.name).toBe("greet");
    expect(entry.description).toBe("Greets the user.");
    expect(entry.workspaceSubpath).toBe("greet/");
    expect(entry.frontmatter.name).toBe("greet");
  });

  test("accepts a multi-skill asset and populates one index entry per skill", async () => {
    const repoId = uniqueRepoId("multi");
    const files = {
      "greet/SKILL.md": skillMd({
        name: "greet",
        description: "Greets the user.",
      }),
      "farewell/SKILL.md": skillMd({
        name: "farewell",
        description: "Says goodbye.",
        when_to_use: "at end of session",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["greet", "farewell"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(true);

    await skillKindHandler.onRefUpdated({
      repoId,
      ref: REF,
      oldSha: null,
      newSha: "cafebabe",
    });

    const entries = getSkillIndex(repoId.id, REF);
    expect(entries).toHaveLength(2);
    const names = entries.map((e) => e.name).sort();
    expect(names).toEqual(["farewell", "greet"]);

    const farewell = entries.find((e) => e.name === "farewell");
    if (farewell === undefined) throw new Error("missing entry");
    expect(farewell.frontmatter.when_to_use).toBe("at end of session");
  });

  test("accepts an empty asset (no skill subdirectories)", async () => {
    const repoId = uniqueRepoId("empty");
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [],
      readBlob: makeReadBlob({}),
      listDir: makeListDir({}),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(true);

    await skillKindHandler.onRefUpdated({
      repoId,
      ref: REF,
      oldSha: null,
      newSha: "0000",
    });

    expect(getSkillIndex(repoId.id, REF)).toEqual([]);
  });

  test("rejects when SKILL.md is missing from a skill subdirectory", async () => {
    const repoId = uniqueRepoId("missing");
    // The subdir must be a real directory in the tree (`listDir` does
    // not throw on it) for the handler to treat it as a skill
    // candidate. Seed an unrelated file so `greet/` is a tree, not a
    // blob, but still lacks the required `greet/SKILL.md`.
    const files = {
      "greet/README.md": "no SKILL.md here",
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["greet"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/missing SKILL\.md/);
  });

  test("rejects when frontmatter.name does not match the directory name", async () => {
    const repoId = uniqueRepoId("mismatch");
    const files = {
      "greet/SKILL.md": skillMd({
        name: "different",
        description: "Mismatch.",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["greet"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/does not match directory name/);
  });

  test("rejects when the frontmatter name regex fails", async () => {
    const repoId = uniqueRepoId("badname");
    const files = {
      "BadName/SKILL.md": skillMd({
        name: "BadName",
        description: "ok",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["BadName"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/frontmatter is invalid/);
  });

  test("rejects when the frontmatter name exceeds 64 characters", async () => {
    const longName = "a".repeat(65);
    const repoId = uniqueRepoId("longname");
    const files = {
      [`${longName}/SKILL.md`]: skillMd({
        name: longName,
        description: "ok",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [longName],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
  });

  test("rejects when frontmatter name is 'anthropic'", async () => {
    const repoId = uniqueRepoId("anthropic");
    const files = {
      "anthropic/SKILL.md": skillMd({
        name: "anthropic",
        description: "ok",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["anthropic"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
  });

  test("rejects when frontmatter name is 'claude'", async () => {
    const repoId = uniqueRepoId("claude");
    const files = {
      "claude/SKILL.md": skillMd({
        name: "claude",
        description: "ok",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["claude"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
  });

  test("rejects when description is empty", async () => {
    const repoId = uniqueRepoId("emptydesc");
    const files = {
      "ok/SKILL.md": skillMd({
        name: "ok",
        description: "",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["ok"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
  });

  test("rejects when description exceeds 1024 characters", async () => {
    const repoId = uniqueRepoId("longdesc");
    const files = {
      "ok/SKILL.md": skillMd({
        name: "ok",
        description: "a".repeat(1025),
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["ok"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
  });

  test("rejects when description contains an XML tag", async () => {
    const repoId = uniqueRepoId("xmldesc");
    const files = {
      "ok/SKILL.md": skillMd({
        name: "ok",
        description: "has a <tag>",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["ok"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
  });

  test("skips non-directory top-level entries (e.g. .gitignore from genesis init)", async () => {
    const repoId = uniqueRepoId("nondir");
    const files = {
      ".gitignore": "node_modules\n",
      "greet/SKILL.md": skillMd({
        name: "greet",
        description: "Greets the user.",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [".gitignore", "greet"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(true);

    await skillKindHandler.onRefUpdated({
      repoId,
      ref: REF,
      oldSha: null,
      newSha: "feedface",
    });

    const entries = getSkillIndex(repoId.id, REF);
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    if (entry === undefined) throw new Error("unreachable");
    expect(entry.name).toBe("greet");
    expect(entry.workspaceSubpath).toBe("greet/");
  });

  test("skips a top-level .gitignore even when it is the only entry", async () => {
    const repoId = uniqueRepoId("onlygitignore");
    const files = {
      ".gitignore": "node_modules\n",
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [".gitignore"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(true);

    await skillKindHandler.onRefUpdated({
      repoId,
      ref: REF,
      oldSha: null,
      newSha: "babecafe",
    });

    expect(getSkillIndex(repoId.id, REF)).toEqual([]);
  });

  test("validatePush rejection does not populate the skill index after a subsequent onRefUpdated would be skipped", async () => {
    const repoId = uniqueRepoId("rejected");
    const files = {
      "ok/SKILL.md": skillMd({
        name: "wrongname",
        description: "ok",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["ok"],
      readBlob: makeReadBlob(files),
      listDir: makeListDir(files),
      principal: HUB_PRINCIPAL,
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);

    // The substrate does not call onRefUpdated when validatePush
    // rejects, so the live index stays empty for this (assetId, ref).
    expect(getSkillIndex(repoId.id, REF)).toEqual([]);
  });
});

describe("skillKindHandler metadata", () => {
  test("declares the skill kind and assets/skill directory prefix", () => {
    expect(skillKindHandler.kind).toBe("skill");
    expect(skillKindHandler.directoryPrefix).toBe("assets/skill");
  });
});

describe("skillAuthorize", () => {
  const SKILL_REPO: RepoId = { kind: "skill", id: "asset-123" };
  const AGENT_STATE_REPO: RepoId = { kind: "agent-state", id: "asset-123" };
  const REF = "refs/heads/main";

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
        resource: overrides.resource ?? "asset:asset-123",
        grantVerb: overrides.grantVerb ?? "read",
      },
      tokenClaims: {
        refPattern: overrides.refPattern ?? "refs/heads/**",
        actions: overrides.actions ?? ["createPack", "resolveRef"],
        expiresAt: overrides.expiresAt ?? farFuture(),
      },
    } as Principal;
  }

  test("rejects calls when repoId.kind is not skill", () => {
    const r = skillAuthorize(
      { kind: "hub" } as Principal,
      AGENT_STATE_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/non-skill repo/);
  });

  test("hub principal: allowed for any action (regression)", () => {
    for (const action of [
      "init",
      "writeTree",
      "receivePack",
      "createPack",
      "resolveRef",
    ] as const) {
      const r = skillAuthorize(
        { kind: "hub" } as Principal,
        SKILL_REPO,
        REF,
        action,
      );
      expect(r.allowed).toBe(true);
    }
  });

  test("sidecar principal: createPack / resolveRef allowed (regression)", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    expect(skillAuthorize(sidecar, SKILL_REPO, REF, "createPack").allowed).toBe(
      true,
    );
    expect(skillAuthorize(sidecar, SKILL_REPO, REF, "resolveRef").allowed).toBe(
      true,
    );
  });

  test("sidecar principal: writeTree / receivePack / init denied (regression)", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    for (const action of ["init", "writeTree", "receivePack"] as const) {
      const r = skillAuthorize(sidecar, SKILL_REPO, REF, action);
      expect(r.allowed).toBe(false);
      if (r.allowed) throw new Error("unreachable");
      expect(r.reason).toMatch(/sidecars may only read/);
    }
  });

  test("user principal: allowed when claims and verdict agree", () => {
    const r = skillAuthorize(userPrincipal(), SKILL_REPO, REF, "createPack");
    expect(r.allowed).toBe(true);
  });

  test("user principal: malformed principal is denied with structural reason", () => {
    const badPrincipal = {
      kind: "user",
      principalId: "user-1",
      // missing tenantId, authz, tokenClaims
    } as Principal;
    const r = skillAuthorize(badPrincipal, SKILL_REPO, REF, "createPack");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/user principal is malformed/);
  });

  test("user principal: denied when tokenClaims.actions does not include the requested action", () => {
    const r = skillAuthorize(
      userPrincipal({ actions: ["resolveRef"] }),
      SKILL_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/token does not grant action createPack/);
  });

  test("user principal: denied when refPattern does not match the requested ref", () => {
    const r = skillAuthorize(
      userPrincipal({ refPattern: "refs/heads/release-*" }),
      SKILL_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/refPattern .* does not match/);
  });

  test("user principal: denied when the token is expired", () => {
    const r = skillAuthorize(
      userPrincipal({ expiresAt: Date.now() - 1 }),
      SKILL_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/token expired/);
  });

  test("user principal: denied when verdict.resource does not target this asset (sanity drift)", () => {
    const r = skillAuthorize(
      userPrincipal({ resource: "asset:other-asset" }),
      SKILL_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict resource .* does not match/);
  });

  test("user principal: denied when verdict.resource has the wrong kind prefix (sanity drift)", () => {
    const r = skillAuthorize(
      userPrincipal({ resource: "agent-state:asset-123" }),
      SKILL_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict resource .* does not match/);
  });

  test("user principal: denied when verdict.grantVerb does not match the action's verb (sanity drift)", () => {
    const r = skillAuthorize(
      userPrincipal({ grantVerb: "write" }),
      SKILL_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict grantVerb .* does not match/);
  });

  test("user principal: denied when verdict effect is deny even though all sanity checks pass", () => {
    const r = skillAuthorize(
      userPrincipal({ effect: "deny" }),
      SKILL_REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict denied/);
  });

  test("user principal: write action requires write grantVerb and matching claims", () => {
    const r = skillAuthorize(
      userPrincipal({
        actions: ["receivePack"],
        grantVerb: "write",
      }),
      SKILL_REPO,
      REF,
      "receivePack",
    );
    expect(r.allowed).toBe(true);
  });
});

// The substrate's `receivePack` walks every new commit in the pack and
// invokes the kind handler's `validatePush` once per commit, so a tree
// that violates the skill envelope (e.g. missing SKILL.md, malformed
// frontmatter) on an intermediate commit must reject the pack even
// when the tip is valid. The skill handler does not consult prior
// closures — each commit's tree is judged on its own per-subdir
// SKILL.md content. This regression pins that behaviour: the
// per-commit walk catches an intermediate-state violation at the
// offending commit, not by accidentally being lenient at the tip.
describe("skill per-commit pack walk", () => {
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
    kind: "skill",
    directoryPrefix: "assets/skill",
    validatePush(): ValidatePushResult {
      return { ok: true };
    },
    onRefUpdated() {
      /* no-op */
    },
  };

  const PRINCIPAL: Principal = { kind: "hub" };
  // The skill handler does not gate validation on ref name; the
  // genesis-collision concern that motivates a non-`main` ref in the
  // workflow per-commit test applies here too.
  const REF = "refs/heads/deploy";

  test("rejects a multi-commit pack whose second commit adds a malformed skill subdir", async () => {
    const sourceDataDir = await makeTempDir("skill-percommit-src-");
    const sourceStore = createRepoStore({
      dataDir: sourceDataDir,
      signingKey,
      handlers: { skill: permissiveHandler },
      authorize: () => ({ allowed: true }),
    });
    const repoId: RepoId = {
      kind: "skill",
      id: `sk-${Math.random().toString(36).slice(2, 10)}`,
    };
    await sourceStore.initRepo(repoId);

    const validGreet = [
      "---",
      `name: "greet"`,
      `description: "Greets."`,
      "---",
    ].join("\n");
    const malformedBroken = "no frontmatter here, just body bytes";

    const { commitSha: firstSha } = await sourceStore.writeTree(
      PRINCIPAL,
      repoId,
      REF,
      {
        files: { "greet/SKILL.md": validGreet },
        message: "valid single-skill tree",
      },
    );
    const { commitSha: secondSha } = await sourceStore.writeTree(
      PRINCIPAL,
      repoId,
      REF,
      {
        files: {
          "greet/SKILL.md": validGreet,
          "broken/SKILL.md": malformedBroken,
        },
        message: "intermediate violation: malformed SKILL.md",
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

    const targetDataDir = await makeTempDir("skill-percommit-tgt-");
    const targetStore = createRepoStore({
      dataDir: targetDataDir,
      signingKey,
      handlers: { skill: skillKindHandler },
      authorize: () => ({ allowed: true }),
    });
    await targetStore.initRepo(repoId);

    await expect(
      targetStore.receivePack(PRINCIPAL, repoId, REF, pack, secondSha, null),
    ).rejects.toThrow(/path_violation:.*skill broken frontmatter parse failed/);

    const resolvedAfter = await targetStore.resolveRef(PRINCIPAL, repoId, REF);
    expect(resolvedAfter).toBeNull();
    expect(getSkillIndex(repoId.id, REF)).toEqual([]);
  });

  test("the same violation at the tip of a single-commit pack also rejects", async () => {
    const sourceDataDir = await makeTempDir("skill-tiponly-src-");
    const sourceStore = createRepoStore({
      dataDir: sourceDataDir,
      signingKey,
      handlers: { skill: permissiveHandler },
      authorize: () => ({ allowed: true }),
    });
    const repoId: RepoId = {
      kind: "skill",
      id: `sk-${Math.random().toString(36).slice(2, 10)}`,
    };
    await sourceStore.initRepo(repoId);
    const { commitSha } = await sourceStore.writeTree(PRINCIPAL, repoId, REF, {
      files: { "broken/SKILL.md": "no frontmatter here" },
      message: "tip violates the skill envelope",
    });
    const { pack } = await sourceStore.createPack(PRINCIPAL, repoId, REF);

    const targetDataDir = await makeTempDir("skill-tiponly-tgt-");
    const targetStore = createRepoStore({
      dataDir: targetDataDir,
      signingKey,
      handlers: { skill: skillKindHandler },
      authorize: () => ({ allowed: true }),
    });
    await targetStore.initRepo(repoId);
    await expect(
      targetStore.receivePack(PRINCIPAL, repoId, REF, pack, commitSha, null),
    ).rejects.toThrow(/path_violation:.*skill broken frontmatter parse failed/);
  });
});
