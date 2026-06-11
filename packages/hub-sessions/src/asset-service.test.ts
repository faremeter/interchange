import { describe, test, expect, beforeEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import type { DB } from "@intx/db";
import {
  agentAsset as agentAssetTable,
  asset as assetTable,
} from "@intx/db/schema";
import { generateKeyPair } from "@intx/crypto-node";
import type { KeyPair } from "@intx/types/runtime";

import {
  AssetServiceError,
  createAssetService,
  type AssetService,
} from "./asset-service";
import { createRepoStore } from "./repo-store";
import type { Principal, RepoStore } from "./repo-store";
import { skillKindHandler, skillAuthorize } from "./skill-kind";

// ---------------------------------------------------------------------------
// DB stub
//
// Mirrors the mocking style used by hub-session-orchestrator.test.ts: a
// minimal in-memory store routed by drizzle table identity. Captures the
// rows passed to insert/values so the test can assert column-level shape,
// and raises a typed Postgres unique-violation error (code "23505") to
// exercise the AssetService's translation of duplicates into typed domain
// errors.
//
// The where-clauses passed by drizzle's `eq` are opaque values; rather
// than inspect them, the stub exposes `nextFindFirstAssetId` and
// `nextSelectAgentId` setters that the tests use to tell the stub which
// id the next query targets.
// ---------------------------------------------------------------------------

type AssetRow = {
  id: string;
  tenantId: string;
  kind: string;
  name: string;
  displayName: string | null;
  creatorPrincipalId: string | null;
  createdAt: Date;
  updatedAt: Date;
};

type AgentAssetRow = {
  id: string;
  agentId: string;
  assetId: string;
  ref: string;
  accessMode: string;
  createdAt: Date;
};

class UniqueViolation extends Error {
  readonly code = "23505";
  constructor(constraint: string) {
    super(`duplicate key value violates unique constraint "${constraint}"`);
  }
}

class ForeignKeyViolation extends Error {
  readonly code = "23503";
  constructor(constraint: string) {
    super(`insert violates foreign key constraint "${constraint}"`);
  }
}

type DBStub = {
  db: DB["db"];
  assets: AssetRow[];
  agentAssets: AgentAssetRow[];
  nextFindFirstAssetId: (id: string) => void;
  nextSelectAgentId: (id: string) => void;
};

function makeDB(): DBStub {
  const assets: AssetRow[] = [];
  const agentAssets: AgentAssetRow[] = [];

  let lastFindFirstId: string | null = null;
  let lastSelectAgentId: string | null = null;

  function isAssetRow(row: AssetRow | AgentAssetRow): row is AssetRow {
    return "tenantId" in row && "kind" in row;
  }

  function insertReturning(
    table: unknown,
    row: AssetRow | AgentAssetRow,
  ): Promise<unknown[]> {
    if (table === assetTable) {
      if (!isAssetRow(row)) {
        throw new Error("insert into asset received non-asset row shape");
      }
      if (
        assets.some(
          (a) =>
            a.tenantId === row.tenantId &&
            a.kind === row.kind &&
            a.name === row.name,
        )
      ) {
        return Promise.reject(new UniqueViolation("asset_tenant_kind_name"));
      }
      assets.push(row);
      return Promise.resolve([row]);
    }
    if (table === agentAssetTable) {
      if (isAssetRow(row)) {
        throw new Error("insert into agent_asset received asset row shape");
      }
      if (!assets.some((a) => a.id === row.assetId)) {
        return Promise.reject(
          new ForeignKeyViolation("agent_asset_asset_id_asset_id_fk"),
        );
      }
      if (
        agentAssets.some(
          (a) => a.agentId === row.agentId && a.assetId === row.assetId,
        )
      ) {
        return Promise.reject(new UniqueViolation("agent_asset_agent_asset"));
      }
      agentAssets.push(row);
      return Promise.resolve([row]);
    }
    throw new Error(`unexpected insert table`);
  }

  function findFirstAsset(): Promise<AssetRow | undefined> {
    if (lastFindFirstId === null) {
      return Promise.resolve(undefined);
    }
    const match = assets.find((a) => a.id === lastFindFirstId);
    lastFindFirstId = null;
    return Promise.resolve(match);
  }

  function joinAgentAssets(): Promise<unknown[]> {
    if (lastSelectAgentId === null) {
      return Promise.resolve([]);
    }
    const id = lastSelectAgentId;
    lastSelectAgentId = null;
    const joined = agentAssets
      .filter((aa) => aa.agentId === id)
      .map((aa) => {
        const a = assets.find((x) => x.id === aa.assetId);
        if (a === undefined) {
          throw new Error(
            `inconsistent stub: agent_asset ${aa.id} references missing asset ${aa.assetId}`,
          );
        }
        return { agentAsset: aa, asset: a };
      });
    return Promise.resolve(joined);
  }

  /* eslint-disable @typescript-eslint/no-unsafe-type-assertion --
   * drizzle PgDatabase type cannot be structurally satisfied in tests */
  const db = {
    insert(t: unknown) {
      return {
        values(row: AssetRow | AgentAssetRow) {
          return {
            returning: () => insertReturning(t, row),
          };
        },
      };
    },
    query: {
      asset: {
        findFirst: (_args: { where: unknown }) => findFirstAsset(),
      },
    },
    select(_cols: unknown) {
      return {
        from: (_t: unknown) => ({
          innerJoin: (_t2: unknown, _on: unknown) => ({
            where: (_w: unknown) => ({
              orderBy: (..._order: unknown[]) => joinAgentAssets(),
            }),
          }),
        }),
      };
    },
  } as unknown as DB["db"];
  /* eslint-enable @typescript-eslint/no-unsafe-type-assertion */

  return {
    db,
    assets,
    agentAssets,
    nextFindFirstAssetId: (id) => {
      lastFindFirstId = id;
    },
    nextSelectAgentId: (id) => {
      lastSelectAgentId = id;
    },
  };
}

// ---------------------------------------------------------------------------
// RepoStore fixture
//
// The skill kind handler runs against a real on-disk RepoStore so the
// validatePush -> path_violation translation is exercised end-to-end.
// ---------------------------------------------------------------------------

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

let signingKey: KeyPair;

async function createSkillRepoStore(): Promise<RepoStore> {
  if (signingKey === undefined) {
    signingKey = await generateKeyPair();
  }
  const dataDir = await makeTempDir("asset-svc-");
  return createRepoStore({
    dataDir,
    signingKey,
    handlers: { skill: skillKindHandler },
    authorize: skillAuthorize,
  });
}

const HUB: Principal = { kind: "hub" };

const VALID_SKILL_TREE = {
  files: {
    "greet/SKILL.md":
      "---\nname: greet\ndescription: Say hello in a chosen language\n---\n\nbody\n",
  },
  message: "initial",
};

const INVALID_SKILL_TREE = {
  files: {
    // Reserved name "anthropic" is rejected by the kind handler's
    // arktype frontmatter schema; the substrate translates the
    // rejection into a "path_violation:"-prefixed Error message.
    "anthropic/SKILL.md":
      "---\nname: anthropic\ndescription: reserved-name skill\n---\n\nbody\n",
  },
  message: "initial",
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("AssetService", () => {
  let dbFixture: DBStub;
  let repoStore: RepoStore;
  let service: AssetService;

  beforeEach(async () => {
    dbFixture = makeDB();
    repoStore = await createSkillRepoStore();
    service = createAssetService({ db: dbFixture.db, repoStore });
  });

  describe("createAsset", () => {
    test("inserts the asset row and initializes the skill repo", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
        displayName: "Greeting skill",
        creatorPrincipalId: "prn_1",
      });

      expect(asset.kind).toBe("skill");
      expect(asset.tenantId).toBe("tnt_1");
      expect(asset.name).toBe("greet");
      expect(asset.displayName).toBe("Greeting skill");
      expect(asset.creatorPrincipalId).toBe("prn_1");
      expect(asset.id).toMatch(/^ast_/);

      expect(dbFixture.assets).toHaveLength(1);
      const inserted = dbFixture.assets[0];
      if (inserted === undefined) throw new Error("unreachable");
      expect(inserted.id).toBe(asset.id);

      // initRepo creates the on-disk repo so subsequent writeTree calls
      // succeed without ENOENT. Writing a tree against the new repo is
      // the cheapest way to assert the repo exists; the substrate would
      // throw at the first git operation otherwise.
      const out = await repoStore.writeTree(
        HUB,
        { kind: "skill", id: asset.id },
        "refs/heads/main",
        VALID_SKILL_TREE,
      );
      expect(out.commitSha).toMatch(/^[0-9a-f]{40}$/);
    });

    test.each([["agent-state"], ["workflow"], ["workflow-run"]] as const)(
      "rejects kind %s",
      async (kind) => {
        const err = await service
          .createAsset({
            tenantId: "tnt_1",
            kind,
            name: "x",
          })
          .catch((e: unknown) => e);

        expect(err).toBeInstanceOf(AssetServiceError);
        if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
        expect(err.reason).toBe("unsupported_kind");
        expect(dbFixture.assets).toHaveLength(0);
      },
    );

    test("accepts a valid lowercase-kebab name", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "my-skills",
      });
      expect(asset.name).toBe("my-skills");
      expect(dbFixture.assets).toHaveLength(1);
    });

    test.each([
      [".", "single dot"],
      ["..", "double dot"],
      ["my skill", "embedded space"],
      ["skills/greet", "embedded slash"],
    ])("rejects name %p (%s) as invalid_name", async (badName) => {
      const err = await service
        .createAsset({
          tenantId: "tnt_1",
          kind: "skill",
          name: badName,
        })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("invalid_name");
      expect(dbFixture.assets).toHaveLength(0);
    });

    test("rejects a package-registry asset whose name shadows a configured HTTP registry", async () => {
      // The session service builds the per-launch tool-package
      // registry map by iterating package-registry assets first and
      // HTTP registries second, with an asset-wins-on-name-collision
      // rule. Without the reserved-name gate, a `package-registry`
      // asset named `npmjs` on any tenant would silently shadow the
      // public npm registry for every session resolving through that
      // tenant — an opaque reroute the operator did not request. Pin
      // the rejection at the asset-creation boundary so the collision
      // surfaces at intent time, not as a debugging exercise later.
      const reservedService = createAssetService({
        db: dbFixture.db,
        repoStore,
        reservedPackageRegistryNames: new Set(["npmjs"]),
      });
      const err = await reservedService
        .createAsset({
          tenantId: "tnt_1",
          kind: "package-registry",
          name: "npmjs",
        })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("name_reserved");
      expect(err.message).toContain("npmjs");
      expect(dbFixture.assets).toHaveLength(0);
    });

    test("a reserved name only shadows the matching kind", async () => {
      // The reserved-name gate scopes to `package-registry` — a skill
      // asset named `npmjs` poses no shadowing risk against the
      // registry map. Surface that asymmetry in the test so a future
      // refactor that broadens the gate has to update this assertion
      // along with the code.
      const reservedService = createAssetService({
        db: dbFixture.db,
        repoStore,
        reservedPackageRegistryNames: new Set(["npmjs"]),
      });
      const asset = await reservedService.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "npmjs",
      });
      expect(asset.kind).toBe("skill");
      expect(asset.name).toBe("npmjs");
    });

    test("surfaces duplicate (tenantId, kind, name) as duplicate_asset", async () => {
      await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });

      const err = await service
        .createAsset({
          tenantId: "tnt_1",
          kind: "skill",
          name: "greet",
        })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("duplicate_asset");
      expect(dbFixture.assets).toHaveLength(1);
    });
  });

  describe("populateAsset", () => {
    test("happy path writes the tree and returns commitSha", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });

      dbFixture.nextFindFirstAssetId(asset.id);
      const out = await service.populateAsset({
        assetId: asset.id,
        ref: "refs/heads/main",
        tree: VALID_SKILL_TREE,
        principal: HUB,
      });
      expect(out.commitSha).toMatch(/^[0-9a-f]{40}$/);
    });

    test("surfaces an unknown assetId as not_found", async () => {
      dbFixture.nextFindFirstAssetId("ast_missing");
      const err = await service
        .populateAsset({
          assetId: "ast_missing",
          ref: "refs/heads/main",
          tree: VALID_SKILL_TREE,
          principal: HUB,
        })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("not_found");
    });

    test("translates path_violation: error from validatePush", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });

      dbFixture.nextFindFirstAssetId(asset.id);
      const err = await service
        .populateAsset({
          assetId: asset.id,
          ref: "refs/heads/main",
          tree: INVALID_SKILL_TREE,
          principal: HUB,
        })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("path_violation");
      expect(err.message).toContain("path_violation:");
    });
  });

  describe("attachAsset", () => {
    test("inserts an agent_asset row", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });

      const aa = await service.attachAsset({
        agentId: "agt_1",
        assetId: asset.id,
        ref: "refs/heads/main",
        accessMode: "read-only",
      });

      expect(aa.id).toMatch(/^aas_/);
      expect(aa.agentId).toBe("agt_1");
      expect(aa.assetId).toBe(asset.id);
      expect(aa.accessMode).toBe("read-only");
      expect(dbFixture.agentAssets).toHaveLength(1);
    });

    test("defaults accessMode to read-only when omitted", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });

      const aa = await service.attachAsset({
        agentId: "agt_1",
        assetId: asset.id,
        ref: "refs/heads/main",
      });
      expect(aa.accessMode).toBe("read-only");
    });

    test("surfaces a missing assetId reference as invalid_reference", async () => {
      const err = await service
        .attachAsset({
          agentId: "agt_1",
          assetId: "ast_missing",
          ref: "refs/heads/main",
        })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("invalid_reference");
      expect(dbFixture.agentAssets).toHaveLength(0);
    });

    test("surfaces duplicate (agentId, assetId) as duplicate_attachment", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });

      await service.attachAsset({
        agentId: "agt_1",
        assetId: asset.id,
        ref: "refs/heads/main",
      });

      const err = await service
        .attachAsset({
          agentId: "agt_1",
          assetId: asset.id,
          ref: "refs/heads/main",
        })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("duplicate_attachment");
      expect(dbFixture.agentAssets).toHaveLength(1);
    });
  });

  describe("listAgentAssets", () => {
    test("returns joined rows including asset.name and asset.kind", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
        displayName: "Greeting",
      });
      await service.attachAsset({
        agentId: "agt_1",
        assetId: asset.id,
        ref: "refs/heads/main",
      });

      dbFixture.nextSelectAgentId("agt_1");
      const rows = await service.listAgentAssets("agt_1");
      expect(rows).toHaveLength(1);
      const row = rows[0];
      if (row === undefined) throw new Error("unreachable");
      expect(row.agentId).toBe("agt_1");
      expect(row.asset.id).toBe(asset.id);
      expect(row.asset.kind).toBe("skill");
      expect(row.asset.name).toBe("greet");
      expect(row.asset.displayName).toBe("Greeting");
    });
  });

  describe("readAssetBlob / listAssetBlobs", () => {
    test("readAssetBlob returns the bytes of a blob at HEAD", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });

      dbFixture.nextFindFirstAssetId(asset.id);
      await service.populateAsset({
        assetId: asset.id,
        ref: "refs/heads/main",
        tree: VALID_SKILL_TREE,
        principal: HUB,
      });

      dbFixture.nextFindFirstAssetId(asset.id);
      const bytes = await service.readAssetBlob({
        assetId: asset.id,
        path: "greet/SKILL.md",
      });
      const text = new TextDecoder().decode(bytes);
      expect(text).toContain("name: greet");
    });

    test("listAssetBlobs returns only blob entries (skips subtrees)", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });

      dbFixture.nextFindFirstAssetId(asset.id);
      await service.populateAsset({
        assetId: asset.id,
        ref: "refs/heads/main",
        tree: VALID_SKILL_TREE,
        principal: HUB,
      });

      dbFixture.nextFindFirstAssetId(asset.id);
      const names = await service.listAssetBlobs({
        assetId: asset.id,
        dir: "",
      });
      // The only root entry produced by VALID_SKILL_TREE is the
      // `greet/` subtree. listAssetBlobs filters to blob entries so
      // callers iterating the result can readBlob each name without a
      // per-entry type check; the subtree is excluded by design.
      expect(names).not.toContain("greet");
    });

    test("readAssetBlob throws not_found on a missing asset", async () => {
      dbFixture.nextFindFirstAssetId("ast_missing");
      const err = await service
        .readAssetBlob({ assetId: "ast_missing", path: "x" })
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("not_found");
    });

    test("readAssetBlob throws not_found on a missing path", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });
      dbFixture.nextFindFirstAssetId(asset.id);
      await service.populateAsset({
        assetId: asset.id,
        ref: "refs/heads/main",
        tree: VALID_SKILL_TREE,
        principal: HUB,
      });

      dbFixture.nextFindFirstAssetId(asset.id);
      const err = await service
        .readAssetBlob({
          assetId: asset.id,
          path: "does-not-exist",
        })
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("not_found");
    });

    test("listAssetBlobs throws not_found on a non-directory path", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });
      dbFixture.nextFindFirstAssetId(asset.id);
      await service.populateAsset({
        assetId: asset.id,
        ref: "refs/heads/main",
        tree: VALID_SKILL_TREE,
        principal: HUB,
      });

      dbFixture.nextFindFirstAssetId(asset.id);
      const err = await service
        .listAssetBlobs({
          assetId: asset.id,
          dir: "missing-dir",
        })
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("not_found");
    });

    test("listAssetBlobs rejects malformed dir arguments at the boundary", async () => {
      const asset = await service.createAsset({
        tenantId: "tnt_1",
        kind: "skill",
        name: "greet",
      });
      dbFixture.nextFindFirstAssetId(asset.id);
      await service.populateAsset({
        assetId: asset.id,
        ref: "refs/heads/main",
        tree: VALID_SKILL_TREE,
        principal: HUB,
      });

      const malformedDirs = ["/", "/tarballs", "tarballs/", "a//b", "a/../b"];
      for (const dir of malformedDirs) {
        const err = await service
          .listAssetBlobs({ assetId: asset.id, dir })
          .catch((e: unknown) => e);
        expect(err).toBeInstanceOf(AssetServiceError);
        if (!(err instanceof AssetServiceError)) {
          throw new Error("unreachable");
        }
        expect(err.reason).toBe("path_violation");
      }
    });
  });
});
