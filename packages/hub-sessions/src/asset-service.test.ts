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
      if (
        agentAssets.some(
          (a) => a.agentId === row.agentId && a.assetId === row.assetId,
        )
      ) {
        return Promise.reject(
          new UniqueViolation("agent_asset_agent_asset"),
        );
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
            where: (_w: unknown) => joinAgentAssets(),
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

    test("rejects kind agent-state", async () => {
      const err = await service
        .createAsset({
          tenantId: "tnt_1",
          kind: "agent-state",
          name: "x",
        })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(AssetServiceError);
      if (!(err instanceof AssetServiceError)) throw new Error("unreachable");
      expect(err.reason).toBe("unsupported_kind");
      expect(dbFixture.assets).toHaveLength(0);
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
});
