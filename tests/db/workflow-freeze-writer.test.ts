import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { createDbFrozenApprovalWriter } from "@intx/hub-sessions";
import { workflowDefinition, workflowDefinitionVersion } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const TENANT = "tnt";
const CREATOR = "prn_creator";
const ASSET = "ast_wf";
const HASH = "a".repeat(64);

// The freeze writer's two statements -- projecting the definition and stamping
// the approved hash onto its version row -- must commit as one. Its gate-level
// callers substitute a mock `persist`, so this is the only coverage of the real
// DB writer.
//
// The writer's rowcount guard (throw unless exactly one row is stamped) is not
// exercised here on purpose: `ensureWorkflowDefinitionForAsset` always ensures a
// version-`FROZEN_VERSION` row before the stamp, so through the real writer the
// update always matches exactly one. The guard is defense against the
// hand-coupled `FROZEN_VERSION` constant drifting from the version the ensure
// helper projects -- a code-level divergence, not a reachable data state.
describe.skipIf(!harnessDbEnvAvailable())(
  "createDbFrozenApprovalWriter (real DB)",
  () => {
    let h: TestDb;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedPrincipal(h.db, {
        id: CREATOR,
        tenantId: TENANT,
        kind: "user",
        refId: "creator",
      });
      await seedAsset(h.db, {
        id: ASSET,
        tenantId: TENANT,
        kind: "workflow",
        name: "wf-name",
        displayName: "WF Display",
        creatorPrincipalId: CREATOR,
      });
    });

    test("projects the definition and stamps its version row in one transaction", async () => {
      const writer = createDbFrozenApprovalWriter(h.db);
      const { definitionId } = await writer({
        assetId: ASSET,
        approvedWireHash: HASH,
        approvedGrants: [],
      });

      // The ensure step created a definition keyed by (asset, wireHash) AND the
      // hash landed on its version row: both writes are present, so the freeze
      // committed atomically rather than leaving a NULL-hash "not yet approved"
      // row behind.
      const defs = await h.db
        .select()
        .from(workflowDefinition)
        .where(eq(workflowDefinition.assetId, ASSET));
      expect(defs).toHaveLength(1);
      expect(defs[0]?.id).toBe(definitionId);
      expect(defs[0]?.wireHash).toBe(HASH);

      const versions = await h.db
        .select()
        .from(workflowDefinitionVersion)
        .where(eq(workflowDefinitionVersion.definitionId, definitionId));
      expect(versions).toHaveLength(1);
      expect(versions[0]?.version).toBe("1");
      expect(versions[0]?.approvedWireHash).toBe(HASH);
    });

    test("same asset with different wire hashes yields two definitions", async () => {
      // The identity space is keyed on (assetId, wireHash), so one asset backs
      // many definitions -- a monorepo asset installs one definition per member,
      // each member projecting to its own wire hash.
      const writer = createDbFrozenApprovalWriter(h.db);
      const otherHash = "b".repeat(64);
      const first = await writer({
        assetId: ASSET,
        approvedWireHash: HASH,
        approvedGrants: [],
      });
      const second = await writer({
        assetId: ASSET,
        approvedWireHash: otherHash,
        approvedGrants: [],
      });

      expect(second.definitionId).not.toBe(first.definitionId);
      const defs = await h.db
        .select()
        .from(workflowDefinition)
        .where(eq(workflowDefinition.assetId, ASSET));
      expect(defs).toHaveLength(2);
    });

    test("different assets with the same wire hash yield two definitions", async () => {
      // The unique key is (assetId, wireHash), NOT a global wireHash, so the
      // same hash under two assets is two distinct definitions.
      const otherAsset = "ast_wf_other";
      await seedAsset(h.db, {
        id: otherAsset,
        tenantId: TENANT,
        kind: "workflow",
        name: "wf-name-2",
        displayName: "WF Display 2",
        creatorPrincipalId: CREATOR,
      });
      const writer = createDbFrozenApprovalWriter(h.db);
      const first = await writer({
        assetId: ASSET,
        approvedWireHash: HASH,
        approvedGrants: [],
      });
      const second = await writer({
        assetId: otherAsset,
        approvedWireHash: HASH,
        approvedGrants: [],
      });

      expect(second.definitionId).not.toBe(first.definitionId);
      const forFirst = await h.db
        .select()
        .from(workflowDefinition)
        .where(eq(workflowDefinition.assetId, ASSET));
      const forSecond = await h.db
        .select()
        .from(workflowDefinition)
        .where(eq(workflowDefinition.assetId, otherAsset));
      expect(forFirst).toHaveLength(1);
      expect(forSecond).toHaveLength(1);
    });

    test("re-freezing the same asset+hash is idempotent and keeps the stamp", async () => {
      const writer = createDbFrozenApprovalWriter(h.db);
      const first = await writer({
        assetId: ASSET,
        approvedWireHash: HASH,
        approvedGrants: [],
      });
      const second = await writer({
        assetId: ASSET,
        approvedWireHash: HASH,
        approvedGrants: [],
      });

      // The (asset, wireHash) selector resolves to the same definition, so a
      // repeat freeze neither forks a second definition nor a second version
      // row, and the approved hash stays stamped.
      expect(second.definitionId).toBe(first.definitionId);

      const defs = await h.db
        .select()
        .from(workflowDefinition)
        .where(eq(workflowDefinition.assetId, ASSET));
      expect(defs).toHaveLength(1);

      const versions = await h.db
        .select()
        .from(workflowDefinitionVersion)
        .where(eq(workflowDefinitionVersion.definitionId, first.definitionId));
      expect(versions).toHaveLength(1);
      expect(versions[0]?.approvedWireHash).toBe(HASH);
    });
  },
);
