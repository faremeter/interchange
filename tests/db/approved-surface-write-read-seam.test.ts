// The write->read seam for the approved grant surface, on a real DB.
//
// This is the feature's core R2 invariant: the surface a deploy STAMPS is the
// surface run materialization READS, at the same (definitionId, version) key.
// A deploy creates the version row via `ensureWorkflowDefinitionForAsset` (at
// INITIAL_WORKFLOW_DEFINITION_VERSION) and stamps it via
// `writeApprovedGrantSurface` (at the same constant); a run reads it via
// `readApprovedGrantSurface` at the definition's currentVersion. This test runs
// exactly those functions against a real Postgres so the real UPDATE query and
// the key round-trip are executed -- not mocked. The stamp-site unit test uses
// a mocked DB whose UPDATE returns a row regardless of key match, so it cannot
// catch a key mismatch; this test does, because `writeApprovedGrantSurface`'s
// one-row assertion runs against real rows.

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { eq } from "drizzle-orm";

import { readApprovedGrantSurface, writeApprovedGrantSurface } from "@intx/db";
import { workflowDefinition } from "@intx/db/schema";
import {
  ensureWorkflowDefinitionForAsset,
  INITIAL_WORKFLOW_DEFINITION_VERSION,
} from "@intx/hub-sessions";
import type { ApprovedGrantSurface } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast_wf";
const CREATOR = "prn_creator";
const WIRE_HASH = "wirehash_seam";

describe.skipIf(!harnessDbEnvAvailable())(
  "approved grant surface write->read seam (real DB)",
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
        name: ASSET,
        creatorPrincipalId: CREATOR,
      });
    });

    test("a surface stamped at the version a deploy creates reads back at currentVersion", async () => {
      // The version row is born the way a deploy creates it.
      const { definitionId } = await ensureWorkflowDefinitionForAsset(h.db, {
        assetId: ASSET,
        wireHash: WIRE_HASH,
      });

      // Stamped the way a deploy stamps it: the real UPDATE, keyed to the same
      // constant the version was created with. Its one-row assertion runs
      // against a real row here.
      const surface: ApprovedGrantSurface = {
        grants: ["tool:child_do", "effect:child_cap"],
        grantEffects: { "tool:child_do": "ask", "effect:child_cap": "allow" },
      };
      await writeApprovedGrantSurface(
        h.db,
        definitionId,
        INITIAL_WORKFLOW_DEFINITION_VERSION,
        surface,
      );

      // Read the way run materialization reads it: at the definition's
      // currentVersion, which is the version the row was born with.
      const definition = await h.db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, definitionId),
      });
      if (definition === undefined) {
        throw new Error("definition row missing after ensure");
      }
      const readBack = await readApprovedGrantSurface(
        h.db,
        definitionId,
        definition.currentVersion,
      );
      expect(readBack).toEqual(surface);
      // The write key and the read key are the same value -- the seam holds.
      expect(definition.currentVersion).toBe(
        INITIAL_WORKFLOW_DEFINITION_VERSION,
      );
    });

    test("stamping a version that does not exist fails the one-row assertion", async () => {
      // The failure mode the mocked stamp-site test cannot catch: a key that
      // matches zero rows. Against a real DB, writeApprovedGrantSurface refuses.
      const { definitionId } = await ensureWorkflowDefinitionForAsset(h.db, {
        assetId: ASSET,
        wireHash: WIRE_HASH,
      });
      const surface: ApprovedGrantSurface = { grants: [], grantEffects: {} };
      await expect(
        writeApprovedGrantSurface(h.db, definitionId, "999", surface),
      ).rejects.toThrow(/expected exactly one version row/);
    });
  },
);
