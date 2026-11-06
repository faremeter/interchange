import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { resolveLocalMailParty } from "@intx/db";
import { tenant as tenantTable, workflowDefinition } from "@intx/db/schema";
import { deriveRunPrincipalId } from "@intx/hub-common";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  seedWorkflowRun,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

const TENANT = "tnt_party";
const DOMAIN = "party.localhost";

describe.skipIf(!harnessDbEnvAvailable())(
  "resolveLocalMailParty (real DB)",
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
      await h.db.insert(tenantTable).values({
        id: TENANT,
        name: TENANT,
        slug: tenantSlugFromId(TENANT),
        domain: DOMAIN,
        parentId: null,
      });
    });

    async function seedAnchor(args: {
      id: string;
      status?: "deployed" | "running" | "completed" | "cancelled";
      principalId?: string;
      definitionId?: string;
    }) {
      await seedWorkflowRun(h.db, {
        id: args.id,
        tenantId: TENANT,
        anchorRunId: args.id,
        address: `${args.id}@${DOMAIN}`,
        status: args.status ?? "running",
        ...(args.principalId === undefined
          ? {}
          : { principalId: args.principalId }),
        ...(args.definitionId === undefined
          ? {}
          : { definitionId: args.definitionId }),
      });
    }

    test("names a deployment that has not fired by its derived run principal", async () => {
      await seedAnchor({ id: "run_fresh", status: "deployed" });

      expect(await resolveLocalMailParty(h.db, `run_fresh@${DOMAIN}`)).toEqual({
        kind: "run",
        anchorRunId: "run_fresh",
        tenantId: TENANT,
        assetId: null,
        linkedPrincipalId: null,
        principalId: await deriveRunPrincipalId(TENANT, "run_fresh"),
      });
    });

    test("names a fired deployment by its linked principal and asset", async () => {
      await seedPrincipal(h.db, {
        id: "prn_linked",
        tenantId: TENANT,
        kind: "workflow",
        refId: "run_fired",
        status: "active",
      });
      await seedAsset(h.db, {
        id: "ast_party",
        tenantId: TENANT,
        kind: "workflow",
        name: "ast_party",
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_party",
        tenantId: TENANT,
        name: "wfd_party",
        assetId: "ast_party",
      });
      await seedAnchor({
        id: "run_fired",
        principalId: "prn_linked",
        definitionId: "wfd_party",
      });

      expect(
        await resolveLocalMailParty(h.db, `run_fired@${DOMAIN}`),
      ).toMatchObject({
        kind: "run",
        assetId: "ast_party",
        linkedPrincipalId: "prn_linked",
        principalId: "prn_linked",
      });
    });

    test("matches every spelling of an address", async () => {
      await seedAnchor({ id: "run_spell" });

      for (const address of [
        `RUN_SPELL@${DOMAIN.toUpperCase()}`,
        `run_spell@${DOMAIN}.`,
      ]) {
        expect(await resolveLocalMailParty(h.db, address)).toMatchObject({
          kind: "run",
          anchorRunId: "run_spell",
        });
      }
    });

    test("names an ended deployment the same way", async () => {
      await seedAnchor({ id: "run_done", status: "completed" });

      expect(
        await resolveLocalMailParty(h.db, `run_done@${DOMAIN}`),
      ).toMatchObject({ kind: "run", anchorRunId: "run_done" });
    });

    test("names nothing for a run on another domain or an unknown run", async () => {
      await seedAnchor({ id: "run_here" });

      expect(
        await resolveLocalMailParty(h.db, "run_here@other.localhost"),
      ).toBeNull();
      expect(
        await resolveLocalMailParty(h.db, `run_absent@${DOMAIN}`),
      ).toBeNull();
    });

    test("names a user principal and nothing for an unknown user", async () => {
      await seedPrincipal(h.db, {
        id: "prn_user",
        tenantId: TENANT,
        kind: "user",
        refId: "usr_ann",
        status: "active",
      });

      expect(await resolveLocalMailParty(h.db, `USR_ANN@${DOMAIN}.`)).toEqual({
        kind: "user",
        principalId: "prn_user",
        tenantId: TENANT,
      });
      expect(await resolveLocalMailParty(h.db, `usr_zed@${DOMAIN}`)).toBeNull();
    });

    test("throws for a user address that names two principals", async () => {
      for (const [id, refId] of [
        ["prn_v1", "Usr_Alice"],
        ["prn_v2", "USR_ALICE"],
      ] as const) {
        await seedPrincipal(h.db, {
          id,
          tenantId: TENANT,
          kind: "user",
          refId,
          status: "active",
        });
      }

      await expect(
        resolveLocalMailParty(h.db, `usr_alice@${DOMAIN}`),
      ).rejects.toThrow(/ambiguous/);
    });
  },
);
