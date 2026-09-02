import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createNoopCredentialCipher } from "@intx/crypto";
import { resolveInstanceModelSources, resolveModelSources } from "@intx/db";
import { workflowDefinition } from "@intx/db/schema";
import type { ModelRequirement } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedCredential,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

// Credential use through a catalog provider is authorized by tenant ownership
// within the hierarchy: a credential owned by the resolving tenant OR an
// ancestor is usable (descendants inherit) and needs no grant. A
// principal-owned credential is not a tenant resource and is never usable
// through a shared catalog offering. These pin the ownership boundary on both
// secret-injecting paths -- launch (resolveModelSources) and rotation
// (resolveInstanceModelSources) -- across a parent `tnt_root` and a child
// `tnt_child` that inherits from it. Ownership needs no creator, so a
// definition with none still launches a tenant-owned source.

const SECRET = "sk-parent-tenant-credential";
const REQ_OPUS: ModelRequirement[] = [{ model: "opus" }];

// Every credential in this suite is seeded as PLAINTEXT, so resolution runs
// through an explicit noop cipher (a plaintext value passes through unchanged).
const noopCipher = createNoopCredentialCipher();

describe.skipIf(!harnessDbEnvAvailable())(
  "credential-use authorization by tenant ownership (real DB)",
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
    });

    // Parent `tnt_root` owns the credential, the model catalog, and the
    // credential-backed "opus" offering; child `tnt_child` inherits all of it.
    // `credentialPrincipalId` sets the credential's ownership: `null` =
    // tenant-owned (usable by ownership), a principal id = personal (not usable
    // through the catalog). The child hosts a definition whose creator is
    // `creatorPrincipalId` -- possibly `null`, since ownership needs none.
    async function seed(opts: {
      credentialPrincipalId: string | null;
      creatorPrincipalId: string | null;
    }): Promise<void> {
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_child", parentId: "tnt_root" },
      ]);
      if (opts.credentialPrincipalId !== null) {
        await seedPrincipal(h.db, {
          id: opts.credentialPrincipalId,
          tenantId: "tnt_root",
        });
      }
      if (
        opts.creatorPrincipalId !== null &&
        opts.creatorPrincipalId !== opts.credentialPrincipalId
      ) {
        await seedPrincipal(h.db, {
          id: opts.creatorPrincipalId,
          tenantId: "tnt_child",
        });
      }
      await seedProvider(h.db, {
        id: "prv_x",
        tenantId: "tnt_root",
        name: "prv-x",
      });
      await seedCredential(h.db, {
        id: "cred_a",
        tenantId: "tnt_root",
        providerId: "prv_x",
        principalId: opts.credentialPrincipalId,
        name: "cred-a",
        secret: SECRET,
      });
      await seedModel(h.db, {
        id: "mdl_opus",
        tenantId: "tnt_root",
        canonicalName: "opus",
      });
      await seedModelProvider(h.db, {
        id: "mpv_anthropic",
        tenantId: "tnt_root",
        name: "anthropic",
        credentialId: "cred_a",
      });
      await seedModelOffering(h.db, {
        id: "mof_a",
        tenantId: "tnt_root",
        modelId: "mdl_opus",
        providerId: "mpv_anthropic",
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_1",
        tenantId: "tnt_child",
        creatorPrincipalId: opts.creatorPrincipalId,
        name: "agent-1",
        modelRequirements: [{ model: "opus" }],
      });
    }

    describe("a parent tenant-owned credential inherited by the child", () => {
      test("launch resolution emits the inherited secret by ownership", async () => {
        await seed({
          credentialPrincipalId: null,
          creatorPrincipalId: "prn_child_creator",
        });
        const result = await resolveModelSources(
          h.db,
          "tnt_child",
          REQ_OPUS,
          noopCipher,
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.materials.map((m) => m.secret)).toEqual([SECRET]);
      });

      test("rotation re-resolution emits the inherited secret by ownership", async () => {
        await seed({
          credentialPrincipalId: null,
          creatorPrincipalId: "prn_child_creator",
        });
        const result = await resolveInstanceModelSources(
          h.db,
          "tnt_child",
          { definitionId: "wfd_1", modelPreferences: null },
          noopCipher,
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.materials.map((m) => m.secret)).toEqual([SECRET]);
      });

      test("a definition with no creator still launches a tenant-owned source", async () => {
        await seed({ credentialPrincipalId: null, creatorPrincipalId: null });
        const launch = await resolveModelSources(
          h.db,
          "tnt_child",
          REQ_OPUS,
          noopCipher,
        );
        expect(launch.ok).toBe(true);
        const rotation = await resolveInstanceModelSources(
          h.db,
          "tnt_child",
          { definitionId: "wfd_1", modelPreferences: null },
          noopCipher,
        );
        expect(rotation.ok).toBe(true);
      });
    });

    describe("a principal-owned credential is not usable through the catalog", () => {
      const denial = {
        ok: false,
        reason: "model_unavailable",
        model: "opus",
        skips: [{ reason: "credential_unauthorized", provider: "anthropic" }],
      };

      test("launch resolution withholds a personal credential", async () => {
        await seed({
          credentialPrincipalId: "prn_owner",
          creatorPrincipalId: "prn_child_creator",
        });
        const result = await resolveModelSources(
          h.db,
          "tnt_child",
          REQ_OPUS,
          noopCipher,
        );
        expect(JSON.stringify(result)).not.toContain(SECRET);
        expect(result).toMatchObject(denial);
      });

      test("rotation re-resolution withholds a personal credential", async () => {
        await seed({
          credentialPrincipalId: "prn_owner",
          creatorPrincipalId: "prn_child_creator",
        });
        const result = await resolveInstanceModelSources(
          h.db,
          "tnt_child",
          { definitionId: "wfd_1", modelPreferences: null },
          noopCipher,
        );
        expect(JSON.stringify(result)).not.toContain(SECRET);
        expect(result).toMatchObject(denial);
      });
    });
  },
);
