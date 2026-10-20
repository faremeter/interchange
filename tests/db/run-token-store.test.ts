import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import { createRunTokenStore, deleteRunTokensForPrincipals } from "@intx/db";
import { principal, runToken } from "@intx/db/schema";
import { isCiphertext, sha256 } from "@intx/crypto";
import { RUN_TOKEN_PREFIX } from "@intx/hub-common";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const TENANT = "tnt_rt";
const RUN_PRINCIPAL = "prn_run";
const OTHER_PRINCIPAL = "prn_other";
// A real env-key cipher, so the AEAD and its AAD binding actually run.
const cipher = createTestCredentialCipher();

describe.skipIf(!harnessDbEnvAvailable())(
  "createRunTokenStore (real DB)",
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
        id: RUN_PRINCIPAL,
        tenantId: TENANT,
        kind: "workflow",
        refId: "run_a",
      });
      await seedPrincipal(h.db, {
        id: OTHER_PRINCIPAL,
        tenantId: TENANT,
        kind: "workflow",
        refId: "run_b",
      });
    });

    function store() {
      return createRunTokenStore({ db: h.db, cipher });
    }

    test("mints one distinct token per consumer and delivers them", async () => {
      const s = store();
      await s.mintIfAbsent(
        {
          principalId: RUN_PRINCIPAL,
          consumers: ["tool:pkg", "action:handler"],
        },
        h.db,
      );

      const delivery = await s.loadDelivery(RUN_PRINCIPAL);
      expect(delivery.map((d) => d.consumer)).toEqual([
        "action:handler",
        "tool:pkg",
      ]);
      const [first, second] = delivery;
      if (first === undefined || second === undefined) {
        throw new Error("expected two tokens");
      }
      expect(first.token.startsWith(RUN_TOKEN_PREFIX)).toBe(true);
      expect(first.id.startsWith("rtk_")).toBe(true);
      expect(first.token).not.toBe(second.token);
    });

    test("a second mint keeps the token already issued", async () => {
      const s = store();
      const args = { principalId: RUN_PRINCIPAL, consumers: ["action:h"] };
      await s.mintIfAbsent(args, h.db);
      const [before] = await s.loadDelivery(RUN_PRINCIPAL);
      await s.mintIfAbsent(args, h.db);
      const after = await s.loadDelivery(RUN_PRINCIPAL);
      expect(after).toEqual(before === undefined ? [] : [before]);
    });

    test("findByHash resolves a token's row and misses an unknown one", async () => {
      const s = store();
      await s.mintIfAbsent(
        { principalId: RUN_PRINCIPAL, consumers: ["action:h"] },
        h.db,
      );
      const [delivered] = await s.loadDelivery(RUN_PRINCIPAL);
      if (delivered === undefined) throw new Error("expected a token");

      expect(await s.findByHash(await sha256(delivered.token))).toEqual({
        id: delivered.id,
        principalId: RUN_PRINCIPAL,
        consumer: "action:h",
      });
      expect(
        await s.findByHash(await sha256(`${RUN_TOKEN_PREFIX}unknown`)),
      ).toBeUndefined();
    });

    test("the token is sealed at rest and bound to its row", async () => {
      const s = store();
      await s.mintIfAbsent(
        { principalId: RUN_PRINCIPAL, consumers: ["action:h"] },
        h.db,
      );
      await s.mintIfAbsent(
        { principalId: OTHER_PRINCIPAL, consumers: ["action:h"] },
        h.db,
      );
      const [row] = await h.db
        .select()
        .from(runToken)
        .where(eq(runToken.principalId, RUN_PRINCIPAL));
      if (row === undefined) throw new Error("expected a row");
      expect(isCiphertext(row.sealedToken)).toBe(true);

      // A sealed token moved onto another row must not unseal there.
      await h.db
        .update(runToken)
        .set({ sealedToken: row.sealedToken })
        .where(eq(runToken.principalId, OTHER_PRINCIPAL));
      await expect(s.loadDelivery(OTHER_PRINCIPAL)).rejects.toThrow();
    });

    test("refuses a consumer that is not a tool or an action", async () => {
      await expect(
        store().mintIfAbsent(
          { principalId: RUN_PRINCIPAL, consumers: ["builtin"] },
          h.db,
        ),
      ).rejects.toThrow(/not a tool or action consumer/);
    });

    test("deleting a principal's tokens leaves other principals' tokens", async () => {
      const s = store();
      await s.mintIfAbsent(
        { principalId: RUN_PRINCIPAL, consumers: ["action:h"] },
        h.db,
      );
      await s.mintIfAbsent(
        { principalId: OTHER_PRINCIPAL, consumers: ["action:h"] },
        h.db,
      );
      await deleteRunTokensForPrincipals(h.db, [RUN_PRINCIPAL]);
      expect(await s.loadDelivery(RUN_PRINCIPAL)).toEqual([]);
      expect(await s.loadDelivery(OTHER_PRINCIPAL)).toHaveLength(1);
    });

    test("deleting the principal deletes its tokens", async () => {
      const s = store();
      await s.mintIfAbsent(
        { principalId: RUN_PRINCIPAL, consumers: ["action:h"] },
        h.db,
      );
      await h.db.delete(principal).where(eq(principal.id, RUN_PRINCIPAL));
      expect(await s.loadDelivery(RUN_PRINCIPAL)).toEqual([]);
    });
  },
);
