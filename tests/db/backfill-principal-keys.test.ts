import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { and, eq } from "drizzle-orm";

import { backfillPrincipalKeys, createPrincipalKeyStore } from "@intx/db";
import { principalKey } from "@intx/db/schema";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const TENANT = "tnt_bf";
const cipher = createTestCredentialCipher();

// Every non-agent principal is keyless at seed time except `prn_keyed`, which
// is pre-keyed below. Covers both kinds and every status, since the backfill
// keys them all.
const KEYLESS_NON_AGENT = [
  { id: "prn_user_active", kind: "user" as const, status: "active" as const },
  { id: "prn_user_invited", kind: "user" as const, status: "invited" as const },
  { id: "prn_wf_active", kind: "workflow" as const, status: "active" as const },
  {
    id: "prn_wf_suspended",
    kind: "workflow" as const,
    status: "suspended" as const,
  },
  {
    id: "prn_wf_deactivated",
    kind: "workflow" as const,
    status: "deactivated" as const,
  },
];

describe.skipIf(!harnessDbEnvAvailable())(
  "backfillPrincipalKeys (real DB)",
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
      for (const p of KEYLESS_NON_AGENT) {
        await seedPrincipal(h.db, {
          id: p.id,
          tenantId: TENANT,
          kind: p.kind,
          status: p.status,
        });
      }
      // An `agent`-kind principal must be skipped entirely.
      await seedPrincipal(h.db, {
        id: "prn_agent",
        tenantId: TENANT,
        kind: "agent",
        status: "active",
      });
      // A principal that already holds an active key must be left untouched.
      await seedPrincipal(h.db, {
        id: "prn_keyed",
        tenantId: TENANT,
        kind: "user",
        status: "active",
      });
    });

    function newStore() {
      return createPrincipalKeyStore({ db: h.db, cipher });
    }

    async function activeKeyRow(principalId: string) {
      const [row] = await h.db
        .select()
        .from(principalKey)
        .where(
          and(
            eq(principalKey.principalId, principalId),
            eq(principalKey.status, "active"),
          ),
        );
      return row;
    }

    async function keyCount(principalId: string): Promise<number> {
      const rows = await h.db
        .select()
        .from(principalKey)
        .where(eq(principalKey.principalId, principalId));
      return rows.length;
    }

    test("keys every keyless non-agent principal, skips agent and already-keyed", async () => {
      const store = newStore();
      await store.generate("prn_keyed");
      const before = await activeKeyRow("prn_keyed");
      if (before === undefined) throw new Error("expected a pre-keyed row");

      const report = await backfillPrincipalKeys(h.db, store);
      expect(report).toEqual({ keysGenerated: 5, alreadyKeyed: 1 });

      // Every keyless non-agent principal now holds exactly one active key.
      for (const p of KEYLESS_NON_AGENT) {
        expect(await keyCount(p.id)).toBe(1);
        expect((await activeKeyRow(p.id))?.status).toBe("active");
      }

      // The agent-kind principal was skipped -- no key minted.
      expect(await keyCount("prn_agent")).toBe(0);

      // The already-keyed principal is byte-for-byte unchanged.
      const after = await activeKeyRow("prn_keyed");
      expect(after?.id).toBe(before.id);
      expect(after?.publicKey).toBe(before.publicKey);
      expect(after?.privateKey).toBe(before.privateKey);
      expect(await keyCount("prn_keyed")).toBe(1);
    });

    test("a re-run is a no-op with positive confirmation", async () => {
      const store = newStore();
      await backfillPrincipalKeys(h.db, store);

      const rerun = await backfillPrincipalKeys(h.db, store);
      expect(rerun).toEqual({ keysGenerated: 0, alreadyKeyed: 6 });
    });
  },
);
