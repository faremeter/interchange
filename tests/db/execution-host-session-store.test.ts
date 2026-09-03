import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import { sha256 } from "@intx/crypto";
import { createExecutionHostSessionStore } from "@intx/db";
import { executionHost, principal } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const TENANT_ID = "tnt-host-session";
const OWNER_PRINCIPAL_ID = "prn-host-session-owner";
const HOST_PRINCIPAL_ID = "prn-host-session";
const HOST_ID = "hst-session";
const TOKEN = "intx_host_session-token";

describe.skipIf(!harnessDbEnvAvailable())(
  "execution host sessions (real DB)",
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
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      await seedPrincipal(h.db, {
        id: OWNER_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "user",
      });
      await seedPrincipal(h.db, {
        id: HOST_PRINCIPAL_ID,
        tenantId: TENANT_ID,
        kind: "host",
        refId: HOST_ID,
      });
      await h.db.insert(executionHost).values({
        id: HOST_ID,
        tenantId: TENANT_ID,
        principalId: HOST_PRINCIPAL_ID,
        ownerPrincipalId: OWNER_PRINCIPAL_ID,
        displayName: "Test host",
        tokenHashSha256: await sha256(TOKEN),
      });
    });

    test("replaces capabilities and advances the session generation", async () => {
      const store = createExecutionHostSessionStore(h.db);
      const first = await store.begin({
        tokenHashSha256: await sha256(TOKEN),
        sessionId: "session-1",
        hubInstanceId: "hub-1",
        capabilities: [{ capability: "runtime:browser", state: "available" }],
        leaseExpiresAt: new Date("2026-09-03T12:01:00Z"),
        now: new Date("2026-09-03T12:00:00Z"),
      });
      const second = await store.begin({
        tokenHashSha256: await sha256(TOKEN),
        sessionId: "session-2",
        hubInstanceId: "hub-1",
        capabilities: [
          { capability: "runtime:ios-jsc-v1", state: "available" },
        ],
        leaseExpiresAt: new Date("2026-09-03T12:02:00Z"),
        now: new Date("2026-09-03T12:01:00Z"),
      });

      expect(first?.generation).toBe(1);
      expect(second).toMatchObject({
        hostId: HOST_ID,
        principalId: HOST_PRINCIPAL_ID,
        ownerPrincipalId: OWNER_PRINCIPAL_ID,
        sessionId: "session-2",
        generation: 2,
        capabilities: [
          { capability: "runtime:ios-jsc-v1", state: "available" },
        ],
      });
      expect(await store.findCurrent(HOST_ID)).toEqual(second);
    });

    test("does not let a stale session refresh or expire its replacement", async () => {
      const store = createExecutionHostSessionStore(h.db);
      await store.begin({
        tokenHashSha256: await sha256(TOKEN),
        sessionId: "session-1",
        hubInstanceId: "hub-1",
        capabilities: [],
        leaseExpiresAt: new Date("2026-09-03T12:01:00Z"),
        now: new Date("2026-09-03T12:00:00Z"),
      });
      await store.begin({
        tokenHashSha256: await sha256(TOKEN),
        sessionId: "session-2",
        hubInstanceId: "hub-1",
        capabilities: [],
        leaseExpiresAt: new Date("2026-09-03T12:02:00Z"),
        now: new Date("2026-09-03T12:01:00Z"),
      });

      expect(
        await store.refresh({
          hostId: HOST_ID,
          sessionId: "session-1",
          generation: 1,
          hubInstanceId: "hub-1",
          leaseExpiresAt: new Date("2026-09-03T12:03:00Z"),
          now: new Date("2026-09-03T12:01:30Z"),
        }),
      ).toBe(false);
      expect(
        await store.expire({
          hostId: HOST_ID,
          sessionId: "session-1",
          generation: 1,
          hubInstanceId: "hub-1",
          now: new Date("2026-09-03T12:01:30Z"),
        }),
      ).toBe(false);
      expect((await store.findCurrent(HOST_ID))?.leaseExpiresAt).toEqual(
        new Date("2026-09-03T12:02:00Z"),
      );
    });

    test("rejects an unknown token and an inactive host principal", async () => {
      const store = createExecutionHostSessionStore(h.db);
      const args = {
        sessionId: "session-1",
        hubInstanceId: "hub-1",
        capabilities: [],
        leaseExpiresAt: new Date("2026-09-03T12:01:00Z"),
        now: new Date("2026-09-03T12:00:00Z"),
      };

      expect(
        await store.begin({
          ...args,
          tokenHashSha256: await sha256("unknown"),
        }),
      ).toBeNull();
      await h.db
        .update(principal)
        .set({ status: "suspended" })
        .where(eq(principal.id, HOST_PRINCIPAL_ID));
      expect(
        await store.begin({
          ...args,
          tokenHashSha256: await sha256(TOKEN),
        }),
      ).toBeNull();
    });
  },
);
