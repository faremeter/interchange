import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { sha256 } from "@intx/crypto";
import { sidecar } from "@intx/db/schema";
import { createSidecarTokenAuthenticator } from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";

// The mock-DB unit test proves the authenticator's control flow, but it
// never exercises the real `bytea` lookup: the token hash is written to
// and read back from Postgres through the customType encoder, and the
// query matches on that stored digest. A flipped byte on the write, a
// mangled encode/decode, or a `bytea` equality that Postgres does not
// evaluate the way drizzle builds it would all pass the mock and fail
// here. These cases drive the shipped `createSidecarTokenAuthenticator`
// against a real migrated schema to defend that write->read round-trip
// and the token-derived identity property.
describe.skipIf(!harnessDbEnvAvailable())(
  "sidecar token authenticator (real DB)",
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

    // Seed a sidecar identity the same way provisioning does: store the
    // SHA-256 digest of the token as the `bytea` hash. `url` is not read
    // by the authenticator; a placeholder satisfies its NOT NULL.
    async function seedSidecar(opts: {
      id: string;
      token: string;
    }): Promise<void> {
      await h.db.insert(sidecar).values({
        id: opts.id,
        url: "ws://dev-sidecar",
        tokenHashSha256: await sha256(opts.token),
      });
    }

    test("resolves a valid token to the seeded sidecar's identity", async () => {
      const token = "sidecar-secret";
      await seedSidecar({ id: "sc-1", token });
      const authenticate = createSidecarTokenAuthenticator({ db: h.db });

      const identity = await authenticate({ sidecarId: "sc-1", token });

      expect(identity).toEqual({ kind: "sidecar", sidecarId: "sc-1" });
    });

    test("rejects a wrong token with null", async () => {
      await seedSidecar({ id: "sc-1", token: "sidecar-secret" });
      const authenticate = createSidecarTokenAuthenticator({ db: h.db });

      const identity = await authenticate({
        sidecarId: "sc-1",
        token: "wrong-secret",
      });

      expect(identity).toBeNull();
    });

    test("rejects an empty token with null", async () => {
      await seedSidecar({ id: "sc-1", token: "sidecar-secret" });
      const authenticate = createSidecarTokenAuthenticator({ db: h.db });

      const identity = await authenticate({ sidecarId: "sc-1", token: "" });

      // `null` here comes from the lookup missing, not an input guard: the
      // authenticator hashes `""` and finds no row carrying `sha256("")`.
      // There is no empty-token validation to short-circuit the query.
      expect(identity).toBeNull();
    });

    test("derives identity from the token, not a spoofed claimed sidecarId", async () => {
      const token = "sidecar-secret";
      await seedSidecar({ id: "sc-real", token });
      const authenticate = createSidecarTokenAuthenticator({ db: h.db });

      const identity = await authenticate({ sidecarId: "sc-spoofed", token });

      expect(identity).toEqual({ kind: "sidecar", sidecarId: "sc-real" });
    });

    test("selects the matching row by hash among several sidecars", async () => {
      // With more than one sidecar present, each token must resolve to its
      // own row. A single-row mock cannot distinguish "matched by hash" from
      // "returned the only row"; a populated table proves the `bytea`
      // equality actually keys the lookup on the presented token's digest.
      const tokenA = "sidecar-secret-a";
      const tokenB = "sidecar-secret-b";
      await seedSidecar({ id: "sc-a", token: tokenA });
      await seedSidecar({ id: "sc-b", token: tokenB });
      const authenticate = createSidecarTokenAuthenticator({ db: h.db });

      expect(await authenticate({ sidecarId: "sc-a", token: tokenA })).toEqual({
        kind: "sidecar",
        sidecarId: "sc-a",
      });
      expect(await authenticate({ sidecarId: "sc-b", token: tokenB })).toEqual({
        kind: "sidecar",
        sidecarId: "sc-b",
      });
    });
  },
);
