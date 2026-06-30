import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import { agentSession, gitToken, sessionMail, user } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAgent, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// The bytea columns serialize through a Uint8Array customType; postgres.js
// hex-encodes on the way out and parses back to a driver Buffer the
// customType copies into a plain Uint8Array. A flipped or dropped byte
// in a token hash or mail body is a silent integrity failure, so each
// boundary the encoding could mangle gets an explicit case: an empty
// payload, the high half of the byte range (0x80-0xFF, where a signed or
// latin1 round-trip would corrupt), and an embedded NUL (where a
// C-string round-trip would truncate).
const byteCases: { name: string; bytes: Uint8Array }[] = [
  { name: "an empty payload", bytes: new Uint8Array(0) },
  {
    name: "high bytes 0x80-0xFF",
    bytes: new Uint8Array([0x80, 0x9f, 0xa5, 0xc3, 0xfe, 0xff]),
  },
  {
    name: "an embedded NUL",
    bytes: new Uint8Array([0x01, 0x00, 0x02, 0x00, 0xff, 0x00]),
  },
];

describe.skipIf(!harnessDbEnvAvailable())("bytea round-trip (real DB)", () => {
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

  describe("git_token.tokenHashSha256", () => {
    for (const c of byteCases) {
      test(`round-trips ${c.name}`, async () => {
        await h.db.insert(user).values({
          id: "usr_1",
          name: "Test User",
          email: "user@example.test",
        });
        await h.db.insert(gitToken).values({
          id: "gt_1",
          userId: "usr_1",
          name: "laptop",
          kind: "pat",
          tokenHashSha256: c.bytes,
          resource: "agent-state:ins_1",
          refPattern: "*",
          actions: ["resolveRef"],
          expiresAt: new Date(Date.now() + 3_600_000),
        });

        const rows = await h.db
          .select({ hash: gitToken.tokenHashSha256 })
          .from(gitToken)
          .where(eq(gitToken.id, "gt_1"));
        expect(rows).toHaveLength(1);
        const got = rows[0]?.hash;
        expect(got).toBeInstanceOf(Uint8Array);
        expect(got).toEqual(c.bytes);
      });
    }
  });

  describe("session_mail.raw", () => {
    for (const c of byteCases) {
      test(`round-trips ${c.name}`, async () => {
        await seedTenants(h.db, [{ id: "tnt_1" }]);
        await seedPrincipal(h.db, { id: "prc_1", tenantId: "tnt_1" });
        await seedAgent(h.db, {
          id: "agt_1",
          tenantId: "tnt_1",
          creatorPrincipalId: "prc_1",
        });
        await h.db.insert(agentSession).values({
          id: "ses_1",
          tenantId: "tnt_1",
          agentId: "agt_1",
          principalId: "prc_1",
        });
        await h.db.insert(sessionMail).values({
          id: "mail_1",
          sessionId: "ses_1",
          tenantId: "tnt_1",
          direction: "inbound",
          status: "pending",
          raw: c.bytes,
        });

        const rows = await h.db
          .select({ raw: sessionMail.raw })
          .from(sessionMail)
          .where(eq(sessionMail.id, "mail_1"));
        expect(rows).toHaveLength(1);
        const got = rows[0]?.raw;
        expect(got).toBeInstanceOf(Uint8Array);
        expect(got).toEqual(c.bytes);
      });
    }
  });
});
