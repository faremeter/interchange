// `tenant_slug_dns_label_check` is the database's own copy of the tenant slug
// grammar. The create route already validates the slug with `TenantSlug`, so
// these cases drive the drizzle client directly and bypass that route: the
// point is that a seed, a migration, or another service cannot write a slug the
// route would have refused.
//
// The create route derives `tenant.domain` from the lowercased slug, and that
// domain is both the sender stamp on outbound mail and the recipient run
// address, so the grammar bounds what that route can derive a domain from. The
// derivation is the route's own convention, not a relation between the columns,
// which is why these cases set `domain` independently of the slug under test.
// The grammar is RFC 1035 section 2.3.1 as relaxed by RFC 1123 section 2.1.

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { type } from "arktype";
import { eq } from "drizzle-orm";

import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { TenantSlug } from "@intx/types";

const CONSTRAINT = "tenant_slug_dns_label_check";

const ILLEGAL_SLUGS: { label: string; slug: string }[] = [
  { label: "empty", slug: "" },
  { label: "leading hyphen", slug: "-acme" },
  { label: "trailing hyphen", slug: "acme-" },
  { label: "only a hyphen", slug: "-" },
  { label: "underscore", slug: "acme_co" },
  { label: "dot", slug: "acme.co" },
  { label: "space", slug: "acme co" },
  { label: "at sign", slug: "acme@evil" },
  { label: "comma", slug: "acme,evil" },
  { label: "semicolon", slug: "acme;evil" },
  { label: "greater than", slug: "acme>evil" },
  { label: "embedded newline", slug: "acme\nevil" },
  { label: "trailing newline", slug: "acme\n" },
  { label: "trailing tab", slug: "acme\t" },
  // The trailing LF above catches a `$` that matches before a line terminator
  // rather than only at the end of the string. These carry that reading to the
  // other characters an engine may treat as a line terminator or as trailing
  // whitespace worth ignoring.
  { label: "trailing carriage return", slug: "acme\r" },
  { label: "trailing CRLF", slug: "acme\r\n" },
  { label: "trailing vertical tab", slug: "acme\v" },
  { label: "trailing form feed", slug: "acme\f" },
  { label: "trailing line separator", slug: "acme\u2028" },
  { label: "trailing paragraph separator", slug: "acme\u2029" },
  { label: "non-ASCII letter", slug: "acmé" },
  { label: "over 63 characters", slug: "a".repeat(64) },
  // 63 UTF-16 code units and 32 code points. The two layers bound a different
  // measure -- arktype's `string<=63` counts code units, the constraint's
  // `length()` counts characters -- and this string is inside both bounds, so
  // what refuses it in both layers is the pattern. The case records that the
  // two bounds are not the same measure. It does not detect a disagreement
  // between them, and no case can while the pattern admits ASCII only, because
  // the two measures coincide on every string the pattern accepts.
  {
    label: "63 code units under 63 code points",
    slug: `${"\u{1F600}".repeat(31)}a`,
  },
];

const LEGAL_SLUGS: { label: string; slug: string }[] = [
  { label: "lowercase letters", slug: "acme" },
  { label: "mixed case", slug: "AcmeMixed" },
  { label: "leading digit", slug: "7acme" },
  { label: "all digits", slug: "123" },
  { label: "interior hyphen", slug: "acme-two" },
  { label: "single character", slug: "a" },
  { label: "exactly 63 characters", slug: "b".repeat(63) },
];

// Names the constraint that refused the write, so a row rejected by the unique
// domain index or a NOT NULL reads differently from one the slug grammar
// refused. Drizzle re-wraps the postgres-js error as the `cause` of its own,
// and only the inner one names the constraint, so the whole chain is read.
// The depth bound keeps a self-referential cause from looping.
function describeRejection(err: unknown): string {
  const seen: string[] = [];
  let cur: unknown = err;
  for (let depth = 0; cur != null && depth < 8; depth++) {
    seen.push(cur instanceof Error ? cur.message : String(cur));
    cur = cur instanceof Error ? cur.cause : undefined;
  }
  const chain = seen.join(" | ");
  return chain.includes(CONSTRAINT)
    ? `rejected by ${CONSTRAINT}`
    : `rejected by something else: ${chain}`;
}

describe.skipIf(!harnessDbEnvAvailable())("tenant slug DNS label", () => {
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

  async function insertTenant(n: number, slug: string): Promise<void> {
    await h.db.insert(tenantTable).values({
      id: `tnt-slug-${String(n)}`,
      name: `tenant ${String(n)}`,
      slug,
      domain: `slug-${String(n)}.example`,
      parentId: null,
    });
  }

  // Collects every case before asserting, so one failing run names every slug
  // the database got wrong rather than only the first.
  async function insertEach(cases: { label: string; slug: string }[]) {
    const seen: { label: string; outcome: string }[] = [];
    for (const [n, { label, slug }] of cases.entries()) {
      try {
        await insertTenant(n, slug);
        seen.push({ label, outcome: "accepted" });
      } catch (err) {
        seen.push({ label, outcome: describeRejection(err) });
      }
    }
    return seen;
  }

  test("rejects a slug that is not a legal DNS label", async () => {
    const seen = await insertEach(ILLEGAL_SLUGS);
    expect(seen).toEqual(
      ILLEGAL_SLUGS.map(({ label }) => ({
        label,
        outcome: `rejected by ${CONSTRAINT}`,
      })),
    );
  });

  test("accepts a slug that is a legal DNS label", async () => {
    const seen = await insertEach(LEGAL_SLUGS);
    expect(seen).toEqual(
      LEGAL_SLUGS.map(({ label }) => ({ label, outcome: "accepted" })),
    );
  });

  test("rejects an update that moves a legal slug off the grammar", async () => {
    await insertTenant(0, "acme");
    let outcome = "accepted";
    try {
      await h.db
        .update(tenantTable)
        .set({ slug: "acme_co" })
        .where(eq(tenantTable.id, "tnt-slug-0"));
    } catch (err) {
      outcome = describeRejection(err);
    }
    expect(outcome).toBe(`rejected by ${CONSTRAINT}`);
  });
});

// Outside the database gate on purpose. This reads the type-layer validator
// against the same case tables the two database cases above drive, so it is
// what fixes those tables as the shared statement of the grammar rather than
// as one layer's private list. It touches no `TestDb`, so gating it on Postgres
// would only stop the layers' agreement from being checked at all on a machine
// without one -- the reading where it matters most, since a drift found there
// is found before CI runs the database half.
describe("tenant slug DNS label type layer", () => {
  test("agrees with the TenantSlug validator on every case", () => {
    // The constraint is a second layer behind the API boundary, not a
    // different rule. A case that drifts between the two layers would let the
    // route and the database disagree about the same slug.
    const seen = [...ILLEGAL_SLUGS, ...LEGAL_SLUGS].map(({ label, slug }) => ({
      label,
      accepted: !(TenantSlug(slug) instanceof type.errors),
    }));
    expect(seen).toEqual([
      ...ILLEGAL_SLUGS.map(({ label }) => ({ label, accepted: false })),
      ...LEGAL_SLUGS.map(({ label }) => ({ label, accepted: true })),
    ]);
  });
});
