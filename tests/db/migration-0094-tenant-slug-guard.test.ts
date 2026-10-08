// The guard ahead of `tenant_slug_dns_label_check` in migration 0094. Its only
// job is the operator's experience of a failed deploy: the bare constraint
// violation names no row, so the guard counts the offending rows, samples them
// and raises with their tenant ids and the remediation. That failure happens
// once, on a database nobody can re-run against, so the message is driven here.
//
// The sibling `tenant-slug-constraint.test.ts` covers the constraint once it is
// in place. This file covers the path where it cannot be added: the claim the
// guard rests that path on is that its predicate is the constraint predicate
// under NOT and nothing else, and the cases below read both predicates out of
// the migration rather than copying them.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq, sql } from "drizzle-orm";

import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";

const MIGRATION_PATH = join(
  import.meta.dir,
  "../../packages/db/migrations/0094_dark_old_lace.sql",
);
const MIGRATION_SQL = readFileSync(MIGRATION_PATH, "utf8");

const CONSTRAINT = "tenant_slug_dns_label_check";
const GUARD_MARKER = "hold a slug that is not a single DNS label";

// Migration 0094 states the slug grammar twice: once as the guard's `WHERE`,
// once as the constraint's `CHECK`. Both are read out of the file rather than
// copied, so an edit to either one moves what the cases below assert; a copy
// would agree with itself forever and prove nothing. The guard marker carries
// `NOT` so it is unique in the file (`WHERE` alone also appears in the sample's
// `FILTER` clause), and the expression is read from just after the keyword, so
// the `NOT` is part of what is compared.
const GUARD_WHERE_KEYWORD = "WHERE ";
const GUARD_PREDICATE_MARKER = `${GUARD_WHERE_KEYWORD}NOT `;
const CONSTRAINT_CHECK_KEYWORD = "CHECK ";
const CONSTRAINT_PREDICATE_MARKER = `${CONSTRAINT_CHECK_KEYWORD}(`;

function soleIndexOf(text: string, marker: string): number {
  const first = text.indexOf(marker);
  if (first < 0) {
    throw new Error(
      `migration 0094 no longer holds ${JSON.stringify(marker)}, so its predicates cannot be read out of it`,
    );
  }
  if (text.indexOf(marker, first + 1) >= 0) {
    throw new Error(
      `migration 0094 holds ${JSON.stringify(marker)} more than once, so this extraction cannot tell which one states the grammar`,
    );
  }
  return first;
}

// One SQL expression, verbatim, from `start`. A single-quoted literal is stepped
// over rather than scanned, because the grammar's regex carries parentheses of
// its own and a doubled quote inside a literal is an escaped quote. An
// expression that opens with `(` ends at the parenthesis closing that group;
// any other ends where a parenthesis closes a group it did not open, or at a
// semicolon.
function readExpression(text: string, start: number): string {
  let depth = 0;
  let inLiteral = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (inLiteral) {
      if (ch === "'") {
        if (text[i + 1] === "'") {
          i++;
          continue;
        }
        inLiteral = false;
      }
      continue;
    }
    if (ch === "'") {
      inLiteral = true;
      continue;
    }
    if (ch === "(") {
      depth++;
      continue;
    }
    if (ch === ")") {
      if (depth === 0) return text.slice(start, i).trim();
      depth--;
      if (depth === 0 && text[start] === "(") return text.slice(start, i + 1);
      continue;
    }
    if (ch === ";" && depth === 0) return text.slice(start, i).trim();
  }
  throw new Error(
    `migration 0094 holds no end for the expression at offset ${String(start)}`,
  );
}

const GUARD_WHERE = readExpression(
  MIGRATION_SQL,
  soleIndexOf(MIGRATION_SQL, GUARD_PREDICATE_MARKER) +
    GUARD_WHERE_KEYWORD.length,
);
const CONSTRAINT_CHECK = readExpression(
  MIGRATION_SQL,
  soleIndexOf(MIGRATION_SQL, CONSTRAINT_PREDICATE_MARKER) +
    CONSTRAINT_CHECK_KEYWORD.length,
);

// The slugs both predicates are read against. Deliberately not the sibling
// file's `ILLEGAL_SLUGS`/`LEGAL_SLUGS`, which state the grammar: these exist to
// separate the predicate from a mutation of itself, so each one isolates one
// part of it -- an anchor, the interior character class, the optional tail, the
// length bound, the conjunction. `refused` is what the constraint answers, and
// therefore also what the guard counts.
//
// The null case is the one the column's NOT NULL forbids; the migration's
// comment claims the two predicates agree there as well -- a check admits a row
// whose predicate is null, and a WHERE does not select one -- and nothing else
// reads that claim.
const PREDICATE_PROBES: {
  label: string;
  slug: string | null;
  refused: boolean;
}[] = [
  { label: "lowercase letters", slug: "acme", refused: false },
  { label: "mixed case", slug: "AcmeMixed", refused: false },
  { label: "single letter", slug: "a", refused: false },
  { label: "single digit", slug: "9", refused: false },
  { label: "interior hyphen", slug: "a-b", refused: false },
  { label: "exactly 63 characters", slug: "a".repeat(63), refused: false },
  { label: "null slug", slug: null, refused: false },
  { label: "empty", slug: "", refused: true },
  { label: "only a hyphen", slug: "-", refused: true },
  { label: "leading hyphen", slug: "-a", refused: true },
  { label: "trailing hyphen", slug: "a-", refused: true },
  { label: "underscore", slug: "a_b", refused: true },
  { label: "trailing newline", slug: "a\n", refused: true },
  { label: "trailing carriage return", slug: "a\r", refused: true },
  { label: "non-ASCII letter", slug: "acmé", refused: true },
  { label: "over 63 characters", slug: "a".repeat(64), refused: true },
];

// The row source is aliased `tenant` because the extracted predicates name
// `"tenant"."slug"`: they are read verbatim, so the alias is what resolves them
// against these probes rather than against the table.
const PROBE_ROWS = sql.join(
  PREDICATE_PROBES.map(
    ({ label, slug }) => sql`(${label}::text, ${slug}::text)`,
  ),
  sql`, `,
);

// A mutation of the constraint predicate, standing for one drift an edit could
// introduce. The probes have to separate every one of them from the predicate
// itself; a probe set that does not separate a mutation is one that would let
// that drift pass the agreement case below.
//
// `octet_length` for `length` is absent on purpose, because no probe can
// separate it: the two count differently only for a multi-byte character, and
// the pattern refuses every string holding one, so the conjunction is false
// either way.
const PREDICATE_MUTATIONS: {
  label: string;
  mutate: (predicate: string) => string;
}[] = [
  {
    label: "drops the length bound",
    mutate: (p) => p.replace(/ and length\("tenant"\."slug"\) <= 63/, ""),
  },
  {
    label: "raises the length bound",
    mutate: (p) => p.replace("<= 63", "<= 64"),
  },
  { label: "drops the start anchor", mutate: (p) => p.replace("'^", "'") },
  { label: "drops the end anchor", mutate: (p) => p.replace("$'", "'") },
  {
    label: "disjunction instead of conjunction",
    mutate: (p) => p.replace(" and ", " or "),
  },
  {
    label: "no hyphen in the interior class",
    mutate: (p) => p.replace("[A-Za-z0-9-]", "[A-Za-z0-9]"),
  },
  {
    label: "lower case only",
    mutate: (p) => p.replaceAll("A-Za-z0-9", "a-z0-9"),
  },
  { label: "mandatory tail", mutate: (p) => p.replace(")?$", ")$") },
];

// The harness migrates a fresh schema by applying every file in order, so 0094
// always runs against an empty `tenant` and its guard branch never executes.
// `runMigrations` takes no "stop before file N" argument, so the pre-0094
// state of this column is reconstructed instead: drop the constraint 0094
// added, plant the rows, then replay 0094 itself. `IF EXISTS` keeps the drop
// idempotent, but postgres emits a "does not exist, skipping" NOTICE whenever
// it skips and the harness client forwards notices to stdout; `SET LOCAL
// client_min_messages = warning` stops the server sending them and scopes that
// to this transaction. The drop has to commit before the replay runs, so it
// gets a transaction of its own rather than joining it.
async function dropConstraint(h: TestDb): Promise<void> {
  await h.db.transaction(async (tx) => {
    await tx.execute(sql.raw(`SET LOCAL client_min_messages = warning`));
    await tx.execute(
      sql.raw(`ALTER TABLE "tenant" DROP CONSTRAINT IF EXISTS "${CONSTRAINT}"`),
    );
  });
}

// Replay 0094 the way `drizzle-kit migrate` applies a pending set: every
// statement in one transaction, so an abort inside the guard rolls the ALTER
// back instead of leaving the file half applied.
async function applyMigration0094(h: TestDb): Promise<void> {
  // `runMigrations` rewrites `"public"."x"` into the target schema before
  // executing. 0094 carries no such reference -- every identifier is
  // unqualified and resolves through the connection's pinned `search_path` --
  // so this replay does no rewriting. Fail loudly rather than quietly apply a
  // statement to `public` if that ever stops being true.
  if (MIGRATION_SQL.includes('"public".')) {
    throw new Error(
      "migration 0094 gained a schema-qualified reference; this replay does not rewrite it",
    );
  }
  await h.db.transaction(async (tx) => {
    for (const statement of MIGRATION_SQL.split("--> statement-breakpoint")) {
      if (statement.trim() === "") continue;
      await tx.execute(sql.raw(statement));
    }
  });
}

// The messages on a rejection's `cause` chain, outermost first. postgres-js
// raises the error and drizzle re-wraps that as the `cause` of its own, so what
// the server said is an inner message; the depth bound keeps a self-referential
// cause from looping.
function errorChain(err: unknown): string[] {
  const chain: string[] = [];
  let cur: unknown = err;
  for (let depth = 0; cur != null && depth < 8; depth++) {
    if (cur instanceof Error) {
      chain.push(cur.message);
      cur = cur.cause;
    } else {
      chain.push(String(cur));
      cur = undefined;
    }
  }
  return chain;
}

// The guard's message, picked out of the chain. The deepest match is the one
// wanted, not the first: Drizzle's outer message is `Failed query: <the whole
// statement>` and the statement embeds the RAISE format string, so the outer
// message contains the marker as well and spans lines, which would make the
// single-line assertion below pass on the wrong string. Only the innermost
// message is what a log pipeline receives.
function guardMessage(err: unknown): string {
  const chain = errorChain(err);
  const found = chain.findLast((m) => m.includes(GUARD_MARKER));
  if (found === undefined) {
    throw new Error(
      `the error chain carries no guard message: ${chain.join(" | ")}`,
    );
  }
  return found;
}

// Names the constraint that refused a write, so a row the unique domain index or
// a NOT NULL refused reads differently from one the slug grammar refused.
function describeRejection(err: unknown): string {
  const chain = errorChain(err);
  return chain.some((message) => message.includes(CONSTRAINT))
    ? `rejected by ${CONSTRAINT}`
    : `rejected by something else: ${chain.join(" | ")}`;
}

async function expectGuardRejection(h: TestDb): Promise<string> {
  try {
    await applyMigration0094(h);
  } catch (err) {
    return guardMessage(err);
  }
  throw new Error(
    "migration 0094 applied, but its guard was expected to stop it",
  );
}

async function constraintExists(h: TestDb): Promise<boolean> {
  const rows = await h.db.execute(sql`
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = ${h.schema}
      AND t.relname = 'tenant'
      AND c.conname = ${CONSTRAINT}
  `);
  return rows.length > 0;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "migration 0094 tenant slug guard (real DB)",
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

    // Every case starts from the pre-0094 state for this column, so a case that
    // leaves the constraint in place (the conforming one) does not decide what
    // the next case sees.
    async function plant(id: string, slug: string): Promise<void> {
      await h.db.insert(tenantTable).values({
        id,
        name: id,
        slug,
        domain: `${id}.example`,
        parentId: null,
      });
    }

    test("names the offending slug and its tenant, and adds no constraint", async () => {
      await dropConstraint(h);
      await plant("tnt-guard-bad", "not a label");

      const message = await expectGuardRejection(h);

      expect(message).toContain(`1 tenant row(s) ${GUARD_MARKER}`);
      expect(message).toContain('"not a label" (tenant "tnt-guard-bad")');
      // The abort rolled the whole file back, so the table is exactly as the
      // operator left it: no constraint, and the row still there to be fixed.
      expect(await constraintExists(h)).toBe(false);
      const rows = await h.db
        .select({ slug: tenantTable.slug })
        .from(tenantTable)
        .where(eq(tenantTable.id, "tnt-guard-bad"));
      expect(rows).toEqual([{ slug: "not a label" }]);
    });

    test("applies and adds the constraint when every slug conforms", async () => {
      await dropConstraint(h);
      await plant("tnt-guard-ok", "conforming-label");

      await applyMigration0094(h);

      expect(await constraintExists(h)).toBe(true);
    });

    // The guard's claim, read end to end: the value it refuses is the value the
    // constraint refuses. The two cases above cover the halves separately --
    // the guard stops the migration, and a clean table lets it through -- and
    // this one joins them, so a guard that refused a value the constraint then
    // accepted would fail here rather than pass both halves.
    test("leaves behind a constraint that refuses the slug the guard refused", async () => {
      await dropConstraint(h);
      await plant("tnt-guard-ok", "conforming-label");

      await applyMigration0094(h);

      let outcome = "accepted";
      try {
        await plant("tnt-guard-late", "not a label");
      } catch (err) {
        outcome = describeRejection(err);
      }
      expect(outcome).toBe(`rejected by ${CONSTRAINT}`);
    });

    test("escapes a control character instead of breaking the message", async () => {
      await dropConstraint(h);
      await plant("tnt-guard-ctl", "bad\nlabel");

      const message = await expectGuardRejection(h);

      // Rendered as a JSON string, so the newline is two characters and the
      // message stays one log line.
      expect(message).toContain('"bad\\nlabel" (tenant "tnt-guard-ctl")');
      expect(message).not.toContain("\n");
    });

    test("caps the sample at 25 rows while reporting the true total", async () => {
      await dropConstraint(h);
      for (let n = 0; n < 26; n++) {
        const label = String(n).padStart(2, "0");
        await plant(`tnt-guard-${label}`, `bad_${label}`);
      }

      const message = await expectGuardRejection(h);

      expect(message).toContain(`26 tenant row(s) ${GUARD_MARKER}`);
      // Ordered by slug, so the sample is the 25 lowest and re-running after
      // fixing them walks the set rather than resampling it.
      expect(message).toContain('"bad_00" (tenant "tnt-guard-00")');
      expect(message).toContain('"bad_24" (tenant "tnt-guard-24")');
      expect(message).not.toContain("bad_25");
      expect(message.split("(tenant ").length - 1).toBe(25);
    });

    // Reads one boolean SQL expression against every probe and returns the
    // labels where it answered the asked-for value. `IS TRUE`/`IS FALSE` rather
    // than the value itself, because a null predicate has to land in neither
    // set: the guard's WHERE does not select such a row, and the constraint
    // admits it.
    async function probeLabelsWhere(
      expr: string,
      answer: "IS TRUE" | "IS FALSE",
    ): Promise<string[]> {
      const rows = await h.db.execute(sql`
        SELECT tenant.label AS label
        FROM (VALUES ${PROBE_ROWS}) AS tenant(label, slug)
        WHERE (${sql.raw(expr)}) ${sql.raw(answer)}
      `);
      const labels: string[] = [];
      for (const row of rows) {
        const label = row["label"];
        if (typeof label !== "string") {
          throw new Error(
            `probeLabelsWhere: unexpected row: ${JSON.stringify(row)}`,
          );
        }
        labels.push(label);
      }
      // Sorted here rather than by the server, so the comparison does not depend
      // on the collation these labels are ordered under.
      return labels.sort();
    }

    // The two predicates answering the same slugs in the same database, one read
    // as the guard reads it and one as the constraint reads it. The expected
    // set is stated rather than derived from either predicate, so an edit that
    // moved both together fails here as well.
    test("counts through the guard exactly what the constraint refuses", async () => {
      const expected = PREDICATE_PROBES.filter(({ refused }) => refused)
        .map(({ label }) => label)
        .sort();

      const counted = await probeLabelsWhere(GUARD_WHERE, "IS TRUE");
      const refused = await probeLabelsWhere(CONSTRAINT_CHECK, "IS FALSE");

      expect(counted).toEqual(expected);
      expect(refused).toEqual(expected);
    });

    // What makes the case above a bar rather than a coincidence: every mutation
    // of the predicate changes the answer on at least one probe. A mutation the
    // probes miss is a divergence the case above would report as agreement.
    test("separates the predicate from every mutation of itself", async () => {
      const refused = await probeLabelsWhere(CONSTRAINT_CHECK, "IS FALSE");
      const missed: string[] = [];
      for (const { label, mutate } of PREDICATE_MUTATIONS) {
        const mutant = mutate(CONSTRAINT_CHECK);
        if (mutant === CONSTRAINT_CHECK) {
          throw new Error(
            `the "${label}" mutation no longer changes the predicate, so it tests nothing: ${CONSTRAINT_CHECK}`,
          );
        }
        const mutantRefused = await probeLabelsWhere(mutant, "IS FALSE");
        if (JSON.stringify(mutantRefused) === JSON.stringify(refused)) {
          missed.push(label);
        }
      }
      expect(missed).toEqual([]);
    });
  },
);

// Outside the database gate on purpose: this reads the migration file and no
// `TestDb`, so gating it on Postgres would only stop the drift it names from
// being caught on a machine without one -- and that is where it matters most,
// since a drift found there is found before CI runs the database half.
//
// A textual comparison, which the behavioural cases inside the gate are not: it
// catches a divergence on any input rather than on a probe, and in exchange it
// fails on a rewrite that changed the predicate's text without changing what it
// answers. An applied migration is immutable by convention, so a rewrite of
// either predicate is a defect either way and a failure here is the right
// answer to it.
describe("migration 0094 predicate equality", () => {
  test("states the guard's WHERE as the constraint's CHECK under NOT", () => {
    const negated = GUARD_WHERE.replace(/^NOT\s+/, "");
    expect(negated).not.toBe(GUARD_WHERE);
    expect(negated).toBe(CONSTRAINT_CHECK);
  });
});
