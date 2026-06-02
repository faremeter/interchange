import { describe, test, expect } from "bun:test";

import { rewriteSchemaQualifiedReferences } from "./migrate";

describe('migration "public" substitution', () => {
  test("rewrites schema-qualified references", () => {
    const sql =
      'ALTER TABLE "x" ADD CONSTRAINT "fk" FOREIGN KEY ("u") REFERENCES "public"."user"("id");';
    expect(rewriteSchemaQualifiedReferences(sql, '"test_schema"')).toBe(
      'ALTER TABLE "x" ADD CONSTRAINT "fk" FOREIGN KEY ("u") REFERENCES "test_schema"."user"("id");',
    );
  });

  test("leaves bare quoted public in string literals alone", () => {
    const sql = `INSERT INTO "x" VALUES ('"public"');`;
    expect(rewriteSchemaQualifiedReferences(sql, '"t"')).toBe(sql);
  });

  test("substitutes multiple occurrences on a single line", () => {
    const sql = 'REFERENCES "public"."a"("id"), "public"."b"("id")';
    expect(rewriteSchemaQualifiedReferences(sql, '"s"')).toBe(
      'REFERENCES "s"."a"("id"), "s"."b"("id")',
    );
  });

  test("no-op when target schema is public", () => {
    const sql = 'REFERENCES "public"."user"("id")';
    expect(rewriteSchemaQualifiedReferences(sql, '"public"')).toBe(sql);
  });

  test("does not match a trailing apostrophe (bare reference)", () => {
    const sql = `SELECT '"public"' AS literal;`;
    expect(rewriteSchemaQualifiedReferences(sql, '"s"')).toBe(sql);
  });
});
