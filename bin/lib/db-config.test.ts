import { describe, expect, test } from "bun:test";

import { resolveBackfillDbConfig } from "./db-config";

const baseEnv = {
  DB_HOST: "db.example",
  DB_PORT: "5432",
  DB_USER: "postgres",
  DB_PASSWORD: "secret",
  DB_NAME: "interchange",
};

describe("resolveBackfillDbConfig", () => {
  test("resolves a fully-specified environment", () => {
    expect(resolveBackfillDbConfig(baseEnv)).toEqual({
      host: "db.example",
      port: 5432,
      user: "postgres",
      password: "secret",
      database: "interchange",
    });
  });

  test("threads PG_SCHEMA when set", () => {
    expect(
      resolveBackfillDbConfig({ ...baseEnv, PG_SCHEMA: "tenant_x" }).schema,
    ).toBe("tenant_x");
  });

  test("omits the schema when PG_SCHEMA is unset or empty", () => {
    expect("schema" in resolveBackfillDbConfig(baseEnv)).toBe(false);
    expect(
      "schema" in resolveBackfillDbConfig({ ...baseEnv, PG_SCHEMA: "" }),
    ).toBe(false);
  });

  test("throws naming a missing required variable", () => {
    expect(() =>
      resolveBackfillDbConfig({ ...baseEnv, DB_HOST: undefined }),
    ).toThrow("DB_HOST is required");
    expect(() => resolveBackfillDbConfig({ ...baseEnv, DB_NAME: "" })).toThrow(
      "DB_NAME is required",
    );
  });

  test("rejects a non-positive-integer DB_PORT", () => {
    expect(() => resolveBackfillDbConfig({ ...baseEnv, DB_PORT: "0" })).toThrow(
      "DB_PORT must be a positive integer",
    );
    expect(() =>
      resolveBackfillDbConfig({ ...baseEnv, DB_PORT: "not-a-number" }),
    ).toThrow("DB_PORT must be a positive integer");
  });
});
