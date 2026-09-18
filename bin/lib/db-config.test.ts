import { describe, expect, test } from "bun:test";

import { resolveDbConfig } from "./db-config";

const baseEnv = {
  DB_HOST: "db.example",
  DB_PORT: "5432",
  DB_USER: "postgres",
  DB_PASSWORD: "secret",
  DB_NAME: "interchange",
};

describe("resolveDbConfig", () => {
  test("resolves a fully-specified environment", () => {
    expect(resolveDbConfig(baseEnv)).toEqual({
      host: "db.example",
      port: 5432,
      user: "postgres",
      password: "secret",
      database: "interchange",
    });
  });

  test("threads PG_SCHEMA when set", () => {
    expect(resolveDbConfig({ ...baseEnv, PG_SCHEMA: "tenant_x" }).schema).toBe(
      "tenant_x",
    );
  });

  test("omits the schema when PG_SCHEMA is unset or empty", () => {
    expect("schema" in resolveDbConfig(baseEnv)).toBe(false);
    expect("schema" in resolveDbConfig({ ...baseEnv, PG_SCHEMA: "" })).toBe(
      false,
    );
  });

  test("throws naming a missing required variable", () => {
    expect(() => resolveDbConfig({ ...baseEnv, DB_HOST: undefined })).toThrow(
      "DB_HOST is required",
    );
    expect(() => resolveDbConfig({ ...baseEnv, DB_NAME: "" })).toThrow(
      "DB_NAME is required",
    );
  });

  test("rejects a non-positive-integer DB_PORT", () => {
    expect(() => resolveDbConfig({ ...baseEnv, DB_PORT: "0" })).toThrow(
      "DB_PORT must be a positive integer",
    );
    expect(() =>
      resolveDbConfig({ ...baseEnv, DB_PORT: "not-a-number" }),
    ).toThrow("DB_PORT must be a positive integer");
  });

  test("threads DB_STATEMENT_TIMEOUT_MS when set", () => {
    expect(
      resolveDbConfig({ ...baseEnv, DB_STATEMENT_TIMEOUT_MS: "120000" })
        .statementTimeoutMs,
    ).toBe(120000);
  });

  test("omits the statement timeout when DB_STATEMENT_TIMEOUT_MS is unset or empty", () => {
    expect("statementTimeoutMs" in resolveDbConfig(baseEnv)).toBe(false);
    expect(
      "statementTimeoutMs" in
        resolveDbConfig({ ...baseEnv, DB_STATEMENT_TIMEOUT_MS: "" }),
    ).toBe(false);
  });

  test("rejects a non-positive-integer DB_STATEMENT_TIMEOUT_MS", () => {
    expect(() =>
      resolveDbConfig({ ...baseEnv, DB_STATEMENT_TIMEOUT_MS: "0" }),
    ).toThrow("DB_STATEMENT_TIMEOUT_MS must be a positive integer");
    expect(() =>
      resolveDbConfig({ ...baseEnv, DB_STATEMENT_TIMEOUT_MS: "soon" }),
    ).toThrow("DB_STATEMENT_TIMEOUT_MS must be a positive integer");
  });
});
