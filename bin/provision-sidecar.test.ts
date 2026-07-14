import { describe, expect, test } from "bun:test";

import { resolveProvisionConfig } from "./provision-sidecar";

const baseEnv = {
  SIDECAR_ID: "dev-sidecar-1",
  SIDECAR_TOKEN: "dev-token",
  DB_HOST: "localhost",
  DB_PORT: "5432",
  DB_USER: "migrator",
  DB_PASSWORD: "secret",
  DB_NAME: "interchange",
};

describe("resolveProvisionConfig", () => {
  test("resolves a fully-specified environment", () => {
    const config = resolveProvisionConfig(baseEnv);
    expect(config).toEqual({
      sidecarId: "dev-sidecar-1",
      sidecarToken: "dev-token",
      db: {
        host: "localhost",
        port: 5432,
        user: "migrator",
        password: "secret",
        database: "interchange",
      },
    });
  });

  test("threads PG_SCHEMA when set", () => {
    const config = resolveProvisionConfig({
      ...baseEnv,
      PG_SCHEMA: "test_123",
    });
    expect(config.db.schema).toBe("test_123");
  });

  test("omits the schema when PG_SCHEMA is unset or empty", () => {
    expect("schema" in resolveProvisionConfig(baseEnv).db).toBe(false);
    expect(
      "schema" in resolveProvisionConfig({ ...baseEnv, PG_SCHEMA: "" }).db,
    ).toBe(false);
  });

  test("rejects a non-integer DB_PORT", () => {
    expect(() =>
      resolveProvisionConfig({ ...baseEnv, DB_PORT: "not-a-number" }),
    ).toThrow(/DB_PORT must be a positive integer/);
  });

  test("rejects a non-positive DB_PORT", () => {
    expect(() => resolveProvisionConfig({ ...baseEnv, DB_PORT: "0" })).toThrow(
      /DB_PORT must be a positive integer/,
    );
  });

  test.each([
    "SIDECAR_ID",
    "SIDECAR_TOKEN",
    "DB_HOST",
    "DB_USER",
    "DB_PASSWORD",
    "DB_NAME",
  ])("rejects a missing %s", (key) => {
    const env = Object.fromEntries(
      Object.entries(baseEnv).filter(([k]) => k !== key),
    );
    expect(() => resolveProvisionConfig(env)).toThrow(
      new RegExp(`${key} is required`),
    );
  });

  test("treats an empty required value as missing", () => {
    expect(() =>
      resolveProvisionConfig({ ...baseEnv, SIDECAR_TOKEN: "" }),
    ).toThrow(/SIDECAR_TOKEN is required/);
  });
});
