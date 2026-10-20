import { describe, expect, test } from "bun:test";

import { isRunTokenRoute, matchesRunTokenRoute } from "./run-token-auth";

const RUN = "/api/tenants/tnt_a/workflows/runs";

describe("matchesRunTokenRoute", () => {
  test("a parameter matches one non-empty segment", () => {
    const pattern = "/api/tenants/:tenantId/workflows/runs/:runId";
    expect(matchesRunTokenRoute(pattern, `${RUN}/run_1`)).toBe(true);
    expect(matchesRunTokenRoute(pattern, `${RUN}/`)).toBe(false);
    expect(matchesRunTokenRoute(pattern, `${RUN}/run_1/events`)).toBe(false);
  });

  test("an encoded slash stays inside its segment", () => {
    const pattern = "/api/tenants/:tenantId/workflows/runs/:runId/events";
    expect(matchesRunTokenRoute(pattern, `${RUN}/a%2Fb/events`)).toBe(true);
  });

  test("a trailing slash is a different path", () => {
    expect(
      matchesRunTokenRoute("/api/tenants/:tenantId/workflows/runs", `${RUN}/`),
    ).toBe(false);
  });
});

describe("isRunTokenRoute", () => {
  test("allows a listed method and path", () => {
    expect(isRunTokenRoute("GET", RUN)).toBe(true);
    expect(isRunTokenRoute("DELETE", `${RUN}/run_1`)).toBe(true);
  });

  test("refuses a listed path with another method", () => {
    expect(isRunTokenRoute("POST", `${RUN}/run_1/mail`)).toBe(false);
    expect(isRunTokenRoute("HEAD", RUN)).toBe(false);
  });

  test("refuses a path that is not listed", () => {
    expect(isRunTokenRoute("GET", "/api/tenants/tnt_a/grants")).toBe(false);
    expect(isRunTokenRoute("POST", "/api/tenants/tnt_a/federation")).toBe(
      false,
    );
  });

  test("a run id that reads `runs` matches the run routes", () => {
    // `/workflows/runs/runs` matches the run-detail pattern; both patterns it
    // could match are read routes the router sends to the run router.
    expect(isRunTokenRoute("GET", `${RUN}/runs`)).toBe(true);
  });
});
