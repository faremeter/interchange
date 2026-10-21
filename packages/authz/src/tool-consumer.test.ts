import { describe, test, expect } from "bun:test";

import { evaluateGrants } from "./evaluate";
import {
  CREDENTIAL_USE_CONDITIONS,
  actionConsumer,
  credentialBindingConsumer,
  toolConsumer,
  toolConsumerEvaluator,
} from "./tool-consumer";
import type { ConditionContext, GrantRule } from "./types";

const CRED = "credential:crd_abc123";
const USE = "use";
const REACTOR = "builtin:reactor";

function grant(
  overrides: Partial<GrantRule> &
    Pick<GrantRule, "resource" | "action" | "effect">,
): GrantRule {
  return {
    id: `grt_${Math.random().toString(36).slice(2, 10)}`,
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
    ...overrides,
  };
}

function ctx(consumer: string): ConditionContext {
  return {
    now: new Date("2026-06-15T14:30:00Z"),
    resource: CRED,
    action: USE,
    principalId: "prn_1",
    tenantId: "tnt_1",
    consumer,
  };
}

describe("toolConsumerEvaluator", () => {
  test("matches a consumer identity that equals the value", () => {
    expect(toolConsumerEvaluator(REACTOR, ctx(REACTOR))).toBe(true);
  });

  test("withholds from a mismatched consumer", () => {
    expect(toolConsumerEvaluator(REACTOR, ctx("tool:other"))).toBe(false);
  });

  test("withholds from an empty consumer before comparing", () => {
    // Even when the grant value is also empty, an empty consumer never matches.
    expect(toolConsumerEvaluator("", ctx(""))).toBe(false);
    expect(toolConsumerEvaluator(REACTOR, ctx(""))).toBe(false);
  });

  test("throws on a non-string condition value", () => {
    expect(() => toolConsumerEvaluator(123, ctx(REACTOR))).toThrow();
  });
});

describe("consumer identities", () => {
  const base = { handle: "gh", provider: "github", locator: "tenant" } as const;

  test("a tool binding names its package's tool consumer", () => {
    expect(
      credentialBindingConsumer({ ...base, package: "@intx/tools-github" }),
    ).toBe("tool:@intx/tools-github");
  });

  test("an action binding names its handler's action consumer", () => {
    expect(credentialBindingConsumer({ ...base, action: "pushRelease" })).toBe(
      "action:pushRelease",
    );
  });

  test("a tool package and an action handler with one name stay disjoint", () => {
    expect(actionConsumer("deploy")).not.toBe(toolConsumer("deploy"));
  });

  test("an action consumer's grant does not authorize the same-named tool", () => {
    expect(
      toolConsumerEvaluator(
        actionConsumer("deploy"),
        ctx(toolConsumer("deploy")),
      ),
    ).toBe(false);
  });
});

describe("CREDENTIAL_USE_CONDITIONS through evaluateGrants", () => {
  const opts = (consumer?: string) => ({
    conditions: CREDENTIAL_USE_CONDITIONS,
    ...(consumer === undefined ? {} : { consumer }),
  });

  test("a {tool} grant authorizes the matching consumer", async () => {
    const result = await evaluateGrants(
      [
        grant({
          resource: CRED,
          action: USE,
          effect: "allow",
          conditions: { tool: REACTOR },
        }),
      ],
      CRED,
      USE,
      opts(REACTOR),
    );
    expect(result.effect).toBe("allow");
  });

  test("a {tool} grant is withheld from a mismatched consumer", async () => {
    const result = await evaluateGrants(
      [
        grant({
          resource: CRED,
          action: USE,
          effect: "allow",
          conditions: { tool: REACTOR },
        }),
      ],
      CRED,
      USE,
      opts("tool:other"),
    );
    expect(result.effect).toBe(null);
  });

  test("a {tool} grant is withheld when no consumer is supplied", async () => {
    const result = await evaluateGrants(
      [
        grant({
          resource: CRED,
          action: USE,
          effect: "allow",
          conditions: { tool: REACTOR },
        }),
      ],
      CRED,
      USE,
      opts(),
    );
    expect(result.effect).toBe(null);
  });

  test("an unconditioned grant authorizes regardless of consumer", async () => {
    const result = await evaluateGrants(
      [
        grant({
          resource: CRED,
          action: USE,
          effect: "allow",
          conditions: null,
        }),
      ],
      CRED,
      USE,
      opts("tool:anything"),
    );
    expect(result.effect).toBe("allow");
  });

  test("an unknown condition key throws under this registry", async () => {
    expect(
      evaluateGrants(
        [
          grant({
            resource: CRED,
            action: USE,
            effect: "allow",
            conditions: { max_spend_per_day: 100 },
          }),
        ],
        CRED,
        USE,
        opts(REACTOR),
      ),
    ).rejects.toThrow();
  });
});
