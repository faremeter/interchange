// The child-grant cap narrows a parent's grant rules to what a spawned child
// body declares, capping the child at the parent as the ceiling. The filter's
// safety rests on ONE direction being safe: it may over-keep an inert rule, but
// it must never drop a restriction or a rule the child legitimately needs.
// These cases pin that direction.

import { describe, test, expect } from "bun:test";

import {
  createDefaultDirectorRegistry,
  defineAgent,
  defineTool,
  type BaseEnv,
  type ToolDeclaration,
} from "@intx/agent";
import { evaluateGrants, toolConsumer } from "@intx/authz";
import type { GrantRule } from "@intx/authz";
import { defineWorkflow, onTrigger, step } from "@intx/workflow";

import {
  collectDeclaredCredentialConsumers,
  collectDeclaredResources,
  filterGrantsToDeclaredResources,
} from "./child-grant-filter";

// A grant rule in the shape the per-run grants file carries. `effect` and
// `action` vary per case; the rest is boilerplate the filter never inspects.
function rule(
  resource: string,
  effect: GrantRule["effect"],
  action = "invoke",
  conditions: GrantRule["conditions"] = null,
): GrantRule {
  return {
    id: `grant-${effect}-${resource}-${action}`,
    resource,
    action,
    effect,
    origin: "creator",
    conditions,
    expiresAt: null,
    roleId: null,
    principalId: null,
  };
}

const NO_CONSUMERS = new Set<string>();

function factory(id: string, names: readonly string[]) {
  return defineTool({
    id,
    definitions: names.map((name) => ({ name })),
    factory: (_env: BaseEnv) => ({
      definitions: [],
      run: () =>
        Promise.resolve({ callId: "", content: "", isError: false as const }),
    }),
  });
}

function resources(grants: readonly unknown[]): string[] {
  return grants.map((g) => {
    if (
      typeof g === "object" &&
      g !== null &&
      "resource" in g &&
      typeof g.resource === "string"
    ) {
      return g.resource;
    }
    throw new Error(`test grant has no string resource: ${JSON.stringify(g)}`);
  });
}

describe("filterGrantsToDeclaredResources", () => {
  test("keeps an allow whose exact resource is declared", () => {
    const declared = new Set(["tool:foo"]);
    const filtered = filterGrantsToDeclaredResources(
      [rule("tool:foo", "allow")],
      declared,
      NO_CONSUMERS,
    );
    expect(resources(filtered)).toEqual(["tool:foo"]);
  });

  test("keeps a wildcard allow that covers a declared resource", () => {
    // The single grant that authorizes the most must survive: string equality
    // would drop `tool:*`, but `matchPattern("tool:*", "tool:foo")` is true.
    const declared = new Set(["tool:foo"]);
    const filtered = filterGrantsToDeclaredResources(
      [rule("tool:*", "allow")],
      declared,
      NO_CONSUMERS,
    );
    expect(resources(filtered)).toEqual(["tool:*"]);
  });

  test("drops an allow for a resource the child never declares", () => {
    // The escalation this cap closes: a parent-only effect/credential/tool the
    // child body did not declare is removed, while a declared one survives.
    const declared = new Set(["tool:foo"]);
    const filtered = filterGrantsToDeclaredResources(
      [
        rule("effect:secretCap", "allow"),
        rule("credential:secretId", "allow"),
        rule("tool:parentOnly", "allow"),
        rule("tool:foo", "allow"),
      ],
      declared,
      NO_CONSUMERS,
    );
    expect(resources(filtered)).toEqual(["tool:foo"]);
  });

  test("keeps every deny and ask rule unconditionally", () => {
    // deny/ask only ever restrict, so they survive even when their resource is
    // undeclared -- dropping one would weaken safety.
    const declared = new Set(["tool:foo"]);
    const filtered = filterGrantsToDeclaredResources(
      [
        rule("tool:bar", "deny"),
        rule("effect:whatever", "ask"),
        rule("tool:bar", "allow"),
      ],
      declared,
      NO_CONSUMERS,
    );
    expect(resources(filtered)).toEqual(["tool:bar", "effect:whatever"]);
  });

  test("keeps an ask floor so the approval gate survives the cap", async () => {
    // A tool's static approval mark rides as an `ask` grant. evaluateGrants
    // ranks ask above allow at equal specificity so a workflow cannot declare
    // its way under the gate. The filter must keep the ask even alongside an
    // allow for the same declared resource, or the child punches through.
    const declared = new Set(["tool:foo"]);
    const filtered = filterGrantsToDeclaredResources(
      [rule("tool:foo", "ask"), rule("tool:foo", "allow")],
      declared,
      NO_CONSUMERS,
    );
    const decision = await evaluateGrants(
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the filter returns unknown[]; the seeded rows are GrantRule by construction here
      [...(filtered as readonly GrantRule[])],
      "tool:foo",
      "invoke",
    );
    expect(decision.effect).toBe("ask");
  });

  test("keeps entries that are not allow rules with a string resource", () => {
    // Keeping-on-doubt is safe: a non-rule entry is inert at evaluation
    // (evaluateGrants reads `.resource`), so it never widens authority, and the
    // filter can never wrongly drop a restriction it failed to classify.
    const declared = new Set<string>();
    const notRules: unknown[] = [
      { effect: "allow" }, // no resource
      { resource: "tool:foo" }, // no effect
      "not-an-object",
      null,
      42,
    ];
    const filtered = filterGrantsToDeclaredResources(
      notRules,
      declared,
      NO_CONSUMERS,
    );
    expect(filtered).toEqual(notRules);
  });

  test("drops a provable allow while keeping unclassifiable entries", () => {
    const declared = new Set(["tool:foo"]);
    const kept = { resource: "tool:foo" }; // no effect -> kept
    const dropped = rule("tool:parentOnly", "allow"); // undeclared allow -> gone
    const filtered = filterGrantsToDeclaredResources(
      [kept, dropped, rule("tool:foo", "allow")],
      declared,
      NO_CONSUMERS,
    );
    expect(filtered).toEqual([kept, rule("tool:foo", "allow")]);
  });
});

describe("collectDeclaredResources", () => {
  test("unions the declared resources across every step", () => {
    // Two steps, two distinct inference sources: the union carries both, so a
    // parent grant for either survives the filter.
    const workflow = defineWorkflow({
      id: "child-wf",
      trigger: { type: "manual" },
      steps: {
        a: step({
          agent: defineAgent({
            id: "a",
            systemPrompt: "s",
            tools: [],
            capabilities: [],
            inference: { sources: [{ provider: "anthropic", model: "m1" }] },
          }),
        }),
        b: step({
          agent: defineAgent({
            id: "b",
            systemPrompt: "s",
            tools: [],
            capabilities: [],
            inference: { sources: [{ provider: "anthropic", model: "m2" }] },
          }),
        }),
      },
    });
    const declared = collectDeclaredResources(
      workflow,
      createDefaultDirectorRegistry(),
      new Map(),
    );
    expect(declared.has("inference.source:anthropic:m1")).toBe(true);
    expect(declared.has("inference.source:anthropic:m2")).toBe(true);
  });
});

const FACTORY_ID = "@intx/tools-credential-probe/sidecar-bundle";
const CONSUMER = toolConsumer(FACTORY_ID);
const OTHER_CONSUMER = toolConsumer("@intx/tools-other/sidecar-bundle");

function useGrant(
  resource: string,
  conditions: GrantRule["conditions"],
): GrantRule {
  return rule(resource, "allow", "use", conditions);
}

function oneStep(tools: ReturnType<typeof factory>[]) {
  return defineWorkflow({
    id: "child-wf",
    trigger: { type: "manual" },
    steps: {
      work: step({
        agent: defineAgent({
          id: "ag",
          systemPrompt: "s",
          tools,
          capabilities: [],
          inference: { sources: [{ provider: "anthropic", model: "m" }] },
        }),
      }),
    },
  });
}

describe("credential allows", () => {
  const declared = new Set(["tool:probe"]);
  const consumers = new Set([CONSUMER]);

  test("keeps a credential allow whose tool condition names an instantiated factory", () => {
    const kept = useGrant("credential:cred-1", { tool: CONSUMER });
    const filtered = filterGrantsToDeclaredResources(
      [kept],
      declared,
      consumers,
    );
    expect(filtered).toEqual([kept]);
    expect(filtered[0]).toBe(kept);
  });

  test("keeps a credential:* allow with the same condition", () => {
    const kept = useGrant("credential:*", {
      tool: CONSUMER,
      scope: "read",
    });
    const filtered = filterGrantsToDeclaredResources(
      [kept],
      declared,
      consumers,
    );
    expect(filtered[0]).toBe(kept);
  });

  test("drops a credential allow for a factory the body does not instantiate", () => {
    const filtered = filterGrantsToDeclaredResources(
      [useGrant("credential:cred-1", { tool: OTHER_CONSUMER })],
      declared,
      consumers,
    );
    expect(filtered).toEqual([]);
  });

  test("drops an unconditioned credential allow", () => {
    const filtered = filterGrantsToDeclaredResources(
      [useGrant("credential:cred-1", null), useGrant("credential:*", null)],
      new Set(["credential:cred-1"]),
      consumers,
    );
    expect(filtered).toEqual([]);
  });

  test("drops a credential allow whose tool condition is not a string", () => {
    const filtered = filterGrantsToDeclaredResources(
      [
        useGrant("credential:cred-1", { tool: 1 }),
        useGrant("credential:*", { tool: null }),
      ],
      declared,
      consumers,
    );
    expect(filtered).toEqual([]);
  });

  test("drops a credential allow whose tool condition is not an exact consumer", () => {
    const filtered = filterGrantsToDeclaredResources(
      [useGrant("credential:cred-1", { tool: `${CONSUMER}*` })],
      declared,
      consumers,
    );
    expect(filtered).toEqual([]);
  });

  test("keeps a credential deny and ask when the body does not instantiate that factory", () => {
    const denied = rule("credential:cred-1", "deny", "use", {
      tool: OTHER_CONSUMER,
    });
    const asked = rule("credential:*", "ask", "use", null);
    const filtered = filterGrantsToDeclaredResources(
      [denied, asked],
      declared,
      NO_CONSUMERS,
    );
    expect(filtered).toEqual([denied, asked]);
  });

  test("keeps a resource of * on the pattern rule", () => {
    const kept = rule("*", "allow");
    const filtered = filterGrantsToDeclaredResources(
      [kept],
      declared,
      NO_CONSUMERS,
    );
    expect(filtered).toEqual([kept]);
  });

  test("does not treat credentials: or credential* as the credential family", () => {
    const filtered = filterGrantsToDeclaredResources(
      [
        useGrant("credentials:cred-1", { tool: CONSUMER }),
        useGrant("credential*", { tool: CONSUMER }),
      ],
      declared,
      consumers,
    );
    expect(filtered).toEqual([]);
  });
});

describe("collectDeclaredCredentialConsumers", () => {
  const directors = createDefaultDirectorRegistry();

  test("includes a factory that appears only inside an inline onTrigger body", () => {
    const body = oneStep([factory(FACTORY_ID, ["probe"])]);
    const workflow = defineWorkflow({
      id: "parent-wf",
      steps: {
        section: onTrigger({ on: { type: "mail", to: "s@x.example" }, body }),
      },
    });
    const pluginDefs = new Map<string, readonly ToolDeclaration[]>();
    const consumers = collectDeclaredCredentialConsumers(
      workflow,
      directors,
      pluginDefs,
    );
    const declared = collectDeclaredResources(workflow, directors, pluginDefs);
    expect(consumers.has(CONSUMER)).toBe(true);
    expect(declared.has(`tool:${FACTORY_ID}`)).toBe(false);
    expect(declared.has("tool:probe")).toBe(true);
  });

  test("includes a factory whose definitions array is empty", () => {
    const workflow = oneStep([factory(FACTORY_ID, [])]);
    const pluginDefs = new Map<string, readonly ToolDeclaration[]>();
    const consumers = collectDeclaredCredentialConsumers(
      workflow,
      directors,
      pluginDefs,
    );
    const declared = collectDeclaredResources(workflow, directors, pluginDefs);
    expect(consumers.has(CONSUMER)).toBe(true);
    expect(declared.has(`tool:${FACTORY_ID}`)).toBe(false);
  });

  test("does not record a plugin package name", () => {
    const pluginName = "@intx/tools-lsp";
    const workflow = defineWorkflow({
      id: "child-wf",
      trigger: { type: "manual" },
      steps: {
        work: step({
          agent: defineAgent({
            id: "ag",
            systemPrompt: "s",
            tools: [factory(FACTORY_ID, ["probe"])],
            plugins: [pluginName],
            capabilities: [],
            inference: { sources: [{ provider: "anthropic", model: "m" }] },
          }),
        }),
      },
    });
    const pluginDefs = new Map([[pluginName, [{ name: "lsp" }]]]);
    const consumers = collectDeclaredCredentialConsumers(
      workflow,
      directors,
      pluginDefs,
    );
    const declared = collectDeclaredResources(workflow, directors, pluginDefs);
    expect(consumers.has(CONSUMER)).toBe(true);
    expect(consumers.has(toolConsumer(pluginName))).toBe(false);
    expect(consumers.has(pluginName)).toBe(false);
    expect(declared.has("tool:lsp")).toBe(true);
    expect(declared.has(`tool:${FACTORY_ID}`)).toBe(false);
  });
});
