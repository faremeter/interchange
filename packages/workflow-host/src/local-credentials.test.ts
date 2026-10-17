// The local credentials snapshot: a deployed source-ref run's flat grant set,
// derived from the live definition and given to every step id the definition
// can execute.

import { describe, test, expect } from "bun:test";

import {
  createDefaultDirectorRegistry,
  defineAgent,
  defineTool,
  type BaseEnv,
  type ToolDeclaration,
} from "@intx/agent";
import type { GrantRule } from "@intx/authz";
import {
  action,
  childWorkflow,
  defineWorkflow,
  loop,
  map,
  onTrigger,
  step,
  type WorkflowDefinition,
} from "@intx/workflow";

import {
  createLocalCredentialsSnapshot,
  snapshotStepGrants,
} from "./local-credentials";

function tool(id: string, definitions: readonly ToolDeclaration[]) {
  return defineTool({
    id,
    definitions,
    factory: (_env: BaseEnv) => ({
      definitions: [],
      run: () =>
        Promise.resolve({ callId: "", content: "", isError: false as const }),
    }),
  });
}

function agentStep(definitions: readonly ToolDeclaration[]) {
  return step({
    agent: defineAgent({
      id: "ag",
      systemPrompt: "s",
      tools: [tool("@acme/tools/fetch", definitions)],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "m" }] },
    }),
  });
}

function body(id: string, steps: WorkflowDefinition["steps"]) {
  return defineWorkflow({ id, trigger: { type: "manual" }, steps });
}

const definition = defineWorkflow({
  id: "local-snapshot",
  trigger: { type: "manual" },
  steps: {
    read: agentStep([{ name: "read" }]),
    push: action({ handler: "push", effect: { requires: ["git.push"] } }),
    fan: map({
      over: { literal: [1] },
      step: agentStep([{ name: "send", approval: "ask" }]),
    }),
    rework: loop({
      body: body("loop-body", { inLoop: agentStep([{ name: "read" }]) }),
      while: "cont",
      carry: "next",
      maxIterations: 1,
      onExhausted: "done",
    }),
    done: action({ handler: "done", after: ["rework"] }),
    spawn: childWorkflow({
      definition: body("child-body", {
        inChild: agentStep([{ name: "read" }]),
      }),
    }),
    section: onTrigger({
      on: { type: "manual" },
      body: body("section-body", { inSection: agentStep([{ name: "read" }]) }),
    }),
  },
});

const rule = (
  resource: string,
  effect: GrantRule["effect"],
  index: number,
): GrantRule => ({
  id: `local-grant:${String(index)}`,
  resource,
  action: "invoke",
  effect,
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
});

describe("createLocalCredentialsSnapshot", () => {
  test("gives every executable step id the run's tool and effect grants", () => {
    const snapshot = createLocalCredentialsSnapshot({
      definition,
      directors: createDefaultDirectorRegistry(),
    });
    expect(snapshot.steps.map((entry) => entry.stepId)).toEqual([
      "read",
      "push",
      "fan",
      "rework",
      "inLoop",
      "done",
      "spawn",
      "inChild",
      "section",
      "inSection",
    ]);
    const [first] = snapshot.steps;
    if (first === undefined) throw new Error("the snapshot has no steps");
    expect(first.grants).toEqual([
      rule("tool:read", "allow", 0),
      rule("effect:git.push", "allow", 1),
      rule("tool:send", "ask", 2),
    ]);
    for (const entry of snapshot.steps) {
      expect(entry.grants).toBe(first.grants);
      expect(entry.address).toBe(entry.stepId);
    }
  });

  test("adds a use grant for each delivered binding and the caller's grants", () => {
    const extra: GrantRule = {
      ...rule("secret:vault", "allow", 0),
      id: "requirement",
      action: "use",
    };
    const snapshot = createLocalCredentialsSnapshot({
      definition: body("plain", { push: action({ handler: "push" }) }),
      directors: createDefaultDirectorRegistry(),
      delivery: {
        bindings: [
          { handle: "gh", credentialId: "c1", consumer: "action:push" },
          { handle: "gh2", credentialId: "c1", consumer: "action:push" },
        ],
        materials: [],
      },
      grants: [extra],
    });
    expect(snapshot.steps[0]?.grants).toEqual([
      {
        id: "local-grant:0",
        resource: "credential:c1",
        action: "use",
        effect: "allow",
        origin: "system",
        conditions: { tool: "action:push" },
        expiresAt: null,
        roleId: null,
        principalId: null,
      },
      extra,
    ]);
  });

  test("walks a plugin's tools from the plugin definitions it is given", () => {
    const withPlugin = body("with-plugin", {
      work: step({
        agent: defineAgent({
          id: "ag",
          systemPrompt: "s",
          tools: [],
          plugins: ["@acme/plugin"],
          capabilities: [],
          inference: { sources: [{ provider: "anthropic", model: "m" }] },
        }),
      }),
    });
    const directors = createDefaultDirectorRegistry();
    expect(() =>
      createLocalCredentialsSnapshot({ definition: withPlugin, directors }),
    ).toThrow("@acme/plugin");
    const snapshot = createLocalCredentialsSnapshot({
      definition: withPlugin,
      directors,
      pluginDefinitions: new Map([["@acme/plugin", [{ name: "lint" }]]]),
    });
    expect(snapshot.steps[0]?.grants).toEqual([rule("tool:lint", "allow", 0)]);
  });

  test("refuses a definition whose director the registry cannot resolve", () => {
    const unresolvable = body("unresolvable", {
      work: step({
        agent: defineAgent({
          id: "ag",
          systemPrompt: "s",
          tools: [],
          capabilities: [],
          director: { id: "@acme/missing-director", config: {} },
          inference: { sources: [{ provider: "anthropic", model: "m" }] },
        }),
      }),
    });
    expect(() =>
      createLocalCredentialsSnapshot({
        definition: unresolvable,
        directors: createDefaultDirectorRegistry(),
      }),
    ).toThrow("unresolvable director: @acme/missing-director");
  });
});

describe("snapshotStepGrants", () => {
  const snapshot = createLocalCredentialsSnapshot({
    definition,
    directors: createDefaultDirectorRegistry(),
  });

  test("reads a step's grants, resolving a map iteration to its base step", () => {
    const ref = { current: snapshot };
    const [first] = snapshot.steps;
    if (first === undefined) throw new Error("the snapshot has no steps");
    const grants: readonly unknown[] = snapshotStepGrants(ref, "fan[3]");
    expect(grants).toBe(first.grants);
  });

  test("refuses an entry whose grants are not grant rules", () => {
    const malformed = {
      current: {
        steps: [
          {
            stepId: "read",
            address: "read",
            grants: [{ resource: "tool:read" }],
            contentHash: "",
          },
        ],
      },
    };
    expect(() => snapshotStepGrants(malformed, "read")).toThrow(
      "local credentials: the grants of step read are not grant rules",
    );
  });

  test("refuses when no snapshot is active or the step has no entry", () => {
    expect(() => snapshotStepGrants({ current: null }, "read")).toThrow(
      "no credentials snapshot is active for step read",
    );
    expect(() => snapshotStepGrants({ current: snapshot }, "missing")).toThrow(
      "the credentials snapshot has no entry for step missing",
    );
  });
});
