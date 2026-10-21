import { describe, test, expect } from "bun:test";

import { defineAgent, defineTool, type BaseEnv } from "@intx/agent";
import type { CredentialBinding } from "@intx/types";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import {
  action,
  childWorkflow,
  defineWorkflow,
  loop,
  map,
  onTrigger,
  projectLiveToInert,
  step,
  type WorkflowDefinition,
} from "@intx/workflow";

import {
  checkCredentialBindings,
  collectHubConsumers,
  describeCredentialBindingViolation,
} from "./credential-binding-check";

const TOOL_ID = "@acme/tools/github";

function tool(id: string, credentials?: readonly string[]) {
  return defineTool({
    id,
    definitions: [{ name: "fetch" }],
    ...(credentials !== undefined
      ? { credentials: credentials.map((handle) => ({ handle })) }
      : {}),
    factory: (_env: BaseEnv) => ({
      definitions: [],
      run: () =>
        Promise.resolve({ callId: "", content: "", isError: false as const }),
    }),
  });
}

function agentStep(
  toolIds: readonly string[],
  credentials?: readonly string[],
) {
  return step({
    agent: defineAgent({
      id: "ag",
      systemPrompt: "s",
      tools: toolIds.map((id) => tool(id, credentials)),
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "m" }] },
    }),
  });
}

function toolBinding(pkg: string, handle = "gh"): CredentialBinding {
  return { package: pkg, handle, provider: "github", locator: "tenant" };
}

function actionBinding(handler: string, handle = "gh"): CredentialBinding {
  return { action: handler, handle, provider: "github", locator: "tenant" };
}

function body(id: string, steps: WorkflowDefinition["steps"]) {
  return defineWorkflow({ id, trigger: { type: "manual" }, steps });
}

function check(
  steps: WorkflowDefinition["steps"],
  credentialBindings: readonly CredentialBinding[],
) {
  return checkCredentialBindings(
    WorkflowProjectionDefinition.assert(
      projectLiveToInert(
        defineWorkflow({
          id: "wf",
          trigger: { type: "mail", to: "wf@acme.test" },
          steps,
          ...(credentialBindings.length > 0 ? { credentialBindings } : {}),
        }),
      ),
    ),
  );
}

describe("checkCredentialBindings holds a declaring tool to its declaration", () => {
  test("a declared tool handle with a binding", () => {
    expect(
      check({ work: agentStep([TOOL_ID], ["gh"]) }, [toolBinding(TOOL_ID)]),
    ).toEqual([]);
  });

  test("a binding for a tool that declares nothing", () => {
    // A factory without a declaration predates them, so its binding is
    // accepted without a declared set to hold it to.
    expect(
      check({ work: agentStep([TOOL_ID]) }, [toolBinding(TOOL_ID, "any")]),
    ).toEqual([]);
  });

  test("refuses a declared tool handle with no binding", () => {
    expect(check({ work: agentStep([TOOL_ID], ["gh"]) }, [])).toEqual([
      { kind: "unbound_declared_tool_handle", factory: TOOL_ID, handle: "gh" },
    ]);
  });

  test("refuses a binding for a handle the declaring tool omits", () => {
    expect(
      check({ work: agentStep([TOOL_ID], ["gh"]) }, [
        toolBinding(TOOL_ID),
        toolBinding(TOOL_ID, "gh-typo"),
      ]),
    ).toEqual([
      {
        kind: "undeclared_tool_binding_handle",
        package: TOOL_ID,
        handle: "gh-typo",
      },
    ]);
  });

  test("describes both tool violations", () => {
    expect(
      describeCredentialBindingViolation({
        kind: "unbound_declared_tool_handle",
        factory: TOOL_ID,
        handle: "gh",
      }),
    ).toContain(`tool factory ${TOOL_ID} declares credential handle "gh"`);
    expect(
      describeCredentialBindingViolation({
        kind: "undeclared_tool_binding_handle",
        package: TOOL_ID,
        handle: "gh",
      }),
    ).toContain("does not declare that handle");
  });
});

describe("checkCredentialBindings accepts deliverable bindings", () => {
  test("a tool binding for a factory the workflow runs", () => {
    expect(
      check({ work: agentStep([TOOL_ID]) }, [toolBinding(TOOL_ID)]),
    ).toEqual([]);
  });

  test("a tool binding consumed only by a map item and only inside an onTrigger body", () => {
    expect(
      check(
        {
          fan: map({
            over: { literal: [1] },
            step: agentStep(["@acme/tools/mapped"]),
          }),
          section: onTrigger({
            on: { type: "mail", to: "s@acme.test" },
            body: body("section-body", { work: agentStep([TOOL_ID]) }),
          }),
        },
        [toolBinding(TOOL_ID), toolBinding("@acme/tools/mapped")],
      ),
    ).toEqual([]);
  });

  test("an action binding for a declared handle at top level, in a loop body, and in a childWorkflow body", () => {
    const declaring = (handler: string) =>
      action({ handler, credentials: ["gh"], effect: { requires: ["call"] } });
    expect(
      check(
        {
          push: declaring("push"),
          rework: loop({
            body: body("loop-body", { pushLoop: declaring("pushLoop") }),
            while: "cont",
            carry: "next",
            maxIterations: 2,
            onExhausted: "done",
          }),
          spawn: childWorkflow({
            definition: body("child-body", {
              pushChild: declaring("pushChild"),
            }),
          }),
          done: action({ handler: "done", after: ["rework"] }),
        },
        [
          actionBinding("push"),
          actionBinding("pushLoop"),
          actionBinding("pushChild"),
        ],
      ),
    ).toEqual([]);
  });

  test("an action in an onTrigger body and a map item tool in a loop body inside it", () => {
    const loopBody = body("loop-body", {
      fan: map({
        over: { literal: [1] },
        step: agentStep(["@acme/tools/mapped"]),
      }),
    });
    const section = body("section-body", {
      push: action({
        handler: "push",
        credentials: ["gh"],
        effect: { requires: ["call"] },
      }),
      rework: loop({
        body: loopBody,
        while: "cont",
        carry: "next",
        maxIterations: 1,
        onExhausted: "done",
      }),
      done: action({ handler: "done", after: ["rework"] }),
    });
    expect(
      check(
        {
          section: onTrigger({
            on: { type: "mail", to: "s@acme.test" },
            body: section,
          }),
        },
        [actionBinding("push"), toolBinding("@acme/tools/mapped")],
      ),
    ).toEqual([]);
  });

  test("a handle declared by one of two steps that share a handler", () => {
    expect(
      check(
        {
          first: action({
            handler: "push",
            credentials: ["gh"],
            effect: { requires: ["call"] },
          }),
          second: action({
            handler: "push",
            credentials: ["gh", "npm"],
            effect: { requires: ["call"] },
            after: ["first"],
          }),
        },
        [actionBinding("push", "gh"), actionBinding("push", "npm")],
      ),
    ).toEqual([]);
  });
});

describe("checkCredentialBindings refuses undeliverable bindings", () => {
  test("a declared action handle with no binding", () => {
    expect(
      check(
        {
          push: action({
            handler: "push",
            credentials: ["gh"],
            effect: { requires: ["call"] },
          }),
        },
        [],
      ),
    ).toEqual([
      { kind: "unbound_declared_handle", handler: "push", handle: "gh" },
    ]);
  });

  test("a binding on a nested body, even an empty one", () => {
    const nested = defineWorkflow({
      id: "child-body",
      trigger: { type: "manual" },
      steps: { work: agentStep([TOOL_ID]) },
      credentialBindings: [],
    });
    expect(check({ spawn: childWorkflow({ definition: nested }) }, [])).toEqual(
      [
        {
          kind: "nested_body_bindings",
          bodyId: "child-body",
          reachedThrough: ["spawn"],
        },
      ],
    );
  });

  test("a binding on a body two levels deep names the full path", () => {
    const deep = defineWorkflow({
      id: "deep-body",
      trigger: { type: "manual" },
      steps: { work: agentStep([TOOL_ID]) },
      credentialBindings: [],
    });
    const mid = body("mid-body", {
      rework: loop({
        body: deep,
        while: "cont",
        carry: "next",
        maxIterations: 1,
        onExhausted: "done",
      }),
      done: action({ handler: "done", after: ["rework"] }),
    });
    expect(
      check(
        {
          section: onTrigger({
            on: { type: "mail", to: "s@acme.test" },
            body: mid,
          }),
        },
        [],
      ),
    ).toEqual([
      {
        kind: "nested_body_bindings",
        bodyId: "deep-body",
        reachedThrough: ["section", "rework"],
      },
    ]);
  });

  test("a tool binding for a factory the workflow does not run", () => {
    expect(
      check({ work: agentStep([TOOL_ID]) }, [toolBinding("@acme/tools/other")]),
    ).toEqual([
      {
        kind: "unknown_binding_package",
        package: "@acme/tools/other",
        handle: "gh",
      },
    ]);
  });

  test("an action binding for a handler that declares no credentials", () => {
    expect(
      check({ push: action({ handler: "push" }) }, [actionBinding("push")]),
    ).toEqual([
      { kind: "unknown_binding_action", action: "push", handle: "gh" },
    ]);
  });

  test("an action binding for a handle the handler did not declare", () => {
    expect(
      check(
        {
          push: action({
            handler: "push",
            credentials: ["gh"],
            effect: { requires: ["call"] },
          }),
        },
        [actionBinding("push", "gh"), actionBinding("push", "npm")],
      ),
    ).toEqual([
      { kind: "undeclared_binding_handle", action: "push", handle: "npm" },
    ]);
  });

  test("two bindings for one consumer and handle", () => {
    expect(
      check(
        {
          push: action({
            handler: "push",
            credentials: ["gh"],
            effect: { requires: ["call"] },
          }),
        },
        [actionBinding("push"), actionBinding("push")],
      ),
    ).toEqual([
      { kind: "duplicate_binding", consumer: "action push", handle: "gh" },
    ]);
  });

  test("every violation is reported, not only the first", () => {
    const kinds = check(
      {
        push: action({
          handler: "push",
          credentials: ["gh"],
          effect: { requires: ["call"] },
        }),
        work: agentStep([TOOL_ID]),
      },
      [toolBinding("@acme/tools/other"), actionBinding("release")],
    ).map((violation) => violation.kind);
    expect(kinds).toEqual([
      "unbound_declared_handle",
      "unknown_binding_package",
      "unknown_binding_action",
    ]);
  });

  test("each violation has a description naming it", () => {
    expect(
      describeCredentialBindingViolation({
        kind: "nested_body_bindings",
        bodyId: "child-body",
        reachedThrough: ["spawn"],
      }),
    ).toBe(
      "body child-body (carried by step spawn) declares credentialBindings; remove them, because only the root definition's bindings are resolved",
    );
  });
});

describe("checkCredentialBindings on a malformed projection", () => {
  test("throws for an action whose credentials are not a list of strings", () => {
    const malformed: WorkflowProjectionDefinition = {
      id: "wf",
      triggers: [],
      stepOrder: ["push"],
      steps: {
        push: {
          kind: "action",
          id: "push",
          handler: "push",
          credentials: "gh",
        },
      },
    };
    expect(() => checkCredentialBindings(malformed)).toThrow(
      "credential binding check: action step push declares credentials that are not a list of strings",
    );
  });

  test("an action literal with an empty credentials list declares no handle", () => {
    const projection: WorkflowProjectionDefinition = {
      id: "wf",
      triggers: [],
      stepOrder: ["push"],
      steps: {
        push: { kind: "action", id: "push", handler: "push", credentials: [] },
      },
      credentialBindings: [actionBinding("push")],
    };
    expect(checkCredentialBindings(projection)).toEqual([
      { kind: "undeclared_binding_handle", action: "push", handle: "gh" },
    ]);
  });
});

const unbound = (factory: string) => ({
  kind: "unbound_declared_tool_handle" as const,
  factory,
  handle: "gh",
});
const loopOf = (b: ReturnType<typeof body>) =>
  loop({
    body: b,
    while: "cont",
    carry: "next",
    maxIterations: 1,
    onExhausted: "done",
  });

describe("declared tool handles in nested positions", () => {
  test("map item", () => {
    expect(
      check(
        {
          fan: map({
            over: { literal: [1] },
            step: agentStep([TOOL_ID], ["gh"]),
          }),
        },
        [],
      ),
    ).toEqual([unbound(TOOL_ID)]);
  });
  test("onTrigger body", () => {
    expect(
      check(
        {
          s: onTrigger({
            on: { type: "mail", to: "s@acme.test" },
            body: body("b", { w: agentStep([TOOL_ID], ["gh"]) }),
          }),
        },
        [],
      ),
    ).toEqual([unbound(TOOL_ID)]);
  });
  test("loop body", () => {
    expect(
      check(
        {
          r: loopOf(body("b", { w: agentStep([TOOL_ID], ["gh"]) })),
          done: action({ handler: "done", after: ["r"] }),
        },
        [],
      ),
    ).toEqual([unbound(TOOL_ID)]);
  });
  test("childWorkflow body", () => {
    expect(
      check(
        {
          c: childWorkflow({
            definition: body("b", { w: agentStep([TOOL_ID], ["gh"]) }),
          }),
        },
        [],
      ),
    ).toEqual([unbound(TOOL_ID)]);
  });
  test("map inside loop inside onTrigger, bound and undeclared extra", () => {
    const inner = body("lb", {
      fan: map({ over: { literal: [1] }, step: agentStep([TOOL_ID], ["gh"]) }),
    });
    const sec = body("sb", {
      r: loopOf(inner),
      done: action({ handler: "done", after: ["r"] }),
    });
    expect(
      check(
        {
          s: onTrigger({ on: { type: "mail", to: "s@acme.test" }, body: sec }),
        },
        [toolBinding(TOOL_ID), toolBinding(TOOL_ID, "x")],
      ),
    ).toEqual([
      { kind: "undeclared_tool_binding_handle", package: TOOL_ID, handle: "x" },
    ]);
  });
  test("same id declared in one step and legacy in another", () => {
    const s1 = agentStep([TOOL_ID], ["gh"]);
    const s2 = agentStep([TOOL_ID]);
    expect(
      check({ a: s1, b: s2 }, [
        toolBinding(TOOL_ID),
        toolBinding(TOOL_ID, "legacy"),
      ]),
    ).toEqual([
      {
        kind: "undeclared_tool_binding_handle",
        package: TOOL_ID,
        handle: "legacy",
      },
    ]);
  });
});

describe("the reserved hub handle", () => {
  const HUB = "interchange.hub";

  function project(steps: WorkflowDefinition["steps"]) {
    return WorkflowProjectionDefinition.assert(
      projectLiveToInert(
        defineWorkflow({
          id: "wf",
          trigger: { type: "mail", to: "wf@acme.test" },
          steps,
        }),
      ),
    );
  }

  test("a tool or action declaring it needs no binding", () => {
    expect(
      check(
        {
          work: agentStep([TOOL_ID], [HUB]),
          push: action({
            handler: "push",
            credentials: [HUB],
            effect: { requires: ["call"] },
          }),
        },
        [],
      ),
    ).toEqual([]);
  });

  test("an authored binding may not name it", () => {
    expect(
      check({ work: agentStep([TOOL_ID], [HUB]) }, [toolBinding(TOOL_ID, HUB)]),
    ).toEqual([
      {
        kind: "reserved_handle_binding",
        consumer: `package ${TOOL_ID}`,
        handle: HUB,
      },
    ]);
  });

  test("collects every declaring consumer, nested bodies included", () => {
    const nested = body("b", {
      w: agentStep(["@acme/tools/nested"], [HUB]),
      a: action({
        handler: "nestedPush",
        credentials: [HUB],
        effect: { requires: ["call"] },
      }),
    });
    expect(
      collectHubConsumers(
        project({
          work: agentStep([TOOL_ID], [HUB]),
          other: agentStep(["@acme/tools/plain"], ["gh"]),
          s: onTrigger({
            on: { type: "mail", to: "s@acme.test" },
            body: nested,
          }),
        }),
      ),
    ).toEqual([
      "action:nestedPush",
      "tool:@acme/tools/github",
      "tool:@acme/tools/nested",
    ]);
  });
});
