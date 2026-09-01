import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import {
  childWorkflow,
  defineWorkflow,
  enumerateInlineLoopBodies,
  loop,
  onTrigger,
  projectLiveToInert,
  rewriteInlineChildWorkflowBodies,
  rewriteInlineOnTriggerBodies,
  step,
  type WorkflowDefinition,
} from "@intx/workflow";
import {
  enumerateInertBodies,
  inertLoopBody,
  readInertStepInference,
} from "./inert-ontrigger-bodies";

type Projection = typeof WorkflowProjectionDefinition.infer;

function projection(
  steps: Record<string, unknown>,
  stepOrder: string[],
): Projection {
  return { id: "wf", triggers: [], stepOrder, steps };
}

function inlineOnTrigger(
  bodyId: string,
  bodySteps: Record<string, unknown>,
  bodyStepOrder: string[],
): unknown {
  return {
    kind: "onTrigger",
    id: "sect",
    on: { type: "mail", to: "s@acme.test" },
    body: {
      inline: {
        id: bodyId,
        triggers: [],
        stepOrder: bodyStepOrder,
        steps: bodySteps,
      },
    },
  };
}

function inlineChildWorkflow(
  bodyId: string,
  bodySteps: Record<string, unknown>,
  bodyStepOrder: string[],
): unknown {
  return {
    kind: "childWorkflow",
    id: "spawn",
    definition: {
      inline: {
        id: bodyId,
        triggers: [],
        stepOrder: bodyStepOrder,
        steps: bodySteps,
      },
    },
  };
}

function agentStep(provider: string, model: string): unknown {
  return { kind: "step", agent: { modelSources: [{ provider, model }] } };
}

describe("enumerateInertBodies", () => {
  test("lifts an inline body, overriding its id to the ref", () => {
    const proj = projection(
      {
        sect: inlineOnTrigger(
          "inner",
          {
            s1: agentStep("anthropic", "m1"),
            w: { kind: "sleep" },
          },
          ["s1", "w"],
        ),
      },
      ["sect"],
    );

    const bodies = enumerateInertBodies(proj);

    expect(bodies).toHaveLength(1);
    const body = bodies[0];
    expect(body?.ref).toBe("wf__sect");
    // The body definition's id is overridden to the ref (not the authored id).
    expect(body?.definition.id).toBe("wf__sect");
    // Every other body field rides verbatim.
    expect(body?.definition.stepOrder).toEqual(["s1", "w"]);
  });

  test("skips an already-ref onTrigger body", () => {
    const proj = projection(
      {
        sect: {
          kind: "onTrigger",
          id: "sect",
          on: { type: "mail", to: "s@acme.test" },
          body: { ref: "somewhere_else" },
        },
      },
      ["sect"],
    );

    expect(enumerateInertBodies(proj)).toHaveLength(0);
  });

  test("is empty for a projection with no onTrigger step", () => {
    const proj = projection({ w: { kind: "sleep" } }, ["w"]);
    expect(enumerateInertBodies(proj)).toHaveLength(0);
  });

  test("enumerates every inline onTrigger body", () => {
    const proj = projection(
      {
        a: inlineOnTrigger("ia", { s: agentStep("anthropic", "m1") }, ["s"]),
        b: inlineOnTrigger("ib", { s: agentStep("openai", "gpt") }, ["s"]),
      },
      ["a", "b"],
    );

    const refs = enumerateInertBodies(proj)
      .map((body) => body.ref)
      .sort();
    expect(refs).toEqual(["wf__a", "wf__b"]);
  });

  test("lifts an inline childWorkflow child, id overridden to the ref", () => {
    const proj = projection(
      {
        spawn: inlineChildWorkflow(
          "authored-child",
          { s: agentStep("anthropic", "m1") },
          ["s"],
        ),
      },
      ["spawn"],
    );

    const bodies = enumerateInertBodies(proj);
    expect(bodies).toHaveLength(1);
    expect(bodies[0]?.ref).toBe("wf__spawn");
    expect(bodies[0]?.definition.id).toBe("wf__spawn");
  });

  test("recurses into a grandchild, reproducing the runtime's recursive ref", () => {
    const proj = projection(
      {
        spawn: inlineChildWorkflow(
          "child",
          {
            gc: inlineChildWorkflow(
              "grandchild",
              { s: agentStep("openai", "gpt") },
              ["s"],
            ),
          },
          ["gc"],
        ),
      },
      ["spawn"],
    );

    // Depth-2 refs match rewriteInlineChildWorkflowBodies applied per rung: the
    // grandchild's enclosing id is the child's own ref.
    const refs = enumerateInertBodies(proj)
      .map((b) => b.ref)
      .sort();
    expect(refs).toEqual(["wf__spawn", "wf__spawn__gc"]);
  });

  test("stages a childWorkflow nested inside an onTrigger body (cross-type)", () => {
    const proj = projection(
      {
        sect: inlineOnTrigger(
          "section",
          {
            spawn: inlineChildWorkflow(
              "child",
              { s: agentStep("anthropic", "m1") },
              ["s"],
            ),
          },
          ["spawn"],
        ),
      },
      ["sect"],
    );

    const refs = enumerateInertBodies(proj)
      .map((b) => b.ref)
      .sort();
    expect(refs).toEqual(["wf__sect", "wf__sect__spawn"]);
  });

  test("rejects an onTrigger section nested inside a spawned body", () => {
    // The runtime lifts onTrigger sections only at the top level, so a section
    // inside a childWorkflow child would reach the runtime inline and fail.
    const proj = projection(
      {
        spawn: inlineChildWorkflow(
          "child",
          { sect: inlineOnTrigger("inner", { s: agentStep("p", "m") }, ["s"]) },
          ["sect"],
        ),
      },
      ["spawn"],
    );

    expect(() => enumerateInertBodies(proj)).toThrow(
      /onTrigger section at step sect is nested inside a spawned body/,
    );
  });

  test("enumerates a childWorkflow body with mixed step kinds and lifts its grandchild", () => {
    // A child body mixing an agent step with three non-agent kinds -- a nested
    // inline childWorkflow, a sleep, and an awaitSignal. The nested child is
    // enumerated as its own body; the non-agent kinds do not obstruct the lift.
    const proj = projection(
      {
        spawn: inlineChildWorkflow(
          "child",
          {
            work: agentStep("anthropic", "m1"),
            nap: { kind: "sleep" },
            wait: { kind: "awaitSignal", signalName: "go" },
            sub: inlineChildWorkflow(
              "grandchild",
              { deep: agentStep("openai", "gpt") },
              ["deep"],
            ),
          },
          ["work", "nap", "wait", "sub"],
        ),
      },
      ["spawn"],
    );

    const byRef = new Map(enumerateInertBodies(proj).map((b) => [b.ref, b]));
    expect([...byRef.keys()].sort()).toEqual(["wf__spawn", "wf__spawn__sub"]);
    expect(byRef.get("wf__spawn")?.definition.stepOrder).toEqual([
      "work",
      "nap",
      "wait",
      "sub",
    ]);
  });

  test("stages a childWorkflow nested in a loop body, but NOT the loop body itself", () => {
    // A loop body runs in-process sharing the parent env, so it is never a
    // staged asset. Its childWorkflow grandchild IS a spawned child that needs
    // its own asset + sources, so it is lifted under the nested ref
    // `wf__<loopStep>__<childStep>`, while the loop body ref `wf__rework` is
    // absent from the output.
    const loopBody = {
      id: "rework-body",
      triggers: [],
      stepOrder: ["spawn"],
      steps: {
        spawn: inlineChildWorkflow(
          "child",
          { s: agentStep("anthropic", "m1") },
          ["s"],
        ),
      },
    };
    const proj = projection({ rework: loopStep(loopBody) }, ["rework"]);

    const byRef = new Map(enumerateInertBodies(proj).map((b) => [b.ref, b]));
    expect([...byRef.keys()]).toEqual(["wf__rework__spawn"]);
    expect(byRef.get("wf__rework__spawn")?.definition.stepOrder).toEqual(["s"]);
  });

  test("rejects an onTrigger section nested inside a loop body", () => {
    // A loop body is recursed into as a non-top-level scope, so a nested
    // onTrigger section is rejected exactly as one nested in any spawned body.
    const loopBody = {
      id: "rework-body",
      triggers: [],
      stepOrder: ["sect"],
      steps: {
        sect: inlineOnTrigger("inner", { s: agentStep("p", "m") }, ["s"]),
      },
    };
    const proj = projection({ rework: loopStep(loopBody) }, ["rework"]);

    expect(() => enumerateInertBodies(proj)).toThrow(
      /onTrigger section at step sect is nested inside a spawned body/,
    );
  });
});

// Collect the refs the RUNTIME mints, by applying the same rewrites the runtime
// applies per rung: onTrigger sections lift only at the top level; childWorkflow
// children lift at every rung; each lifted body recurses using its own ref as
// the enclosing id (the extracted body's id is set to its ref).
function liveBodyRefs(def: WorkflowDefinition, isTopLevel: boolean): string[] {
  const refs: string[] = [];
  const bodies = [
    ...(isTopLevel ? rewriteInlineOnTriggerBodies(def).bodies : []),
    ...rewriteInlineChildWorkflowBodies(def).bodies,
  ];
  for (const body of bodies) {
    refs.push(body.ref);
    refs.push(...liveBodyRefs(body.definition, false));
  }
  // A loop body is not a lifted body itself, but the runtime rewrites its inline
  // childWorkflow grandchildren (run-local / run-child), so mirror that descent
  // to collect the grandchild refs without adding the loop body's own ref.
  for (const loopBody of enumerateInlineLoopBodies(def)) {
    refs.push(...liveBodyRefs(loopBody.definition, false));
  }
  return refs;
}

describe("enumerateInertBodies agrees with the runtime rewrite refs", () => {
  const mkAgent = (id: string) =>
    defineAgent({
      id,
      systemPrompt: id,
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "openai", model: "gpt" }] },
    });

  // The deploy enumerator stages each body's sources.json under a ref, and the
  // runtime reads it back by the ref its rewrite mints. If the two ever diverge
  // the child fails loud on a missing source. Pin them equal at every depth.
  const fixtures: Record<string, WorkflowDefinition> = {
    "depth-2 child chain": defineWorkflow({
      id: "P",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({
          definition: defineWorkflow({
            id: "child",
            steps: {
              gc: childWorkflow({
                definition: defineWorkflow({
                  id: "grandchild",
                  steps: { work: step({ agent: mkAgent("g") }) },
                }),
              }),
            },
          }),
        }),
      },
    }),
    "childWorkflow nested in an onTrigger body": defineWorkflow({
      id: "P",
      trigger: { type: "manual" },
      steps: {
        section: onTrigger({
          on: { type: "mail", to: "s@acme.test" },
          body: defineWorkflow({
            id: "sect",
            steps: {
              spawn: childWorkflow({
                definition: defineWorkflow({
                  id: "child",
                  steps: { work: step({ agent: mkAgent("c") }) },
                }),
              }),
            },
          }),
        }),
      },
    }),
    "childWorkflow nested in a loop body": defineWorkflow({
      id: "P",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: defineWorkflow({
            id: "rework-body",
            steps: {
              spawn: childWorkflow({
                definition: defineWorkflow({
                  id: "child",
                  steps: { work: step({ agent: mkAgent("g") }) },
                }),
              }),
            },
          }),
          while: "cont",
          carry: "next",
          maxIterations: 2,
          onExhausted: "esc",
        }),
        esc: step({ agent: mkAgent("esc"), after: ["rework"] }),
      },
    }),
  };

  for (const [name, def] of Object.entries(fixtures)) {
    test(name, () => {
      // Validate through the wire projection type, exactly as the deploy path
      // hands the enumerator its frozen projection.
      const projection = WorkflowProjectionDefinition.assert(
        projectLiveToInert(def),
      );
      const inertRefs = enumerateInertBodies(projection)
        .map((body) => body.ref)
        .sort();
      const runtimeRefs = liveBodyRefs(def, true).sort();
      expect(inertRefs).toEqual(runtimeRefs);
    });
  }
});

function loopStep(body: unknown): unknown {
  return {
    kind: "loop",
    id: "",
    body,
    while: "keepGoing",
    carry: "nextCount",
    maxIterations: 3,
    onExhausted: "esc",
  };
}

describe("inertLoopBody", () => {
  test("returns a loop step's body projection", () => {
    const body = projection({ turn: agentStep("anthropic", "m") }, ["turn"]);
    const result = inertLoopBody(loopStep(body));
    expect(result?.stepOrder).toEqual(["turn"]);
    expect(result?.steps["turn"]).toEqual(agentStep("anthropic", "m"));
  });

  test("returns null for a non-loop step", () => {
    expect(inertLoopBody(agentStep("anthropic", "m"))).toBeNull();
    expect(
      inertLoopBody(inlineOnTrigger("body", { s: agentStep("p", "m") }, ["s"])),
    ).toBeNull();
  });

  test("throws when a loop step's body is not a valid projection", () => {
    expect(() => inertLoopBody(loopStep({ not: "a projection" }))).toThrow(
      /not a valid workflow projection/,
    );
  });
});

describe("readInertStepInference", () => {
  test("reads a step's agent preference and marks it agent-bearing", () => {
    expect(
      readInertStepInference(
        {
          kind: "step",
          agent: { modelSources: [{ provider: "anthropic", model: "m1" }] },
        },
        "ctx: ",
        "s1",
      ),
    ).toEqual({
      isAgent: true,
      preference: { provider: "anthropic", model: "m1" },
    });
  });

  test("reads a map step's preference from its inner step's agent", () => {
    expect(
      readInertStepInference(
        {
          kind: "map",
          step: {
            agent: { modelSources: [{ provider: "openai", model: "gpt" }] },
          },
        },
        "ctx: ",
        "m",
      ),
    ).toEqual({
      isAgent: true,
      preference: { provider: "openai", model: "gpt" },
    });
  });

  test("marks an agent with an empty modelSources as agent-bearing with no preference", () => {
    expect(
      readInertStepInference(
        { kind: "step", agent: { modelSources: [] } },
        "ctx: ",
        "s1",
      ),
    ).toEqual({ isAgent: true, preference: null });
  });

  test("marks a non-agent step as non-agent with no preference", () => {
    expect(readInertStepInference({ kind: "sleep" }, "ctx: ", "w")).toEqual({
      isAgent: false,
      preference: null,
    });
  });

  test("throws on a step that claims to be an agent but has no valid modelSources", () => {
    expect(() =>
      readInertStepInference({ kind: "step" }, "body ref: ", "s1"),
    ).toThrow(
      /body ref: step s1 is a step primitive but carries no valid agent\.modelSources/,
    );
  });
});
