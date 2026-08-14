import { describe, test, expect } from "bun:test";

import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { enumerateInertOnTriggerBodies } from "./inert-ontrigger-bodies";

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

function agentStep(provider: string, model: string): unknown {
  return { kind: "step", agent: { modelSources: [{ provider, model }] } };
}

describe("enumerateInertOnTriggerBodies", () => {
  test("lifts an inline body and reads each step's declared preference", () => {
    const proj = projection(
      {
        sect: inlineOnTrigger(
          "inner",
          {
            s1: agentStep("anthropic", "m1"),
            // A non-agent step declares no preference.
            w: { kind: "sleep" },
          },
          ["s1", "w"],
        ),
      },
      ["sect"],
    );

    const bodies = enumerateInertOnTriggerBodies(proj);

    expect(bodies).toHaveLength(1);
    const body = bodies[0];
    expect(body?.ref).toBe("wf__sect");
    // The body definition's id is overridden to the ref (not the authored id).
    expect(body?.definition.id).toBe("wf__sect");
    // Every other body field rides verbatim.
    expect(body?.definition.stepOrder).toEqual(["s1", "w"]);
    expect(body?.preferredByStep).toEqual({
      s1: { provider: "anthropic", model: "m1" },
      w: null,
    });
  });

  test("reads a map body step's preference from its inner step's agent", () => {
    const proj = projection(
      {
        sect: inlineOnTrigger(
          "inner",
          {
            m: {
              kind: "map",
              step: {
                agent: { modelSources: [{ provider: "openai", model: "gpt" }] },
              },
            },
          },
          ["m"],
        ),
      },
      ["sect"],
    );

    const bodies = enumerateInertOnTriggerBodies(proj);
    expect(bodies[0]?.preferredByStep).toEqual({
      m: { provider: "openai", model: "gpt" },
    });
  });

  test("yields a null preference for an agent with an empty modelSources", () => {
    const proj = projection(
      {
        sect: inlineOnTrigger(
          "inner",
          { s1: { kind: "step", agent: { modelSources: [] } } },
          ["s1"],
        ),
      },
      ["sect"],
    );

    const bodies = enumerateInertOnTriggerBodies(proj);
    expect(bodies[0]?.preferredByStep).toEqual({ s1: null });
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

    expect(enumerateInertOnTriggerBodies(proj)).toHaveLength(0);
  });

  test("is empty for a projection with no onTrigger step", () => {
    const proj = projection({ w: { kind: "sleep" } }, ["w"]);
    expect(enumerateInertOnTriggerBodies(proj)).toHaveLength(0);
  });

  test("enumerates every inline onTrigger body", () => {
    const proj = projection(
      {
        a: inlineOnTrigger("ia", { s: agentStep("anthropic", "m1") }, ["s"]),
        b: inlineOnTrigger("ib", { s: agentStep("openai", "gpt") }, ["s"]),
      },
      ["a", "b"],
    );

    const refs = enumerateInertOnTriggerBodies(proj)
      .map((body) => body.ref)
      .sort();
    expect(refs).toEqual(["wf__a", "wf__b"]);
  });

  test("throws on a body step step that carries no agent.modelSources", () => {
    const proj = projection(
      {
        sect: inlineOnTrigger("inner", { s1: { kind: "step" } }, ["s1"]),
      },
      ["sect"],
    );

    expect(() => enumerateInertOnTriggerBodies(proj)).toThrow(
      /step s1 is a step primitive but carries no valid agent\.modelSources/,
    );
  });
});
