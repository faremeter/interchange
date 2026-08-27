import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import {
  defineAgent,
  defineTool,
  type AgentDefinition,
  type AnnotatedToolFactory,
  type InferencePreference,
  type ToolDeclaration,
} from "@intx/agent";
import {
  action,
  awaitSignal,
  childWorkflow,
  defineWorkflow,
  escalation,
  gate,
  loop,
  map,
  onTrigger,
  sleep,
  step,
  type BodyFailurePolicy,
  type Primitive,
  type WorkflowDefinition,
} from "./definition/index";
import {
  WorkflowProjectionDefinition,
  WorkflowStep,
} from "@intx/types/sidecar";
import {
  canonicalJsonStringify,
  computeWireDefinitionHash,
} from "@intx/types/wire-definition-hash";

import {
  computeLiveDefinitionHash,
  projectLiveToInert,
  type InertStepStep,
} from "./live-inert-projector";

// ---------------------------------------------------------------------------
// Corpus builders
// ---------------------------------------------------------------------------

function mkTool(
  id: string,
  definitions: readonly ToolDeclaration[],
  requires: readonly string[] = [],
): AnnotatedToolFactory {
  return defineTool({
    id,
    requires,
    definitions,
    factory: () => ({
      definitions: [],
      run: async () => ({ callId: "c", content: "" }),
    }),
  });
}

const alphaTool = mkTool(
  "test/alpha",
  [{ name: "alpha_read" }, { name: "alpha_write", approval: "ask" }],
  ["storage"],
);
const betaTool = mkTool("test/beta", [{ name: "beta_do" }]);
// Same declarations as `alphaTool` plus one extra tool name.
const alphaToolPlus = mkTool(
  "test/alpha",
  [
    { name: "alpha_read" },
    { name: "alpha_write", approval: "ask" },
    { name: "alpha_extra" },
  ],
  ["storage"],
);
// Same as `alphaTool` but `alpha_write` no longer requires approval.
const alphaToolNoApproval = mkTool(
  "test/alpha",
  [{ name: "alpha_read" }, { name: "alpha_write" }],
  ["storage"],
);
// Same declarations as `betaTool` under a different factory id.
const gammaTool = mkTool("test/gamma", [{ name: "beta_do" }]);

function mkAgent(
  factories: readonly AnnotatedToolFactory[],
  capabilities: readonly string[],
  sources: readonly InferencePreference[],
): AgentDefinition {
  return defineAgent({
    id: "agent_main",
    systemPrompt: "main agent",
    tools: factories,
    capabilities,
    inference: { sources },
  });
}

const OPENAI: InferencePreference = { provider: "openai", model: "gpt-4o" };

const baseCapabilities = ["mail.send:acme.test"] as const;

function mkSingleStep(agent: AgentDefinition): WorkflowDefinition {
  return defineWorkflow({
    id: "wf_main",
    trigger: { type: "mail", to: "wf@acme.test" },
    steps: { main: step({ agent }) },
  });
}

/** The baseline workflow: one step, a two-factory agent, one model source. */
function baseWorkflow(): WorkflowDefinition {
  return mkSingleStep(
    mkAgent([alphaTool, betaTool], baseCapabilities, [OPENAI]),
  );
}

function mainStepOf(definition: WorkflowDefinition): InertStepStep {
  const projection = projectLiveToInert(definition);
  const main = projection.steps["main"];
  if (main === undefined || main.kind !== "step") {
    throw new Error("expected a projected `step` at key `main`");
  }
  return main;
}

function toolNamesOf(step_: InertStepStep): string[] {
  return step_.agent.toolFactories.flatMap((factory) =>
    factory.definitions.map((declaration) => declaration.name),
  );
}

// ---------------------------------------------------------------------------
// Grant-surface reification
// ---------------------------------------------------------------------------

describe("grant-surface reification", () => {
  test("projects a plain-data entry for every tool declaration name", () => {
    const main = mainStepOf(baseWorkflow());
    const names = toolNamesOf(main);
    // Positive assertion on the names themselves. A "grant set is non-empty"
    // check would pass on the toolFactories:[null] bug; naming each grant is
    // the only assertion that catches a silently-lost grant surface.
    expect(names).toContain("alpha_read");
    expect(names).toContain("alpha_write");
    expect(names).toContain("beta_do");
  });

  test("carries the approval flag per declaration", () => {
    const main = mainStepOf(baseWorkflow());
    const declarations = main.agent.toolFactories.flatMap(
      (factory) => factory.definitions,
    );
    const alphaWrite = declarations.find((d) => d.name === "alpha_write");
    const alphaRead = declarations.find((d) => d.name === "alpha_read");
    expect(alphaWrite?.approval).toBe("ask");
    expect(alphaRead?.approval).toBeUndefined();
  });

  test("N factories produce N non-null entries with their ids", () => {
    const main = mainStepOf(baseWorkflow());
    expect(main.agent.toolFactories).toHaveLength(2);
    for (const factory of main.agent.toolFactories) {
      expect(factory).not.toBeNull();
      expect(typeof factory.id).toBe("string");
      expect(factory.id.length).toBeGreaterThan(0);
    }
    expect(main.agent.toolFactories.map((f) => f.id)).toEqual([
      "test/alpha",
      "test/beta",
    ]);
  });

  test("canonicalizes model sources to (provider, model)", () => {
    const main = mainStepOf(baseWorkflow());
    expect(main.agent.modelSources).toEqual([
      { provider: "openai", model: "gpt-4o" },
    ]);
  });

  test("projects the agent's declared plugin-package names", () => {
    const agent = defineAgent({
      id: "agent_main",
      systemPrompt: "main agent",
      tools: [],
      plugins: ["@intx/tools-lsp"],
      capabilities: [],
      inference: { sources: [OPENAI] },
    });
    const main = mainStepOf(mkSingleStep(agent));
    expect(main.agent.plugins).toEqual(["@intx/tools-lsp"]);
  });

  test("omits plugins from the projection when the agent declares none", () => {
    const main = mainStepOf(baseWorkflow());
    expect(main.agent.plugins).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Hash sensitivity to the grant surface
// ---------------------------------------------------------------------------

describe("hash binds to the grant surface", () => {
  test("hash(with-tools) != hash(with-tools-stripped)", async () => {
    const withTools = baseWorkflow();
    const stripped = mkSingleStep(mkAgent([], baseCapabilities, [OPENAI]));
    expect(await computeLiveDefinitionHash(withTools)).not.toBe(
      await computeLiveDefinitionHash(stripped),
    );
  });

  // Each grant-surface mutation must move BOTH the projection bytes and the
  // hash. Testing the projection change alone would not prove the hash is
  // bound to the surface; testing the hash alone would not prove the surface
  // is what moved it.
  const baselineWorkflow = baseWorkflow();
  const baselineBytes = canonicalJsonStringify(
    projectLiveToInert(baselineWorkflow),
  );

  async function expectGrantMutation(
    mutated: WorkflowDefinition,
  ): Promise<void> {
    const mutatedBytes = canonicalJsonStringify(projectLiveToInert(mutated));
    expect(mutatedBytes).not.toBe(baselineBytes);
    expect(await computeLiveDefinitionHash(mutated)).not.toBe(
      await computeLiveDefinitionHash(baselineWorkflow),
    );
  }

  test("adding a tool name changes projection and hash", async () => {
    await expectGrantMutation(
      mkSingleStep(
        mkAgent([alphaToolPlus, betaTool], baseCapabilities, [OPENAI]),
      ),
    );
  });

  test("flipping an approval flag changes projection and hash", async () => {
    await expectGrantMutation(
      mkSingleStep(
        mkAgent([alphaToolNoApproval, betaTool], baseCapabilities, [OPENAI]),
      ),
    );
  });

  test("swapping a factory id changes projection and hash", async () => {
    await expectGrantMutation(
      mkSingleStep(mkAgent([alphaTool, gammaTool], baseCapabilities, [OPENAI])),
    );
  });

  test("adding a declared plugin package changes projection and hash", async () => {
    // A plugin package contributes tool grants the operator approves, so a
    // tampered plugin set must move the hashed surface and fail re-verify.
    const withPlugin = defineAgent({
      id: "agent_main",
      systemPrompt: "main agent",
      tools: [alphaTool, betaTool],
      plugins: ["@intx/tools-lsp"],
      capabilities: [...baseCapabilities],
      inference: { sources: [OPENAI] },
    });
    await expectGrantMutation(mkSingleStep(withPlugin));
  });

  test("adding a mail.send domain changes projection and hash", async () => {
    await expectGrantMutation(
      mkSingleStep(
        mkAgent(
          [alphaTool, betaTool],
          ["mail.send:acme.test", "mail.send:beta.test"],
          [OPENAI],
        ),
      ),
    );
  });

  test("changing provider or model changes projection and hash", async () => {
    await expectGrantMutation(
      mkSingleStep(
        mkAgent([alphaTool, betaTool], baseCapabilities, [
          { provider: "anthropic", model: "claude-3-7-sonnet" },
        ]),
      ),
    );
  });

  test("adding a credential binding changes projection and hash", async () => {
    // A binding names WHICH provider-backed credential the code may request;
    // it is part of the operator-approved surface, so it must move both the
    // projection bytes and the hash. Same agent/steps as the baseline -- only
    // the binding differs -- so the binding is the isolated mover.
    await expectGrantMutation(
      defineWorkflow({
        id: "wf_main",
        trigger: { type: "mail", to: "wf@acme.test" },
        steps: {
          main: step({
            agent: mkAgent([alphaTool, betaTool], baseCapabilities, [OPENAI]),
          }),
        },
        credentialBindings: [
          {
            package: "test/alpha",
            handle: "api_key",
            provider: "openai",
            locator: "tenant",
          },
        ],
      }),
    );
  });
});

// ---------------------------------------------------------------------------
// Wire boundary preserves credentialBindings (the `"+": "delete"` guard)
// ---------------------------------------------------------------------------

describe("wire validator preserves credentialBindings", () => {
  test("a projected binding survives WorkflowProjectionDefinition validation", () => {
    // `WorkflowProjectionDefinition` carries `"+": "delete"`, so any key it
    // does not declare is stripped at the wire boundary. If the projector
    // emitted `credentialBindings` but the schema omitted it, the binding
    // would vanish between the hub-resolved projection and the one the sidecar
    // validates -- silently, with no error. Round-trip a projected definition
    // through the schema and assert the binding survives.
    const binding = {
      package: "test/alpha",
      handle: "api_key",
      provider: "openai",
      locator: "tenant",
    } as const;
    const projection = projectLiveToInert(
      defineWorkflow({
        id: "wf_main",
        trigger: { type: "mail", to: "wf@acme.test" },
        steps: {
          main: step({
            agent: mkAgent([alphaTool], baseCapabilities, [OPENAI]),
          }),
        },
        credentialBindings: [binding],
      }),
    );
    const validated = WorkflowProjectionDefinition(projection);
    if (validated instanceof type.errors) {
      throw new Error(
        `projection failed the wire schema: ${validated.summary}`,
      );
    }
    expect(validated.credentialBindings).toEqual([binding]);
  });
});

// ---------------------------------------------------------------------------
// Credentials excluded from the hashed preimage
// ---------------------------------------------------------------------------

describe("credential material is excluded", () => {
  test("rotating per-source parameters is a hash no-op", async () => {
    const v1 = mkSingleStep(
      mkAgent([alphaTool], baseCapabilities, [
        {
          provider: "openai",
          model: "gpt-4o",
          parameters: { apiKeyRef: "v1" },
        },
      ]),
    );
    const v2 = mkSingleStep(
      mkAgent([alphaTool], baseCapabilities, [
        {
          provider: "openai",
          model: "gpt-4o",
          parameters: { apiKeyRef: "v2" },
        },
      ]),
    );
    // A credential/parameter rotation must not trip re-verify: the projection
    // keeps only the (provider, model) identity, so the two hash equal.
    expect(await computeLiveDefinitionHash(v1)).toBe(
      await computeLiveDefinitionHash(v2),
    );
    const main = mainStepOf(v1);
    expect(main.agent.modelSources).toEqual([
      { provider: "openai", model: "gpt-4o" },
    ]);
    expect(canonicalJsonStringify(projectLiveToInert(v1))).not.toContain(
      "apiKeyRef",
    );
  });
});

// ---------------------------------------------------------------------------
// No-op mutations do not move the hash
// ---------------------------------------------------------------------------

function reorderKeysDeep(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(reorderKeysDeep);
  }
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value).reverse()) {
      out[k] = reorderKeysDeep(v);
    }
    return out;
  }
  return value;
}

describe("no-op mutations do not move the hash", () => {
  test("key reordering leaves the hash unchanged", async () => {
    const projection = projectLiveToInert(baseWorkflow());
    const reordered = reorderKeysDeep(projection);
    expect(await computeWireDefinitionHash(reordered)).toBe(
      await computeWireDefinitionHash(projection),
    );
  });

  test("whitespace differences leave the hash unchanged", async () => {
    const projection = projectLiveToInert(baseWorkflow());
    const spaced: unknown = JSON.parse(JSON.stringify(projection, null, 4));
    expect(await computeWireDefinitionHash(spaced)).toBe(
      await computeWireDefinitionHash(projection),
    );
  });
});

// ---------------------------------------------------------------------------
// Unreifiable / corrupt shapes fail loud
// ---------------------------------------------------------------------------

describe("unreifiable shapes throw", () => {
  test("an unknown step kind throws rather than projecting to null", () => {
    const raw: unknown = {
      id: "wf_bad",
      triggers: [{ type: "manual" }],
      stepOrder: ["s"],
      steps: { s: { kind: "teleport", id: "s" } },
    };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- deliberately-invalid input to exercise the fail-loud path
    const bad = raw as WorkflowDefinition;
    expect(() => projectLiveToInert(bad)).toThrow(/unknown kind/);
  });

  test("a tool factory that reified to null throws", () => {
    const raw: unknown = {
      id: "wf_nullfac",
      triggers: [{ type: "manual" }],
      stepOrder: ["s"],
      steps: {
        s: {
          kind: "step",
          id: "s",
          agent: {
            id: "a",
            systemPrompt: "p",
            toolFactories: [null],
            capabilities: [],
            inference: { sources: [] },
          },
        },
      },
    };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- simulates the JSON-round-tripped toolFactories:[null] grant-loss bug
    const bad = raw as WorkflowDefinition;
    expect(() => projectLiveToInert(bad)).toThrow(/not a function/);
  });
});

// ---------------------------------------------------------------------------
// Nested grant surfaces (loop body, inline onTrigger body)
// ---------------------------------------------------------------------------

describe("nested grant surfaces are reified", () => {
  const loopWorkflow = defineWorkflow({
    id: "wf_loop",
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body: defineWorkflow({
          id: "body",
          steps: {
            work: step({
              agent: mkAgent([alphaTool], baseCapabilities, [OPENAI]),
            }),
          },
        }),
        while: "loop.again",
        carry: "loop.carry",
        maxIterations: 3,
        onExhausted: "done",
      }),
      done: step({
        agent: mkAgent([], baseCapabilities, [OPENAI]),
        after: ["rework"],
      }),
    },
  });

  const onTriggerWorkflow = defineWorkflow({
    id: "wf_ontrigger",
    trigger: { type: "manual" },
    steps: {
      sect: onTrigger({
        on: { type: "mail", to: "sect@acme.test" },
        body: defineWorkflow({
          id: "sec",
          steps: {
            handle: step({
              agent: mkAgent([betaTool], baseCapabilities, [OPENAI]),
            }),
          },
        }),
      }),
    },
  });

  test("a loop body's tool names appear in the projection", () => {
    const bytes = canonicalJsonStringify(projectLiveToInert(loopWorkflow));
    expect(bytes).toContain("alpha_read");
    expect(bytes).toContain("alpha_write");
  });

  test("an inline onTrigger body's tool names appear in the projection", () => {
    const projection = projectLiveToInert(onTriggerWorkflow);
    const sect = projection.steps["sect"];
    if (sect === undefined || sect.kind !== "onTrigger") {
      throw new Error("expected an onTrigger step");
    }
    if (!("inline" in sect.body)) {
      throw new Error("expected an inline onTrigger body");
    }
    const bytes = canonicalJsonStringify(sect.body.inline);
    expect(bytes).toContain("beta_do");
  });

  const childWorkflowParent = defineWorkflow({
    id: "wf_childparent",
    trigger: { type: "manual" },
    steps: {
      spawn: childWorkflow({
        definition: defineWorkflow({
          id: "authored-child",
          steps: {
            work: step({
              agent: mkAgent([betaTool], baseCapabilities, [OPENAI]),
            }),
          },
        }),
      }),
    },
  });

  test("an inline childWorkflow's tool names appear in the projection", () => {
    // The inline child projects recursively, mirroring an onTrigger body, so
    // its grant surface survives the child->hub boundary and rides the parent's
    // hashed projection.
    const projection = projectLiveToInert(childWorkflowParent);
    const spawn = projection.steps["spawn"];
    if (spawn === undefined || spawn.kind !== "childWorkflow") {
      throw new Error("expected a childWorkflow step");
    }
    if (!("inline" in spawn.definition)) {
      throw new Error("expected an inline childWorkflow definition");
    }
    const bytes = canonicalJsonStringify(spawn.definition.inline);
    expect(bytes).toContain("beta_do");
  });

  test("the parent hash folds vendored child content", async () => {
    // A parent whose sole step spawns an inline child differing only in the
    // child step's tool. The operative deploy/approval/re-verify hash
    // (computeLiveDefinitionHash, via projectChildWorkflow recursing into the
    // inline body) must distinguish the two -- otherwise two parents vendoring
    // different children would share an approved hash and one could run the
    // other's child content.
    const withChild = (tool: typeof alphaTool) =>
      defineWorkflow({
        id: "wf_hashparent",
        trigger: { type: "manual" },
        steps: {
          spawn: childWorkflow({
            definition: defineWorkflow({
              id: "authored-child",
              steps: {
                work: step({
                  agent: mkAgent([tool], baseCapabilities, [OPENAI]),
                }),
              },
            }),
          }),
        },
      });
    const alphaParent = withChild(alphaTool);
    const betaParent = withChild(betaTool);
    const alphaParentAgain = withChild(alphaTool);

    expect(await computeLiveDefinitionHash(alphaParent)).not.toBe(
      await computeLiveDefinitionHash(betaParent),
    );
    expect(await computeLiveDefinitionHash(alphaParent)).toBe(
      await computeLiveDefinitionHash(alphaParentAgain),
    );
  });
});

// ---------------------------------------------------------------------------
// Serialize -> round-trip -> recompute hash is byte-identical
// ---------------------------------------------------------------------------

describe("child->hub determinism", () => {
  const stateful = defineWorkflow({
    id: "wf_state",
    trigger: { type: "mail", to: "wf@acme.test" },
    steps: {
      main: step({
        agent: mkAgent([alphaTool, betaTool], baseCapabilities, [OPENAI]),
      }),
    },
    state: { schema: type({ counter: "number" }) },
  });

  test("a JSON round-trip recomputes a byte-identical hash", async () => {
    const projection = projectLiveToInert(stateful);
    const roundTripped: unknown = JSON.parse(JSON.stringify(projection));
    expect(canonicalJsonStringify(roundTripped)).toBe(
      canonicalJsonStringify(projection),
    );
    expect(await computeWireDefinitionHash(roundTripped)).toBe(
      await computeWireDefinitionHash(projection),
    );
  });

  test("the projector output validates against the closed wire schema", () => {
    const projection = projectLiveToInert(stateful);
    expect(
      WorkflowProjectionDefinition(projection) instanceof type.errors,
    ).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Compatibility guard: the closed schema accepts every real Primitive
// ---------------------------------------------------------------------------

describe("closed step schema accepts every real Primitive variant", () => {
  // Build the corpus with `defineWorkflow` so the step shapes are exactly the
  // ones the existing live-deploy path passes through the wire projection.
  const toolAgent = mkAgent([alphaTool, betaTool], baseCapabilities, [OPENAI]);
  const plainAgent = mkAgent([], baseCapabilities, [OPENAI]);

  const corpus: WorkflowDefinition[] = [
    defineWorkflow({
      id: "wf_stepmap",
      trigger: { type: "mail", to: "sm@acme.test" },
      steps: {
        plan: step({ agent: toolAgent }),
        fan: map({
          over: { from: "steps.plan.output" },
          step: step({ agent: toolAgent }),
          after: ["plan"],
        }),
      },
    }),
    defineWorkflow({
      id: "wf_gate",
      trigger: { type: "manual" },
      steps: {
        decide: gate({
          when: { from: "trigger.payload" },
          then: "yes",
          else: "no",
        }),
        yes: step({ agent: plainAgent, after: ["decide"] }),
        no: step({ agent: plainAgent, after: ["decide"] }),
      },
    }),
    defineWorkflow({
      id: "wf_misc",
      trigger: { type: "manual" },
      steps: {
        act: action({ handler: "do.thing" }),
        wait: awaitSignal({ name: "go", after: ["act"] }),
        nap: sleep({ duration: 1000, after: ["wait"] }),
        child: childWorkflow({
          definition: defineWorkflow({
            id: "wf_child",
            steps: { childStep: step({ agent: plainAgent }) },
          }),
          after: ["nap"],
        }),
        esc: escalation({ to: "ops@acme.test", after: ["child"] }),
      },
    }),
    defineWorkflow({
      id: "wf_loop",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: defineWorkflow({
            id: "body",
            steps: { work: step({ agent: toolAgent }) },
          }),
          while: "loop.again",
          carry: "loop.carry",
          maxIterations: 3,
          onExhausted: "done",
        }),
        done: step({ agent: plainAgent, after: ["rework"] }),
      },
    }),
    defineWorkflow({
      id: "wf_ontrigger",
      trigger: { type: "manual" },
      steps: {
        sect: onTrigger({
          on: { type: "mail", to: "sect@acme.test" },
          body: defineWorkflow({
            id: "sec",
            steps: { handle: step({ agent: toolAgent }) },
          }),
        }),
      },
    }),
  ];

  const seenKinds = new Set<Primitive["kind"]>();
  for (const definition of corpus) {
    for (const [stepId, primitive] of Object.entries(definition.steps)) {
      seenKinds.add(primitive.kind);
      test(`${definition.id}.${stepId} (${primitive.kind}) validates as a live primitive`, () => {
        // The live primitive still carries function-valued tool factories;
        // the closed schema must accept it because the live-deploy path ships
        // exactly this shape (a JSON round-trip nulls the functions later).
        const result = WorkflowStep(primitive);
        // A validation failure here is a STOP-and-report blocker: the fix
        // would require editing the shared producer `toWireWorkflowDefinition`,
        // which this task must not touch.
        expect(result instanceof type.errors).toBe(false);
      });
    }
  }

  test("every projected step validates against the closed schema", () => {
    for (const definition of corpus) {
      const projection = projectLiveToInert(definition);
      for (const [stepId, projected] of Object.entries(projection.steps)) {
        const result = WorkflowStep(projected);
        if (result instanceof type.errors) {
          throw new Error(
            `projected step ${definition.id}.${stepId} failed the closed ` +
              `schema: ${result.summary}`,
          );
        }
      }
      expect(
        WorkflowProjectionDefinition(projection) instanceof type.errors,
      ).toBe(false);
    }
  });

  test("the corpus exercises all ten primitive kinds", () => {
    const expectedKinds: Primitive["kind"][] = [
      "action",
      "awaitSignal",
      "childWorkflow",
      "escalation",
      "gate",
      "loop",
      "map",
      "onTrigger",
      "sleep",
      "step",
    ];
    expect([...seenKinds].sort()).toEqual(expectedKinds.sort());
  });
});

// ---------------------------------------------------------------------------
// onTrigger onBodyFailure: projection carries the policy through the hash, and
// an absent policy leaves the projection (and hash) unchanged so existing
// deployments never face forced re-approval.
// ---------------------------------------------------------------------------

describe("onTrigger onBodyFailure projection and hash", () => {
  function sectionWorkflow(
    onBodyFailure?: BodyFailurePolicy,
  ): WorkflowDefinition {
    const section: Primitive = {
      kind: "onTrigger",
      id: "",
      on: { type: "mail", to: "run_sec@t.example" },
      body: { ref: "body-ref" },
      drainBehavior: "wait",
      ...(onBodyFailure !== undefined ? { onBodyFailure } : {}),
    };
    return defineWorkflow({ id: "on-trigger-hash", steps: { section } });
  }

  test("a default section omits onBodyFailure from the inert projection", () => {
    const json = canonicalJsonStringify(projectLiveToInert(sectionWorkflow()));
    expect(json).not.toContain("onBodyFailure");
  });

  test("tolerate moves both the projection bytes and the hash", async () => {
    const dflt = sectionWorkflow();
    const tolerate = sectionWorkflow("tolerate");
    expect(canonicalJsonStringify(projectLiveToInert(tolerate))).not.toBe(
      canonicalJsonStringify(projectLiveToInert(dflt)),
    );
    expect(await computeLiveDefinitionHash(tolerate)).not.toBe(
      await computeLiveDefinitionHash(dflt),
    );
  });
});

// ---------------------------------------------------------------------------
// onFailure is bound into the projection and the approval hash
// ---------------------------------------------------------------------------

describe("onFailure projection and hash", () => {
  // onFailure names WHICH handler a permanent failure routes to, so it is part
  // of the operator-approved surface: it must move both the projected bytes and
  // the approval hash, or a tampered registry could serve a different handler
  // than was approved. The escape surface is three separate projectors -- step,
  // action, childWorkflow -- so each is pinned independently; the existing
  // suite would stay green if a new field escaped any one of them.
  function handlerAgent(): AgentDefinition {
    return mkAgent([alphaTool], baseCapabilities, [OPENAI]);
  }

  function stepWorkflow(onFailure?: string): WorkflowDefinition {
    return defineWorkflow({
      id: "wf_onfailure_step",
      trigger: { type: "mail", to: "wf@acme.test" },
      steps: {
        unit: step({
          agent: mkAgent([alphaTool, betaTool], baseCapabilities, [OPENAI]),
          ...(onFailure !== undefined ? { onFailure } : {}),
        }),
        handler: step({ agent: handlerAgent(), after: ["unit"] }),
      },
    });
  }

  function actionWorkflow(onFailure?: string): WorkflowDefinition {
    return defineWorkflow({
      id: "wf_onfailure_action",
      trigger: { type: "mail", to: "wf@acme.test" },
      steps: {
        unit: action({
          handler: "do-thing",
          ...(onFailure !== undefined ? { onFailure } : {}),
        }),
        handler: step({ agent: handlerAgent(), after: ["unit"] }),
      },
    });
  }

  function childWorkflowWorkflow(onFailure?: string): WorkflowDefinition {
    const child = defineWorkflow({
      id: "wf_child",
      trigger: { type: "mail", to: "child@acme.test" },
      steps: { inner: step({ agent: handlerAgent() }) },
    });
    return defineWorkflow({
      id: "wf_onfailure_child",
      trigger: { type: "mail", to: "wf@acme.test" },
      steps: {
        unit: childWorkflow({
          definition: child,
          ...(onFailure !== undefined ? { onFailure } : {}),
        }),
        handler: step({ agent: handlerAgent(), after: ["unit"] }),
      },
    });
  }

  test("a default step omits onFailure from the inert projection", () => {
    const json = canonicalJsonStringify(projectLiveToInert(stepWorkflow()));
    expect(json).not.toContain("onFailure");
  });

  test("onFailure on a step moves both the projection bytes and the hash", async () => {
    const dflt = stepWorkflow();
    const routed = stepWorkflow("handler");
    expect(canonicalJsonStringify(projectLiveToInert(routed))).not.toBe(
      canonicalJsonStringify(projectLiveToInert(dflt)),
    );
    expect(await computeLiveDefinitionHash(routed)).not.toBe(
      await computeLiveDefinitionHash(dflt),
    );
  });

  test("a default action omits onFailure from the inert projection", () => {
    const json = canonicalJsonStringify(projectLiveToInert(actionWorkflow()));
    expect(json).not.toContain("onFailure");
  });

  test("onFailure on an action moves both the projection bytes and the hash", async () => {
    const dflt = actionWorkflow();
    const routed = actionWorkflow("handler");
    expect(canonicalJsonStringify(projectLiveToInert(routed))).not.toBe(
      canonicalJsonStringify(projectLiveToInert(dflt)),
    );
    expect(await computeLiveDefinitionHash(routed)).not.toBe(
      await computeLiveDefinitionHash(dflt),
    );
  });

  test("a default childWorkflow omits onFailure from the inert projection", () => {
    const json = canonicalJsonStringify(
      projectLiveToInert(childWorkflowWorkflow()),
    );
    expect(json).not.toContain("onFailure");
  });

  test("onFailure on a childWorkflow moves both the projection bytes and the hash", async () => {
    const dflt = childWorkflowWorkflow();
    const routed = childWorkflowWorkflow("handler");
    expect(canonicalJsonStringify(projectLiveToInert(routed))).not.toBe(
      canonicalJsonStringify(projectLiveToInert(dflt)),
    );
    expect(await computeLiveDefinitionHash(routed)).not.toBe(
      await computeLiveDefinitionHash(dflt),
    );
  });

  test("a projected onFailure survives WorkflowProjectionDefinition validation", () => {
    // `WorkflowProjectionDefinition` carries `"+": "delete"`, but a step's own
    // keys are validated by the open `WorkflowStep` schema, which passes
    // unknown keys through -- onFailure rides that passthrough. If `WorkflowStep`
    // were ever closed, onFailure would vanish at the wire boundary silently and
    // the sidecar's re-verify would diverge from the hub hash. Round-trip a
    // projected step through the schema and assert onFailure survives. One step
    // case locks the invariant for all three primitives (shared passthrough).
    const projection = projectLiveToInert(stepWorkflow("handler"));
    const validated = WorkflowProjectionDefinition(projection);
    if (validated instanceof type.errors) {
      throw new Error(
        `projection failed the wire schema: ${validated.summary}`,
      );
    }
    expect(canonicalJsonStringify(validated)).toContain(
      '"onFailure":"handler"',
    );
  });
});

describe("sidecar capability projection", () => {
  test("folds every inline body into the deployment projection", () => {
    const agent = mkAgent([], [], [OPENAI]);
    const capabilityBody = (
      id: string,
      capability: string,
    ): WorkflowDefinition =>
      defineWorkflow({
        id,
        steps: { run: step({ agent }) },
        sidecarPlacement: {
          capabilities: [{ capability, effect: "require" }],
        },
      });
    const child = capabilityBody("child", "device:simulator");
    const loopBody = capabilityBody("loop-body", "storage:host-local");
    const sectionBody = capabilityBody("section-body", "runtime:browser");
    const parent = defineWorkflow({
      id: "parent",
      steps: {
        repeat: loop({
          body: loopBody,
          while: "continue",
          carry: "carry",
          maxIterations: 1,
          onExhausted: "done",
        }),
        done: step({ agent, after: ["repeat"] }),
        section: onTrigger({ on: { type: "manual" }, body: sectionBody }),
        child: childWorkflow({ definition: child, after: ["done"] }),
      },
      sidecarPlacement: {
        capabilities: [{ capability: "platform:ios", effect: "require" }],
      },
    });

    const projection = projectLiveToInert(parent);
    expect(projection.sidecarPlacement).toEqual({
      capabilities: [
        { capability: "platform:ios", effect: "require" },
        { capability: "storage:host-local", effect: "require" },
        { capability: "runtime:browser", effect: "require" },
        { capability: "device:simulator", effect: "require" },
      ],
    });
    expect(
      WorkflowProjectionDefinition(projection) instanceof type.errors,
    ).toBe(false);
  });

  test("deduplicates identical folded rules while preserving conflicts", () => {
    const agent = mkAgent([], [], [OPENAI]);
    const child = defineWorkflow({
      id: "dedup-child",
      steps: { run: step({ agent }) },
      sidecarPlacement: {
        capabilities: [
          { capability: "runtime:browser", effect: "require" },
          { capability: "runtime:browser", effect: "block" },
        ],
      },
    });
    const parent = defineWorkflow({
      id: "dedup-parent",
      steps: { child: childWorkflow({ definition: child }) },
      sidecarPlacement: {
        capabilities: [{ capability: "runtime:browser", effect: "require" }],
      },
    });

    expect(projectLiveToInert(parent).sidecarPlacement).toEqual({
      capabilities: [
        { capability: "runtime:browser", effect: "require" },
        { capability: "runtime:browser", effect: "block" },
      ],
    });
  });

  test("capability requirements move the wire hash", async () => {
    const agent = mkAgent([], [], [OPENAI]);
    const base = defineWorkflow({
      id: "placement-hash",
      steps: { run: step({ agent }) },
    });
    const required = defineWorkflow({
      id: "placement-hash",
      steps: { run: step({ agent }) },
      sidecarPlacement: {
        capabilities: [{ capability: "runtime:browser", effect: "require" }],
      },
    });

    expect(await computeLiveDefinitionHash(required)).not.toBe(
      await computeLiveDefinitionHash(base),
    );
  });
});
