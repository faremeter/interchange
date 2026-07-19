import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { APPROVAL_SNAPSHOT_MAX_BYTES } from "./runtime";
import {
  AgentDeployFrame,
  DeployApplyErrorCategory,
  SidecarFrame,
  SignalCorrelationRegisterFrame,
  SourcesUpdateFrame,
} from "./sidecar";

describe("DeployApplyErrorCategory", () => {
  const allCategories = [
    "tarball.missing",
    "integrity.mismatch",
    "registry.fetch.failed",
    "registry.unknown",
    "registry.auth.failed",
    "tarball.extract.failed",
    "manifest.invalid",
    "package.entry.missing",
    "package.entry.invalid",
    "factory.construct.failed",
    "tool.name.duplicate",
    "apply.swap.failed",
    "apply.previous-rotation.failed",
  ] as const;

  for (const category of allCategories) {
    test(`accepts ${category}`, () => {
      const result = DeployApplyErrorCategory(category);
      expect(result instanceof type.errors).toBe(false);
    });
  }

  test("rejects an unknown category", () => {
    const result = DeployApplyErrorCategory("network.timeout");
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("AgentDeployFrame", () => {
  const baseConfig = {
    sessionId: "ses_1",
    agentId: "agt_1",
    tenantId: "ten_1",
    principalId: "pri_1",
    agentAddress: "agt_1@example.test",
    systemPrompt: "system prompt",
    tools: [],
    grants: [],
    sources: [
      {
        id: "src_default",
        provider: "openai",
        baseURL: "https://api.openai.test",
        apiKey: "sk-test",
        model: "gpt-test",
      },
    ],
    defaultSource: "src_default",
  };

  const trivialFrame = {
    type: "agent.deploy" as const,
    agentAddress: "agt_1@example.test",
    agentId: "agt_1",
    config: baseConfig,
    hubPublicKey: "hub_pubkey_hex",
  };

  const stepSource = {
    id: "src_step",
    provider: "openai",
    baseURL: "https://api.openai.test",
    apiKey: "sk-step",
    model: "gpt-step",
  };

  test("accepts the existing trivial-shape frame (no workflow field)", () => {
    const result = AgentDeployFrame(trivialFrame);
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a multi-step frame with matching definition and sources", () => {
    const result = AgentDeployFrame({
      ...trivialFrame,
      workflow: {
        definition: {
          id: "wf_demo",
          triggers: [{ type: "manual" }],
          stepOrder: ["plan", "act"],
          steps: { plan: {}, act: {} },
        },
        sources: { plan: [stepSource], act: [stepSource] },
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a frame whose workflow.definition is present without sources", () => {
    const result = AgentDeployFrame({
      ...trivialFrame,
      workflow: {
        definition: {
          id: "wf_demo",
          triggers: [{ type: "manual" }],
          stepOrder: ["plan"],
          steps: { plan: {} },
        },
      },
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a frame whose stepOrder names a step missing from sources", () => {
    const result = AgentDeployFrame({
      ...trivialFrame,
      workflow: {
        definition: {
          id: "wf_demo",
          triggers: [{ type: "manual" }],
          stepOrder: ["plan", "act"],
          steps: { plan: {}, act: {} },
        },
        sources: { plan: [stepSource] },
      },
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a frame whose workflow.definition is missing triggers", () => {
    // The wire validator must require `triggers` because the sidecar
    // deploy router serializes `definition` verbatim into
    // `workflow.json` and the workflow-process child re-validates the
    // envelope (`workflowDefinitionEnvelopeSchema`) which requires it.
    const result = AgentDeployFrame({
      ...trivialFrame,
      workflow: {
        definition: {
          id: "wf_demo",
          stepOrder: ["plan"],
          steps: { plan: {} },
        },
        sources: { plan: [stepSource] },
      },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("SourcesUpdateFrame", () => {
  const source = {
    id: "src_a",
    provider: "openai",
    baseURL: "https://api.openai.test",
    apiKey: "sk-a",
    model: "gpt-a",
  };
  const base = {
    type: "sources.update" as const,
    requestId: "req_1",
    agentAddress: "agt_1@example.test",
    defaultSource: "src_a",
  };

  test("accepts a frame with a non-empty sources list", () => {
    const result = SourcesUpdateFrame({ ...base, sources: [source] });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a frame whose sources list is empty", () => {
    // The hub never emits an empty rotation -- `pushInstanceSourceUpdate`
    // returns early when there is no head source -- so the boundary
    // rejects an empty `sources` rather than accepting a rotation the
    // agent could not swap to any live source.
    const result = SourcesUpdateFrame({ ...base, sources: [] });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("SignalCorrelationRegisterFrame snapshot requirement", () => {
  const base = {
    type: "signal.correlation.register",
    correlationId: "corr-1",
    runId: "run-1",
    deploymentId: "dep-1",
    agentAddress: "ins_dep@integration.interchange",
    kind: "approval",
  };
  const snapshot = {
    name: "charge_card",
    description: "Charge the customer's card",
    inputSchema: { type: "object" },
    arguments: { amount: 100 },
  };

  test("accepts a register frame carrying a snapshot", () => {
    const frame = { ...base, snapshot };
    expect(SignalCorrelationRegisterFrame(frame) instanceof type.errors).toBe(
      false,
    );
    expect(SidecarFrame(frame) instanceof type.errors).toBe(false);
  });

  test("rejects a register frame with no snapshot", () => {
    // The ask rail is the only producer and always carries a snapshot, so a
    // snapshot-absent frame is malformed at the receive boundary -- it fails
    // the union parse and is logged and dropped, never co-written as null.
    expect(SignalCorrelationRegisterFrame(base) instanceof type.errors).toBe(
      true,
    );
    expect(SidecarFrame(base) instanceof type.errors).toBe(true);
  });

  test("rejects a register frame whose snapshot exceeds the size cap", () => {
    // The snapshot crosses the sidecar->hub boundary as a
    // `BoundedApprovalSnapshot`, so an oversized one -- here an inputSchema
    // padded past the byte cap -- fails the frame parse and is dropped rather
    // than co-written onto an approval row. Only the pad pushes it over; every
    // other field is the valid baseline, so the cap is the sole reason for
    // rejection.
    const frame = {
      ...base,
      snapshot: {
        ...snapshot,
        inputSchema: { pad: "a".repeat(APPROVAL_SNAPSHOT_MAX_BYTES) },
      },
    };
    expect(SignalCorrelationRegisterFrame(frame) instanceof type.errors).toBe(
      true,
    );
    expect(SidecarFrame(frame) instanceof type.errors).toBe(true);
  });
});
