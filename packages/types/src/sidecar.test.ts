import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { APPROVAL_SNAPSHOT_MAX_BYTES } from "./runtime";
import {
  AgentDeployFrame,
  CredentialsUpdateFrame,
  DeployApplyErrorCategory,
  PackRejectFrame,
  PackRejectReason,
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

describe("PackRejectFrame reason forward-compat", () => {
  const base = {
    type: "repo.pack.reject" as const,
    agentAddress: "agt_1@example.test",
    repoId: { kind: "workflow-run", id: "dep-1" },
    transferId: "xfer_1",
  };

  test("accepts a known reason", () => {
    const result = PackRejectFrame({ ...base, reason: "path_violation" });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an unknown reason a newer peer may add", () => {
    // The whole point of the widening: a reject carrying a reason this build
    // does not know still validates, so it reaches the reject handler (which
    // latches the transfer) instead of failing HubFrame validation and being
    // dropped -- a dropped reject stalls the transfer until the next disconnect.
    const result = PackRejectFrame({ ...base, reason: "some_future_reason" });
    expect(result instanceof type.errors).toBe(false);
  });

  test("still requires the structural fields (transferId)", () => {
    const result = PackRejectFrame({
      type: "repo.pack.reject",
      agentAddress: "agt_1@example.test",
      repoId: { kind: "workflow-run", id: "dep-1" },
      reason: "timeout",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("PackRejectReason stays strict for producers", () => {
    // Producers classify and construct through the enum, which is unchanged, so
    // a typo'd reason is still caught at the producer, not on the wire.
    expect(PackRejectReason("path_violation") instanceof type.errors).toBe(
      false,
    );
    expect(PackRejectReason("some_future_reason") instanceof type.errors).toBe(
      true,
    );
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
        credentialId: "sk-test",
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
    credentialId: "sk-step",
    model: "gpt-step",
  };

  // The source-ref pin every workflow frame carries: where the definition's
  // bytes come from plus the frozen dependency closure (empty here -- a
  // workflow that pins no tool packages).
  const validSourceRef = {
    source: { kind: "registry", registry: "npmjs" },
    closure: { schemaVersion: "1", topLevel: [], entries: [] },
  };

  test("accepts the existing trivial-shape frame (no workflow field)", () => {
    const result = AgentDeployFrame(trivialFrame);
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a workflow frame with per-step sources and a source-ref pin", () => {
    const result = AgentDeployFrame({
      ...trivialFrame,
      workflow: {
        sources: { plan: [stepSource], act: [stepSource] },
        sourceRef: validSourceRef,
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a workflow frame with no source-ref pin", () => {
    // Source-ref is the only deploy lineage; without the pin the sidecar has
    // no closure to evaluate the definition from, so the frame is rejected.
    const result = AgentDeployFrame({
      ...trivialFrame,
      workflow: {
        sources: { plan: [stepSource] },
      },
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a workflow frame with no per-step sources", () => {
    const result = AgentDeployFrame({
      ...trivialFrame,
      workflow: {
        sourceRef: validSourceRef,
      },
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a workflow frame whose step source chain is empty", () => {
    // Every step's failover chain must carry at least one source.
    const result = AgentDeployFrame({
      ...trivialFrame,
      workflow: {
        sources: { plan: [] },
        sourceRef: validSourceRef,
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
    credentialId: "sk-a",
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

describe("CredentialsUpdateFrame", () => {
  const material = {
    credentialId: "cred_a",
    providerKey: "http",
    origin: "https://api.example.test",
    secret: "sk-real",
  };
  const binding = {
    handle: "gh",
    credentialId: "cred_a",
    consumer: "tool:@intx/tools-example",
  };
  const base = {
    type: "credentials.update" as const,
    requestId: "req_1",
    agentAddress: "agt_1@example.test",
  };

  test("accepts a well-formed delivery", () => {
    const result = CredentialsUpdateFrame({
      ...base,
      delivery: { bindings: [binding], materials: [material] },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an empty delivery (a revocation that evicts every credential)", () => {
    const result = CredentialsUpdateFrame({
      ...base,
      delivery: { bindings: [], materials: [] },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a material entry missing its secret", () => {
    const result = CredentialsUpdateFrame({
      ...base,
      delivery: {
        bindings: [binding],
        materials: [
          {
            credentialId: "cred_a",
            providerKey: "http",
            origin: "https://api.example.test",
          },
        ],
      },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("SignalCorrelationRegisterFrame snapshot requirement", () => {
  const base = {
    type: "signal.correlation.register",
    correlationId: "corr-1",
    runId: "run-1",
    anchorRunId: "dep-1",
    agentAddress: "run_dep@integration.interchange",
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
