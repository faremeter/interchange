import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { APPROVAL_SNAPSHOT_MAX_BYTES } from "./runtime";
import {
  AgentDeployFrame,
  CredentialsUpdateFrame,
  DeployApplyErrorCategory,
  HubFrame,
  MAX_AGENT_ADDRESSES_FRAME,
  MAX_CACHED_SENDER_ADDRESSES_FRAME,
  MAX_CREDENTIAL_REVOCATIONS_FRAME,
  MAX_MAIL_ADDRESSES_FRAME,
  MAX_MAIL_OUTBOUND_BODY_BYTES,
  MAX_PROBE_GRANTS_FRAME,
  MAX_SIDECAR_FRAME_BYTES,
  MailOutboundFrame,
  PackRejectFrame,
  PackRejectReason,
  ReconnectFrame,
  RegisterFrame,
  RunGrantsFrame,
  SidecarFrame,
  SignalCorrelationRegisterFrame,
  SourcesUpdateFrame,
  WorkflowProbeResultFrame,
} from "./sidecar";

describe("MailOutboundFrame sender ownership claim", () => {
  const frame = {
    type: "mail.outbound",
    senderAddress: "run_sender@example.test",
    rawMessage: "bWFpbA==",
    recipients: ["recipient@example.test"],
  };

  test("accepts a frame with a sender address", () => {
    expect(MailOutboundFrame(frame) instanceof type.errors).toBe(false);
    expect(SidecarFrame(frame) instanceof type.errors).toBe(false);
  });

  test("rejects a frame with no sender address", () => {
    const { senderAddress: _, ...missingSender } = frame;
    expect(MailOutboundFrame(missingSender) instanceof type.errors).toBe(true);
    expect(SidecarFrame(missingSender) instanceof type.errors).toBe(true);
  });
});

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

describe("CredentialsUpdateFrame revoke", () => {
  // `revoke` is the whole point of removal-capable rotation. It must survive
  // the wire-frame validation the hub applies on send (the HubFrame union) and
  // the sidecar applies on receive (CredentialsUpdateFrame). An arktype narrow
  // that dropped it would silently defeat every revoke while every unit test
  // above the wire still passed.
  const pureRevoke = {
    type: "credentials.update",
    requestId: "req_1",
    agentAddress: "dep@integration.interchange",
    delivery: { bindings: [], materials: [] },
    revoke: ["cred_x"],
  };

  test("the HubFrame union preserves a pure-revoke frame's revoke list", () => {
    const out = HubFrame(pureRevoke);
    if (out instanceof type.errors) {
      throw new Error(`expected a valid HubFrame: ${out.summary}`);
    }
    if (out.type !== "credentials.update") {
      throw new Error(`expected a credentials.update frame, got ${out.type}`);
    }
    expect(out.revoke).toEqual(["cred_x"]);
  });

  test("CredentialsUpdateFrame accepts an empty delivery paired with revoke", () => {
    const out = CredentialsUpdateFrame(pureRevoke);
    if (out instanceof type.errors) {
      throw new Error(`expected a valid frame: ${out.summary}`);
    }
    expect(out.revoke).toEqual(["cred_x"]);
  });

  test("a frame with no revoke validates and omits the key", () => {
    const out = CredentialsUpdateFrame({
      type: "credentials.update",
      requestId: "req_1",
      agentAddress: "dep@integration.interchange",
      delivery: { bindings: [], materials: [] },
    });
    if (out instanceof type.errors) {
      throw new Error(`expected a valid frame: ${out.summary}`);
    }
    expect("revoke" in out).toBe(false);
  });

  test("a non-string revoke entry is rejected", () => {
    const bad = { ...pureRevoke, revoke: [123] };
    expect(CredentialsUpdateFrame(bad) instanceof type.errors).toBe(true);
  });
});

describe("RunGrantsFrame senderIdentities co-delivery", () => {
  const base = {
    type: "run.grants" as const,
    agentAddress: "dep@integration.interchange",
    runId: "run_1",
    stepGrants: [],
  };
  const identities = [
    {
      address: "run_sender@integration.interchange",
      publicKey: "aa".repeat(32),
    },
  ];

  test("the HubFrame union admits a run.grants frame carrying identities", () => {
    // The sidecar parses inbound frames through the HubFrame union, so the
    // co-delivered keys must reach the run.grants member and round-trip.
    const out = HubFrame({ ...base, senderIdentities: identities });
    if (out instanceof type.errors) {
      throw new Error(`expected a valid HubFrame: ${out.summary}`);
    }
    if (out.type !== "run.grants") {
      throw new Error(`expected a run.grants frame, got ${out.type}`);
    }
    expect(out.senderIdentities).toEqual(identities);
  });

  test("the HubFrame union rejects a malformed identity entry", () => {
    // arktype passes undeclared keys through unchanged, so a valid-input
    // round-trip alone cannot prove the field is declared on the wire path:
    // it would survive even if senderIdentities were dropped from the schema.
    // A malformed entry rejected THROUGH the union is the real guard -- were
    // the field undeclared, the bad entry would ride the union as a harmless
    // passthrough key and this parse would succeed, silently starving the
    // recipient's key cache.
    const bad = {
      ...base,
      senderIdentities: [{ address: "run_sender@integration.interchange" }],
    };
    expect(HubFrame(bad) instanceof type.errors).toBe(true);
  });

  test("a frame with no senderIdentities validates and omits the key", () => {
    const out = RunGrantsFrame(base);
    if (out instanceof type.errors) {
      throw new Error(`expected a valid frame: ${out.summary}`);
    }
    expect("senderIdentities" in out).toBe(false);
  });

  test("an identity entry missing its public key is rejected", () => {
    const bad = {
      ...base,
      senderIdentities: [{ address: "run_sender@integration.interchange" }],
    };
    expect(RunGrantsFrame(bad) instanceof type.errors).toBe(true);
  });

  test("an identity entry with a non-string public key is rejected", () => {
    const bad = {
      ...base,
      senderIdentities: [
        { address: "run_sender@integration.interchange", publicKey: 123 },
      ],
    };
    expect(RunGrantsFrame(bad) instanceof type.errors).toBe(true);
  });

  test("an identity entry missing its address is rejected", () => {
    const bad = { ...base, senderIdentities: [{ publicKey: "aa".repeat(32) }] };
    expect(RunGrantsFrame(bad) instanceof type.errors).toBe(true);
  });
});

describe("frame array-length ceilings", () => {
  const addresses = (n: number) =>
    Array.from({ length: n }, (_, i) => `addr-${String(i)}@example.test`);

  describe("RegisterFrame agentAddresses", () => {
    const base = { type: "register", sidecarId: "sc-1", token: "tok" };

    test("accepts a frame at the ceiling", () => {
      const frame = {
        ...base,
        agentAddresses: addresses(MAX_AGENT_ADDRESSES_FRAME),
      };
      expect(RegisterFrame(frame) instanceof type.errors).toBe(false);
      expect(SidecarFrame(frame) instanceof type.errors).toBe(false);
    });

    test("rejects a frame past the ceiling through the union", () => {
      const frame = {
        ...base,
        agentAddresses: addresses(MAX_AGENT_ADDRESSES_FRAME + 1),
      };
      expect(RegisterFrame(frame) instanceof type.errors).toBe(true);
      expect(SidecarFrame(frame) instanceof type.errors).toBe(true);
    });
  });

  describe("RegisterFrame cachedSenderAddresses", () => {
    const base = {
      type: "register",
      sidecarId: "sc-1",
      token: "tok",
      agentAddresses: ["wf@example.test"],
    };

    test("accepts a count above the resync handler cap but within the ceiling", () => {
      // The ceiling sits far above the hub-sessions `MAX_RESYNC_SENDER_ADDRESSES`
      // handler cap (2048) so a report over that cap still parses and reaches the
      // handler's graceful "resync the first N, log the overflow" degrade rather
      // than dropping the whole register frame and stalling the reconnect.
      const frame = { ...base, cachedSenderAddresses: addresses(2049) };
      expect(RegisterFrame(frame) instanceof type.errors).toBe(false);
      expect(SidecarFrame(frame) instanceof type.errors).toBe(false);
    });

    test("rejects a report past the ceiling through the union", () => {
      const frame = {
        ...base,
        cachedSenderAddresses: addresses(MAX_CACHED_SENDER_ADDRESSES_FRAME + 1),
      };
      expect(RegisterFrame(frame) instanceof type.errors).toBe(true);
      expect(SidecarFrame(frame) instanceof type.errors).toBe(true);
    });
  });

  describe("MailOutboundFrame recipients", () => {
    const base = {
      type: "mail.outbound",
      senderAddress: "sender@example.test",
      rawMessage: "bWFpbA==",
    };

    test("accepts a frame at the ceiling", () => {
      const frame = {
        ...base,
        recipients: addresses(MAX_MAIL_ADDRESSES_FRAME),
      };
      expect(MailOutboundFrame(frame) instanceof type.errors).toBe(false);
    });

    test("rejects recipients past the ceiling through the union", () => {
      const frame = {
        ...base,
        recipients: addresses(MAX_MAIL_ADDRESSES_FRAME + 1),
      };
      expect(MailOutboundFrame(frame) instanceof type.errors).toBe(true);
      expect(SidecarFrame(frame) instanceof type.errors).toBe(true);
    });

    test("rejects a cc list past the ceiling", () => {
      const frame = {
        ...base,
        recipients: ["recipient@example.test"],
        cc: addresses(MAX_MAIL_ADDRESSES_FRAME + 1),
      };
      expect(MailOutboundFrame(frame) instanceof type.errors).toBe(true);
    });
  });

  describe("WorkflowProbeResultFrame grants", () => {
    const projection = {
      id: "wf-probe",
      triggers: [],
      stepOrder: ["s1"],
      steps: { s1: { kind: "step", id: "s1" } },
    };
    const grantWalkSnapshot = {
      perStep: [{ stepId: "s1", grants: [], grantEffects: {} }],
      grantRequirements: [],
    };
    const base = {
      type: "workflow.probe.result",
      requestId: "req_1",
      projection,
      grantWalkSnapshot,
      wireHash: "abc123",
    };

    test("accepts a frame at the ceiling", () => {
      const frame = {
        ...base,
        grants: Array.from(
          { length: MAX_PROBE_GRANTS_FRAME },
          (_, i) => `grant-${String(i)}`,
        ),
      };
      expect(WorkflowProbeResultFrame(frame) instanceof type.errors).toBe(
        false,
      );
    });

    test("rejects grants past the ceiling through the union", () => {
      const frame = {
        ...base,
        grants: Array.from(
          { length: MAX_PROBE_GRANTS_FRAME + 1 },
          (_, i) => `grant-${String(i)}`,
        ),
      };
      expect(WorkflowProbeResultFrame(frame) instanceof type.errors).toBe(true);
      expect(SidecarFrame(frame) instanceof type.errors).toBe(true);
    });
  });

  // The bounded optional fields moved from the `"string[]"` DSL to a chained
  // `type("string").array().atMostLength(n)` value under a `"key?"` key. The
  // regression that mechanical change risks is losing optionality (the key
  // becomes required) or gaining a lower bound (an empty array is rejected).
  describe("bounded optional fields stay optional", () => {
    test("RegisterFrame validates with cachedSenderAddresses omitted or empty", () => {
      const base = {
        type: "register",
        sidecarId: "sc-1",
        token: "tok",
        agentAddresses: ["wf@example.test"],
      };
      expect(RegisterFrame(base) instanceof type.errors).toBe(false);
      expect(
        RegisterFrame({ ...base, cachedSenderAddresses: [] }) instanceof
          type.errors,
      ).toBe(false);
    });

    test("ReconnectFrame validates with cachedSenderAddresses omitted or empty", () => {
      const base = {
        type: "reconnect",
        sidecarId: "sc-1",
        token: "tok",
        agentAddresses: ["wf@example.test"],
      };
      expect(ReconnectFrame(base) instanceof type.errors).toBe(false);
      expect(
        ReconnectFrame({ ...base, cachedSenderAddresses: [] }) instanceof
          type.errors,
      ).toBe(false);
    });

    test("MailOutboundFrame validates with empty to and cc lists", () => {
      const frame = {
        type: "mail.outbound",
        senderAddress: "sender@example.test",
        rawMessage: "bWFpbA==",
        recipients: ["recipient@example.test"],
        to: [],
        cc: [],
      };
      expect(MailOutboundFrame(frame) instanceof type.errors).toBe(false);
    });

    test("CredentialsUpdateFrame validates with an empty revoke list", () => {
      const frame = {
        type: "credentials.update",
        requestId: "req_1",
        agentAddress: "dep@example.test",
        delivery: { bindings: [], materials: [] },
        revoke: [],
      };
      expect(CredentialsUpdateFrame(frame) instanceof type.errors).toBe(false);
    });
  });

  describe("CredentialsUpdateFrame revoke", () => {
    const base = {
      type: "credentials.update",
      requestId: "req_1",
      agentAddress: "dep@example.test",
      delivery: { bindings: [], materials: [] },
    };

    test("accepts a revoke list at the ceiling", () => {
      const frame = {
        ...base,
        revoke: Array.from(
          { length: MAX_CREDENTIAL_REVOCATIONS_FRAME },
          (_, i) => `cred-${String(i)}`,
        ),
      };
      expect(CredentialsUpdateFrame(frame) instanceof type.errors).toBe(false);
    });

    test("rejects a revoke list past the ceiling through the union", () => {
      const frame = {
        ...base,
        revoke: Array.from(
          { length: MAX_CREDENTIAL_REVOCATIONS_FRAME + 1 },
          (_, i) => `cred-${String(i)}`,
        ),
      };
      expect(CredentialsUpdateFrame(frame) instanceof type.errors).toBe(true);
      expect(HubFrame(frame) instanceof type.errors).toBe(true);
    });
  });
});

describe("frame payload byte limits", () => {
  test("the sidecar frame ceiling stays above the mail body cap", () => {
    // maxPayloadLength must clear the largest legit received frame -- a
    // mail.outbound whose rawMessage sits at the body cap, plus framing
    // overhead -- or Bun would close the sidecar's control socket on a
    // legitimate max-size mail. This pins that ordering, which the whole
    // payload-limit design depends on.
    expect(MAX_SIDECAR_FRAME_BYTES).toBeGreaterThan(
      MAX_MAIL_OUTBOUND_BODY_BYTES,
    );
  });
});
