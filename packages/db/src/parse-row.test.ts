import { describe, expect, test } from "bun:test";

import { RepoAction } from "@intx/types/sidecar";

import {
  GitTokenKindValidator,
  parseGitTokenRow,
  parseModelOfferingRow,
  parseModelProviderRow,
  parsePrincipalRow,
  parseWorkflowDefinitionRow,
  parseWorkflowDefinitionVersionRow,
  parseWorkflowRunRow,
} from "./parse-row";
import type {
  gitToken,
  modelOffering,
  modelProvider,
  principal,
  workflowDefinition,
  workflowDefinitionVersion,
  workflowRun,
} from "./schema";

type GitTokenRow = typeof gitToken.$inferSelect;

function makeRow(overrides: Partial<GitTokenRow> = {}): GitTokenRow {
  const now = new Date();
  return {
    id: "gtk_0123456789abcdef0123456789abcdef",
    tenantId: null,
    userId: "user_alice",
    principalId: null,
    name: "laptop",
    kind: "pat",
    tokenHashSha256: new Uint8Array(32),
    resource: "agent-state:ins_test",
    refPattern: "refs/heads/*",
    actions: ["receivePack", "createPack", "resolveRef"],
    expiresAt: new Date("2027-01-01T00:00:00Z"),
    revokedAt: null,
    createdAt: now,
    ...overrides,
  };
}

describe("parseGitTokenRow", () => {
  test("round-trips a personal pat with concrete repo scope", () => {
    const row = makeRow();
    const parsed = parseGitTokenRow(row);

    expect(parsed.id).toBe(row.id);
    expect(parsed.tenantId).toBeNull();
    expect(parsed.userId).toBe(row.userId);
    expect(parsed.principalId).toBeNull();
    expect(parsed.name).toBe(row.name);
    expect(parsed.kind).toBe("pat");
    expect(parsed.tokenHashSha256).toBe(row.tokenHashSha256);
    expect(parsed.actions).toEqual(["receivePack", "createPack", "resolveRef"]);
    expect(parsed.resource).toBe("agent-state:ins_test");
    expect(parsed.refPattern).toBe("refs/heads/*");
    expect(parsed.expiresAt).toBe(row.expiresAt);
    expect(parsed.revokedAt).toBeNull();
    expect(parsed.createdAt).toBe(row.createdAt);
  });

  test("round-trips a tenant-restricted pat", () => {
    const row = makeRow({
      tenantId: "tnt_acme",
      name: "acme-only",
    });
    const parsed = parseGitTokenRow(row);

    expect(parsed.kind).toBe("pat");
    expect(parsed.tenantId).toBe("tnt_acme");
    expect(parsed.principalId).toBeNull();
  });

  test("round-trips a tenant-bound svc token", () => {
    const row = makeRow({
      kind: "svc",
      tenantId: "tnt_acme",
      principalId: "prn_tenant_user",
      name: "ci-runner",
      actions: ["createPack", "resolveRef"],
      resource: "asset:def_skill_xyz",
      refPattern: "refs/tags/v*",
    });
    const parsed = parseGitTokenRow(row);

    expect(parsed.kind).toBe("svc");
    expect(parsed.tenantId).toBe("tnt_acme");
    expect(parsed.principalId).toBe("prn_tenant_user");
    expect(parsed.actions).toEqual(["createPack", "resolveRef"]);
    expect(parsed.resource).toBe("asset:def_skill_xyz");
    expect(parsed.refPattern).toBe("refs/tags/v*");
  });

  test("preserves revokedAt for soft-revoked rows", () => {
    const revoked = new Date("2026-01-15T00:00:00Z");
    const row = makeRow({ revokedAt: revoked });
    const parsed = parseGitTokenRow(row);

    expect(parsed.revokedAt).toBe(revoked);
  });

  test("preserves expiresAt", () => {
    const expires = new Date("2027-01-01T00:00:00Z");
    const row = makeRow({ expiresAt: expires });
    const parsed = parseGitTokenRow(row);

    expect(parsed.expiresAt).toBe(expires);
  });

  test("rejects an unknown kind", () => {
    expect(() => GitTokenKindValidator.assert("rogue")).toThrow();
  });

  test("rejects an unknown action in the actions array", () => {
    expect(() =>
      RepoAction.array().assert(["receivePack", "fly-the-helicopter"]),
    ).toThrow();
  });

  test("accepts an empty actions array", () => {
    const row = makeRow({ actions: [] });
    const parsed = parseGitTokenRow(row);
    expect(parsed.actions).toEqual([]);
  });
});

type PrincipalRow = typeof principal.$inferSelect;

function makePrincipalRow(overrides: Partial<PrincipalRow> = {}): PrincipalRow {
  const now = new Date();
  return {
    id: "prn_0123456789abcdef",
    tenantId: "tnt_acme",
    kind: "user",
    refId: "usr_alice",
    status: "active",
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

describe("parsePrincipalRow", () => {
  test("accepts a user principal", () => {
    const parsed = parsePrincipalRow(makePrincipalRow());
    expect(parsed.kind).toBe("user");
    expect(parsed.status).toBe("active");
  });

  test("accepts a workflow principal", () => {
    const parsed = parsePrincipalRow(makePrincipalRow({ kind: "workflow" }));
    expect(parsed.kind).toBe("workflow");
  });
});

type ModelProviderRow = typeof modelProvider.$inferSelect;

function makeProviderRow(
  overrides: Partial<ModelProviderRow> = {},
): ModelProviderRow {
  const now = new Date();
  return {
    id: "mpv_0123456789abcdef",
    tenantId: "ten_root",
    name: "Anthropic direct",
    plugin: "anthropic",
    baseURL: "https://api.anthropic.com",
    credentialId: "cred_anthropic",
    walletId: null,
    disabled: false,
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

describe("parseModelProviderRow", () => {
  test("accepts a known plugin", () => {
    const parsed = parseModelProviderRow(makeProviderRow());
    expect(parsed.plugin).toBe("anthropic");
  });
});

type ModelOfferingRow = typeof modelOffering.$inferSelect;

function makeOfferingRow(
  overrides: Partial<ModelOfferingRow> = {},
): ModelOfferingRow {
  const now = new Date();
  return {
    id: "mof_0123456789abcdef",
    tenantId: "ten_root",
    modelId: "mdl_opus",
    providerId: "mpv_anthropic",
    priority: 0,
    deploymentTags: [],
    capabilities: ["vision-input", "function-calling-multi-turn"],
    quirks: null,
    disabled: false,
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

describe("parseModelOfferingRow", () => {
  test("accepts curated capabilities", () => {
    const parsed = parseModelOfferingRow(makeOfferingRow());
    expect(parsed.capabilities).toEqual([
      "vision-input",
      "function-calling-multi-turn",
    ]);
  });

  test("accepts an empty capabilities array", () => {
    const parsed = parseModelOfferingRow(makeOfferingRow({ capabilities: [] }));
    expect(parsed.capabilities).toEqual([]);
  });

  test("rejects a non-curated capability", () => {
    expect(() =>
      parseModelOfferingRow(makeOfferingRow({ capabilities: ["telepathy"] })),
    ).toThrow();
  });

  test("passes a null quirks bag through unchanged", () => {
    const parsed = parseModelOfferingRow(makeOfferingRow({ quirks: null }));
    expect(parsed.quirks).toBeNull();
  });

  test("preserves an empty quirks object", () => {
    const parsed = parseModelOfferingRow(makeOfferingRow({ quirks: {} }));
    expect(parsed.quirks).toEqual({});
  });

  test("preserves a populated quirks bag", () => {
    const quirks = { forceAssistantReasoningContent: true };
    const parsed = parseModelOfferingRow(makeOfferingRow({ quirks }));
    expect(parsed.quirks).toEqual(quirks);
  });

  test("rejects a scalar quirks value", () => {
    expect(() =>
      parseModelOfferingRow(makeOfferingRow({ quirks: "not-an-object" })),
    ).toThrow();
  });
});

type WorkflowDefinitionVersionRow =
  typeof workflowDefinitionVersion.$inferSelect;

function makeWorkflowDefinitionVersionRow(
  overrides: Partial<WorkflowDefinitionVersionRow> = {},
): WorkflowDefinitionVersionRow {
  return {
    id: "wdv_0123456789abcdef",
    definitionId: "wfd_0123456789abcdef",
    version: "1",
    status: "active",
    approvedWireHash: null,
    createdAt: new Date(),
    ...overrides,
  };
}

describe("parseWorkflowDefinitionVersionRow", () => {
  test("passes a null approvedWireHash through as null", () => {
    const parsed = parseWorkflowDefinitionVersionRow(
      makeWorkflowDefinitionVersionRow(),
    );
    expect(parsed.approvedWireHash).toBeNull();
  });

  test("passes a present approvedWireHash through as a string", () => {
    const approvedWireHash = "sha256:abc123";
    const parsed = parseWorkflowDefinitionVersionRow(
      makeWorkflowDefinitionVersionRow({ approvedWireHash }),
    );
    expect(parsed.approvedWireHash).toBe(approvedWireHash);
  });
});

type WorkflowRunRow = typeof workflowRun.$inferSelect;

function makeWorkflowRunRow(
  overrides: Partial<WorkflowRunRow> = {},
): WorkflowRunRow {
  const now = new Date();
  return {
    id: "run_0123456789abcdef",
    definitionId: "wfd_0123456789abcdef",
    deploymentId: "dep_0123456789abcdef",
    tenantId: "tnt_acme",
    principalId: null,
    status: "running",
    address: null,
    publicKey: null,
    sidecarId: null,
    kernelId: null,
    modelPreferences: null,
    createdAt: now,
    endedAt: null,
    ...overrides,
  };
}

describe("parseWorkflowRunRow", () => {
  test("passes a null modelPreferences through as null", () => {
    const parsed = parseWorkflowRunRow(makeWorkflowRunRow());
    expect(parsed.modelPreferences).toBeNull();
  });

  test("validates a well-formed modelPreferences", () => {
    const modelPreferences = [
      {
        model: "opus",
        providers: { mode: "prefer" as const, order: ["anthropic"] },
      },
    ];
    const parsed = parseWorkflowRunRow(
      makeWorkflowRunRow({ modelPreferences }),
    );
    expect(parsed.modelPreferences).toEqual(modelPreferences);
  });

  test("rejects a modelPreferences entry missing its providers", () => {
    expect(() =>
      parseWorkflowRunRow(
        makeWorkflowRunRow({ modelPreferences: [{ model: "opus" }] }),
      ),
    ).toThrow();
  });
});

describe("parseWorkflowDefinitionRow", () => {
  type DefRow = typeof workflowDefinition.$inferSelect;

  function makeDefRow(overrides: Partial<DefRow> = {}): DefRow {
    const now = new Date();
    return {
      id: "wfd_test",
      tenantId: "tnt_test",
      creatorPrincipalId: null,
      assetId: null,
      name: "Test Definition",
      description: null,
      wireHash: null,
      grantRequirements: null,
      modelRequirements: null,
      credentialBindings: null,
      currentVersion: "1",
      status: "deployed",
      createdAt: now,
      updatedAt: now,
      ...overrides,
    };
  }

  test("passes null grant/model/credential manifests through as null", () => {
    const parsed = parseWorkflowDefinitionRow(makeDefRow());
    expect(parsed.grantRequirements).toBeNull();
    expect(parsed.modelRequirements).toBeNull();
    expect(parsed.credentialBindings).toBeNull();
  });

  test("validates a well-formed grantRequirements", () => {
    const grantRequirements = [
      { resource: "tool:bash", action: "invoke", source: "creator" as const },
    ];
    const parsed = parseWorkflowDefinitionRow(
      makeDefRow({ grantRequirements }),
    );
    expect(parsed.grantRequirements).toEqual(grantRequirements);
  });

  test("validates a well-formed modelRequirements", () => {
    const modelRequirements = [{ model: "opus" }];
    const parsed = parseWorkflowDefinitionRow(
      makeDefRow({ modelRequirements }),
    );
    expect(parsed.modelRequirements).toEqual(modelRequirements);
  });

  test("validates a well-formed credentialBindings", () => {
    const credentialBindings = [
      {
        package: "@acme/tools",
        handle: "gh",
        provider: "github",
        locator: "tenant" as const,
      },
    ];
    const parsed = parseWorkflowDefinitionRow(
      makeDefRow({ credentialBindings }),
    );
    expect(parsed.credentialBindings).toEqual(credentialBindings);
  });

  // The credentialBindings branch tolerates `undefined` (a partial row-shaped
  // stub predating the column) and treats it like `null`. Pin that so a
  // "simplification" collapsing the branch cannot silently start asserting
  // `undefined` as an array.
  test("treats an undefined credentialBindings as null", () => {
    const parsed = parseWorkflowDefinitionRow(
      makeDefRow({ credentialBindings: undefined }),
    );
    expect(parsed.credentialBindings).toBeNull();
  });

  test("rejects a credentialBindings entry with an unknown locator", () => {
    expect(() =>
      parseWorkflowDefinitionRow(
        makeDefRow({
          credentialBindings: [
            {
              package: "@acme/tools",
              handle: "gh",
              provider: "github",
              locator: "stranger",
            },
          ],
        }),
      ),
    ).toThrow();
  });
});
