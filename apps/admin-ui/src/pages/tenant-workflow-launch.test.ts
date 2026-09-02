import { describe, test, expect } from "bun:test";

import {
  buildDeployInput,
  definitionReady,
  isSourceKind,
  launchReady,
  SOURCE_KIND_LABELS,
  SOURCE_KINDS,
  type LaunchDefinition,
  type LaunchInferenceSource,
} from "./tenant-workflow-launch";

// A definition whose every field is populated. Each kind test starts here so
// that a kind that ignores a field proves it drops the stale value rather than
// leaking it into the payload.
function fullDefinition(
  overrides: Partial<LaunchDefinition> = {},
): LaunchDefinition {
  return {
    kind: "asset-source",
    entry: "./workflow.mjs",
    registry: "acme-registry",
    assetId: "asset_1",
    commitSha: "abc123",
    packageName: "@acme/flow",
    pin: "@acme/flow@^1.0.0",
    ...overrides,
  };
}

function validSource(
  overrides: Partial<LaunchInferenceSource> = {},
): LaunchInferenceSource {
  return {
    id: "anthropic:claude-sonnet-5",
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    credentialId: "sk-secret",
    model: "claude-sonnet-5",
    ...overrides,
  };
}

describe("buildDeployInput", () => {
  test("builds a registry source with a pin and no asset fields", () => {
    const input = buildDeployInput(
      fullDefinition({ kind: "registry" }),
      validSource(),
    );
    expect(input).toEqual({
      source: { kind: "registry", registry: "acme-registry" },
      entry: "./workflow.mjs",
      sources: [
        {
          id: "anthropic:claude-sonnet-5",
          provider: "anthropic",
          baseURL: "https://api.anthropic.com",
          credentialId: "sk-secret",
          model: "claude-sonnet-5",
        },
      ],
      defaultSource: "anthropic:claude-sonnet-5",
      pin: "@acme/flow@^1.0.0",
    });
  });

  test("builds an asset tarball source with a pin", () => {
    const input = buildDeployInput(
      fullDefinition({ kind: "asset-tarball" }),
      validSource(),
    );
    expect(input.source).toEqual({
      kind: "asset",
      assetId: "asset_1",
      package: { format: "tarball" },
    });
    expect(input.pin).toBe("@acme/flow@^1.0.0");
  });

  test("builds an asset source tree with a package name and no pin", () => {
    const input = buildDeployInput(
      fullDefinition({ kind: "asset-source" }),
      validSource(),
    );
    expect(input.source).toEqual({
      kind: "asset",
      assetId: "asset_1",
      package: {
        format: "source",
        commitSha: "abc123",
        packageName: "@acme/flow",
      },
    });
    // Asset-source selects its member by packageName; it must never carry a pin.
    expect("pin" in input).toBe(false);
  });

  test("omits packageName from an asset source tree when it is blank", () => {
    const input = buildDeployInput(
      fullDefinition({ kind: "asset-source", packageName: "  " }),
      validSource(),
    );
    expect(input.source).toEqual({
      kind: "asset",
      assetId: "asset_1",
      package: { format: "source", commitSha: "abc123" },
    });
    if (input.source.kind === "asset") {
      expect("packageName" in input.source.package).toBe(false);
    }
  });

  test("trims definition and source fields including the credential id", () => {
    const input = buildDeployInput(
      fullDefinition({
        kind: "asset-source",
        entry: "  ./workflow.mjs  ",
        assetId: "  asset_1  ",
        commitSha: "  abc123  ",
        packageName: "  @acme/flow  ",
      }),
      validSource({ id: "  src-1  ", credentialId: "  cred-1  " }),
    );
    expect(input.entry).toBe("./workflow.mjs");
    expect(input.defaultSource).toBe("src-1");
    expect(input.sources[0]?.id).toBe("src-1");
    expect(input.sources[0]?.credentialId).toBe("cred-1");
    expect(input.source).toEqual({
      kind: "asset",
      assetId: "asset_1",
      package: {
        format: "source",
        commitSha: "abc123",
        packageName: "@acme/flow",
      },
    });
  });

  test("drops fields the selected kind does not use", () => {
    // A registry deploy from a definition that also carries asset fields must
    // emit only the registry source, never the stale assetId or commitSha.
    const input = buildDeployInput(
      fullDefinition({ kind: "registry" }),
      validSource(),
    );
    const serialized = JSON.stringify(input);
    expect(serialized).not.toContain("asset_1");
    expect(serialized).not.toContain("abc123");
    expect(serialized).not.toContain("tarball");
  });
});

describe("definitionReady", () => {
  test("registry requires a registry name and a pin", () => {
    expect(definitionReady(fullDefinition({ kind: "registry" }))).toBe(true);
    expect(
      definitionReady(fullDefinition({ kind: "registry", registry: "  " })),
    ).toBe(false);
    expect(definitionReady(fullDefinition({ kind: "registry", pin: "" }))).toBe(
      false,
    );
  });

  test("asset tarball requires an asset id and a pin", () => {
    expect(definitionReady(fullDefinition({ kind: "asset-tarball" }))).toBe(
      true,
    );
    expect(
      definitionReady(fullDefinition({ kind: "asset-tarball", assetId: "" })),
    ).toBe(false);
    expect(
      definitionReady(fullDefinition({ kind: "asset-tarball", pin: "  " })),
    ).toBe(false);
  });

  test("asset source requires an asset id and a commit but not a pin", () => {
    expect(definitionReady(fullDefinition({ kind: "asset-source" }))).toBe(
      true,
    );
    // A missing pin does not block an asset-source deploy.
    expect(
      definitionReady(fullDefinition({ kind: "asset-source", pin: "" })),
    ).toBe(true);
    expect(
      definitionReady(fullDefinition({ kind: "asset-source", assetId: "" })),
    ).toBe(false);
    expect(
      definitionReady(
        fullDefinition({ kind: "asset-source", commitSha: "  " }),
      ),
    ).toBe(false);
  });
});

describe("launchReady", () => {
  test("is true when the kind's fields, entry, and source are all present", () => {
    for (const kind of SOURCE_KINDS) {
      expect(launchReady(fullDefinition({ kind }), validSource())).toBe(true);
    }
  });

  test("is false when the entry module is blank for any kind", () => {
    for (const kind of SOURCE_KINDS) {
      expect(
        launchReady(fullDefinition({ kind, entry: "  " }), validSource()),
      ).toBe(false);
    }
  });

  test("is false when a required definition field for the kind is missing", () => {
    expect(
      launchReady(fullDefinition({ kind: "registry", pin: "" }), validSource()),
    ).toBe(false);
    expect(
      launchReady(
        fullDefinition({ kind: "asset-tarball", assetId: "" }),
        validSource(),
      ),
    ).toBe(false);
    expect(
      launchReady(
        fullDefinition({ kind: "asset-source", commitSha: "" }),
        validSource(),
      ),
    ).toBe(false);
  });

  test("is false when any inference source field is missing", () => {
    const def = fullDefinition({ kind: "asset-source" });
    expect(launchReady(def, validSource({ id: "" }))).toBe(false);
    expect(launchReady(def, validSource({ provider: "  " }))).toBe(false);
    expect(launchReady(def, validSource({ baseURL: "" }))).toBe(false);
    expect(launchReady(def, validSource({ credentialId: "" }))).toBe(false);
    expect(launchReady(def, validSource({ model: "  " }))).toBe(false);
  });

  test("does not require a pin or package name for asset source", () => {
    expect(
      launchReady(
        fullDefinition({ kind: "asset-source", pin: "", packageName: "" }),
        validSource(),
      ),
    ).toBe(true);
  });
});

describe("isSourceKind", () => {
  test("accepts the known kinds and rejects anything else", () => {
    for (const kind of SOURCE_KINDS) {
      expect(isSourceKind(kind)).toBe(true);
    }
    expect(isSourceKind("asset")).toBe(false);
    expect(isSourceKind("source")).toBe(false);
    expect(isSourceKind("")).toBe(false);
  });
});

describe("source kind metadata", () => {
  test("defaults to asset-source and labels every kind", () => {
    expect(SOURCE_KINDS[0]).toBe("asset-source");
    for (const kind of SOURCE_KINDS) {
      expect(SOURCE_KIND_LABELS[kind].length).toBeGreaterThan(0);
    }
  });
});
