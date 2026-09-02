import type { DeployWorkflowInput } from "@intx/hub-client";

// The `WorkflowDefinitionSource` variants the launch form can build. The
// `asset` union member splits into two picker kinds because a tarball asset
// and a source-tree asset need different operator input.
export const SOURCE_KINDS = [
  "asset-source",
  "asset-tarball",
  "registry",
] as const;
export type SourceKind = (typeof SOURCE_KINDS)[number];

export function isSourceKind(v: string): v is SourceKind {
  return (SOURCE_KINDS as readonly string[]).includes(v);
}

export const SOURCE_KIND_LABELS: Record<SourceKind, string> = {
  "asset-source": "Asset source tree",
  "asset-tarball": "Asset tarball",
  registry: "Registry",
};

/**
 * The launch form's definition-source fields. `kind` selects which
 * `WorkflowDefinitionSource` variant is built; fields not used by the selected
 * kind are ignored when the deploy input is built.
 */
export type LaunchDefinition = {
  kind: SourceKind;
  entry: string;
  registry: string;
  assetId: string;
  commitSha: string;
  packageName: string;
  pin: string;
};

/** The single inference source the launch form collects. */
export type LaunchInferenceSource = {
  id: string;
  provider: string;
  baseURL: string;
  // A registered credential the tenant owns. The launch form selects one rather
  // than accepting a typed key: every source references a credential by id, so
  // the deploy carries no inline secret (the hub resolves the material).
  credentialId: string;
  model: string;
};

/**
 * Builds the deploy request from the launch form's fields. Only the fields the
 * selected `kind` uses reach the payload: `pin` travels for the `registry` and
 * asset-`tarball` variants and is omitted for asset-`source`, whose optional
 * `packageName` is included only when non-empty.
 */
export function buildDeployInput(
  definition: LaunchDefinition,
  source: LaunchInferenceSource,
): DeployWorkflowInput {
  const base = {
    entry: definition.entry.trim(),
    sources: [
      {
        id: source.id.trim(),
        provider: source.provider.trim(),
        baseURL: source.baseURL.trim(),
        credentialId: source.credentialId.trim(),
        model: source.model.trim(),
      },
    ],
    defaultSource: source.id.trim(),
  };
  switch (definition.kind) {
    case "registry":
      return {
        ...base,
        source: { kind: "registry", registry: definition.registry.trim() },
        pin: definition.pin.trim(),
      };
    case "asset-tarball":
      return {
        ...base,
        source: {
          kind: "asset",
          assetId: definition.assetId.trim(),
          package: { format: "tarball" },
        },
        pin: definition.pin.trim(),
      };
    case "asset-source": {
      const packageName = definition.packageName.trim();
      return {
        ...base,
        source: {
          kind: "asset",
          assetId: definition.assetId.trim(),
          package: {
            format: "source",
            commitSha: definition.commitSha.trim(),
            ...(packageName === "" ? {} : { packageName }),
          },
        },
      };
    }
  }
}

/** Whether the selected kind's required definition fields are all present. */
export function definitionReady(definition: LaunchDefinition): boolean {
  switch (definition.kind) {
    case "registry":
      return definition.registry.trim() !== "" && definition.pin.trim() !== "";
    case "asset-tarball":
      return definition.assetId.trim() !== "" && definition.pin.trim() !== "";
    case "asset-source":
      return (
        definition.assetId.trim() !== "" && definition.commitSha.trim() !== ""
      );
  }
}

/**
 * Whether the launch form can be submitted: the entry module, the selected
 * kind's required definition fields, and the inference source are all present.
 */
export function launchReady(
  definition: LaunchDefinition,
  source: LaunchInferenceSource,
): boolean {
  return (
    definition.entry.trim() !== "" &&
    definitionReady(definition) &&
    source.id.trim() !== "" &&
    source.provider.trim() !== "" &&
    source.baseURL.trim() !== "" &&
    source.credentialId.trim() !== "" &&
    source.model.trim() !== ""
  );
}
