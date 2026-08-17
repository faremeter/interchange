import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  WorkflowDefinitionRegistrySource,
  WorkflowDefinitionAssetSource,
  WorkflowDefinitionSource,
} from "./workflow-sources";

describe("WorkflowDefinitionRegistrySource", () => {
  test("accepts a registry source", () => {
    const result = WorkflowDefinitionRegistrySource({
      kind: "registry",
      registry: "npmjs",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a missing kind", () => {
    const result = WorkflowDefinitionRegistrySource({ registry: "npmjs" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a missing registry", () => {
    const result = WorkflowDefinitionRegistrySource({ kind: "registry" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a non-string registry", () => {
    const result = WorkflowDefinitionRegistrySource({
      kind: "registry",
      registry: 42,
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("WorkflowDefinitionAssetSource", () => {
  test("accepts a tarball asset source", () => {
    const result = WorkflowDefinitionAssetSource({
      kind: "asset",
      assetId: "asset_abc",
      package: { format: "tarball" },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a source asset with a member selector", () => {
    const result = WorkflowDefinitionAssetSource({
      kind: "asset",
      assetId: "asset_abc",
      package: {
        format: "source",
        commitSha: "0123456789abcdef0123456789abcdef01234567",
        packageName: "@scope/workflow-a",
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a source asset without a member selector", () => {
    const result = WorkflowDefinitionAssetSource({
      kind: "asset",
      assetId: "asset_abc",
      package: {
        format: "source",
        commitSha: "0123456789abcdef0123456789abcdef01234567",
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a missing assetId", () => {
    const result = WorkflowDefinitionAssetSource({
      kind: "asset",
      package: { format: "tarball" },
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a missing package", () => {
    const result = WorkflowDefinitionAssetSource({
      kind: "asset",
      assetId: "asset_abc",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a source package missing commitSha", () => {
    const result = WorkflowDefinitionAssetSource({
      kind: "asset",
      assetId: "asset_abc",
      package: { format: "source" },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("WorkflowDefinitionSource", () => {
  test("accepts a registry source", () => {
    const result = WorkflowDefinitionSource({
      kind: "registry",
      registry: "npmjs",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a tarball asset source", () => {
    const result = WorkflowDefinitionSource({
      kind: "asset",
      assetId: "asset_abc",
      package: { format: "tarball" },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a source asset source", () => {
    const result = WorkflowDefinitionSource({
      kind: "asset",
      assetId: "asset_abc",
      package: {
        format: "source",
        commitSha: "0123456789abcdef0123456789abcdef01234567",
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an unknown kind", () => {
    const result = WorkflowDefinitionSource({
      kind: "svn",
      url: "svn://example.com/trunk",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a missing kind", () => {
    const result = WorkflowDefinitionSource({ registry: "npmjs" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a non-string registry", () => {
    const result = WorkflowDefinitionSource({
      kind: "registry",
      registry: 42,
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a registry kind carrying only another arm's fields", () => {
    const result = WorkflowDefinitionSource({
      kind: "registry",
      assetId: "asset_abc",
    });
    expect(result instanceof type.errors).toBe(true);
  });
});
