import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  WorkflowDefinitionRegistrySource,
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

describe("WorkflowDefinitionSource", () => {
  test("accepts a registry source", () => {
    const result = WorkflowDefinitionSource({
      kind: "registry",
      registry: "npmjs",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an unknown kind", () => {
    const result = WorkflowDefinitionSource({
      kind: "asset",
      assetId: "asset_abc",
      path: "definitions/foo.json",
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
});
