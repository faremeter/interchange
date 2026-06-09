import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  DeployApplyErrorCategory,
  DeployApplyErrorFrame,
  SidecarFrame,
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

describe("DeployApplyErrorFrame", () => {
  const validFrame = {
    type: "deploy.apply.error" as const,
    agentAddress: "agt_1",
    attemptId: "atp_1",
    previousDeployId: "dpl_0",
    category: "integrity.mismatch" as const,
    message: "fetched bytes for left-pad@1.3.0 did not match pinned integrity",
    occurredAt: "2026-06-05T20:00:00.000Z",
  };

  test("accepts a minimal valid frame", () => {
    const result = DeployApplyErrorFrame(validFrame);
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a frame with package context", () => {
    const result = DeployApplyErrorFrame({
      ...validFrame,
      package: { name: "left-pad", version: "1.3.0" },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a wrong type discriminator", () => {
    const result = DeployApplyErrorFrame({
      ...validFrame,
      type: "deploy.apply.ok",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an unknown category on the frame", () => {
    const result = DeployApplyErrorFrame({
      ...validFrame,
      category: "network.timeout",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a frame missing previousDeployId", () => {
    const { previousDeployId: _omitted, ...partial } = validFrame;
    const result = DeployApplyErrorFrame(partial);
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a package field with a missing version", () => {
    const result = DeployApplyErrorFrame({
      ...validFrame,
      package: { name: "left-pad" },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("SidecarFrame union", () => {
  test("accepts a DeployApplyErrorFrame as a member", () => {
    const result = SidecarFrame({
      type: "deploy.apply.error",
      agentAddress: "agt_1",
      attemptId: "atp_1",
      previousDeployId: "dpl_0",
      category: "manifest.invalid",
      message: "schemaVersion was '2', expected '1'",
      occurredAt: "2026-06-05T20:00:00.000Z",
    });
    expect(result instanceof type.errors).toBe(false);
  });
});
