import { describe, test, expect } from "bun:test";

import { deriveWorkflowRunId } from "@intx/types";

import {
  deriveRunAddress,
  deriveRunAgentId,
  deriveStepAddress,
  deriveStepAgentId,
  deriveWorkflowRunRepoId,
} from "./orchestrator";

describe("per-step address derivation", () => {
  test("deriveStepAddress concatenates the run id, step, and deployment domain", () => {
    expect(
      deriveStepAddress({
        runId: "run_abc",
        stepId: "step1",
        domain: "workflow.interchange",
      }),
    ).toBe("run_abc-step1@workflow.interchange");
  });

  test("deriveStepAgentId concatenates the run id and step", () => {
    expect(deriveStepAgentId({ runId: "run_abc", stepId: "x" })).toBe(
      "run_abc-x",
    );
  });

  test("derivation is deterministic across calls", () => {
    const a = deriveStepAddress({
      runId: "run_a",
      stepId: "s",
      domain: "d",
    });
    const b = deriveStepAddress({
      runId: "run_a",
      stepId: "s",
      domain: "d",
    });
    expect(a).toBe(b);
  });

  test("deriveRunAddress drops the per-step suffix", () => {
    expect(
      deriveRunAddress({
        runId: "run_abc",
        domain: "workflow.interchange",
      }),
    ).toBe("run_abc@workflow.interchange");
  });

  test("deriveRunAgentId drops the per-step suffix", () => {
    expect(deriveRunAgentId({ runId: "run_abc" })).toBe("run_abc");
  });

  test("deriveWorkflowRunRepoId sanitizes the deployment address into a SAFE_REPO_ID slug", () => {
    const address = deriveRunAddress({
      runId: "run_abc",
      domain: "acme.localhost",
    });
    expect(address).toBe("run_abc@acme.localhost");
    const repoId = deriveWorkflowRunRepoId(address);
    expect(repoId).toBe("run_abc-acme-localhost");
    expect(repoId).toMatch(/^[a-zA-Z0-9_-]+$/);
  });

  test("deriveWorkflowRunRepoId substitutes every @ and . that SAFE_REPO_ID rejects", () => {
    expect(deriveWorkflowRunRepoId("run_x@a.b.c")).toBe("run_x-a-b-c");
    // Already-safe slugs are passed through unchanged so a repo id that
    // never crossed an address boundary is stable.
    expect(deriveWorkflowRunRepoId("run_safe-slug_1")).toBe("run_safe-slug_1");
  });
});

describe("deriveRunAddress / deriveWorkflowRunId round-trip", () => {
  const domain = "workflow.interchange";

  test("deriveWorkflowRunId recovers the runId from deriveRunAddress", () => {
    expect(
      deriveWorkflowRunId(deriveRunAddress({ runId: "run_abc", domain })),
    ).toBe("run_abc");
  });

  test("round-trips a per-step address to its step-suffixed run id", () => {
    const stepAddress = deriveStepAddress({
      runId: "run_abc",
      stepId: "plan",
      domain,
    });
    expect(deriveWorkflowRunId(stepAddress)).toBe("run_abc-plan");
  });

  test("throws on an address whose local part is not a run id", () => {
    expect(() => deriveWorkflowRunId("usr_abc@workflow.interchange")).toThrow(
      "Invalid run address",
    );
  });
});
