import { describe, test, expect } from "bun:test";

import { deriveWorkflowRunId } from "@intx/types";

import { deriveRunAddress, deriveWorkflowRunRepoId } from "./addresses";

describe("run address derivation", () => {
  test("deriveRunAddress joins the run id and the domain", () => {
    expect(
      deriveRunAddress({
        runId: "run_abc",
        domain: "workflow.interchange",
      }),
    ).toBe("run_abc@workflow.interchange");
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

  test("throws on an address whose local part is not a run id", () => {
    expect(() => deriveWorkflowRunId("usr_abc@workflow.interchange")).toThrow(
      "Invalid run address",
    );
  });
});
