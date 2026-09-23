import { describe, expect, test } from "bun:test";

import { workflowSourceRepoKind } from "./workflow-source-repo-kind";

describe("workflowSourceRepoKind", () => {
  test("a source codebase is read from a workflow asset", () => {
    expect(
      workflowSourceRepoKind({
        kind: "asset",
        assetId: "ast_src",
        package: { format: "source", commitSha: "abc" },
      }),
    ).toBe("workflow");
  });

  test("a packed tarball is read from a package-registry asset", () => {
    expect(
      workflowSourceRepoKind({
        kind: "asset",
        assetId: "ast_tgz",
        package: { format: "tarball" },
      }),
    ).toBe("package-registry");
  });
});
