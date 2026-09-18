import { describe, test, expect } from "bun:test";

import { createStubRepoStore } from "./stub-repo-store";

const REPO_ID = { kind: "workflow-run", id: "deployment-x" } as const;

describe("createStubRepoStore", () => {
  test("resolves the repo dir it is built for", () => {
    const store = createStubRepoStore("/base");
    expect(store.getRepoDir(REPO_ID)).toBe("/base/workflow-run/deployment-x");
  });

  test("a method the opts did not enable throws rather than answering", () => {
    const store = createStubRepoStore("/base");
    // The throw is what these stubs are for: they assert, by failing, that
    // the code under them touches nothing beyond the named surface. A
    // working store in their place would silently permit paths that a test
    // deliberately leaves unimplemented.
    expect(() => store.initRepo(REPO_ID)).toThrow(/not implemented/);
  });

  test("the write surface is opted into rather than inherited", async () => {
    const narrow = createStubRepoStore("/base");
    expect(() =>
      narrow.writeTreePreservingPrefix(
        { kind: "supervisor" },
        REPO_ID,
        "refs/heads/main",
        { preservePrefix: "state/", message: "m", merge: async () => ({}) },
      ),
    ).toThrow(/not implemented/);

    const writable = createStubRepoStore("/base", { writeTree: true });
    const result = await writable.writeTreePreservingPrefix(
      { kind: "supervisor" },
      REPO_ID,
      "refs/heads/main",
      { preservePrefix: "state/", message: "m", merge: async () => ({}) },
    );
    expect(result.newlyTerminalRuns).toEqual([]);
  });
});
