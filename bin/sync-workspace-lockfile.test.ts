import { describe, expect, test } from "bun:test";

import {
  rewriteWorkspaceVersions,
  workspaceVersions,
} from "./sync-workspace-lockfile";

// A miniature bun.lock (JSONC, trailing commas) exercising the shapes that
// matter: a root entry with no version, a normal package, an app whose `bin`
// field precedes `version`, a tests/lib member, catalog ranges, and a
// third-party package in the array syntax that must never be touched.
const LOCKFILE = `{
  "lockfileVersion": 1,
  "workspaces": {
    "": {
      "name": "interchange",
      "devDependencies": {
        "prettier": "^3.0.0",
      },
    },
    "packages/a": {
      "name": "@intx/a",
      "version": "0.1.2",
      "dependencies": {
        "@intx/b": "workspace:*",
      },
    },
    "apps/b": {
      "name": "@intx/b-app",
      "bin": {
        "b": "./bin/b",
      },
      "version": "0.1.2",
      "dependencies": {
        "@intx/a": "workspace:*",
      },
    },
    "tests/lib": {
      "name": "@intx/test-harness",
      "version": "0.1.2",
    },
  },
  "catalog": {
    "arktype": "^2.1.29",
  },
  "packages": {
    "@babel/core": ["@babel/core@7.29.0", "", { "dependencies": {} }, "sha512-deadbeef=="],
  },
}
`;

describe("workspaceVersions", () => {
  test("reads the version of each workspace member that has one", () => {
    expect(workspaceVersions(LOCKFILE)).toEqual({
      "packages/a": "0.1.2",
      "apps/b": "0.1.2",
      "tests/lib": "0.1.2",
    });
  });
});

describe("rewriteWorkspaceVersions", () => {
  test("sets every workspace version to the release version", () => {
    const out = rewriteWorkspaceVersions(LOCKFILE, "0.2.0");
    expect(workspaceVersions(out)).toEqual({
      "packages/a": "0.2.0",
      "apps/b": "0.2.0",
      "tests/lib": "0.2.0",
    });
  });

  test("handles a block whose bin field precedes version", () => {
    const out = rewriteWorkspaceVersions(LOCKFILE, "0.2.0");
    // apps/b declares `bin` before `version`; it must still be bumped.
    expect(workspaceVersions(out)["apps/b"]).toBe("0.2.0");
  });

  test("leaves third-party packages and catalog ranges untouched", () => {
    const out = rewriteWorkspaceVersions(LOCKFILE, "0.2.0");
    expect(out).toContain('"@babel/core@7.29.0"');
    expect(out).toContain('"arktype": "^2.1.29"');
    // the only lines that change are the three workspace versions
    expect(out).not.toContain('"version": "0.1.2"');
  });

  test("does not invent a version for the root workspace", () => {
    const out = rewriteWorkspaceVersions(LOCKFILE, "0.2.0");
    expect(workspaceVersions(out)).not.toHaveProperty("");
  });

  test("does not touch a dependency literally named version", () => {
    const lock = `{
  "workspaces": {
    "packages/c": {
      "name": "@intx/c",
      "version": "0.1.2",
      "dependencies": {
        "version": "^1.0.0",
      },
    },
  },
  "packages": {},
}
`;
    const out = rewriteWorkspaceVersions(lock, "0.2.0");
    expect(workspaceVersions(out)).toEqual({ "packages/c": "0.2.0" });
    expect(out).toContain('"version": "^1.0.0"'); // the dependency is untouched
  });

  test("throws when a member version uses spacing the edit misses", () => {
    // The block edit matches `"version": "` (with a space); a member written
    // `"version":"x"` is left stale and the completeness re-parse must catch
    // it rather than silently shipping the previous version.
    const lock = `{
  "workspaces": {
    "packages/d": {
      "name": "@intx/d",
      "version":"0.1.2",
    },
  },
  "packages": {},
}
`;
    expect(() => rewriteWorkspaceVersions(lock, "0.2.0")).toThrow(/not set/);
  });

  test("round-trips a prerelease version", () => {
    const out = rewriteWorkspaceVersions(LOCKFILE, "0.2.0-rc.1");
    expect(workspaceVersions(out)["packages/a"]).toBe("0.2.0-rc.1");
  });

  test("preserves a string value that contains a trailing-comma sequence", () => {
    // The JSONC stripper must not corrupt a `,}` inside a string value.
    const lock = `{
  "workspaces": {
    "packages/e": {
      "name": "@intx/e",
      "version": "0.1.2",
      "description": "a,}b",
    },
  },
  "packages": {},
}
`;
    const out = rewriteWorkspaceVersions(lock, "0.2.0");
    expect(out).toContain('"description": "a,}b"');
    expect(workspaceVersions(out)).toEqual({ "packages/e": "0.2.0" });
  });

  test("throws when there is no workspaces section", () => {
    expect(() =>
      rewriteWorkspaceVersions('{ "packages": {} }', "0.2.0"),
    ).toThrow(/no workspaces section/);
  });
});

// Exercises the `escaped` branch of the shared stepStringScan: a string value
// with an escaped quote or backslash must not be mistaken for closing the
// string, so a structural-looking `,}` inside it is left intact.
describe("escaped characters inside string values", () => {
  test("escaped quote followed by comma-brace is not corrupted", () => {
    const lock = `{
  "workspaces": {
    "packages/e": {
      "name": "@intx/e",
      "description": "he said \\",}\\" loudly",
      "version": "0.1.2",
    },
  },
  "packages": {},
}
`;
    const out = rewriteWorkspaceVersions(lock, "0.2.0");
    expect(out).toContain('"description": "he said \\",}\\" loudly"');
    expect(workspaceVersions(out)).toEqual({ "packages/e": "0.2.0" });
  });

  test("escaped backslash at end of string still closes the string", () => {
    const lock = `{
  "workspaces": {
    "packages/f": {
      "name": "@intx/f",
      "path": "c:\\\\",
      "version": "0.1.2",
    },
  },
  "packages": {},
}
`;
    const out = rewriteWorkspaceVersions(lock, "0.2.0");
    expect(out).toContain('"path": "c:\\\\"');
    expect(workspaceVersions(out)).toEqual({ "packages/f": "0.2.0" });
  });

  test("comma-brace inside a string with escaped content is preserved", () => {
    const lock = `{
  "workspaces": {
    "packages/g": {
      "name": "@intx/g",
      "note": "a\\\\,}b",
      "version": "0.1.2",
    },
  },
  "packages": {},
}
`;
    const out = rewriteWorkspaceVersions(lock, "0.2.0");
    expect(out).toContain('"note": "a\\\\,}b"');
    expect(workspaceVersions(out)).toEqual({ "packages/g": "0.2.0" });
  });
});
