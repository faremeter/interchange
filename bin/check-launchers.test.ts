import { afterEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import {
  bunTsTarget,
  checkLaunchers,
  importsIntx,
  scanLauncher,
  scriptBunTsTarget,
  shadowingImports,
} from "./check-launchers";

// A dirname-form launcher resolves its target against its own directory, so a
// fixture only needs the wrapper plus the target .ts beside it.
const WRAPPER = (flag: boolean): string =>
  `#!/usr/bin/env bash\nset -euo pipefail\nexec bun ${
    flag ? "--conditions=intx-src " : ""
  }"$(dirname -- "\${BASH_SOURCE[0]}")/seed.ts" "$@"\n`;

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true });
});

function makeRepo(files: Record<string, string>): string {
  const root = mkdtempSync(join(tmpdir(), "check-launchers-"));
  roots.push(root);
  for (const [rel, content] of Object.entries(files)) {
    const path = join(root, rel);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, content);
  }
  return root;
}

test("importsIntx sees a direct @intx import", () => {
  expect(importsIntx(`import { x } from "@intx/agent";\n`)).toBe(true);
  expect(importsIntx(`import "@intx/log";\n`)).toBe(true);
});

test("importsIntx ignores @intx inside strings and comments", () => {
  const code = `// resolves @intx/agent at run time\nconst s = "@intx/agent";\nimport { readFileSync } from "node:fs";\n`;
  expect(importsIntx(code)).toBe(false);
});

test("bunTsTarget resolves the dirname form against the wrapper's directory", () => {
  const line = `exec bun --conditions=intx-src "$(dirname -- "\${BASH_SOURCE[0]}")/seed.ts" "$@"`;
  expect(bunTsTarget(line, "bin/seed")).toBe("bin/seed.ts");
});

test("bunTsTarget resolves the $REPODIR form against the repo root", () => {
  const line = `exec bun run --conditions=intx-src --watch "$REPODIR/apps/hub/src/index.ts"`;
  expect(bunTsTarget(line, "bin/hub")).toBe("apps/hub/src/index.ts");
});

test("bunTsTarget resolves a bare repo-relative target", () => {
  const line = `\tbun bin/sync-workspace-lockfile.ts "$NEW_VERSION" \\`;
  expect(bunTsTarget(line, "bin/release")).toBe(
    "bin/sync-workspace-lockfile.ts",
  );
});

test("bunTsTarget returns null for bun invocations without a .ts target", () => {
  expect(bunTsTarget("bun install", "bin/add-package")).toBeNull();
  expect(
    bunTsTarget(
      `\t(cd "$DBPKG" && bun drizzle-kit generate) \\`,
      "bin/db-migrate",
    ),
  ).toBeNull();
  expect(
    bunTsTarget(
      `\tbun pm pack --destination "$scratch" --quiet`,
      "bin/release",
    ),
  ).toBeNull();
});

test("bunTsTarget ignores bun that is not at a command position", () => {
  expect(bunTsTarget(`log::info "running bun install..."`, "bin/x")).toBeNull();
  expect(
    bunTsTarget(`\tcommand -v bun >/dev/null 2>&1 \\`, "bin/check-env"),
  ).toBeNull();
});

test("bunTsTarget throws when a .ts target uses an unrecognized path form", () => {
  expect(() => bunTsTarget(`exec bun "$WEIRD/thing.ts"`, "bin/x")).toThrow(
    /unrecognized/,
  );
});

test("bunTsTarget accepts the space form of the condition flag", () => {
  const line = `exec bun --conditions intx-src "$(dirname -- "\${BASH_SOURCE[0]}")/seed.ts" "$@"`;
  expect(bunTsTarget(line, "bin/seed")).toBe("bin/seed.ts");
});

test("scanLauncher ignores a .ts named in a trailing comment", () => {
  const text = `#!/usr/bin/env bash\nbun install # regenerates foo.ts\n`;
  expect(scanLauncher(text, "bin/add-package")).toEqual([]);
});

test("scriptBunTsTarget extracts the bun .ts target from a package script", () => {
  expect(scriptBunTsTarget("bun --conditions=intx-src run src/cli.ts")).toBe(
    "src/cli.ts",
  );
  expect(scriptBunTsTarget("bun run src/cli.ts")).toBe("src/cli.ts");
  expect(scriptBunTsTarget("tsc --noEmit")).toBeNull();
  expect(scriptBunTsTarget("bun install")).toBeNull();
});

test("scanLauncher skips comment lines and the shebang", () => {
  const text = `#!/usr/bin/env bash\n# Run seed.ts via bun\nexec bun --conditions=intx-src "$(dirname -- "\${BASH_SOURCE[0]}")/seed.ts" "$@"\n`;
  const found = scanLauncher(text, "bin/seed");
  expect(found).toHaveLength(1);
  expect(found[0]?.target).toBe("bin/seed.ts");
  expect(found[0]?.hasFlag).toBe(true);
});

test("shadowingImports flags a bare ./<name> that a wrapper shadows", () => {
  const wrappers = new Set(["seed", "provision-sidecar"]);
  expect(shadowingImports(`import { x } from "./seed";\n`, wrappers)).toEqual([
    "./seed",
  ]);
});

test("shadowingImports ignores lib subpaths and explicit .ts and packages", () => {
  const wrappers = new Set(["seed"]);
  const code = `import { a } from "./lib/seed-config";\nimport { b } from "./seed.ts";\nimport { c } from "@intx/agent";\nimport { d } from "./other";\n`;
  expect(shadowingImports(code, wrappers)).toEqual([]);
});

test("checkLaunchers flags a bin/*.ts importing a wrapper-shadowed name", () => {
  const root = makeRepo({
    "bin/seed": WRAPPER(true),
    "bin/seed.ts": `import { x } from "@intx/agent";\n`,
    "bin/other.ts": `import { resolveSeed } from "./seed";\n`,
  });
  const report = checkLaunchers(root);
  expect(report.violations).toHaveLength(1);
  expect(report.violations[0]).toContain("bin/other.ts");
  expect(report.violations[0]).toContain("bin/lib/");
});

test("checkLaunchers does not flag a bin/*.ts importing from bin/lib", () => {
  const root = makeRepo({
    "bin/seed": WRAPPER(true),
    "bin/seed.ts": `import { x } from "@intx/agent";\n`,
    "bin/other.ts": `import { resolveSeed } from "./lib/seed-config";\n`,
  });
  expect(checkLaunchers(root).violations).toEqual([]);
});

test("checkLaunchers passes a flagged wrapper whose target imports @intx", () => {
  const root = makeRepo({
    "bin/seed": WRAPPER(true),
    "bin/seed.ts": `import { x } from "@intx/agent";\n`,
  });
  const report = checkLaunchers(root);
  expect(report.violations).toEqual([]);
  expect(report.guardedCount).toBe(1);
});

test("checkLaunchers flags an unflagged wrapper whose target imports @intx", () => {
  const root = makeRepo({
    "bin/seed": WRAPPER(false),
    "bin/seed.ts": `import { x } from "@intx/agent";\n`,
  });
  const report = checkLaunchers(root);
  expect(report.violations).toHaveLength(1);
  expect(report.violations[0]).toContain("bin/seed");
  expect(report.violations[0]).toContain("--conditions=intx-src");
});

test("checkLaunchers ignores an unflagged wrapper whose target has no @intx import", () => {
  const root = makeRepo({
    "bin/seed": WRAPPER(false),
    "bin/seed.ts": `import { readFileSync } from "node:fs";\n`,
  });
  const report = checkLaunchers(root);
  expect(report.violations).toEqual([]);
  expect(report.guardedCount).toBe(0);
});

test("checkLaunchers counts a non-.ts launcher without flagging it", () => {
  const root = makeRepo({
    "bin/add-package": `#!/usr/bin/env bash\nset -euo pipefail\nbun install\n`,
  });
  const report = checkLaunchers(root);
  expect(report.violations).toEqual([]);
  expect(report.launcherCount).toBe(1);
  expect(report.guardedCount).toBe(0);
});

test("checkLaunchers throws when a wrapper's bun target does not exist", () => {
  const root = makeRepo({ "bin/seed": WRAPPER(true) });
  expect(() => checkLaunchers(root)).toThrow(/does not exist/);
});

test("checkLaunchers flags an apps/*/bin shebang binary missing the flag", () => {
  const root = makeRepo({
    "apps/sidecar/bin/child": `#!/usr/bin/env -S bun\nimport { x } from "@intx/agent";\n`,
  });
  const report = checkLaunchers(root);
  expect(report.violations).toHaveLength(1);
  expect(report.violations[0]).toContain("apps/sidecar/bin/child");
  expect(report.violations[0]).toContain("shebang");
});

test("checkLaunchers passes an apps/*/bin shebang binary carrying the flag", () => {
  const root = makeRepo({
    "apps/sidecar/bin/child": `#!/usr/bin/env -S bun --conditions=intx-src\nimport { x } from "@intx/agent";\n`,
  });
  expect(checkLaunchers(root).violations).toEqual([]);
});

test("checkLaunchers flags an example package.json script missing the flag", () => {
  const root = makeRepo({
    "examples/quickstart/package.json": JSON.stringify({
      scripts: { start: "bun run src/cli.ts" },
    }),
    "examples/quickstart/src/cli.ts": `import { x } from "@intx/agent";\n`,
  });
  const report = checkLaunchers(root);
  expect(report.violations).toHaveLength(1);
  expect(report.violations[0]).toContain("examples/quickstart/package.json");
  expect(report.violations[0]).toContain("--conditions=intx-src");
});

test("checkLaunchers passes an example script carrying the flag", () => {
  const root = makeRepo({
    "examples/quickstart/package.json": JSON.stringify({
      scripts: { start: "bun --conditions=intx-src run src/cli.ts" },
    }),
    "examples/quickstart/src/cli.ts": `import { x } from "@intx/agent";\n`,
  });
  expect(checkLaunchers(root).violations).toEqual([]);
});

test("checkLaunchers passes against the real repository", () => {
  const here = import.meta.dirname;
  if (here === undefined) throw new Error("import.meta.dirname is undefined");
  const report = checkLaunchers(join(here, ".."));
  expect(report.violations).toEqual([]);
  expect(report.guardedCount).toBeGreaterThanOrEqual(20);
});
