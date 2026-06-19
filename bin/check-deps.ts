#!/usr/bin/env bun
/* eslint-disable no-console */

// Dependency-hygiene guard for the workspace. Three checks, one gate:
//
//   1. Phantom dependencies — every module a workspace member imports must
//      be declared in that member's own package.json. Imports are extracted
//      with the TypeScript pre-processor, covering value, default, namespace,
//      re-export, dynamic import(), require(), and the type-only forms
//      (`import type`, inline `{ type X }`, and `typeof import()`), while
//      strings, comments, and template literals are ignored. Triple-slash
//      `/// <reference types="..." />` directives are out of scope. Bun's
//      flat, hoisted node_modules otherwise lets a package resolve a
//      dependency it never declared, so the manifest lies and a strict
//      resolver breaks.
//   2. Catalog convergence — any external dependency used by two or more
//      members must be referenced as "catalog:", and every root catalog
//      entry must in turn serve two or more members, so a version lives in
//      exactly one place and the catalog carries no dead or single-use entries.
//   3. Lockfile honesty — `bun install --frozen-lockfile` must be a no-op, so a
//      manifest can declare no dependency that is missing from or stale in
//      bun.lock. (bun's frozen check validates dependency resolution; it does
//      not police every lockfile bookkeeping detail, such as bin-map or
//      workspace-link metadata.)
//
// `checkWorkspace` performs the static checks (1 and 2) and is exported for
// tests; the lockfile check and CLI gate run only when this file is the entry
// point. Exits non-zero with a list of violations, or zero when clean.

import { join, relative } from "node:path";
import { type } from "arktype";
import ts from "typescript";

const NODE_BUILTINS = new Set([
  "assert",
  "async_hooks",
  "buffer",
  "child_process",
  "cluster",
  "console",
  "constants",
  "crypto",
  "dgram",
  "diagnostics_channel",
  "dns",
  "domain",
  "events",
  "fs",
  "http",
  "http2",
  "https",
  "inspector",
  "module",
  "net",
  "os",
  "path",
  "perf_hooks",
  "process",
  "punycode",
  "querystring",
  "readline",
  "repl",
  "stream",
  "string_decoder",
  "sys",
  "timers",
  "tls",
  "trace_events",
  "tty",
  "url",
  "util",
  "v8",
  "vm",
  "wasi",
  "worker_threads",
  "zlib",
]);

// package.json is external data (a filesystem read), so validate its shape at
// the boundary and fail loudly rather than trusting an unchecked cast. The
// narrow rejects arrays, whose numeric indices would otherwise satisfy a bare
// string-keyed record.
const depMap = type({ "[string]": "string" }).narrow((value, ctx) =>
  Array.isArray(value) ? ctx.mustBe("a non-array object") : true,
);
const manifestSchema = type({
  "name?": "string",
  "dependencies?": depMap,
  "devDependencies?": depMap,
  "peerDependencies?": depMap,
  "optionalDependencies?": depMap,
  "catalog?": depMap,
});
type Manifest = typeof manifestSchema.infer;

/** Map an import specifier to the package name it belongs to, or null when
 *  it is relative, a builtin, or a `node:`/`bun:` reference. */
function packageOf(spec: string): string | null {
  if (spec.startsWith(".") || spec.startsWith("/")) return null;
  if (spec.startsWith("node:") || spec.startsWith("bun:")) return null;
  if (spec === "bun") return null; // the Bun runtime module, always available
  const name = spec.startsWith("@")
    ? spec.split("/").slice(0, 2).join("/")
    : (spec.split("/")[0] ?? spec);
  if (NODE_BUILTINS.has(name)) return null;
  return name;
}

type AliasMatcher = { exact: Set<string>; prefixes: string[] };
type MemberConfig = { aliases: AliasMatcher; outDir: string | null };

/** Read a member's tsconfig for the two things the phantom scan needs: the
 *  `paths` alias matchers to exclude (a key with a trailing `*` matches by
 *  prefix; a key without one matches exactly, so "react" never suppresses
 *  "react-dom"), and the `outDir` to skip so emitted JS is not scanned. Uses
 *  the TypeScript config reader so JSONC and `extends` chains resolve as the
 *  compiler sees them; a config that cannot be read — including an unresolvable
 *  `extends` target — raises rather than silently dropping aliases. */
function memberConfig(dir: string): MemberConfig {
  const empty: MemberConfig = {
    aliases: { exact: new Set(), prefixes: [] },
    outDir: null,
  };
  const configPath = join(dir, "tsconfig.json");
  if (!ts.sys.fileExists(configPath)) return empty;
  const read = ts.readConfigFile(configPath, ts.sys.readFile);
  if (read.error) {
    throw new Error(
      `check-deps: cannot read ${configPath}: ${ts.flattenDiagnosticMessageText(read.error.messageText, "\n")}`,
    );
  }
  const parsed = ts.parseJsonConfigFileContent(read.config, ts.sys, dir);
  // TS5083 = "Cannot read file" — an unreadable config or extends target.
  const unreadable = parsed.errors.filter((e) => e.code === 5083);
  if (unreadable.length > 0) {
    throw new Error(
      `check-deps: cannot resolve ${configPath}: ${unreadable.map((e) => ts.flattenDiagnosticMessageText(e.messageText, "\n")).join("; ")}`,
    );
  }
  const exact = new Set<string>();
  const prefixes: string[] = [];
  for (const key of Object.keys(parsed.options.paths ?? {})) {
    if (key.endsWith("*")) prefixes.push(key.slice(0, -1));
    else exact.add(key);
  }
  let outDir: string | null = null;
  if (parsed.options.outDir !== undefined) {
    const rel = relative(dir, parsed.options.outDir);
    if (rel !== "" && rel !== "." && !rel.startsWith("..")) outDir = rel;
  }
  return { aliases: { exact, prefixes }, outDir };
}

async function readManifest(path: string): Promise<Manifest> {
  const parsed = manifestSchema(await Bun.file(path).json());
  if (parsed instanceof type.errors) {
    throw new Error(
      `check-deps: ${path} is not a well-formed manifest: ${parsed.summary}`,
    );
  }
  return parsed;
}

function declaredNames(m: Manifest): Set<string> {
  return new Set([
    ...Object.keys(m.dependencies ?? {}),
    ...Object.keys(m.devDependencies ?? {}),
    ...Object.keys(m.peerDependencies ?? {}),
    ...Object.keys(m.optionalDependencies ?? {}),
  ]);
}

// Catalog accounting. Every external dependency declaration is recorded with
// whether it sits in a runtime section (dependencies/devDependencies) or a
// peer/optional one. Check 2a (a dep shared by two runtime members must be
// "catalog:") ignores peer/optional, so deliberately-wide peer ranges are never
// forced onto the catalog; check 2b (a catalog entry must serve two members)
// counts every section, since a peer consumer is still a consumer.
type ExternalUse = { member: string; spec: string; runtime: boolean };

export type WorkspaceReport = {
  violations: string[];
  memberCount: number;
  fileCount: number;
};

/** Run the static dependency-hygiene checks (phantom imports and catalog
 *  convergence) over every workspace member under `repoRoot`. The lockfile
 *  check is left to the CLI, since it shells out to `bun install`. */
export async function checkWorkspace(
  repoRoot: string,
): Promise<WorkspaceReport> {
  const violations: string[] = [];
  const externalUses = new Map<string, ExternalUse[]>();
  const workspaceNames = new Set<string>();
  let memberCount = 0;
  let fileCount = 0;

  const manifestPaths = [
    ...new Bun.Glob("packages/*/package.json").scanSync(repoRoot),
    ...new Bun.Glob("apps/*/package.json").scanSync(repoRoot),
    ...new Bun.Glob("examples/*/package.json").scanSync(repoRoot),
  ].sort();

  for (const manifestRel of manifestPaths) {
    const dir = join(repoRoot, manifestRel.replace(/\/package\.json$/, ""));
    const manifest = await readManifest(join(repoRoot, manifestRel));
    const self = manifest.name ?? manifestRel;
    const declared = declaredNames(manifest);
    const { aliases, outDir } = memberConfig(dir);
    workspaceNames.add(self);
    memberCount += 1;

    // --- check 1: phantom dependencies ---
    const sourceFiles = [
      ...new Bun.Glob("**/*.{ts,tsx,mts,cts,js,jsx,mjs,cjs}").scanSync(dir),
    ].filter((f) => {
      const segments = f.split("/");
      if (segments.includes("node_modules") || segments.includes("dist"))
        return false;
      if (outDir !== null && (f === outDir || f.startsWith(`${outDir}/`)))
        return false;
      return true;
    });

    for (const file of sourceFiles) {
      fileCount += 1;
      const code = await Bun.file(join(dir, file)).text();
      const specs = new Set<string>();
      // detectJavaScriptImports=true so require() is included alongside every
      // ES import/export form, including type-only ones.
      for (const imported of ts.preProcessFile(code, true, true)
        .importedFiles) {
        specs.add(imported.fileName);
      }

      for (const spec of specs) {
        const name = packageOf(spec);
        if (name === null || name === manifest.name) continue;
        if (
          aliases.exact.has(spec) ||
          aliases.prefixes.some((p) => spec.startsWith(p))
        )
          continue;
        if (!declared.has(name)) {
          violations.push(
            `${self}: imports "${name}" (${manifestRel.replace(/\/package\.json$/, "")}/${file}) but does not declare it in package.json`,
          );
        }
      }
    }

    // --- gather for check 2 ---
    for (const section of [
      "dependencies",
      "devDependencies",
      "peerDependencies",
      "optionalDependencies",
    ] as const) {
      const deps = manifest[section] ?? {};
      const runtime =
        section === "dependencies" || section === "devDependencies";
      for (const [name, spec] of Object.entries(deps)) {
        if (name.startsWith("@intx/") || spec.startsWith("workspace:"))
          continue;
        const uses = externalUses.get(name) ?? [];
        uses.push({ member: self, spec, runtime });
        externalUses.set(name, uses);
      }
    }
  }

  // --- check 2a: a dep used by >= 2 runtime members must be "catalog:" ---
  for (const [name, uses] of externalUses) {
    const runtimeUses = uses.filter((u) => u.runtime);
    const members = new Set(runtimeUses.map((u) => u.member));
    if (members.size < 2) continue;
    const reported = new Set<string>();
    for (const use of runtimeUses) {
      if (use.spec.startsWith("catalog:") || reported.has(use.member)) continue;
      reported.add(use.member);
      violations.push(
        `${use.member}: "${name}" is used by ${members.size} packages and must be "catalog:", not "${use.spec}"`,
      );
    }
  }

  // --- check 2b: every root catalog entry must in turn serve >= 2 members ---
  const rootManifest = await readManifest(join(repoRoot, "package.json"));
  for (const name of Object.keys(rootManifest.catalog ?? {})) {
    const consumers = new Set(
      (externalUses.get(name) ?? []).map((u) => u.member),
    );
    if (consumers.size < 2) {
      violations.push(
        `root catalog entry "${name}" is referenced by ${consumers.size} package(s); catalog entries must serve at least 2 (inline a literal specifier or remove the entry)`,
      );
    }
  }

  // --- check 1 for the root scripts: bin/ and tests/ are not workspace
  // members (no package.json of their own) and are never published, so their
  // imports resolve against the root manifest. Workspace packages are always
  // available to them; only external imports must be declared in root. ---
  const rootDeclared = declaredNames(rootManifest);
  for (const area of ["bin", "tests"]) {
    const areaDir = join(repoRoot, area);
    if (!ts.sys.directoryExists(areaDir)) continue;
    for (const file of new Bun.Glob(
      "**/*.{ts,tsx,mts,cts,js,jsx,mjs,cjs}",
    ).scanSync(areaDir)) {
      const segments = file.split("/");
      if (segments.includes("node_modules") || segments.includes("dist"))
        continue;
      fileCount += 1;
      const code = await Bun.file(join(repoRoot, area, file)).text();
      for (const imported of ts.preProcessFile(code, true, true)
        .importedFiles) {
        const name = packageOf(imported.fileName);
        if (name === null || workspaceNames.has(name)) continue;
        if (!rootDeclared.has(name)) {
          violations.push(
            `${area}/${file}: imports "${name}" but the root package.json does not declare it`,
          );
        }
      }
    }
  }

  return { violations, memberCount, fileCount };
}

if (import.meta.main) {
  if (import.meta.dirname === undefined) {
    throw new Error(
      "check-deps: import.meta.dirname is undefined; cannot locate the repository root",
    );
  }
  const repoRoot = join(import.meta.dirname, "..");
  const { violations, memberCount, fileCount } = await checkWorkspace(repoRoot);

  // --- check 3: lockfile honesty ---
  const frozen = Bun.spawnSync(["bun", "install", "--frozen-lockfile"], {
    cwd: repoRoot,
    stdout: "pipe",
    stderr: "pipe",
  });
  if (frozen.exitCode !== 0) {
    const detail =
      frozen.stderr.toString().trim() || frozen.stdout.toString().trim();
    violations.push(
      `bun.lock is out of date with the manifests; run \`bun install\` and commit bun.lock.\n  ${detail.replace(/\n/g, "\n  ")}`,
    );
  }

  if (violations.length > 0) {
    console.error(`check-deps: ${violations.length} violation(s)\n`);
    for (const v of violations) console.error(`  - ${v}`);
    console.error(
      "\nDeclare missing dependencies, converge shared ones onto the root catalog, or refresh bun.lock.",
    );
    process.exit(1);
  }

  console.log(
    `check-deps: ok (${memberCount} workspace members, ${fileCount} source files, lockfile current)`,
  );
}
