import { describe, test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";

// Boot-graph guard for the spawned workflow-child.
//
// The workflow-child evaluates its whole module graph on every cold start. It
// must never pull the control-plane database layer (`@intx/db` / `drizzle`) or
// the sidecar orchestrator (`@intx/hub-agent`) into that graph: the child
// never executes either, the database dependency is a layering violation, and
// importing the orchestrator that spawns the child is a backwards dependency.
// The db-free substrate is reachable via `@intx/hub-sessions/substrate` and
// the path helpers via `@intx/hub-agent/paths`; the package barrels are not.
//
// This walks the child's VALUE-import graph and fails if any forbidden module
// is reachable. `Bun.build` erases `import type` and type-only specifiers
// before resolution (the project's `verbatimModuleSyntax` + `isolatedModules`
// guarantee every value-syntax import that survives is a real runtime import),
// so the captured graph is exactly what the child evaluates at runtime.
// Dynamic `import()` edges are included too, so even a lazy import of a
// forbidden module is caught. npm and `node:` packages are recorded but not
// traversed; workspace (`@intx/*`) packages are resolved to their real source
// (honoring `exports` subpaths) and walked through.

const repoRoot = process.cwd();

// The binary the sidecar spawns as the workflow-child. The graph walk derives
// its entrypoints from this file's actual imports, so anything the binary
// loads -- in any import form -- is part of the checked graph.
const CHILD_BINARY = "apps/sidecar/bin/workflow-child";

// The roots the binary is expected to import. The drift guard asserts the
// binary imports exactly these; a new root trips it so a human confirms the
// addition (and the walk covers the new root regardless).
const EXPECTED_CHILD_ROOTS = [
  "../src/workflow-substrate-factory",
  "@intx/workflow-host",
];

// The module specifiers the binary VALUE-imports, in every runtime form
// (static `from`, side-effect `import`, dynamic `import()`, `require`). Bun's
// transpiler is the same machinery the bundler uses, so it ignores comments,
// strings, and member expressions like `Array.from(...)`, and -- crucially --
// erases `import type` / type-only specifiers, keeping this list aligned with
// the runtime value graph the walk below captures.
function binaryImportSpecifiers(): string[] {
  const raw = readFileSync(join(repoRoot, CHILD_BINARY), "utf8");
  // The transpiler does not accept the binary's `#!/usr/bin/env bun` shebang.
  const source = raw.replace(/^#![^\n]*\n/, "");
  const transpiler = new Bun.Transpiler({ loader: "ts" });
  const specs = new Set(transpiler.scanImports(source).map((i) => i.path));
  return [...specs].sort();
}

// Resolve the binary's imports to real entrypoint files. A root that cannot
// resolve throws here and fails the test loudly rather than yielding a graph
// built from an incomplete root set.
function binaryEntrypoints(): string[] {
  const binDir = dirname(join(repoRoot, CHILD_BINARY));
  return binaryImportSpecifiers().map((spec) => Bun.resolveSync(spec, binDir));
}

/**
 * Returns the package reason if a value-import specifier is forbidden in the
 * child boot graph, or null otherwise. `@intx/db` / `drizzle-orm` are banned
 * entirely; the `@intx/hub-sessions` and `@intx/hub-agent` *barrels* are banned
 * (their `/substrate` and `/paths` subpaths are the supported db-free doors).
 */
function forbiddenReason(spec: string): string | null {
  if (spec === "@intx/db" || spec.startsWith("@intx/db/")) {
    return "control-plane database layer";
  }
  if (spec === "drizzle-orm" || spec.startsWith("drizzle-orm/")) {
    return "database ORM";
  }
  if (spec === "@intx/hub-sessions") {
    return "control-plane barrel; import @intx/hub-sessions/substrate instead";
  }
  if (spec === "@intx/hub-agent") {
    return "sidecar orchestrator barrel; import @intx/hub-agent/paths instead";
  }
  return null;
}

async function childValueImportGraph(entrypoints: string[]): Promise<{
  importers: Map<string, Set<string>>;
  resolveFailures: string[];
  success: boolean;
}> {
  const importers = new Map<string, Set<string>>();
  const resolveFailures: string[] = [];
  const relativize = (p: string) => p.replace(`${repoRoot}/`, "");

  const result = await Bun.build({
    entrypoints,
    target: "bun",
    throw: false,
    plugins: [
      {
        name: "track-value-imports",
        setup(build) {
          build.onResolve({ filter: /.*/ }, (args) => {
            const spec = args.path;
            const from = args.importer ? relativize(args.importer) : "entry";
            const seenFrom = importers.get(spec) ?? new Set<string>();
            seenFrom.add(from);
            importers.set(spec, seenFrom);

            // Relative imports: let Bun traverse natively.
            if (spec.startsWith(".") || spec.startsWith("/")) return undefined;
            // Leaves we record but do not walk into.
            if (spec.startsWith("node:")) return { path: spec, external: true };
            if (!spec.startsWith("@intx/")) {
              return { path: spec, external: true };
            }
            // Workspace packages: resolve to real source so traversal continues
            // through them (honoring `exports` subpaths like `/substrate`). A
            // failure here would silently leave the subtree unwalked, so record
            // it -- the test asserts there are none.
            const fromDir = args.importer ? dirname(args.importer) : repoRoot;
            try {
              return { path: Bun.resolveSync(spec, fromDir) };
            } catch {
              resolveFailures.push(`${spec} (from ${from})`);
              return { path: spec, external: true };
            }
          });
        },
      },
    ],
  });

  return { importers, resolveFailures, success: result.success };
}

describe("workflow-child boot graph", () => {
  test("the child binary imports only the two known roots", () => {
    expect(binaryImportSpecifiers()).toEqual([...EXPECTED_CHILD_ROOTS].sort());
  });

  test("never value-imports the control-plane database or the orchestrator", async () => {
    const { importers, resolveFailures, success } =
      await childValueImportGraph(binaryEntrypoints());

    expect(success).toBe(true);
    // A workspace specifier that fails to resolve would drop its subtree from
    // the walk while the build still succeeds; surface it rather than silently
    // un-guarding that subtree.
    expect(resolveFailures).toEqual([]);

    const reached = new Set(importers.keys());
    // Anti-vacuity: a workflow-child necessarily evaluates the workflow
    // runtime, so a graph that did not reach it never walked the real child
    // (e.g. empty entrypoints) and the forbidden check would pass over nothing.
    expect(reached).toContain("@intx/workflow");

    const violations = [...reached]
      .map((spec) => ({ spec, reason: forbiddenReason(spec) }))
      .filter((v): v is { spec: string; reason: string } => v.reason !== null);

    if (violations.length > 0) {
      const detail = violations
        .map((v) => {
          const from = [...(importers.get(v.spec) ?? new Set())].join(
            "\n      ",
          );
          return `  ${v.spec} -- ${v.reason}\n    imported by:\n      ${from}`;
        })
        .join("\n");
      throw new Error(
        `The workflow-child boot graph reached forbidden modules:\n${detail}\n\n` +
          "The spawned child must not evaluate the control-plane database or the " +
          "sidecar orchestrator at boot. Use @intx/hub-sessions/substrate for the " +
          "db-free repo/substrate symbols and @intx/hub-agent/paths for the deploy-tree " +
          "and address helpers.",
      );
    }
  });
});
