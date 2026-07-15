#!/usr/bin/env bun
/* eslint-disable no-console */

// Load smoke test for the published tool packages.
//
// The three `@intx/tools-*` packages ship a compiled `dist/sidecar-bundle.js`
// that the interchange sidecar loads as a tool package. This proves that
// bundle — and the real `@intx/*` dependency closure it imports — installs
// and loads from npm-style tarballs, exactly as a consumer would get it:
//
//   1. Emit `dist` for the tool packages' full `@intx/*` closure.
//   2. `bun pm pack` each into a scratch registry directory. Packing
//      rewrites `workspace:*`/`catalog:` specifiers to concrete versions,
//      so the tarballs carry the same dependency graph a publish would.
//   3. `npm install` the tarball set into a scratch consumer, resolving the
//      `@intx/*` closure from the local tarballs and the third-party leaves
//      from the public registry.
//   4. Import each compiled `sidecar-bundle.js` in a plain `bun` subprocess
//      that does NOT carry `--conditions=intx-src`, so `@intx/*` resolves
//      through `default` -> `dist` (the compiled output), exactly as a
//      consumer or the production sidecar loader does — never the
//      repo-internal source condition. Assert each exposes its tool or
//      plugin factory.
//
// The interchange tool loader runs under Bun, so the load is exercised
// under Bun to mirror the real runtime.
//
// This is build- and network-heavy (it emits dist and installs from npm),
// so it runs from its own `make verify-tool-load` target, not `make all`.

import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { buildDist } from "./build-dist";
import { readWorkspacePackages } from "./lib/packages";

const TOOL_ROOTS = ["@intx/tools-lsp", "@intx/tools-mail", "@intx/tools-posix"];

// The factory each tool's sidecar-bundle exports, and whether it is a
// plugin (vs. a tool) factory.
const EXPECTED = [
  { pkg: "@intx/tools-mail", named: "mail", plugin: false },
  { pkg: "@intx/tools-posix", named: "posix", plugin: false },
  { pkg: "@intx/tools-lsp", named: "lsp", plugin: true },
];

/** The full `@intx/*` dependency closure of the tool packages, walking
 *  every internal dependency field (dependencies, peer, optional) so no
 *  sibling is omitted from the packed set. */
export function toolClosure(repoRoot: string): string[] {
  const internalDepsByName = new Map(
    readWorkspacePackages(repoRoot).map((p) => [p.name, p.internalDeps]),
  );
  const seen = new Set<string>();
  const stack = [...TOOL_ROOTS];
  while (stack.length > 0) {
    const name = stack.pop();
    if (name === undefined || seen.has(name)) continue;
    seen.add(name);
    for (const dep of internalDepsByName.get(name) ?? []) stack.push(dep);
  }
  return [...seen].sort();
}

/** The load-assertion program run in a plain (no-`intx-src`) Bun subprocess
 *  from the scratch consumer, where the tool closure is installed. */
const LOAD_PROGRAM = `
import { isAnnotatedPluginFactory } from "@intx/agent";
const expected = ${JSON.stringify(EXPECTED)};
for (const { pkg, named, plugin } of expected) {
  const spec = pkg + "/sidecar-bundle";
  const mod = await import(spec);
  const factory = mod[named];
  const id = pkg + "/sidecar-bundle";
  if (factory?.id !== id) {
    throw new Error(spec + ": export '" + named + "' has id " + JSON.stringify(factory?.id) + ", expected " + id);
  }
  if (plugin && !isAnnotatedPluginFactory(factory)) {
    throw new Error(spec + ": export '" + named + "' is not a plugin factory");
  }
  if (!plugin && isAnnotatedPluginFactory(factory)) {
    throw new Error(spec + ": export '" + named + "' is a plugin factory, expected a tool factory");
  }
  console.log("  loaded " + id + (plugin ? " (plugin)" : " (tool)"));
}
console.log("verify-tool-load: ok");
`;

function run(cmd: string[], cwd: string): void {
  const proc = Bun.spawnSync(cmd, { cwd, stdout: "pipe", stderr: "pipe" });
  if (proc.exitCode !== 0) {
    const detail = [
      proc.stdout.toString().trim(),
      proc.stderr.toString().trim(),
    ]
      .filter(Boolean)
      .join("\n");
    throw new Error(
      `verify-tool-load: \`${cmd.join(" ")}\` failed in ${cwd}:\n${detail}`,
    );
  }
}

async function main(repoRoot: string): Promise<void> {
  const closure = toolClosure(repoRoot);
  console.log(`closure: ${closure.join(" ")}`);

  const scratch = mkdtempSync(join(tmpdir(), "verify-tool-load-"));
  const tarballs = join(scratch, "tarballs");
  const consumer = join(scratch, "consumer");
  try {
    run(["mkdir", "-p", tarballs, consumer], repoRoot);

    // 1. emit dist for the closure.
    await buildDist(repoRoot, closure);

    // 2. pack each closure package (rewrites deps to concrete versions).
    for (const name of closure) {
      const dir = join(repoRoot, "packages", name.replace("@intx/", ""));
      run(["bun", "pm", "pack", "--destination", tarballs, "--quiet"], dir);
    }

    // 3. install the closure into the scratch consumer.
    const tgz = readdirSync(tarballs)
      .filter((f) => f.endsWith(".tgz"))
      .map((f) => join(tarballs, f));
    run(["npm", "init", "-y"], consumer);
    run(["npm", "install", "--silent", ...tgz], consumer);

    // 4. load the compiled bundles under plain Bun (no intx-src condition).
    await Bun.write(join(consumer, "load.ts"), LOAD_PROGRAM);
    run(["bun", "load.ts"], consumer);
    console.log(
      `verify-tool-load: ok (${EXPECTED.length} tool bundles loaded from a packed ${closure.length}-package closure)`,
    );
  } finally {
    rmSync(scratch, { recursive: true, force: true });
    for (const name of closure) {
      rmSync(join(repoRoot, "packages", name.replace("@intx/", ""), "dist"), {
        recursive: true,
        force: true,
      });
    }
  }
}

if (import.meta.main) {
  if (import.meta.dirname === undefined) {
    throw new Error(
      "verify-tool-load: import.meta.dirname is undefined; cannot locate the repository root",
    );
  }
  await main(join(import.meta.dirname, ".."));
}
