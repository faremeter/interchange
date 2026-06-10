// Source-level discipline check.
//
// This file is *only* a source-text grep against `run.ts`; it asserts
// no environment-shaped discriminator identifier appears in the body
// (no behavioral assertions live here -- the runtime's behavior is
// validated by the dispatch-shape and resume-seam fixtures). The
// check exists because the single-runtime-body discipline forbids
// branching on which host process the body runs in; the grep
// enforces it at the source level so a future refactor cannot
// quietly introduce one. Comment text is stripped before the grep
// so a docstring naming the forbidden identifier (the file header
// explains *why* the identifier is forbidden) does not fail the gate.
//
// Adding a new branching identifier later requires a deliberate
// extension here and full review of the runtime body it slipped
// into.

import { describe, test, expect } from "bun:test";

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));

function stripComments(source: string): string {
  // Strip /* ... */ blocks and // line comments. The discipline applies
  // to executable code only -- a comment that names a forbidden
  // identifier to explain why it must not appear in code is fine.
  return source
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/(^|[^:])\/\/.*$/gm, "$1");
}

describe("source-level discipline grep (no behavioral assertions)", () => {
  test("run.ts source contains no isChildProcess identifier in code", () => {
    const source = stripComments(readFileSync(join(here, "run.ts"), "utf8"));
    expect(source).not.toMatch(/isChildProcess/);
  });

  test("run.ts source contains no processRole or runtimeMode identifier in code", () => {
    const source = stripComments(readFileSync(join(here, "run.ts"), "utf8"));
    expect(source).not.toMatch(/processRole/);
    expect(source).not.toMatch(/runtimeMode/);
  });
});
