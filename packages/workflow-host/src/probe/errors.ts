export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// Node/Bun's module-not-found message shape. The specifier is the missing
// package the workflow entry imported at evaluation time.
const MISSING_MODULE_RE = /Cannot find (?:module|package) ['"]([^'"]+)['"]/;

/**
 * Enrich a probe evaluation failure whose cause is a module that could not be
 * resolved from the workflow's dependency closure. The evaluator is the layer
 * that KNOWS what the workflow imports at run time (it actually ran the
 * import), so a missing specifier here means the closure did not carry it --
 * the common cause is a runtime import declared only under `devDependencies`
 * (which the closure does not materialize), whether a workspace-local member or
 * an external package. Rewrite the opaque "Cannot find module" into that
 * actionable diagnostic. A non-resolution failure passes through unchanged.
 */
export function enrichProbeError(err: unknown): string {
  const message = errorMessage(err);
  const match = MISSING_MODULE_RE.exec(message);
  const specifier = match?.[1];
  if (specifier === undefined) return message;
  return (
    `workflow entry could not resolve ${JSON.stringify(specifier)} from its dependency closure; ` +
    `if the workflow imports it at run time, declare it under "dependencies" rather than "devDependencies" ` +
    `(a devDependencies-only import is not materialized into the closure). ${message}`
  );
}
