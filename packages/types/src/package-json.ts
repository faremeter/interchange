// Schema for the subset of `package.json` fields the asset substrate
// and closure tooling read, plus the credential declaration `defineTool`
// takes, which is not a `package.json` field.
//
// Promoted here so the package-registry kind handler (in
// `@intx/hub-sessions`), closure materialization and the workflow child
// share one definition of what a package's manifest must hold.

import { type } from "arktype";

import { normalize } from "./posix-path";

/**
 * A tool factory's static declaration of one provider-backed credential it
 * needs: an abstract handle plus optional scopes. Advisory only -- a request
 * the workflow definition later binds to a concrete credential and the launch-time
 * grant gate authorizes; a declaration consents to nothing on its own. The
 * handle is the key the binding and the runtime delivery use.
 */
export const ToolCredentialHandle = type(/^[a-z0-9][a-z0-9._-]*$/);

export const ToolCredentialDeclaration = type({
  handle: ToolCredentialHandle,
  "scopes?": "string[]",
});
export type ToolCredentialDeclaration = typeof ToolCredentialDeclaration.infer;

/** A validated declaration as code holds it: frozen, so read-only. */
export type ReadonlyToolCredentialDeclaration = {
  readonly [K in keyof ToolCredentialDeclaration]: Readonly<
    ToolCredentialDeclaration[K]
  >;
};

/**
 * The credential declarations for one tool factory, with the unique-handle
 * invariant enforced at parse time: a handle is the binding/delivery key, so a
 * duplicate is a defect `defineTool` and the inert projector must reject rather
 * than let collapse silently downstream.
 */
export const ToolCredentialDeclarationArray =
  ToolCredentialDeclaration.array().narrow((decls, ctx) => {
    const seen = new Set<string>();
    for (const decl of decls) {
      if (seen.has(decl.handle)) {
        return ctx.mustBe(
          `an array with no duplicate credential handles; "${decl.handle}" appears more than once`,
        );
      }
      seen.add(decl.handle);
    }
    return true;
  });
export type ToolCredentialDeclarationArray =
  typeof ToolCredentialDeclarationArray.infer;

/**
 * Required fields plus the `interchange` extensions used to identify
 * interchange packages: `tools` names the module plugin factories are imported
 * from, `workflow` names the module whose evaluation produces a workflow
 * package's `WorkflowDefinition`, `directors` names the module whose exports are
 * the package's custom `defineDirector` factories, `loops` names the module
 * whose exports are the package's `loop` `while`/`carry` functions, and
 * `actions` names the module whose exports are the package's `action` handlers.
 * `loops` and `actions` refs are resolved by export name at establish.
 * `onUndeclaredKey("ignore")` lets the arbitrary upstream npm fields pass
 * through without listing them.
 */
export const PackageJSON = type({
  name: "string",
  version: "string",
  "interchange?": type({
    "tools?": "string",
    "workflow?": "string",
    "directors?": "string",
    "loops?": "string",
    "actions?": "string",
  }).onUndeclaredKey("ignore"),
}).onUndeclaredKey("ignore");
export type PackageJSON = typeof PackageJSON.infer;

/**
 * True when `entry` -- an `interchange.workflow`/`interchange.directors`
 * module path relative to its package -- stays inside the package directory.
 * An absolute path or a `..` traversal escapes and returns false.
 *
 * This is the string-level half of the containment rule. The workflow
 * definition loader (`resolveContainedEntry`) pairs it with a realpath-based
 * symlink-escape check that only a materialized directory can run; the
 * push-time asset validator, which has no filesystem, relies on this string
 * half alone. Both boundaries call this one predicate so they cannot diverge
 * on what "contained" means. The check uses POSIX path semantics so the
 * result does not depend on the host's separator or cwd.
 */
export function isContainedEntryPath(entry: string): boolean {
  if (entry.startsWith("/")) {
    return false;
  }
  const normalized = normalize(entry);
  return normalized !== ".." && !normalized.startsWith("../");
}
