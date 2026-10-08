// Schema for the subset of `package.json` fields the asset substrate and
// tool-package builders read. Shared by the package-registry kind handler
// (`@intx/hub-sessions`) and the builtin-packing script
// (`bin/build-builtins.ts`), so an uploaded tarball validates against the
// same field set the build path emits.

import path from "node:path";

import { type } from "arktype";

/**
 * A tool package's static declaration of one provider-backed credential it
 * needs: an abstract handle plus optional scopes. Advisory only -- a request
 * the workflow definition later binds to a concrete credential and the
 * launch-time grant gate authorizes. The handle is the binding/delivery key.
 */
export const ToolCredentialHandle = type(/^[a-z0-9][a-z0-9._-]*$/);

export const ToolCredentialDeclaration = type({
  handle: ToolCredentialHandle,
  "scopes?": "string[]",
});
export type ToolCredentialDeclaration = typeof ToolCredentialDeclaration.infer;

/**
 * The credential declarations for one package, with the unique-handle
 * invariant enforced at parse time: a duplicate handle is a defect the upload
 * boundary must reject, since the handle is the binding/delivery key.
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
 * Required fields plus the `interchange` extensions identifying interchange
 * packages: `tools` (sidecar-bundle entry), `credentials` (provider-backed
 * credential declarations), `workflow` (module producing the
 * `WorkflowDefinition`), `directors` (custom `defineDirector` factories),
 * `loops` (the package's `loop` `while`/`carry` functions), and `actions`
 * (the package's `action` handlers). `loops`/`actions` refs resolve by
 * export name at establish; `onUndeclaredKey("ignore")` passes arbitrary
 * upstream npm fields through.
 */
export const PackageJSON = type({
  name: "string",
  version: "string",
  "interchange?": type({
    "tools?": "string",
    "credentials?": ToolCredentialDeclarationArray,
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
 * The string-level half of the loader's containment rule: the load-time
 * loader (`resolveContainedEntry`) adds a realpath symlink-escape check; the
 * push-time asset validator has no filesystem and relies on this half alone.
 * Both boundaries call this predicate so they cannot diverge. POSIX path
 * semantics, so the result is independent of the host separator or cwd.
 */
export function isContainedEntryPath(entry: string): boolean {
  if (path.posix.isAbsolute(entry)) {
    return false;
  }
  const normalized = path.posix.normalize(entry);
  return normalized !== ".." && !normalized.startsWith(`..${path.posix.sep}`);
}
