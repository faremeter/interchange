// Schema for the subset of `package.json` fields the asset substrate
// and tool-package builders read.
//
// Promoted here so the package-registry kind handler (in
// `@intx/hub-sessions`) and the workspace builtin-packing script
// (`bin/build-builtins.ts`) share one definition: the asset
// substrate's validation of an uploaded tarball must match the field
// set the build path emits, otherwise a freshly-packed builtin would
// be rejected for shape reasons the build did not anticipate.

import { type } from "arktype";

/**
 * A tool package's static declaration of one provider-backed credential it
 * needs: an abstract handle plus optional scopes. Advisory only -- a request
 * the agent definition later binds to a concrete credential and the launch-time
 * grant gate authorizes; a declaration consents to nothing on its own. The
 * handle is the key the binding and the runtime delivery use.
 */
export const ToolCredentialHandle = type(/^[a-z0-9][a-z0-9._-]*$/);

export const ToolCredentialDeclaration = type({
  handle: ToolCredentialHandle,
  "scopes?": "string[]",
});
export type ToolCredentialDeclaration = typeof ToolCredentialDeclaration.infer;

/**
 * The credential declarations for one package, with the unique-handle
 * invariant enforced at parse time: a handle is the binding/delivery key, so a
 * duplicate within a single package is a defect the upload boundary must
 * reject rather than let collapse silently downstream.
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
 * interchange packages: `tools` names the sidecar-bundle entry, `credentials`
 * statically declares the provider-backed credentials the package's tools may
 * need, and `workflow` names the module whose evaluation produces a workflow
 * package's `WorkflowDefinition`. `onUndeclaredKey("ignore")` lets the
 * arbitrary upstream npm fields pass through without listing them.
 */
export const PackageJSON = type({
  name: "string",
  version: "string",
  "interchange?": type({
    "tools?": "string",
    "credentials?": ToolCredentialDeclarationArray,
    "workflow?": "string",
  }).onUndeclaredKey("ignore"),
}).onUndeclaredKey("ignore");
export type PackageJSON = typeof PackageJSON.infer;
