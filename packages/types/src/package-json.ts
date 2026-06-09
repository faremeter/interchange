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
 * Required fields plus the `interchange.tools` extension used to
 * identify tool packages. `onUndeclaredKey("ignore")` lets the
 * arbitrary upstream npm fields pass through without listing them.
 */
export const PackageJSON = type({
  name: "string",
  version: "string",
  "interchange?": type({
    "tools?": "string",
  }).onUndeclaredKey("ignore"),
}).onUndeclaredKey("ignore");
export type PackageJSON = typeof PackageJSON.infer;
