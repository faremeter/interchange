// Per-run grants read primitives shared by the two sidecar modules that
// resolve a run's authorization from disk: the supervisor wiring
// (`workflow-host-wiring.ts`), which reads a run's grants at the
// `onRunStart` barrier, and the in-process child runtime
// (`workflow-substrate-factory.ts`), which reads the PARENT run's grants
// to bind a spawned child's authorize. Both destinations read the same
// `runs/<runId>/grants.json` file in a deployment's `workflow-run` repo,
// so the read lives in one place.
//
// The module's dependency surface is deliberately narrow -- node fs/path,
// arktype, and the substrate `RepoStore` type -- so importing it into the
// child subprocess module does not drag the hub-agent deploy-router
// surface `workflow-host-wiring.ts` also depends on into the child
// binary.

import { readFile } from "node:fs/promises";
import { join as pathJoin } from "node:path";

import { type } from "arktype";

import type { RepoStore } from "@intx/hub-sessions";
import { isErrnoNotFound } from "@intx/workflow-host";

/**
 * Path inside a deployment's `workflow-run` repo that carries a single
 * run's grants. It sits under the run's own `runs/<runId>/` subtree --
 * sibling to that run's `events/` blobs -- so a run's grants live and are
 * reclaimed with the rest of the run's state.
 */
export function runGrantsPath(runId: string): string {
  return `runs/${runId}/grants.json`;
}

/**
 * Envelope the per-run grants file carries: the canonical `{ grants: [] }`
 * shape the deploy-time snapshot writes and validates. The inner entries
 * stay `unknown` -- the child's authorize layer narrows each against its
 * own grant-rule validator, exactly as the deploy-time snapshot's grants
 * are surfaced untyped.
 */
export const RunGrantsFile = type({
  grants: "unknown[]",
}).onUndeclaredKey("ignore");

/**
 * Read a single run's grants from `runs/<runId>/grants.json` inside the
 * deployment's `workflow-run` repo. The read is a working-tree read via
 * `getRepoDir` (the same substrate access pattern the deploy-time
 * per-step read uses), so it observes the tip the grants handler wrote.
 *
 * Returns the run's grants when the file exists. Returns `undefined`
 * -- distinct from an empty grants array -- when the file is ABSENT
 * (ENOENT), so the caller can distinguish "this run got no per-run
 * grants file" from "this run's grants are the empty set". A file that
 * exists but is malformed (invalid JSON or a shape the envelope rejects)
 * THROWS: the file's presence implies a grants frame was delivered, and a
 * structural failure is a boundary bug, not a default.
 */
export async function readRunGrants(args: {
  repoStore: RepoStore;
  deploymentId: string;
  runId: string;
}): Promise<readonly unknown[] | undefined> {
  const dir = args.repoStore.getRepoDir({
    kind: "workflow-run",
    id: args.deploymentId,
  });
  const filePath = pathJoin(dir, runGrantsPath(args.runId));
  let raw: string;
  try {
    raw = await readFile(filePath, "utf8");
  } catch (cause) {
    if (isErrnoNotFound(cause)) return undefined;
    throw cause;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      `workflow-run/${args.deploymentId}:${runGrantsPath(args.runId)} is not valid JSON`,
      { cause },
    );
  }
  const validated = RunGrantsFile(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `workflow-run/${args.deploymentId}:${runGrantsPath(args.runId)} failed validation: ${validated.summary}`,
    );
  }
  return validated.grants;
}
