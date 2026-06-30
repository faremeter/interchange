// Per-step `credentialsSnapshot` assembly.
//
// At spawn time, the supervisor reads each workflow step's grants out
// of its `agent-state` repo and forwards the resulting per-step
// snapshot to the workflow-process over the control IPC. The snapshot
// never lands on disk in the child's view and is never placed in
// spawn-time env -- the env carries only the IPC trust anchors. The
// child receives credentials over the authenticated control channel
// once both halves are connected.
//
// Each step's snapshot is pinned to a content hash so the child can
// detect a stale snapshot (e.g. one that arrived after a `grants-
// updated` push the child already processed). The hash is the
// sha256 of the canonicalized JSON serialization of the snapshot's
// per-step `grants` array -- not the git tree SHA of the underlying
// `agent-state` repo's `grants` ref. The substrate's tree SHA is
// implementation-coupled to the path of the file inside the repo;
// hashing the canonical JSON gives a stable identifier the child can
// compare across pushes without taking a dependency on the
// substrate's git layout.
//
// Per-step address derivation (Q6.4 discovery decision):
//   - Multi-step deployments use `<deploymentId>-<stepId>@<domain>`.
//   - Trivial (single-step) deployments use the deployment's own
//     mail address as the sole step's address.
// The derivation is supplied by the caller as a `deriveStepAddress`
// callback so the supervisor doesn't have to encode the deployment-
// domain into the workflow-host package.

import { type } from "arktype";

import { hexEncode } from "@intx/types";
import type {
  Principal,
  RepoId,
  RepoStore,
} from "@intx/hub-sessions/substrate";

/**
 * Path inside each step's `agent-state` repo that carries the step's
 * grants. The agent-state kind handler accepts state writes under the
 * `state/` subtree; the grants snapshot rides at
 * `state/grants.json` as a single canonical JSON document.
 */
export const STEP_GRANTS_PATH = "state/grants.json";

/**
 * Ref the supervisor reads each step's grants from. The agent-state
 * kind handler defines the deploy ref under `refs/heads/deploy`; the
 * state-bearing refs the sidecar writes to use the default
 * `refs/heads/main` convention.
 */
export const STEP_GRANTS_REF = "refs/heads/main";

/**
 * Shape of the per-step grants file. The supervisor surfaces this
 * untyped to the child -- the child's authorize layer narrows the
 * inner `grants` entries against its own grant-rule validator.
 */
const StepGrantsFile = type({
  grants: "unknown[]",
}).onUndeclaredKey("ignore");

export type CredentialsSnapshotStep = {
  /** Workflow step id from `WorkflowDefinition.stepOrder`. */
  stepId: string;
  /** Mail address the step's agent presents to the bus. */
  address: string;
  /** Opaque `grants` array as committed in the step's repo. */
  grants: readonly unknown[];
  /** sha256 hex over the canonical JSON of `grants`. */
  contentHash: string;
};

export type CredentialsSnapshot = {
  /** Step-id keyed entries in `stepOrder` traversal order. */
  steps: readonly CredentialsSnapshotStep[];
};

/**
 * Caller-supplied derivation of the per-step mail address from the
 * deployment id and step id. The supervisor cannot encode the
 * deployment-domain inside library code; the wiring module supplies
 * the strategy the host owns.
 */
export type DeriveStepAddress = (args: {
  deploymentId: string;
  stepId: string;
}) => string;

/**
 * Caller-supplied override of the per-step `agent-state` repo identity
 * the supervisor reads grants from. Defaults to the
 * `<deploymentId>-<stepId>` convention (`defaultStepRepoId`); the
 * single-step launched-agent deploy supplies a derivation that returns
 * the legacy agent-state repo so the child reads grants from the same
 * repo the legacy agent identity already keys.
 */
export type DeriveStepRepoId = (args: {
  deploymentId: string;
  stepId: string;
}) => RepoId;

export type AssembleCredentialsSnapshotOpts = {
  /** Substrate handle the supervisor reads from. */
  repoStore: RepoStore;
  /** Principal presented for each step's read. */
  principal: Principal;
  /**
   * Step ids in the deployment's `stepOrder`. The trivial workflow
   * passes a single entry; multi-step deployments pass every step in
   * the order the workflow asset declared.
   */
  stepOrder: readonly string[];
  /** Deployment id used in agent-state repo identity and address derivation. */
  deploymentId: string;
  /** Per-step mail-address derivation callback. */
  deriveStepAddress: DeriveStepAddress;
  /**
   * Optional override for the `agent-state` repo's id. Callers that
   * follow the documented convention (`<deploymentId>-<stepId>`) can
   * omit this; tests and bespoke layouts can supply their own.
   */
  deriveStepRepoId?: DeriveStepRepoId;
};

/**
 * Default mapping from `(deploymentId, stepId)` to the agent-state
 * repo's identity. Multi-step deployments use `<deploymentId>-
 * <stepId>` to keep each step's grants isolated to its own repo;
 * trivial deployments collapse to the same convention with the
 * sole step's id appended.
 */
export function defaultStepRepoId(args: {
  deploymentId: string;
  stepId: string;
}): RepoId {
  return {
    kind: "agent-state",
    id: `${args.deploymentId}-${args.stepId}`,
  };
}

/**
 * Read one step's grants file from disk via the substrate's working-
 * tree directory. The substrate documents `getRepoDir` as a pure path
 * computation -- the sibling production adapters (`repo-store.ts`,
 * `spawn-child.ts`) use the same working-tree-read pattern when they
 * need synchronous access to a ref's tip without round-tripping
 * through the git object database.
 *
 * A missing grants file is treated as "no grants" (empty array) so
 * the trivial path with no operator-supplied grants does not crash;
 * a malformed file does crash, because the file's presence implies
 * the deploy orchestrator intended a snapshot and a structural
 * failure is a programming bug at the boundary.
 */
async function readStepGrants(
  opts: AssembleCredentialsSnapshotOpts,
  repoId: RepoId,
): Promise<readonly unknown[]> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const dir = opts.repoStore.getRepoDir(repoId);
  const filePath = path.join(dir, STEP_GRANTS_PATH);
  let raw: string;
  try {
    raw = await fs.readFile(filePath, "utf8");
  } catch (cause) {
    if (isErrnoNotFound(cause)) return [];
    throw cause;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      `supervisor credentialsSnapshot: ${repoId.kind}/${repoId.id}:${STEP_GRANTS_PATH} is not valid JSON`,
      { cause },
    );
  }
  const validated = StepGrantsFile(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `supervisor credentialsSnapshot: ${repoId.kind}/${repoId.id}:${STEP_GRANTS_PATH} failed validation: ${validated.summary}`,
    );
  }
  return validated.grants;
}

/**
 * Compute the per-step content hash used to pin a credentialsSnapshot
 * push to a specific grants payload. Stable across processes because
 * the JSON.stringify pass produces the same byte string for a given
 * grants array.
 */
export async function hashGrants(grants: readonly unknown[]): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(JSON.stringify(grants)),
  );
  return hexEncode(new Uint8Array(digest));
}

/**
 * Assemble the per-deployment `credentialsSnapshot` from each step's
 * `agent-state` repo. The supervisor invokes this at spawn time and
 * again on every `grants-updated` mail. The result rides the
 * control IPC as a single payload; the child pins each step's grants
 * to the supplied hash so it can ignore an out-of-order push that
 * arrives after a fresher one.
 */
export async function assembleCredentialsSnapshot(
  opts: AssembleCredentialsSnapshotOpts,
): Promise<CredentialsSnapshot> {
  const deriveRepoId = opts.deriveStepRepoId ?? defaultStepRepoId;
  const steps: CredentialsSnapshotStep[] = [];
  for (const stepId of opts.stepOrder) {
    const repoId = deriveRepoId({
      deploymentId: opts.deploymentId,
      stepId,
    });
    const grants = await readStepGrants(opts, repoId);
    const address = opts.deriveStepAddress({
      deploymentId: opts.deploymentId,
      stepId,
    });
    steps.push({
      stepId,
      address,
      grants,
      contentHash: await hashGrants(grants),
    });
  }
  return { steps };
}

function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  const code = (cause as { code?: unknown }).code;
  return code === "ENOENT";
}
