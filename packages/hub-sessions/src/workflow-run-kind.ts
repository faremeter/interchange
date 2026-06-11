// KindHandler for the `workflow-run` repo kind.
//
// A workflow-run repo holds per-deployment runtime state for one or
// more in-flight workflow runs. `RepoId.id` is the owning deployment
// id. The repo's top-level layout in this commit is:
//
//   - `runs/<runId>/events/<seq>.json` — per-run event log entries.
//     Each entry is a JSON object whose body carries a `type`
//     discriminator (the on-disk event vocabulary used by the
//     workflow-run repo) and a `seq` field that matches the integer
//     in the filename. Filenames are decimal integers ranging from
//     `0` upward; the on-disk seq numbering owns the ordering and
//     the per-blob `seq` field is the redundant cross-check.
//   - `.gitignore` — supplied by the asset routes' genesis init body.
//
// The per-address claim-check subtree (`addresses/<address>/...`)
// and the control-plane subtree (`control/...`) are not part of this
// commit's surface. validatePush rejects any top-level entry outside
// the `runs/` and `.gitignore` set. The per-address subtree lands in
// a sibling commit; `control/` has no v1 use case and is deferred
// indefinitely.
//
// Event-log invariants enforced at push:
//   - Each event body's `seq` matches the integer in its filename.
//   - Per-run event filenames are unique decimal integers (guaranteed
//     by the tree shape) and validatePush verifies the body's `seq`
//     field carries the same number, so the on-disk seq sequence and
//     the per-blob seq cannot diverge.
//   - Terminal-phase lock: once a run's events include a `RunCompleted`,
//     `RunFailed`, or `RunCancelled` entry, no event with a strictly
//     greater seq may appear for the same run.
//   - Append-only via prior-tree byte comparison: every event blob
//     that exists at the same path in the parent commit's tree must
//     match the prospective blob byte-for-byte. Newly-added event
//     paths (those absent from the prior tree) are accepted. The
//     substrate exposes the prior tree via `priorReadBlob` /
//     `priorListDir` on the validatePush args so the constraint is
//     owned by this handler rather than relying on caller-layer
//     discipline.
//   - A `CancelRequested` event must carry an `origin` in the known
//     set (`self`, `supervisor-drain`, `supervisor-operator`,
//     `hub-admin`) and a non-empty `reason`.
//   - Principal-vs-origin enforcement for `CancelRequested`: a
//     `hub-admin` origin requires the signing principal to be `hub`;
//     the other three
//     origins (`self`, `supervisor-drain`, `supervisor-operator`)
//     require the signing principal to be `supervisor` — the
//     supervisor signs on the child's behalf for `self`, and signs
//     for itself on the drain / operator cases. A principal that does
//     not match the declared origin produces a rejection naming both
//     sides so a misconfigured writer surfaces at the boundary
//     rather than as a downstream mystery.
//
// Authz:
//   - `hub` principal: full access.
//   - `workflow-process` principal: read/write its own deployment's
//     event log. The principal carries `{ deploymentId, runId? }`;
//     this handler verifies `repoId.id === deploymentId`.
//   - `supervisor` principal: read/write its own deployment's event
//     log. The principal carries `{ deploymentId }`; this handler
//     verifies `repoId.id === deploymentId`.
//   - `sidecar` principal: read-only (createPack, resolveRef) for
//     resume.
//   - `user` principal: gated by bearer-token claims and the route
//     layer's pre-resolved authz verdict, mirroring the convention
//     used by the other kinds.

import { type } from "arktype";
import { getLogger } from "@intx/log";
import { glob, repoActionToGrantVerb } from "@intx/hub-common";
import {
  UserPrincipal,
  type AuthorizeFn,
  type KindHandler,
  type Principal,
  type ValidatePushResult,
} from "./repo-store";

const logger = getLogger(["hub-sessions", "workflow-run-kind"]);

export type WorkflowRunHubPrincipal = { readonly kind: "hub" };

export type WorkflowRunSidecarPrincipal = {
  readonly kind: "sidecar";
  readonly agentId: string;
};

export type WorkflowRunWorkflowProcessPrincipal = {
  readonly kind: "workflow-process";
  readonly deploymentId: string;
  readonly runId?: string;
};

export type WorkflowRunSupervisorPrincipal = {
  readonly kind: "supervisor";
  readonly deploymentId: string;
};

export type WorkflowRunPrincipal =
  | WorkflowRunHubPrincipal
  | WorkflowRunSidecarPrincipal
  | WorkflowRunWorkflowProcessPrincipal
  | WorkflowRunSupervisorPrincipal;

export const WORKFLOW_RUN_GITIGNORE_PATH = ".gitignore";
export const WORKFLOW_RUN_RUNS_PREFIX = "runs";
export const WORKFLOW_RUN_EVENTS_DIR = "events";
export const WORKFLOW_RUN_ADDRESSES_PREFIX = "addresses";
export const WORKFLOW_RUN_CONTROL_PREFIX = "control";

/**
 * Allowed top-level entries in the prospective tree. Anything else
 * fails the push. The `addresses/` subtree is intentionally absent;
 * it lands in a sibling commit when the per-address claim-check
 * substrate is added. `control/` has no v1 use and stays absent.
 */
const ALLOWED_TOP_LEVEL = new Set<string>([
  WORKFLOW_RUN_RUNS_PREFIX,
  WORKFLOW_RUN_GITIGNORE_PATH,
]);

/** Per-event filename shape: a decimal integer followed by `.json`. */
const EVENT_FILENAME_RE = /^(0|[1-9][0-9]*)\.json$/;

/**
 * Terminal event discriminators. A run whose log contains an entry
 * with one of these `type` values must not receive any event with a
 * strictly greater seq.
 */
const TERMINAL_EVENT_TYPES = new Set<string>([
  "RunCompleted",
  "RunFailed",
  "RunCancelled",
]);

/**
 * Recognised CancelRequested origins. Mirrors the workflow package's
 * `CANCEL_ORIGINS` vocabulary; inlined here so the substrate does
 * not depend on `@intx/workflow`.
 */
const CANCEL_REQUESTED_ORIGINS = new Set<string>([
  "self",
  "supervisor-drain",
  "supervisor-operator",
  "hub-admin",
]);

/**
 * Per-origin signing-principal kind. `hub-admin` is the only origin
 * a `hub` principal may mint; the other three originate inside the
 * supervisor's trust boundary (the supervisor signs `self` on behalf
 * of the workflow-process since the child has no asymmetric keypair,
 * and signs the `supervisor-drain` / `supervisor-operator` audit-
 * distinction
 * cases for itself). Lookup misses fail the push.
 */
const CANCEL_ORIGIN_TO_PRINCIPAL_KIND: ReadonlyMap<string, string> = new Map([
  ["self", "supervisor"],
  ["supervisor-drain", "supervisor"],
  ["supervisor-operator", "supervisor"],
  ["hub-admin", "hub"],
]);

/**
 * Cross-event shape carried by every blob committed under
 * `runs/<runId>/events/`. The discriminator field on disk is `type`,
 * matching the convention used by the substrate's `subscribeKind`
 * helper and the workflow-host scheduler.
 */
const EventEnvelope = type({
  type: "string",
  seq: "number >= 0",
  "+": "ignore",
});

/**
 * Structural validator for the `CancelRequested` payload's
 * cancellation-specific fields. The kind handler verifies the origin
 * is a known CancelOrigin and the reason is a non-empty string; the
 * principal-vs-origin map collapses because every origin is
 * supervisor-signed in this design.
 */
const CancelRequestedFields = type({
  origin: "string",
  reason: "string > 0",
  "+": "ignore",
});

const SidecarPrincipal = type({
  kind: "'sidecar'",
  agentId: "string",
});

const WorkflowProcessPrincipal = type({
  kind: "'workflow-process'",
  deploymentId: "string",
  "runId?": "string",
});

const SupervisorPrincipal = type({
  kind: "'supervisor'",
  deploymentId: "string",
});

type RunEventBlob = {
  runId: string;
  filename: string;
  filenameSeq: number;
  blobPath: string;
};

/**
 * Build the (runId → events[]) map by walking the prospective tree.
 * The substrate's listDir yields names directly under the given
 * directory, so the walk is `runs/` → run-id subdirs → `events/` →
 * event filenames. Filenames outside the `<seq>.json` shape fail the
 * push.
 */
async function enumerateEventBlobs(
  listDir: (path: string) => Promise<string[]>,
): Promise<
  | { ok: true; runs: Map<string, RunEventBlob[]> }
  | { ok: false; reason: string }
> {
  const runs = new Map<string, RunEventBlob[]>();
  const runIds = await listDir(WORKFLOW_RUN_RUNS_PREFIX);
  for (const runId of runIds) {
    const runDirPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
    const runChildren = await listDir(runDirPath);
    const offender = runChildren.find((c) => c !== WORKFLOW_RUN_EVENTS_DIR);
    if (offender !== undefined) {
      return {
        ok: false,
        reason: `run directory ${runDirPath} contains unexpected entry ${JSON.stringify(offender)}; only "${WORKFLOW_RUN_EVENTS_DIR}" is allowed`,
      };
    }
    if (!runChildren.includes(WORKFLOW_RUN_EVENTS_DIR)) {
      return {
        ok: false,
        reason: `run directory ${runDirPath} is missing required "${WORKFLOW_RUN_EVENTS_DIR}" subdirectory`,
      };
    }
    const eventsDirPath = `${runDirPath}/${WORKFLOW_RUN_EVENTS_DIR}`;
    const filenames = await listDir(eventsDirPath);
    const entries: RunEventBlob[] = [];
    for (const filename of filenames) {
      const match = EVENT_FILENAME_RE.exec(filename);
      if (match === null) {
        return {
          ok: false,
          reason: `event filename ${eventsDirPath}/${filename} does not match <seq>.json`,
        };
      }
      const seqStr = match[1];
      if (seqStr === undefined) {
        return {
          ok: false,
          reason: `event filename ${eventsDirPath}/${filename} produced no seq capture`,
        };
      }
      entries.push({
        runId,
        filename,
        filenameSeq: Number.parseInt(seqStr, 10),
        blobPath: `${eventsDirPath}/${filename}`,
      });
    }
    entries.sort((a, b) => a.filenameSeq - b.filenameSeq);
    runs.set(runId, entries);
  }
  return { ok: true, runs };
}

type ParsedEventBlob = {
  entry: RunEventBlob;
  body: { type: string; seq: number; [k: string]: unknown };
};

async function parseEventBlob(
  entry: RunEventBlob,
  readBlob: (path: string) => Promise<Uint8Array>,
): Promise<
  { ok: true; parsed: ParsedEventBlob } | { ok: false; reason: string }
> {
  let raw: Uint8Array;
  try {
    raw = await readBlob(entry.blobPath);
  } catch (cause) {
    return {
      ok: false,
      reason: `event ${entry.blobPath} could not be read from the tree: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  let body: unknown;
  try {
    body = JSON.parse(new TextDecoder().decode(raw));
  } catch (cause) {
    return {
      ok: false,
      reason: `event ${entry.blobPath} is not valid JSON: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  const validated = EventEnvelope(body);
  if (validated instanceof type.errors) {
    return {
      ok: false,
      reason: `event ${entry.blobPath} envelope invalid: ${validated.summary}`,
    };
  }
  if (validated.seq !== entry.filenameSeq) {
    return {
      ok: false,
      reason: `event ${entry.blobPath} body.seq ${String(validated.seq)} does not match filename seq ${String(entry.filenameSeq)}`,
    };
  }
  if (validated.type === "CancelRequested") {
    const cancelFields = CancelRequestedFields(body);
    if (cancelFields instanceof type.errors) {
      return {
        ok: false,
        reason: `event ${entry.blobPath} CancelRequested payload invalid: ${cancelFields.summary}`,
      };
    }
    if (!CANCEL_REQUESTED_ORIGINS.has(cancelFields.origin)) {
      return {
        ok: false,
        reason: `event ${entry.blobPath} CancelRequested origin ${JSON.stringify(cancelFields.origin)} is not a recognised CancelOrigin`,
      };
    }
  }
  return { ok: true, parsed: { entry, body: validated } };
}

/**
 * Compare the prospective bytes of `blobPath` against the bytes at
 * the same path in the prior tree. Returns `{ ok: true }` when the
 * blob is newly added (no prior entry) or when the prior and
 * prospective bytes are byte-identical; returns a rejection otherwise.
 * Surfaces append-only at the handler scope: the event log invariant
 * lives here rather than relying on caller-layer discipline at
 * `writeTreePreservingPrefix`.
 */
async function checkPriorByteEquality(
  blobPath: string,
  readBlob: (path: string) => Promise<Uint8Array>,
  priorReadBlob: (path: string) => Promise<Uint8Array | null>,
): Promise<ValidatePushResult> {
  const prior = await priorReadBlob(blobPath);
  if (prior === null) return { ok: true };
  const prospective = await readBlob(blobPath);
  if (prior.byteLength !== prospective.byteLength) {
    return {
      ok: false,
      reason: `event ${blobPath} bytes diverge from the prior tree (lengths ${String(prior.byteLength)} vs ${String(prospective.byteLength)}); event blobs are append-only`,
    };
  }
  for (let i = 0; i < prior.byteLength; i++) {
    if (prior[i] !== prospective[i]) {
      return {
        ok: false,
        reason: `event ${blobPath} bytes diverge from the prior tree at offset ${String(i)}; event blobs are append-only`,
      };
    }
  }
  return { ok: true };
}

/**
 * Enforce the Q3 principal-vs-origin map for a parsed
 * `CancelRequested` event. The principal kind is matched against the
 * origin's required-signer kind; a mismatch rejects with both the
 * declared origin and the actual principal kind in the message so a
 * misconfigured writer surfaces concretely at the push boundary.
 */
function checkCancelOriginPrincipal(
  blobPath: string,
  origin: string,
  principal: Principal,
): ValidatePushResult {
  const required = CANCEL_ORIGIN_TO_PRINCIPAL_KIND.get(origin);
  if (required === undefined) {
    return {
      ok: false,
      reason: `event ${blobPath} CancelRequested origin ${JSON.stringify(origin)} has no principal-kind binding`,
    };
  }
  if (principal.kind !== required) {
    return {
      ok: false,
      reason: `event ${blobPath} CancelRequested origin ${JSON.stringify(origin)} requires principal.kind=${JSON.stringify(required)} but the push was signed by principal.kind=${JSON.stringify(principal.kind)}`,
    };
  }
  return { ok: true };
}

export const workflowRunKindHandler: KindHandler = {
  kind: "workflow-run",
  directoryPrefix: "workflow-runs",
  async validatePush({
    repoId,
    ref,
    principal,
    topLevelTreePaths,
    readBlob,
    listDir,
    priorReadBlob,
  }): Promise<ValidatePushResult> {
    for (const entry of topLevelTreePaths) {
      if (
        entry.startsWith(`${WORKFLOW_RUN_ADDRESSES_PREFIX}/`) ||
        entry === WORKFLOW_RUN_ADDRESSES_PREFIX
      ) {
        // The per-address claim-check subtree lands in a sibling
        // commit. Reject any path under the addresses/ prefix until
        // the substrate that owns its shape is in place.
        return {
          ok: false,
          reason: `top-level entry ${JSON.stringify(entry)} is under the deferred ${WORKFLOW_RUN_ADDRESSES_PREFIX}/ subtree`,
        };
      }
      if (
        entry.startsWith(`${WORKFLOW_RUN_CONTROL_PREFIX}/`) ||
        entry === WORKFLOW_RUN_CONTROL_PREFIX
      ) {
        return {
          ok: false,
          reason: `top-level entry ${JSON.stringify(entry)} is under the unsupported ${WORKFLOW_RUN_CONTROL_PREFIX}/ subtree`,
        };
      }
      if (!ALLOWED_TOP_LEVEL.has(entry)) {
        return {
          ok: false,
          reason: `unexpected top-level entry ${JSON.stringify(entry)}; allowed: "${WORKFLOW_RUN_RUNS_PREFIX}", "${WORKFLOW_RUN_GITIGNORE_PATH}"`,
        };
      }
    }

    if (!topLevelTreePaths.includes(WORKFLOW_RUN_RUNS_PREFIX)) {
      // A workflow-run repo without any `runs/` directory is a
      // genesis state — `.gitignore`-only trees are accepted so the
      // asset routes' init can land before any run has produced an
      // event.
      return { ok: true };
    }

    const enumerated = await enumerateEventBlobs(listDir);
    if (!enumerated.ok) {
      logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${enumerated.reason}`;
      return { ok: false, reason: enumerated.reason };
    }

    for (const [runId, entries] of enumerated.runs) {
      if (entries.length === 0) {
        return {
          ok: false,
          reason: `run ${runId} has an empty events directory`,
        };
      }
      let terminalSeq: number | null = null;
      let terminalType: string | null = null;
      for (const entry of entries) {
        const priorCheck = await checkPriorByteEquality(
          entry.blobPath,
          readBlob,
          priorReadBlob,
        );
        if (!priorCheck.ok) {
          logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${priorCheck.reason}`;
          return priorCheck;
        }
        const parsed = await parseEventBlob(entry, readBlob);
        if (!parsed.ok) {
          logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${parsed.reason}`;
          return { ok: false, reason: parsed.reason };
        }
        if (parsed.parsed.body.type === "CancelRequested") {
          const origin = parsed.parsed.body.origin;
          if (typeof origin !== "string") {
            return {
              ok: false,
              reason: `event ${entry.blobPath} CancelRequested origin must be a string`,
            };
          }
          const principalCheck = checkCancelOriginPrincipal(
            entry.blobPath,
            origin,
            principal,
          );
          if (!principalCheck.ok) {
            logger.debug`workflow-run validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${principalCheck.reason}`;
            return principalCheck;
          }
        }
        if (terminalSeq !== null) {
          return {
            ok: false,
            reason: `run ${runId} has event at seq ${String(entry.filenameSeq)} after terminal ${terminalType} at seq ${String(terminalSeq)}`,
          };
        }
        if (TERMINAL_EVENT_TYPES.has(parsed.parsed.body.type)) {
          terminalSeq = entry.filenameSeq;
          terminalType = parsed.parsed.body.type;
        }
      }
    }

    return { ok: true };
  },
  onRefUpdated() {
    // No cached index today. Consumers read events through the
    // substrate's subscribe / blob-read API.
  },
};

export const workflowRunAuthorize: AuthorizeFn = (
  principal: Principal,
  repoId,
  ref,
  action,
) => {
  if (repoId.kind !== "workflow-run") {
    return {
      allowed: false,
      reason: `workflow-run authorize received non-workflow-run repo ${repoId.kind}/${repoId.id}`,
    };
  }

  if (principal.kind === "hub") {
    return { allowed: true };
  }

  if (principal.kind === "workflow-process") {
    const parsed = WorkflowProcessPrincipal(principal);
    if (parsed instanceof type.errors) {
      return {
        allowed: false,
        reason: `workflow-process principal is malformed: ${parsed.summary}`,
      };
    }
    if (parsed.deploymentId !== repoId.id) {
      return {
        allowed: false,
        reason: `workflow-process deployment ${parsed.deploymentId} cannot access workflow-run ${repoId.id}`,
      };
    }
    switch (action) {
      case "init":
      case "writeTree":
      case "receivePack":
      case "createPack":
      case "resolveRef":
        return { allowed: true };
      default: {
        const _exhaustive: never = action;
        return {
          allowed: false,
          reason: `unhandled action: ${String(_exhaustive)}`,
        };
      }
    }
  }

  if (principal.kind === "supervisor") {
    const parsed = SupervisorPrincipal(principal);
    if (parsed instanceof type.errors) {
      return {
        allowed: false,
        reason: `supervisor principal is malformed: ${parsed.summary}`,
      };
    }
    if (parsed.deploymentId !== repoId.id) {
      return {
        allowed: false,
        reason: `supervisor deployment ${parsed.deploymentId} cannot access workflow-run ${repoId.id}`,
      };
    }
    switch (action) {
      case "init":
      case "writeTree":
      case "receivePack":
      case "createPack":
      case "resolveRef":
        return { allowed: true };
      default: {
        const _exhaustive: never = action;
        return {
          allowed: false,
          reason: `unhandled action: ${String(_exhaustive)}`,
        };
      }
    }
  }

  if (principal.kind === "sidecar") {
    const parsed = SidecarPrincipal(principal);
    if (parsed instanceof type.errors) {
      return {
        allowed: false,
        reason: `sidecar principal is malformed: ${parsed.summary}`,
      };
    }
    switch (action) {
      case "createPack":
      case "resolveRef":
        return { allowed: true };
      case "init":
      case "writeTree":
      case "receivePack":
        return {
          allowed: false,
          reason: `sidecars may only read workflow-run repos, not ${action}`,
        };
      default: {
        const _exhaustive: never = action;
        return {
          allowed: false,
          reason: `unhandled action: ${String(_exhaustive)}`,
        };
      }
    }
  }

  if (principal.kind === "user") {
    // The route layer has already pre-resolved the grant verdict and
    // attached it as `authz`. The substrate does NOT re-query the
    // grant store here; it (a) checks the bearer-token's claims
    // bound the requested (ref, action) and have not expired, and
    // (b) sanity-checks that the pre-resolved verdict targets this
    // exact resource and grant verb. Both gates must pass before the
    // verdict's `effect` is honoured.
    const parsed = UserPrincipal(principal);
    if (parsed instanceof type.errors) {
      return {
        allowed: false,
        reason: `user principal is malformed: ${parsed.summary}`,
      };
    }
    if (!parsed.tokenClaims.actions.includes(action)) {
      return {
        allowed: false,
        reason: `token does not grant action ${action}`,
      };
    }
    // `ref === "*"` is the substrate's sentinel for the bulk read
    // performed by `listRefs`. Per-ref filtering is the advertise-refs
    // layer's responsibility, so the bulk read is gated on action and
    // expiry alone.
    if (ref !== "*" && !glob.match(parsed.tokenClaims.refPattern, ref)) {
      return {
        allowed: false,
        reason: `token refPattern ${parsed.tokenClaims.refPattern} does not match ${ref}`,
      };
    }
    if (Date.now() >= parsed.tokenClaims.expiresAt) {
      return {
        allowed: false,
        reason: `token expired at ${parsed.tokenClaims.expiresAt}`,
      };
    }
    const expectedResource = `workflow-run:${repoId.id}`;
    if (parsed.authz.resource !== expectedResource) {
      return {
        allowed: false,
        reason: `authz verdict resource ${parsed.authz.resource} does not match ${expectedResource}`,
      };
    }
    const expectedGrantVerb = repoActionToGrantVerb(action);
    if (parsed.authz.grantVerb !== expectedGrantVerb) {
      return {
        allowed: false,
        reason: `authz verdict grantVerb ${parsed.authz.grantVerb} does not match ${expectedGrantVerb}`,
      };
    }
    if (parsed.authz.effect === "allow") {
      return { allowed: true };
    }
    return {
      allowed: false,
      reason: `authz verdict denied for ${expectedResource} ${expectedGrantVerb}`,
    };
  }

  return {
    allowed: false,
    reason: `unknown principal kind: ${principal.kind}`,
  };
};
