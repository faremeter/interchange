// Spawn-time env parser for the workflow-process child.
//
// The supervisor's spawn path constructs a fresh env object carrying
// only the IPC trust anchors plus a tightly-scoped set of deployment
// identifiers. The binary parses `process.env` once at start and hands
// the validated struct to `runWorkflowChild`. The struct shape is the
// only env-shaped surface the runtime body sees; everything else flows
// through IPC frames.
//
// The IPC trust anchors carried here are public-half values: the
// supervisor's Ed25519 PUBLIC key (`HOST_PUBKEY`) plus the shared HMAC
// key (`IPC_HMAC_KEY`) the supervisor minted at spawn time. The
// supervisor's Ed25519 PRIVATE key never appears in env per the IPC
// threat model; the child verifies but never signs control frames.

import { type } from "arktype";

import { hexDecode } from "@intx/types";
import { IPC_CRYPTO } from "../ipc/index";

/**
 * The required spawn-time env keys, named once so the supervisor-side
 * producer (`buildChildSpawnEnv`) and this child-side parser share a
 * single required-key contract. `WARM_KEEP` is optional and lives only in
 * the shape below. The producer types its output against this list
 * (`Record<RequiredSpawnEnvKey, string>`), so omitting a listed key is a
 * compile error. That this list stays in step with the validator shape
 * below -- a hand-maintained arktype object -- is covered by the recycle
 * env-contract regression test, which drives the real producer through
 * this parser. This is the contract whose drift once omitted `STEP_COUNT`
 * from the recycle env and broke every recycle.
 */
export const REQUIRED_SPAWN_ENV_KEYS = [
  "IPC_CHANNEL_ID",
  "IPC_HMAC_KEY",
  "HOST_PUBKEY",
  "DEPLOYMENT_ID",
  "DEFINITION_HASH",
  "MAILBOX_ADDRESS",
  "STEP_COUNT",
] as const;
export type RequiredSpawnEnvKey = (typeof REQUIRED_SPAWN_ENV_KEYS)[number];

/**
 * Required env keys carried by the supervisor at spawn time. The
 * validator surface is intentionally narrow: every key documented at
 * the supervisor's `spawn(opts)` method is represented here, and
 * anything the supervisor did not place in the env causes a targeted
 * failure rather than a silent fallback.
 */
const SpawnTimeEnvShape = type({
  IPC_CHANNEL_ID: "string > 0",
  IPC_HMAC_KEY: "string > 0",
  HOST_PUBKEY: "string > 0",
  DEPLOYMENT_ID: "string > 0",
  DEFINITION_HASH: "string > 0",
  MAILBOX_ADDRESS: "string > 0",
  // Step count of the deployed `WorkflowDefinition` (`stepOrder.length`),
  // stringified by the supervisor. The child's deploy-tree read collapses
  // onto the head for a single-step deployment (`resolveStepAddress`), so
  // producer and consumer never derive divergent step addresses. Parsed to
  // a positive integer below; a non-integer or non-positive value throws.
  STEP_COUNT: "string > 0",
  // Warm-keep signal (design §3b). The supervisor sets this to the
  // string `"true"` only for the single-step long-lived deployment the
  // deploy projection marked a warm candidate; any other value (or the
  // key's absence) means cold instantiate-send-teardown per message.
  // Carried explicitly rather than re-derived heuristically in the child
  // so the warm-keep decision is deterministic and a multi-step agent is
  // never warm-kept by a silent default.
  "WARM_KEEP?": "string",
  // JSON object mapping each referenced onTrigger body id to the hub-approved
  // wire hash of that body's projection. The sidecar's deploy router injects
  // it (via the substrate env) from the deploy frame's per-body approved
  // hashes so a body child can re-verify its own recompute against the hub
  // authority. Optional: only an onTrigger deploy that carried referenced
  // bodies with approved hashes sets it; absent otherwise. Parsed to a record
  // below; malformed JSON or a non-string value throws.
  "REFERENCED_DEFINITION_HASHES?": "string",
  // Deployment lineage marker selecting the child's definition load path.
  // `"source-ref"` for a deployment whose definition was sourced from a pinned
  // code closure the sidecar materialized; the child evaluates that closure to
  // a LIVE definition and re-verifies it by project-then-hash. Absent (or
  // `"live-authored"`) means the child reads the inert `workflow.json` off the
  // deploy tree. Optional so a live-authored deployment, which ships no marker,
  // still parses; parsed to the `lineage` field below and cross-checked against
  // CLOSURE_PACKAGE_DIR.
  "WORKFLOW_LINEAGE?": "string > 0",
  // Sidecar-local directory of the materialized workflow-definition closure a
  // source-ref deployment evaluates. The sidecar computes it when it applies
  // the frozen closure and threads it here; it never travels on the hub deploy
  // frame. Present iff the lineage is `"source-ref"`; a mismatch between the
  // two throws below.
  "CLOSURE_PACKAGE_DIR?": "string > 0",
}).onUndeclaredKey("ignore");

/**
 * Parsed and validated spawn-time env. The hex-encoded trust anchors
 * decode to their raw byte representations so the IPC channel
 * constructors can consume them without re-validating the hex shape.
 */
export interface SpawnTimeEnv {
  /** Channel identifier minted by the supervisor for this spawn. */
  channelId: string;
  /** 32-byte shared HMAC key for the event channel. */
  hmacKey: Uint8Array;
  /** Supervisor's 32-byte Ed25519 public key for control-frame verification. */
  hostPublicKey: Uint8Array;
  /** Anchor run id the supervisor manages. */
  anchorRunId: string;
  /**
   * Content hash of the deployed `WorkflowDefinition`. This is the
   * hub-approved wire hash the deploy frame carried
   * (`AgentDeployWorkflow.approvedWireHash`), not a sidecar recompute -- the
   * hub is the authority, so the child re-verifies its own recompute against
   * this value.
   */
  definitionHash: string;
  /**
   * Hub-approved wire hash per referenced onTrigger body id. Empty when the
   * deployment carried no referenced bodies (or none with an approved hash).
   * A body child re-verifies its body projection recompute against the entry
   * keyed by the body id.
   */
  referencedDefinitionHashes: Record<string, string>;
  /** Mail address the deployment registered on the bus. */
  mailboxAddress: string;
  /**
   * Number of steps in the deployed `WorkflowDefinition`
   * (`stepOrder.length`). Selects the head/step collapse in the sidecar's
   * `resolveStepAddress`: a single-step deployment reads its deploy tree
   * at the head, a multi-step deployment at the per-step address.
   */
  stepCount: number;
  /**
   * Whether this deployment's agent is warm-kept across messages (design
   * §3b). True only for the single-step long-lived deployment the deploy
   * projection marked a warm candidate; the run-loop builds a warm-agent
   * cache when set and keeps cold instantiate-send-teardown otherwise.
   */
  warmKeep: boolean;
  /**
   * Definition lineage selecting the run child's load path. `"source-ref"`
   * evaluates the pinned code closure at `closurePackageDir` to a live
   * definition and re-verifies it by project-then-hash; `"live-authored"`
   * reads the inert `workflow.json` off the deploy tree. Absent marker parses
   * as `"live-authored"`.
   */
  lineage: "source-ref" | "live-authored";
  /**
   * Sidecar-local directory of the materialized workflow-definition closure.
   * Defined iff `lineage` is `"source-ref"`; `undefined` for a live-authored
   * deployment.
   */
  closurePackageDir: string | undefined;
}

/**
 * Parse and validate `process.env`-shaped input into the typed
 * `SpawnTimeEnv` struct. Any missing key, malformed hex, or off-size
 * byte payload throws so the binary aborts before opening IPC.
 *
 * The validator runs at the boundary; downstream consumers trust the
 * parsed struct without re-checking.
 */
export function parseSpawnTimeEnv(
  rawEnv: Record<string, string | undefined>,
): SpawnTimeEnv {
  const present: Record<string, string> = {};
  for (const [key, value] of Object.entries(rawEnv)) {
    if (value !== undefined) present[key] = value;
  }
  const validated = SpawnTimeEnvShape(present);
  if (validated instanceof type.errors) {
    throw new Error(
      `workflow-child spawn-time env failed validation: ${validated.summary}`,
    );
  }
  const hmacKey = hexDecode(validated.IPC_HMAC_KEY);
  if (hmacKey.length !== IPC_CRYPTO.HMAC_KEY_BYTES) {
    throw new Error(
      `workflow-child IPC_HMAC_KEY must decode to ${String(IPC_CRYPTO.HMAC_KEY_BYTES)} bytes; got ${String(hmacKey.length)}`,
    );
  }
  const hostPublicKey = hexDecode(validated.HOST_PUBKEY);
  if (hostPublicKey.length !== IPC_CRYPTO.ED25519_KEY_BYTES) {
    throw new Error(
      `workflow-child HOST_PUBKEY must decode to ${String(IPC_CRYPTO.ED25519_KEY_BYTES)} bytes; got ${String(hostPublicKey.length)}`,
    );
  }
  // The channelId is supervisor-minted and the receiver compares it
  // byte-for-byte against incoming frames. Hex-decoding here would
  // surface a malformed value but the IPC primitives expect the
  // hex-encoded string form, so we only verify the encoded length
  // matches the documented channelId byte width.
  const expectedChannelIdHex = IPC_CRYPTO.CHANNEL_ID_BYTES * 2;
  if (validated.IPC_CHANNEL_ID.length !== expectedChannelIdHex) {
    throw new Error(
      `workflow-child IPC_CHANNEL_ID must be ${String(expectedChannelIdHex)} hex chars; got ${String(validated.IPC_CHANNEL_ID.length)}`,
    );
  }
  const stepCount = Number(validated.STEP_COUNT);
  if (!Number.isInteger(stepCount) || stepCount <= 0) {
    throw new Error(
      `workflow-child STEP_COUNT must be a positive integer; got ${JSON.stringify(validated.STEP_COUNT)}`,
    );
  }
  const referencedDefinitionHashes = parseReferencedDefinitionHashes(
    validated.REFERENCED_DEFINITION_HASHES,
  );
  const { lineage, closurePackageDir } = parseLineage(
    validated.WORKFLOW_LINEAGE,
    validated.CLOSURE_PACKAGE_DIR,
  );
  return {
    channelId: validated.IPC_CHANNEL_ID,
    hmacKey,
    hostPublicKey,
    anchorRunId: validated.DEPLOYMENT_ID,
    definitionHash: validated.DEFINITION_HASH,
    referencedDefinitionHashes,
    mailboxAddress: validated.MAILBOX_ADDRESS,
    stepCount,
    // Strict `=== "true"` so any other value (including the key's
    // absence) reads false. Warm-keep is opt-in and deterministic; a
    // typo'd or partial value must not silently enable it.
    warmKeep: validated.WARM_KEEP === "true",
    lineage,
    closurePackageDir,
  };
}

/**
 * Resolve the deployment lineage and its closure package directory from the
 * two optional spawn-env keys, cross-checking them so an inconsistent pair
 * fails closed rather than loading the wrong definition path.
 *
 * An absent `WORKFLOW_LINEAGE` marker is the live-authored common case. A
 * source-ref lineage MUST carry `CLOSURE_PACKAGE_DIR` (there is nothing to
 * evaluate without it); a live-authored lineage must NOT carry one (only a
 * source-ref deployment materializes a closure). Any other pairing is a
 * boundary wiring bug and throws.
 */
function parseLineage(
  rawLineage: string | undefined,
  rawClosurePackageDir: string | undefined,
): {
  lineage: "source-ref" | "live-authored";
  closurePackageDir: string | undefined;
} {
  let lineage: "source-ref" | "live-authored";
  if (rawLineage === undefined || rawLineage === "live-authored") {
    lineage = "live-authored";
  } else if (rawLineage === "source-ref") {
    lineage = "source-ref";
  } else {
    throw new Error(
      `workflow-child WORKFLOW_LINEAGE must be "source-ref" or "live-authored"; got ${JSON.stringify(rawLineage)}`,
    );
  }
  if (lineage === "source-ref" && rawClosurePackageDir === undefined) {
    throw new Error(
      "workflow-child source-ref deployment requires CLOSURE_PACKAGE_DIR; the sidecar must thread the materialized closure package directory",
    );
  }
  if (lineage === "live-authored" && rawClosurePackageDir !== undefined) {
    throw new Error(
      "workflow-child live-authored deployment must not carry CLOSURE_PACKAGE_DIR; only a source-ref deployment evaluates a closure",
    );
  }
  return {
    lineage,
    closurePackageDir:
      lineage === "source-ref" ? rawClosurePackageDir : undefined,
  };
}

/** A JSON object of `bodyId -> approved wire hash`, each a non-empty string. */
const ReferencedDefinitionHashesShape = type({
  "[string]": "string > 0",
});

/**
 * Parse the `REFERENCED_DEFINITION_HASHES` env value into a validated
 * `bodyId -> approvedWireHash` record. An absent value is the common case (a
 * deployment with no referenced onTrigger bodies) and yields an empty record.
 * A present value must be a JSON object whose every value is a non-empty
 * string; malformed JSON or an off-shape object throws so the child aborts
 * before it trusts an unparseable per-body hash map.
 */
function parseReferencedDefinitionHashes(
  raw: string | undefined,
): Record<string, string> {
  if (raw === undefined) return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      "workflow-child REFERENCED_DEFINITION_HASHES must be valid JSON",
      { cause },
    );
  }
  const validated = ReferencedDefinitionHashesShape(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `workflow-child REFERENCED_DEFINITION_HASHES failed validation: ${validated.summary}`,
    );
  }
  return validated;
}
