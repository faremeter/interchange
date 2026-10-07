// Sidecar workflow-probe handler: the airlocked one-shot probe child.
//
// A `workflow.probe.request` frame asks this sidecar to inspect a
// code-sourced workflow WITHOUT deploying it. The inspection evaluates
// author code (the workflow package's `interchange.workflow` entry), so
// it must never run in the sidecar host's address space. This module
// spawns a ONE-SHOT child process behind the IPC airlock that loads and
// evaluates the entry, runs the capability walk plus the live->inert
// projector, and ships the inert projection + advisory grant set + wire
// hash back over an HMAC-authenticated result frame (reusing the same
// per-frame HMAC discipline the event channel uses).
//
// Reaping is sidecar-owned and independent of the hub's probe timeout,
// which only rejects the hub-side promise. `runOneShotProbeChild` owns a
// self-contained lifecycle with its OWN deadline and reaps the child on
// every exit path -- eval success, eval throw, malformed code, or a probe
// that outruns the deadline -- so a wedged or runaway child can never
// survive the probe call.
//
// The frozen dependency closure is materialized sidecar-side (host, no
// eval) through the injected `MaterializeWorkflowClosure` seam; only the
// load+evaluate+walk+project step runs in the child, which receives the
// materialized package directory in a fresh, minimal env -- no ambient
// inputs, no sidecar keys.

import { type } from "arktype";

import type { DirectorRegistry, ToolDeclaration } from "@intx/agent";
import { getLogger } from "@intx/log";
import {
  GrantWalkSnapshot,
  hexDecode,
  hexEncode,
  type GrantEffect,
  type GrantRequirement,
} from "@intx/types";
import type { WorkflowProbeRequestFrame } from "@intx/types/sidecar";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import { collectDeclaredPluginNames, projectLiveToInert } from "@intx/workflow";
import type { WorkflowDefinition } from "@intx/workflow/definition";

import {
  MacedEnvelope,
  encodeEnvelope,
  generateChannelId,
  generateHmacKey,
  signHmac,
  verifyHmac,
  type FrameEnvelope,
} from "../ipc";
import { DEFAULT_KILL_TIMEOUT_MS } from "../supervisor/index";
import {
  loadWorkflowDefinitionFromClosure,
  loadWorkflowDirectorRegistryFromClosure,
  loadWorkflowPluginToolDefinitionsFromClosure,
} from "../workflow-definition-loader";

const logger = getLogger(["sidecar", "workflow-probe"]);

const IPC_HMAC_KEY_BYTES = 32;

/**
 * Self-owned upper bound on how long a single probe child may run before
 * the sidecar reaps it and fails the probe. Independent of the hub's
 * `probeTimeoutMs`, which only rejects the hub-side promise; this
 * deadline is what actually kills a wedged child.
 */
export const DEFAULT_PROBE_CHILD_TIMEOUT_MS = 30_000;

/**
 * Self-owned SIGTERM->SIGKILL escalation window when reaping the child.
 * Mirrors the supervisor's `DEFAULT_KILL_TIMEOUT_MS` so a probe child
 * that ignores SIGTERM is force-killed on the same schedule a supervised
 * child is.
 */
export const DEFAULT_PROBE_CHILD_KILL_TIMEOUT_MS = DEFAULT_KILL_TIMEOUT_MS;

// Env keys the host sets on the child's fresh spawn env. The child reads
// exactly these plus PATH/HOME/TMPDIR (for exec + tmp); nothing else
// crosses the airlock.
const PROBE_CHANNEL_ID_ENV = "PROBE_IPC_CHANNEL_ID";
const PROBE_HMAC_KEY_ENV = "PROBE_IPC_HMAC_KEY";
const PROBE_PACKAGE_DIR_ENV = "PROBE_PACKAGE_DIR";

/**
 * Capability walk the probe child runs over the evaluated definition.
 * The process that boots the probe passes the real walk. This module
 * does not import it.
 */
export type WalkCapabilities = (
  workflow: WorkflowDefinition,
  registry: DirectorRegistry,
  pluginDefs: ReadonlyMap<string, readonly ToolDeclaration[]>,
) => {
  readonly perStep: ReadonlyMap<
    string,
    {
      readonly grants: readonly string[];
      readonly grantEffects: ReadonlyMap<string, GrantEffect>;
    }
  >;
  readonly unresolvedDirectors: readonly string[];
};

// ---------------------------------------------------------------------------
// Result payload wire (child -> host)
// ---------------------------------------------------------------------------

/**
 * The child's single result payload, carried inside the HMAC-signed
 * envelope. `ok: true` ships the inert projection, the advisory grant
 * set, the un-flattened grant walk snapshot, and the wire hash; `ok:
 * false` ships the failure reason (eval throw, malformed code) so the
 * host can reject the probe with a meaningful message rather than a bare
 * "child exited" surface.
 */ const ProbeResultPayload = type({
  ok: "true",
  projection: "unknown",
  grants: "string[]",
  grantWalkSnapshot: GrantWalkSnapshot,
  wireHash: "string > 0",
}).or({
  ok: "false",
  error: "string",
});
type ProbeResultPayload = typeof ProbeResultPayload.infer;

/**
 * The inert answer a probe execution produces: the workflow's inert
 * needs-surface projection, the advisory grant set derived from it, the
 * un-flattened grant walk snapshot the set is derived from, and the
 * projection's content hash. Structurally the `WorkflowProbeResult` the
 * hub-agent probe seam consumes.
 *
 * `grantWalkSnapshot` carries the per-step grant declarations (grant
 * strings plus each step's tool-grant `grantEffects` map) and the
 * definition's full `grantRequirements` -- the grouping and effect data
 * the flattened `grants` union discards.
 */
export interface WorkflowProbeResult {
  readonly projection: WorkflowProjectionDefinition;
  readonly grants: string[];
  readonly grantWalkSnapshot: GrantWalkSnapshot;
  readonly wireHash: string;
}

// ---------------------------------------------------------------------------
// Closure materialization seam
// ---------------------------------------------------------------------------

/**
 * A materialized workflow package closure: the directory holding the
 * workflow package's `package.json` (with `node_modules/` laid out so the
 * entry's bare-specifier imports resolve), plus a `cleanup` the handler
 * always calls once the child has been reaped.
 */
export interface MaterializedWorkflowClosure {
  readonly packageDir: string;
  cleanup(): Promise<void>;
}

/**
 * Host-side materializer for a probe frame's frozen closure. Fetches,
 * verifies, extracts, and lays out the workflow package (and its
 * dependency closure) into a resolvable tree, returning the package
 * directory the child loads from. Runs on the sidecar host -- it is I/O,
 * not author-code evaluation -- so the airlocked child only performs the
 * load+evaluate step. Host-supplied so `@intx/workflow-host` stays free
 * of a `@intx/tool-packaging` dependency.
 */
export type MaterializeWorkflowClosure = (
  frame: WorkflowProbeRequestFrame,
) => Promise<MaterializedWorkflowClosure>;

// ---------------------------------------------------------------------------
// Child spawn seam
// ---------------------------------------------------------------------------

/**
 * Minimal handle over a spawned probe child. The probe needs only the
 * child's stdout (the single result line), a kill primitive, and the
 * `exited` promise for reaping -- no control/event channels, because the
 * probe carries no bidirectional control traffic.
 */
export interface ProbeChildHandle {
  readonly pid: number;
  readonly stdout: ReadableStream<Uint8Array>;
  kill(signal?: number | string): void;
  readonly exited: Promise<number>;
}

/**
 * Spawner the handler invokes to launch the one-shot probe child. The
 * sidecar process passes a `Bun.spawn` spawner; tests pass one that
 * records the spawned pid so they can assert the child was reaped.
 */
export type ProbeChildSpawner = (args: {
  binaryPath: string;
  env: Record<string, string>;
}) => ProbeChildHandle;

// ---------------------------------------------------------------------------
// Executor (host side)
// ---------------------------------------------------------------------------

export interface WorkflowProbeExecutorOpts {
  /** Host-side materializer for the frame's frozen closure. */
  materialize: MaterializeWorkflowClosure;
  /** Child spawner. The sidecar process passes its `Bun.spawn` spawner. */
  spawnProbeChild: ProbeChildSpawner;
  /** Path of `bin/workflow-probe-child`. The sidecar process resolves it. */
  binaryPath: string;
  /**
   * Self-owned deadline before the child is reaped and the probe fails.
   * Independent of the hub's `probeTimeoutMs`.
   */
  childTimeoutMs?: number;
  /** SIGTERM->SIGKILL escalation window when reaping. */
  killTimeoutMs?: number;
}

/**
 * Build the sidecar's workflow-probe executor. The returned object
 * satisfies the hub-agent `WorkflowProbeExecutor` seam: `probe(frame)`
 * returns the inert projection + advisory grant set + wire hash, and
 * throws when any step fails so the link answers `workflow.probe.error`.
 * `probe` materializes the frozen closure, spawns a one-shot airlocked
 * child to evaluate the workflow, and reaps that child on every exit
 * path independent of the hub's probe timeout.
 */
export function createWorkflowProbeExecutor(opts: WorkflowProbeExecutorOpts): {
  probe(frame: WorkflowProbeRequestFrame): Promise<WorkflowProbeResult>;
} {
  const spawnProbeChild = opts.spawnProbeChild;
  const binaryPath = opts.binaryPath;
  const childTimeoutMs = opts.childTimeoutMs ?? DEFAULT_PROBE_CHILD_TIMEOUT_MS;
  const killTimeoutMs =
    opts.killTimeoutMs ?? DEFAULT_PROBE_CHILD_KILL_TIMEOUT_MS;

  async function probe(
    frame: WorkflowProbeRequestFrame,
  ): Promise<WorkflowProbeResult> {
    const materialized = await opts.materialize(frame);
    try {
      return await runOneShotProbeChild({
        packageDir: materialized.packageDir,
        spawnProbeChild,
        binaryPath,
        childTimeoutMs,
        killTimeoutMs,
      });
    } finally {
      await materialized.cleanup();
    }
  }

  return { probe };
}

interface RunOneShotProbeChildArgs {
  readonly packageDir: string;
  readonly spawnProbeChild: ProbeChildSpawner;
  readonly binaryPath: string;
  readonly childTimeoutMs: number;
  readonly killTimeoutMs: number;
}

/**
 * Spawn a single probe child, drive it to its one result frame, and reap
 * it on every exit path. The `finally` guarantees the child is killed
 * whether the read succeeds, the child ships an error frame, the child
 * exits without a frame (malformed code / crash), or the self-owned
 * deadline fires first.
 */
async function runOneShotProbeChild(
  args: RunOneShotProbeChildArgs,
): Promise<WorkflowProbeResult> {
  const channelId = generateChannelId();
  const hmacKey = generateHmacKey();
  const env = buildProbeChildEnv({
    packageDir: args.packageDir,
    channelId,
    hmacKey,
  });
  const handle = args.spawnProbeChild({ binaryPath: args.binaryPath, env });

  let reaped = false;
  async function reap(): Promise<void> {
    if (reaped) return;
    reaped = true;
    await reapChild(handle, args.killTimeoutMs);
  }

  // Attach a catch so a post-reap stdout read error (the kill closes the
  // pipe mid-read) resolves to null instead of surfacing as an unhandled
  // rejection on the losing race branch.
  const linePromise: Promise<string | null> = readResultLine(
    handle.stdout,
  ).catch((err: unknown) => {
    logger.debug`probe child ${String(handle.pid)} stdout read errored: ${errorMessage(err)}`;
    return null;
  });

  const deadline = createDeadline(args.childTimeoutMs);
  try {
    // Race the result line against the deadline ONLY. Child exit is
    // deliberately not a race arm: a child writes its result line and then
    // exits promptly, so `handle.exited` and the buffered-line read both
    // become ready, and an exit arm winning that race would discard an
    // already-written result and fail the probe spuriously. Exit is not a
    // distinct outcome the line read misses -- when the child exits its
    // stdout write end closes, so `readResultLine` settles either with the
    // trailing line (returned below) or null (the "closed its output"
    // case). A child that neither writes nor exits is still caught by the
    // deadline.
    const outcome = await Promise.race([
      linePromise.then((line) => ({ kind: "line" as const, line })),
      deadline.promise.then(() => ({ kind: "timeout" as const })),
    ]);

    if (outcome.kind === "timeout") {
      throw new Error(
        `workflow probe child ${String(handle.pid)} did not produce a result within ${String(args.childTimeoutMs)}ms`,
      );
    }
    if (outcome.line === null) {
      throw new Error(
        `workflow probe child ${String(handle.pid)} closed its output without producing a result`,
      );
    }
    return await parseProbeResult(outcome.line, channelId, hmacKey);
  } finally {
    deadline.cancel();
    await reap();
  }
}

function buildProbeChildEnv(args: {
  packageDir: string;
  channelId: string;
  hmacKey: Uint8Array;
}): Record<string, string> {
  // A fresh, minimal env: exactly the IPC anchors and the materialized
  // package dir, plus the OS handles the shebang needs to exec `bun` and
  // land temp files on the host's temp root. No `process.env` spread, so
  // no sidecar secret or ambient input crosses the airlock.
  const env: Record<string, string> = {
    [PROBE_CHANNEL_ID_ENV]: args.channelId,
    [PROBE_HMAC_KEY_ENV]: hexEncode(args.hmacKey),
    [PROBE_PACKAGE_DIR_ENV]: args.packageDir,
  };
  const path = process.env["PATH"];
  if (path !== undefined) env["PATH"] = path;
  const home = process.env["HOME"];
  if (home !== undefined) env["HOME"] = home;
  const tmpdir = process.env["TMPDIR"];
  if (tmpdir !== undefined) env["TMPDIR"] = tmpdir;
  return env;
}

/**
 * Reap a probe child: SIGTERM, then SIGKILL if the exit does not land
 * within `killTimeoutMs`. SIGKILL is unignorable, so `exited` is
 * guaranteed to settle -- a child that traps or ignores SIGTERM cannot
 * wedge this call. A kill against an already-exited child is a no-op.
 */ async function reapChild(
  handle: ProbeChildHandle,
  killTimeoutMs: number,
): Promise<void> {
  try {
    handle.kill("SIGTERM");
  } catch (err) {
    logger.debug`probe child ${String(handle.pid)} SIGTERM raised (already exited?): ${errorMessage(err)}`;
  }
  const deadline = createDeadline(killTimeoutMs);
  const first = await Promise.race([
    handle.exited.then(() => "exited" as const),
    deadline.promise.then(() => "deadline" as const),
  ]);
  deadline.cancel();
  if (first === "exited") return;
  logger.warn`workflow probe child ${String(handle.pid)} did not exit on SIGTERM within ${String(killTimeoutMs)}ms; escalating to SIGKILL`;
  try {
    handle.kill("SIGKILL");
  } catch (err) {
    logger.debug`probe child ${String(handle.pid)} SIGKILL raised (already exited?): ${errorMessage(err)}`;
  }
  await handle.exited.catch(() => {
    // A non-zero exit on SIGKILL is the expected outcome; reaping treats
    // child exit as success regardless of code.
  });
}

/**
 * Authenticate and parse the child's single result frame. Verifies the
 * HMAC over the re-encoded envelope BEFORE trusting any field (mirroring
 * the event channel's receiver), binds the frame to this spawn's
 * channelId, then narrows the payload. A `ok: false` payload is turned
 * into a throw so the probe fails with the child's reason.
 */ async function parseProbeResult(
  line: string,
  channelId: string,
  hmacKey: Uint8Array,
): Promise<WorkflowProbeResult> {
  let raw: unknown;
  try {
    raw = JSON.parse(line);
  } catch (cause) {
    throw new Error("workflow probe child result is not valid JSON", { cause });
  }
  const maced = MacedEnvelope(raw);
  if (maced instanceof type.errors) {
    throw new Error(
      `workflow probe child result envelope failed validation: ${maced.summary}`,
    );
  }
  const envelopeBytes = encodeEnvelope(maced.envelope);
  const macBytes = hexDecode(maced.mac);
  const ok = await verifyHmac(envelopeBytes, macBytes, hmacKey);
  if (!ok) {
    throw new Error(
      `workflow probe child result HMAC did not verify (channelId=${maced.envelope.channelId})`,
    );
  }
  if (maced.envelope.channelId !== channelId) {
    throw new Error(
      `workflow probe child result carried a foreign channelId ${JSON.stringify(maced.envelope.channelId)}`,
    );
  }
  const payload = ProbeResultPayload(maced.envelope.payload);
  if (payload instanceof type.errors) {
    throw new Error(
      `workflow probe child result payload failed validation: ${payload.summary}`,
    );
  }
  if (!payload.ok) {
    throw new Error(`workflow probe evaluation failed: ${payload.error}`);
  }
  const projection = WorkflowProjectionDefinition(payload.projection);
  if (projection instanceof type.errors) {
    throw new Error(
      `workflow probe child projection failed validation: ${projection.summary}`,
    );
  }
  return {
    projection,
    grants: payload.grants,
    grantWalkSnapshot: payload.grantWalkSnapshot,
    wireHash: payload.wireHash,
  };
}

// ---------------------------------------------------------------------------
// Child side
// ---------------------------------------------------------------------------

/**
 * One line the child writes to a sink. Production wraps `process.stdout`;
 * tests inject a capture. The bytes are handed to the OS before the child
 * exits so the result is not truncated.
 */
export type ProbeChildLineWriter = (line: string) => Promise<void>;

export interface RunProbeChildOpts {
  /** Raw env the child reads its anchors from (defaults to `process.env`). */
  rawEnv?: Readonly<Record<string, string | undefined>>;
  /** Result-line sink (defaults to a drained `process.stdout` write). */
  writeLine?: ProbeChildLineWriter;
}

/**
 * The airlocked child's whole job: read the materialized package dir and
 * IPC anchors from its fresh env, load+evaluate the workflow entry, run
 * the capability walk plus the live->inert projector, and ship the inert
 * projection + advisory grant set + wire hash back inside one
 * HMAC-signed result frame.
 *
 * An evaluation failure (malformed code, an entry that throws, a package
 * with no `interchange.workflow`) is caught and shipped as an `ok: false`
 * frame so the host reaps cleanly and answers `workflow.probe.error`
 * with the reason -- rather than the child crashing and the host seeing a
 * bare "exited without result".
 */
export async function runWorkflowProbeChildFromProcessEnv(
  walkCapabilities: WalkCapabilities,
  opts: RunProbeChildOpts = {},
): Promise<void> {
  const rawEnv = opts.rawEnv ?? process.env;
  const writeLine = opts.writeLine ?? defaultStdoutWriteLine;
  const { channelId, hmacKey, packageDir } = parseProbeChildEnv(rawEnv);

  let payload: ProbeResultPayload;
  try {
    payload = await computeProbePayload(packageDir, walkCapabilities);
  } catch (err) {
    payload = { ok: false, error: enrichProbeError(err) };
  }

  const envelope: FrameEnvelope = { seq: 0, channelId, payload };
  const envelopeBytes = encodeEnvelope(envelope);
  const mac = hexEncode(await signHmac(envelopeBytes, hmacKey));
  await writeLine(`${JSON.stringify({ envelope, mac })}\n`);
}

async function computeProbePayload(
  packageDir: string,
  walkCapabilities: WalkCapabilities,
): Promise<ProbeResultPayload> {
  const definition = await loadWorkflowDefinitionFromClosure({ packageDir });
  const projection = projectLiveToInert(definition);
  const wireHash = await computeWireDefinitionHash(projection);
  // Compose the director registry from the SAME closure the run-child
  // will, so the `director:<id>` grants advertised here match what the
  // runtime resolves. Built-ins-only when the closure ships no
  // `interchange.directors`.
  const directors = await loadWorkflowDirectorRegistryFromClosure({
    packageDir,
  });
  // Load the static tool `definitions` each declared plugin package
  // contributes from the SAME closure, so the walk emits `tool:<name>`
  // grants for plugin-contributed tools (Tier-2 governance). A plugin
  // package reaches an agent only through `env.plugins`, so its tool
  // grant surface is invisible to the walk otherwise -- the run-child
  // would then load the plugin from the closure and the reactor would
  // fail closed on an un-approved `tool:<name>`. Loading here (over the
  // frozen closure the run-child also materializes from) keeps the
  // approved snapshot and the runtime plugin in lockstep.
  const pluginToolDefinitions =
    await loadWorkflowPluginToolDefinitionsFromClosure({
      packageDir,
      plugins: collectDeclaredPluginNames(definition),
    });
  const walk = walkCapabilities(definition, directors, pluginToolDefinitions);
  // Fail closed on an unresolved director: the runtime does not re-gate
  // `director:<id>` against the approved grant set, so this advertisement
  // is the only approval checkpoint for a director. Shipping an ok probe
  // whose grant set silently omits a director the runtime would still try
  // to resolve would let the operator approve an incomplete manifest.
  // Mirrors the live-authored approval gate (`createApprovalSetGate`).
  const [unresolved] = walk.unresolvedDirectors;
  if (unresolved !== undefined) {
    return { ok: false, error: `unresolvable director: ${unresolved}` };
  }
  return {
    ok: true,
    projection,
    grants: collectDeploymentGrants(walk),
    grantWalkSnapshot: buildGrantWalkSnapshot(
      walk,
      definition.grantRequirements,
    ),
    wireHash,
  };
}

/**
 * Flatten the per-step walk output into the deployment-wide advisory
 * grant set: the deduplicated, sorted union of every step's grant
 * strings. Sorting makes the shipped set order-independent.
 */ function collectDeploymentGrants(
  walk: ReturnType<WalkCapabilities>,
): string[] {
  const grants = new Set<string>();
  for (const declarations of walk.perStep.values()) {
    for (const grant of declarations.grants) {
      grants.add(grant);
    }
  }
  return [...grants].sort();
}

/**
 * Serialize the un-flattened capability walk into a plain-data
 * `GrantWalkSnapshot`: the per-step grant declarations (each step's grant
 * strings plus its tool-grant `grantEffects` map, converted from the
 * walk's `Map` to a plain object) and the definition's full, unfiltered
 * `grantRequirements`. Unlike `collectDeploymentGrants`, this preserves
 * the per-step grouping and the effect data the flattened set discards.
 * A definition that declares no requirements snapshots an empty list.
 */ function buildGrantWalkSnapshot(
  walk: ReturnType<WalkCapabilities>,
  grantRequirements: readonly GrantRequirement[] | undefined,
): GrantWalkSnapshot {
  const perStep = [...walk.perStep].map(([stepId, declarations]) => ({
    stepId,
    grants: [...declarations.grants],
    grantEffects: Object.fromEntries(declarations.grantEffects),
  }));
  return {
    perStep,
    grantRequirements: [...(grantRequirements ?? [])],
  };
}

interface ProbeChildEnv {
  readonly channelId: string;
  readonly hmacKey: Uint8Array;
  readonly packageDir: string;
}

const NonEmptyString = type("string > 0");

function parseProbeChildEnv(
  rawEnv: Readonly<Record<string, string | undefined>>,
): ProbeChildEnv {
  const channelId = requireEnv(rawEnv, PROBE_CHANNEL_ID_ENV);
  const packageDir = requireEnv(rawEnv, PROBE_PACKAGE_DIR_ENV);
  const hmacKeyHex = requireEnv(rawEnv, PROBE_HMAC_KEY_ENV);
  const hmacKey = hexDecode(hmacKeyHex);
  if (hmacKey.length !== IPC_HMAC_KEY_BYTES) {
    throw new Error(
      `workflow probe child env: ${PROBE_HMAC_KEY_ENV} must decode to ${String(IPC_HMAC_KEY_BYTES)} bytes, got ${String(hmacKey.length)}`,
    );
  }
  return { channelId, hmacKey, packageDir };
}

function requireEnv(
  rawEnv: Readonly<Record<string, string | undefined>>,
  key: string,
): string {
  const value = NonEmptyString(rawEnv[key]);
  if (value instanceof type.errors) {
    throw new Error(
      `workflow probe child env: required key ${key} is unset or empty`,
    );
  }
  return value;
}

function defaultStdoutWriteLine(line: string): Promise<void> {
  return new Promise((resolve, reject) => {
    process.stdout.write(line, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/**
 * Read one newline-delimited line from a byte stream. Resolves the first
 * complete line, or `null` when the stream closes without one (the child
 * exited before writing). Releases the reader lock on every exit.
 */ async function readResultLine(
  stream: ReadableStream<Uint8Array>,
): Promise<string | null> {
  const reader = stream.getReader();
  const decoder = new TextDecoder("utf-8");
  let pending = "";
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (value !== undefined) {
        pending += decoder.decode(value, { stream: true });
        const nl = pending.indexOf("\n");
        if (nl >= 0) {
          return pending.slice(0, nl).replace(/\r$/, "");
        }
      }
      if (done) {
        const trailing = pending.replace(/\r?\n$/, "");
        return trailing.length > 0 ? trailing : null;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

function createDeadline(ms: number): {
  promise: Promise<void>;
  cancel: () => void;
} {
  let handle: ReturnType<typeof setTimeout> | undefined;
  const promise = new Promise<void>((resolve) => {
    handle = setTimeout(resolve, ms);
  });
  return {
    promise,
    cancel(): void {
      if (handle !== undefined) clearTimeout(handle);
    },
  };
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// Node/Bun's module-not-found message shape. The specifier is the missing
// package the workflow entry imported at evaluation time.
const MISSING_MODULE_RE = /Cannot find (?:module|package) ['"]([^'"]+)['"]/;

/**
 * Enrich a probe evaluation failure whose cause is a module that could
 * not be resolved from the workflow's dependency closure. The evaluator
 * actually ran the import, so a missing specifier here means the closure
 * did not carry it -- the common cause is a runtime import declared only
 * under `devDependencies` (which the closure does not materialize),
 * whether a workspace-local member or an external package. Rewrite the
 * opaque "Cannot find module" into that actionable diagnostic. A
 * non-resolution failure passes through unchanged.
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
