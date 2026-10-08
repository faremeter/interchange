// Host-side workflow-probe handler: spawns and supervises a one-shot probe child.
//
// A `workflow.probe.request` frame asks this sidecar to inspect a
// code-sourced workflow WITHOUT deploying it. The inspection evaluates
// author code (the workflow package's `interchange.workflow` entry), so
// it must never run in the sidecar host's address space. This module
// spawns a ONE-SHOT child process behind the IPC airlock that loads and
// evaluates the entry, runs the capability walk plus the live->inert
// projector, and ships the inert projection + advisory grant set + wire
// hash back over an HMAC-authenticated result frame (reusing the same
// per-frame HMAC discipline `@intx/workflow-host`'s event channel uses).
//
// Reaping is sidecar-owned and independent of the hub's probe timeout.
// The hub timeout only rejects the hub-side promise; it does not kill
// the child. `runOneShotProbeChild` owns a self-contained lifecycle with
// its OWN deadline and reaps the child on every exit path -- eval
// success, eval throw, malformed code, and a probe that outruns the
// self-owned deadline -- so a wedged or runaway child can never survive
// the probe call.
//
// The frozen dependency closure is materialized sidecar-side (host,
// no eval) through the injected `MaterializeWorkflowClosure` seam; only
// the load+evaluate+walk+project step runs in the child. The child
// receives the materialized package directory in its fresh, minimal env
// -- no ambient inputs, no sidecar keys.

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { hexDecode } from "@intx/types/hex";
import {
  WorkflowProjectionDefinition,
  type WorkflowProbeRequestFrame,
} from "@intx/types/sidecar";

import { MacedEnvelope, encodeEnvelope } from "../ipc/envelope";
import { generateChannelId, generateHmacKey, verifyHmac } from "../ipc/crypto";
import { DEFAULT_KILL_TIMEOUT_MS } from "../supervisor/child-termination";
import { buildProbeChildEnv } from "./env";
import { errorMessage } from "./errors";
import { ProbeResultPayload, type WorkflowProbeResult } from "./protocol";

export type { WorkflowProbeResult } from "./protocol";

const logger = getLogger(["sidecar", "workflow-probe"]);

/**
 * Self-owned upper bound on how long a single probe child may run before
 * the sidecar reaps it and fails the probe. Independent of the hub's
 * `probeTimeoutMs`: the hub timeout only rejects the hub-side promise,
 * whereas this deadline is what actually kills a wedged child.
 */
export const DEFAULT_PROBE_CHILD_TIMEOUT_MS = 30_000;

/**
 * Self-owned SIGTERM->SIGKILL escalation window when reaping the child.
 * Mirrors the supervisor's `DEFAULT_KILL_TIMEOUT_MS` so a probe child
 * that ignores SIGTERM is force-killed on the same schedule a supervised
 * child is.
 */
export const DEFAULT_PROBE_CHILD_KILL_TIMEOUT_MS = DEFAULT_KILL_TIMEOUT_MS;

// ---------------------------------------------------------------------------
// Closure materialization seam
// ---------------------------------------------------------------------------

/**
 * A materialized workflow package closure: the directory holding the
 * workflow package's `package.json` (with its `node_modules/` laid out
 * so the entry's bare-specifier imports resolve), plus a `cleanup` the
 * handler always calls once the child has been reaped.
 */
export interface MaterializedWorkflowClosure {
  readonly packageDir: string;
  cleanup(): Promise<void>;
}

/**
 * Host-side materializer for a probe frame's frozen closure. Fetches,
 * verifies, extracts, and lays out the workflow package (and its
 * dependency closure) into a resolvable tree, returning the package
 * directory the child loads from. This runs on the sidecar host -- it is
 * I/O, not author-code evaluation -- so the airlocked child only performs
 * the load+evaluate step. The production materializer is host-supplied so
 * `@intx/workflow-host` stays free of a `@intx/tool-packaging` dependency.
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
 * Spawner the handler invokes to launch the one-shot probe child.
 * The sidecar process passes a `Bun.spawn` spawner. Tests pass a spawner
 * that records the spawned pid so they can assert the child was reaped.
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
 *
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
    // exits promptly, so `handle.exited` and the buffered-line read both become
    // ready, and an exit arm winning that race would discard an already-written
    // result and fail the probe spuriously. Exit is not a distinct outcome the
    // line read misses -- when the child exits its stdout write end closes, so
    // `readResultLine` settles either with the trailing line (returned below)
    // or null (the "closed its output" case). A child that neither writes nor
    // exits is still caught by the deadline.
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

/**
 * Reap a probe child: SIGTERM, then SIGKILL if the exit does not land
 * within `killTimeoutMs`. SIGKILL is unignorable, so `exited` is
 * guaranteed to settle -- a child that traps or ignores SIGTERM cannot
 * wedge this call. A kill against an already-exited child is a no-op.
 */
async function reapChild(
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
 */
async function parseProbeResult(
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
// Shared helpers
// ---------------------------------------------------------------------------

/**
 * Read one newline-delimited line from a byte stream. Resolves the first
 * complete line, or `null` when the stream closes without one (the child
 * exited before writing). Releases the reader lock on every exit.
 */
async function readResultLine(
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
