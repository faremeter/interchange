// Bun-backed spawn edge for the workflow child.
//
// The deploy program takes a spawner and a binary path as required
// arguments. This module is the process that owns both: it resolves
// `bin/workflow-child` and launches it in its own process group.

import { fileURLToPath } from "node:url";

import { getLogger } from "@intx/log";
import type {
  FrameReader,
  NdjsonReader,
  NdjsonWriter,
  SubprocessHandle,
  SubprocessSpawner,
} from "@intx/workflow-host";

const logger = getLogger(["interchange", "sidecar", "workflow-host-wiring"]);

/**
 * Path of this package's `bin/workflow-child`. Resolved at load so the
 * boot edge can hand the deploy program a concrete path.
 */
export const SIDECAR_WORKFLOW_CHILD_BINARY: string = (() => {
  const url = import.meta.resolve("../bin/workflow-child");
  return fileURLToPath(url);
})();

/**
 * Child fd the supervisor inherits the event-channel pipe on. The
 * supervisor's spawn-time convention is:
 *
 *   fd 0 stdin  -- downstream control channel (supervisor -> child)
 *   fd 1 stdout -- upstream control channel (child -> supervisor)
 *   fd 2 stderr -- inherited so child diagnostics land on the
 *                  sidecar's stderr
 *   fd 3        -- event-channel write side (child writes
 *                  HMAC-authenticated InferenceEvent frames here;
 *                  the supervisor reads the parent end as a
 *                  `FrameReader`)
 *
 * The child opens fd 3 via `EVENT_CHANNEL_FD` in
 * `@intx/workflow-host`'s `from-process-env`. The two ends of the
 * pipe are provisioned by `Bun.spawn`'s `stdio` slot: setting
 * `stdio[3] = "pipe"` makes Bun mint a pipe pair where the child
 * inherits the write half at fd 3 and the parent receives the read
 * half as a numeric fd at `proc.stdio[3]` in its own address space.
 */
const CHILD_EVENT_CHANNEL_FD = 3;

/**
 * Wrap a Bun `FileSink` as the supervisor's `NdjsonWriter`. The
 * supervisor's control-channel sender writes one JSON line per
 * frame (already including the trailing newline); the writer is
 * responsible for passing the bytes through to the child's stdin
 * without buffering across frames so each frame surfaces on the
 * far side as soon as `write()` resolves.
 */
function ndjsonWriterFromFileSink(sink: Bun.FileSink): NdjsonWriter {
  return {
    async write(line: string): Promise<void> {
      const result = sink.write(line);
      if (typeof result !== "number") await result;
      const flushed = sink.flush();
      if (typeof flushed !== "number") await flushed;
    },
  };
}

/**
 * Wrap a Bun stdout `ReadableStream` as the supervisor's
 * `NdjsonReader`. The pipe is a byte stream; this reader buffers
 * partial chunks and yields one complete line per iteration. The
 * receiver's iterator finalises only on EOF, which mirrors the
 * `defaultControlReader` shape the child wires for `process.stdin`.
 */
function ndjsonReaderFromReadableStream(
  stream: ReadableStream<Uint8Array>,
): NdjsonReader {
  return {
    read(): AsyncIterableIterator<string> {
      return (async function* () {
        const decoder = new TextDecoder("utf-8");
        let pending = "";
        const reader = stream.getReader();
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (value !== undefined) {
              pending += decoder.decode(value, { stream: true });
              let nl = pending.indexOf("\n");
              while (nl >= 0) {
                const line = pending.slice(0, nl).replace(/\r$/, "");
                pending = pending.slice(nl + 1);
                if (line.length > 0) yield line;
                nl = pending.indexOf("\n");
              }
            }
            if (done) break;
          }
          if (pending.length > 0) yield pending;
        } finally {
          reader.releaseLock();
        }
      })();
    },
  };
}

/**
 * Wrap the parent-side read fd of the event-channel pipe as the
 * supervisor's `FrameReader`. The child publishes one HMAC-
 * authenticated envelope per `FileSink.write()` and the supervisor's
 * `receiveEventChannel` parses each yielded `Uint8Array` as one
 * complete envelope. The pipe is a byte stream; this reader yields
 * each raw chunk the kernel delivers and trusts the sender's
 * one-write-per-envelope discipline. The buffer-overflow / framing
 * discipline lives in `receiveEventChannel`'s parser.
 */
function frameReaderFromFd(fd: number): FrameReader {
  const stream = Bun.file(fd).stream();
  return {
    read(): AsyncIterableIterator<Uint8Array> {
      return (async function* () {
        const reader = stream.getReader();
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (value !== undefined && value.byteLength > 0) yield value;
            if (done) break;
          }
        } finally {
          reader.releaseLock();
        }
      })();
    },
  };
}

/**
 * Real `Bun.spawn`-backed subprocess spawner. Constructs a fresh env
 * carrying exactly the trust anchors and substrate-config keys the
 * supervisor passed in (no inheritance of the sidecar's process env);
 * inherits stdio 0/1/2 as control + stderr; pipes fd 3 for the event
 * channel and surfaces the parent-side read fd as the supervisor's
 * `FrameReader`.
 *
 * Failure modes flow through the returned handle's `exited` promise.
 * A `Bun.spawn` that fails to launch (binary missing, env malformed,
 * `EXEC` error) settles `exited` with a non-zero code; the
 * supervisor's `wireChild` races `exited` against `readyPromise`
 * inside `spawn()` so a spawn-time crash surfaces as a rejected
 * spawn rather than a wedged `starting` state.
 */
export const defaultSubprocessSpawner: SubprocessSpawner = ({
  binaryPath,
  env,
}): SubprocessHandle => {
  // The child leads its own process group so a stop reaches the tool
  // subprocesses it started, not just the child's own pid.
  const proc = Bun.spawn([binaryPath], {
    stdio: ["pipe", "pipe", "inherit", "pipe"],
    env,
    detached: true,
  });
  // Once the final group kill has run, the group id may be reused by an
  // unrelated process group, so later signals must not reach it.
  let groupReleased = false;
  function signalGroup(signal: number | NodeJS.Signals): void {
    if (groupReleased) return;
    try {
      process.kill(-proc.pid, signal);
    } catch (err) {
      if (!(err instanceof Error && "code" in err && err.code === "ESRCH"))
        throw err;
    }
  }
  const eventFd = proc.stdio[CHILD_EVENT_CHANNEL_FD];
  if (typeof eventFd !== "number") {
    throw new Error(
      `workflow-host-wiring: Bun.spawn did not return a numeric fd at stdio[${String(CHILD_EVENT_CHANNEL_FD)}] for the event channel; got ${typeof eventFd}`,
    );
  }
  return {
    pid: proc.pid,
    controlWriter: ndjsonWriterFromFileSink(proc.stdin),
    controlReader: ndjsonReaderFromReadableStream(proc.stdout),
    eventReader: frameReaderFromFd(eventFd),
    kill(signal?: number | string): void {
      // The supervisor's `SubprocessHandle.kill` widens the signal
      // to `number | string`; `process.kill` accepts
      // `number | NodeJS.Signals`. The supervisor's call sites pass
      // `"SIGTERM"` / `"SIGKILL"` (recycle path) or no argument
      // (shutdown path). Cast at the boundary so the inner call
      // matches the narrower type without coercing valid input.
      if (signal === undefined) {
        signalGroup("SIGTERM");
        return;
      }
      if (typeof signal === "number") {
        signalGroup(signal);
        return;
      }
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- supervisor's kill widens to `string`; the runtime accepts the same `"SIG*"` strings, narrowed back at the boundary.
      signalGroup(signal as NodeJS.Signals);
    },
    // Tool subprocesses outlive a child that exits or is killed, so the
    // group is killed before `exited` settles. If the group was already
    // empty when Bun reaped the child, its id could be reused before this
    // kill lands; the window is brief, and closing it needs a process
    // handle such as a pidfd, which Bun does not expose.
    exited: proc.exited.then((code) => {
      try {
        signalGroup("SIGKILL");
        groupReleased = true;
      } catch (err) {
        logger.error`Cannot stop processes left by workflow child ${String(proc.pid)}: ${err instanceof Error ? err.message : String(err)}`;
      }
      return code;
    }),
  };
};
