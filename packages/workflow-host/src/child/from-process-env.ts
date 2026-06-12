// Process-shaped convenience wrapper around `runWorkflowChild`.
//
// The wrapper crosses the only boundary that touches `process.env`,
// `process.stdin`/`process.stdout`, and the inherited event-channel
// file descriptor. Each host ships a ~5-line entry script
// (`#!/usr/bin/env bun` + an `import` + an `await` of this function)
// against a substrate-factory of its own; the factory consumes a
// narrow typed env struct rather than `NodeJS.ProcessEnv`, and the
// runtime body never sees the process boundary.
//
// The factory's typed env carries the spawn-time keys the supervisor
// promises plus the substrate-config keys the host injected on top.
// The supervisor's `WorkflowSupervisorBindings.substrateEnv` is the
// only documented surface for the substrate-config slot; the host
// places its own data-dir / signing-key / definition-repo keys there
// at supervisor-construction time and reads them back here.

import fs from "node:fs";

import { type } from "arktype";

import { parseSpawnTimeEnv, type SpawnTimeEnv } from "./env-bootstrap";
import {
  runWorkflowChild,
  type RunWorkflowChildBindings,
  type RunWorkflowChildResult,
} from "./run-child";
import type { FrameWriter, NdjsonReader, NdjsonWriter } from "../ipc/index";

/**
 * File descriptor the supervisor's `Bun.spawn` wires the
 * event-channel socketpair onto. The supervisor's spawn convention
 * inherits stdio 0/1/2 for stdin/stdout/stderr and the
 * event-channel write side at fd 3 in the child's address space.
 * The wrapper opens fd 3 as the child's `FrameWriter`.
 */
export const EVENT_CHANNEL_FD = 3;

/**
 * Substrate-config env keys the host promises to its factory. The
 * supervisor's spawn-time env always carries the IPC trust anchors
 * plus the deployment identifiers (parsed via `parseSpawnTimeEnv`).
 * `substrateConfig` carries every key the host placed in
 * `WorkflowSupervisorBindings.substrateEnv` -- the host's own narrow
 * struct (data-dir, signing-key paths, definition-repo identifiers)
 * lives there, and the factory narrows it again on the way in.
 *
 * The factory does NOT receive `NodeJS.ProcessEnv` directly. The
 * surface is intentionally narrow so a future env-shaped surface
 * (a CLI launcher that prepends keys, a test harness that injects
 * extra knobs) crosses this contract explicitly rather than via
 * an opaque process-shaped slot.
 */
export interface SubstrateFactoryEnv {
  /** Parsed spawn-time env (IPC trust anchors + deployment ids). */
  readonly spawn: SpawnTimeEnv;
  /**
   * Substrate-config keys the host placed in
   * `WorkflowSupervisorBindings.substrateEnv`. The factory narrows
   * its own required-key shape against this record at the boundary.
   */
  readonly substrateConfig: Readonly<Record<string, string>>;
}

/**
 * Substrate-factory callback the host supplies to
 * `runWorkflowChildFromProcessEnv`. The factory constructs the
 * `RunWorkflowChildBindings` the runtime body consumes:
 * substrate-shaped `RepoStore`, principal, per-deployment repo ids,
 * scheduler, step invoker, child spawner, grant evaluator. The
 * factory owns every concrete dependency the runtime body needs to
 * see; the wrapper itself depends on nothing host-specific.
 */
export type SubstrateFactory = (
  env: SubstrateFactoryEnv,
) => Promise<RunWorkflowChildBindings>;

/**
 * Optional overrides for the process-shaped surfaces the wrapper
 * crosses. Production hosts use the defaults; tests can inject
 * in-memory streams. The fields exist so a host that wants to
 * compose the wrapper around its own logging layer or telemetry
 * surface can pass through, without exposing `process.env` to the
 * factory.
 */
export interface RunWorkflowChildFromProcessEnvOpts {
  /** Override the raw env record (defaults to `process.env`). */
  rawEnv?: Readonly<Record<string, string | undefined>>;
  /** Override the control-channel reader (defaults to `process.stdin`). */
  controlReader?: NdjsonReader;
  /** Override the control-channel writer (defaults to `process.stdout`). */
  controlWriter?: NdjsonWriter;
  /** Override the event-channel writer (defaults to a wrap of fd 3). */
  eventWriter?: FrameWriter;
  /**
   * Override which env keys are forwarded to the factory's
   * `substrateConfig`. Keys not in this allowlist are filtered out.
   * Production hosts list their own substrate-config keys here so the
   * factory never sees spawn-time IPC keys (those flow through the
   * typed `spawn` slot) or unrelated process env. The default is the
   * empty allowlist -- a host that wants its factory to receive
   * substrate-config keys MUST name them here.
   */
  substrateConfigKeys?: readonly string[];
}

/**
 * Process-boundary wrapper around `runWorkflowChild`. The wrapper
 * parses `process.env` into the typed `SpawnTimeEnv` plus a narrow
 * substrate-config record, opens stdin/stdout for the control
 * channel, wraps the inherited event-channel fd into a
 * `FrameWriter`, hands the typed env to the host's substrate
 * factory to mint the runtime body's bindings, and invokes
 * `runWorkflowChild`.
 *
 * Failures surface loudly:
 *   - missing or malformed spawn-time env throws via
 *     `parseSpawnTimeEnv`;
 *   - a substrate-config key listed in `substrateConfigKeys` whose
 *     value is missing or empty throws;
 *   - factory rejection propagates;
 *   - `runWorkflowChild` rejection propagates.
 *
 * The wrapper does not catch or coerce failures. The host's binary
 * is the layer that decides what to do with a thrown error; the
 * convention shown in the documentation is `process.exit(1)` with
 * a stderr message.
 */
export async function runWorkflowChildFromProcessEnv(
  factory: SubstrateFactory,
  opts: RunWorkflowChildFromProcessEnvOpts = {},
): Promise<RunWorkflowChildResult> {
  const rawEnv = opts.rawEnv ?? process.env;
  const spawn = parseSpawnTimeEnv(rawEnv);
  const substrateConfig = filterSubstrateConfig(
    rawEnv,
    opts.substrateConfigKeys ?? [],
  );
  const controlReader = opts.controlReader ?? defaultControlReader();
  const controlWriter = opts.controlWriter ?? defaultControlWriter();
  const eventWriter = opts.eventWriter ?? defaultEventWriter();
  const bindings = await factory({ spawn, substrateConfig });
  return runWorkflowChild({
    env: spawn,
    controlReader,
    controlWriter,
    eventWriter,
    bindings,
  });
}

const SubstrateConfigValue = type("string > 0");

function filterSubstrateConfig(
  rawEnv: Readonly<Record<string, string | undefined>>,
  keys: readonly string[],
): Readonly<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const key of keys) {
    const value = rawEnv[key];
    if (value === undefined) {
      throw new Error(
        `workflow-child substrate-config env: required key ${key} is unset`,
      );
    }
    const validated = SubstrateConfigValue(value);
    if (validated instanceof type.errors) {
      throw new Error(
        `workflow-child substrate-config env: ${key} failed validation: ${validated.summary}`,
      );
    }
    out[key] = validated;
  }
  return out;
}

function defaultControlReader(): NdjsonReader {
  return {
    read(): AsyncIterableIterator<string> {
      return readNdjsonLines(process.stdin);
    },
  };
}

function defaultControlWriter(): NdjsonWriter {
  return {
    write(line: string): Promise<void> {
      return new Promise((resolve, reject) => {
        process.stdout.write(line, (err) => {
          if (err) reject(err);
          else resolve();
        });
      });
    },
  };
}

function defaultEventWriter(): FrameWriter {
  // The supervisor inherits the event-channel write side on fd 3 in
  // the child's address space. Wrap it as a Node writable so the
  // wire matches the supervisor's `FrameReader` half. Failing to
  // open fd 3 surfaces loudly: the child cannot publish
  // InferenceEvents without it.
  const stream = fs.createWriteStream("", { fd: EVENT_CHANNEL_FD });
  return {
    write(bytes: Uint8Array): Promise<void> {
      return new Promise((resolve, reject) => {
        stream.write(bytes, (err) => {
          if (err) reject(err);
          else resolve();
        });
      });
    },
  };
}

async function* readNdjsonLines(
  source: NodeJS.ReadableStream,
): AsyncIterableIterator<string> {
  // Buffered line splitter over the source stream. Yields one JSON
  // line per iteration; trailing newlines are stripped so callers
  // see exactly what the sender wrote without a wire-shape
  // re-decode.
  const decoder = new TextDecoder("utf-8");
  let pending = "";
  for await (const chunk of source) {
    const text =
      typeof chunk === "string"
        ? chunk
        : decoder.decode(chunk as Uint8Array, { stream: true });
    pending += text;
    let nl = pending.indexOf("\n");
    while (nl >= 0) {
      const line = pending.slice(0, nl).replace(/\r$/, "");
      pending = pending.slice(nl + 1);
      if (line.length > 0) yield line;
      nl = pending.indexOf("\n");
    }
  }
  if (pending.length > 0) yield pending;
}
