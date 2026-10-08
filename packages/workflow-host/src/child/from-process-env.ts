// Process-shaped convenience wrapper around `runWorkflowChild`.
//
// The wrapper crosses the only boundary that touches `process.env`,
// `process.stdin`/`process.stdout`, and the inherited event-channel
// file descriptor. Each host ships a ~5-line entry script against a
// substrate-factory of its own; the factory consumes a narrow typed
// env struct rather than `NodeJS.ProcessEnv`, and the runtime body
// never sees the process boundary.

import fs from "node:fs";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto";

import { parseSpawnTimeEnv, type SpawnTimeEnv } from "./env-bootstrap";
import {
  runWorkflowChild,
  type RunWorkflowChildBindings,
  type RunWorkflowChildResult,
} from "./run-child";
import {
  createChildSubstrateWriteBridge,
  type ChildSubstrateWriteBridge,
} from "./substrate-write-bridge";
import {
  createChildOutboundMailBridge,
  type ChildOutboundMailBridge,
} from "./outbound-mail-bridge";
import {
  createChildMailboxMutationBridge,
  type ChildMailboxMutationBridge,
} from "./mailbox-mutation-bridge";
import {
  createChildMailboxCallBridge,
  type ChildMailboxCallBridge,
} from "./mailbox-call-bridge";
import {
  createControlChannelSender,
  type FrameWriter,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";

/**
 * File descriptor the supervisor's `Bun.spawn` wires the event-channel
 * socketpair onto: stdio 0/1/2 stay as stdin/stdout/stderr, the
 * event-channel write side lands at fd 3. The wrapper opens fd 3 as the
 * child's `FrameWriter`.
 */
export const EVENT_CHANNEL_FD = 3;

/**
 * Typed env the host promises its factory: the parsed spawn-time env
 * (IPC trust anchors + deployment ids) plus the substrate-config keys
 * the host placed in `WorkflowSupervisorBindings.substrateEnv`. The
 * factory narrows the record again on the way in.
 */
export interface SubstrateFactoryEnv {
  /** Parsed spawn-time env (IPC trust anchors + deployment ids). */
  readonly spawn: SpawnTimeEnv;
  /** Substrate-config keys the host placed in `WorkflowSupervisorBindings.substrateEnv`; the factory narrows its own required-key shape against this record. */
  readonly substrateConfig: Readonly<Record<string, string>>;
  /**
   * Child-side IPC bridge for the workflow-run substrate-write surface.
   * The factory uses it for its proxy `RepoStore`, whose
   * `writeTreePreservingPrefix` forwards over IPC: `submit` sends a
   * `substrate.write.request` upstream, the supervisor runs the write
   * (firing the boot-edge pack-push wrap) and replies with
   * `substrate.write.response`. The supervisor's merge callback runs as a
   * `substrate.merge.request` / `substrate.merge.response` pair so the
   * child's merge closure stays in the child's address space.
   */
  readonly substrateWriteBridge: ChildSubstrateWriteBridge;
  /**
   * Child-side IPC bridge for the OUTBOUND half of mailbox ownership
   * (§3a). The factory uses it for the supervisor-backed
   * `MessageTransport` it supplies as the step agent's `env.transport`:
   * `send` emits an `outbound.message` frame and resolves once the
   * supervisor's `outbound.result` lands. The supervisor does the actual
   * signed send, so outbound mail carries the agent's signature without
   * the child holding the key.
   */
  readonly outboundMailBridge: ChildOutboundMailBridge;
  /**
   * Child-side IPC bridge for the INBOUND half of mailbox ownership
   * (§3b). The factory uses it for the supervisor-backed transport's
   * write surface: `setFlags` / `clearFlags` / `expunge` emit a
   * `mailbox.mutate.request` frame and resolve on `mailbox.mutate.response`.
   * The supervisor -- the sole mailbox writer -- applies the mutation, so
   * the child never flushes the run ref itself.
   */
  readonly mailboxMutationBridge: ChildMailboxMutationBridge;
  /**
   * Child-side IPC bridge for every mailbox method that is not `send` and
   * not a flag or expunge mutation; the factory attaches it to the warm
   * agent's transport. A build without an inbound surface does not receive
   * it, and those methods fail as unwired.
   */
  readonly mailboxCallBridge: ChildMailboxCallBridge;
}

/**
 * Substrate-factory callback the host supplies to
 * `runWorkflowChildFromProcessEnv`, constructing the
 * `RunWorkflowChildBindings` the runtime body consumes: substrate-shaped
 * `RepoStore`, principal, per-deployment repo ids, scheduler, step
 * invoker, child spawner, grant evaluator. The factory owns every
 * concrete dependency the runtime body needs; the wrapper itself depends
 * on nothing host-specific.
 */
export type SubstrateFactory = (
  env: SubstrateFactoryEnv,
) => Promise<RunWorkflowChildBindings>;

/**
 * Optional overrides for the process-shaped surfaces the wrapper crosses.
 * Production hosts use the defaults; tests can inject in-memory streams.
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
   * `substrateConfig`. Keys not in this allowlist are filtered out, so the
   * factory never sees spawn-time IPC keys (those flow through the typed
   * `spawn` slot) or unrelated process env. The default is the empty
   * allowlist -- a host that wants substrate-config keys MUST name them
   * here.
   */
  substrateConfigKeys?: readonly string[];
}

/**
 * Process-boundary wrapper around `runWorkflowChild`: parses `process.env`
 * into the typed `SpawnTimeEnv` plus a narrow substrate-config record,
 * opens stdin/stdout for the control channel, wraps the inherited
 * event-channel fd into a `FrameWriter`, hands the typed env to the host's
 * substrate factory, and invokes `runWorkflowChild`.
 *
 * Failures surface loudly: a missing or malformed spawn-time env throws via
 * `parseSpawnTimeEnv`; a listed substrate-config key whose value is missing
 * or empty throws; factory and `runWorkflowChild` rejections propagate. The
 * wrapper does not catch or coerce failures -- the host's binary decides
 * what to do with a thrown error (conventionally `process.exit(1)` with a
 * stderr message).
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
  // Mint the child's upstream-signing keypair here so the wrapper can
  // construct the upstream sender before invoking the factory; the same
  // sender carries the `ready` frame `runWorkflowChild` emits.
  const childKeyPair = await generateKeyPair();
  const upstreamSender = createControlChannelSender({
    privateKeySeed: childKeyPair.privateKey,
    channelId: spawn.channelId,
    writer: controlWriter,
  });
  const substrateWriteBridge = createChildSubstrateWriteBridge({
    upstreamSender,
  });
  const outboundMailBridge = createChildOutboundMailBridge({
    upstreamSender,
  });
  const mailboxMutationBridge = createChildMailboxMutationBridge({
    upstreamSender,
  });
  const mailboxCallBridge = createChildMailboxCallBridge({
    upstreamSender,
  });
  const bindings = await factory({
    spawn,
    substrateConfig,
    substrateWriteBridge,
    outboundMailBridge,
    mailboxMutationBridge,
    mailboxCallBridge,
  });
  return runWorkflowChild({
    env: spawn,
    controlReader,
    controlWriter,
    eventWriter,
    bindings: {
      ...bindings,
      // Override the factory's keypair path so `runWorkflowChild` does not
      // re-mint a different key and break verification: the upstream sender
      // and the `ready` frame's `childPublicKey` come from one keypair.
      ipcChildKeyPairFactory: () => Promise.resolve(childKeyPair),
    },
    upstreamSender,
    substrateWriteBridge,
    outboundMailBridge,
    mailboxMutationBridge,
    mailboxCallBridge,
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
  // The supervisor inherits the event-channel write side on fd 3 in the
  // child's address space. Wrap it as a Node writable so the wire matches
  // the supervisor's `FrameReader` half; failing to open fd 3 surfaces
  // loudly, as the child cannot publish InferenceEvents without it.
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
  // Buffered line splitter over the source stream: yields one JSON line per
  // iteration, trailing newlines stripped so callers see exactly what the
  // sender wrote without a wire-shape re-decode.
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
