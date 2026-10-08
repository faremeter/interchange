// Warm-agent cache for the workflow-process child (design §3b).
//
// A long-lived single-step agent is built once -- tools materialized,
// plugins instantiated, the LSP subprocess spawned -- and reused across
// every inbound message, removing instantiate-send-teardown cost and
// preserving in-memory conversation continuity (durability across child
// respawns lands in §3c).
//
// Ownership and lifetime: the cache lives in the child, owned by the
// run-loop (`run-child.ts`), not the supervisor. The step-invoker consults
// it per step invocation: a hit reuses the warm agent, a miss builds and
// stores one lazily. The cached agent is torn down (wrapped `agent.close()`
// disposes plugins and kills the LSP subprocess) only at the run-loop's
// eviction points, never between messages; on recycle the child dies with
// the LSP grandchild and the respawned child re-warms lazily.
//
// Per-message event sink: the agent's `stream()` is consumed once, for its
// whole life, by a single forwarder; the per-step `onEvent` sink differs per
// message, so the forwarder routes through a mutable reference the
// step-invoker rewrites before each `agent.send`.
//
// Warm-keep is gated explicitly: the cache is built only when the deploy
// projection marks the deployment a warm candidate. Multi-step deployments
// pass no cache and keep instantiate-send-teardown per step.

import { getLogger } from "@intx/log";
import type { Agent } from "@intx/agent";
import type { InferenceEvent, InferenceSource } from "@intx/types/runtime";

const logger = getLogger(["workflow-host", "child", "warm-agent-cache"]);

/**
 * Mutable per-entry event sink the warm agent's stream forwarder reads
 * before forwarding each event. The step-invoker swaps `current` to the
 * active step's `onEvent` before every `agent.send`, so events from the
 * agent's single lifetime stream reach whichever run is in flight. A
 * `null` `current` drops events (no run is driving the agent), which is
 * the correct behaviour in the gap between sends.
 */
export interface WarmEventSinkRef {
  current: ((event: InferenceEvent) => void) | null;
}

/**
 * Per-turn settle barrier the connector reply drain exposes to the warm
 * step (§3c durability). The step snapshots `replySeq()` before its send
 * and, for a turn that produced a reply, awaits `waitForReplyAfter(snapshot)`
 * so the run parks -- and the supervisor consumes the inbound mail -- only
 * after the reply is durably sent. The structural subset of the harness
 * `ConnectorReplyDrain` the warm path needs.
 */
export type WarmReplySettlement =
  | { readonly ok: true }
  | { readonly ok: false; readonly cause: unknown };

export interface WarmReplyDrive {
  /** Settles when the drain loop exits at eviction. Folded into `eventForward`. */
  readonly done: Promise<void>;
  /** Monotonic count of replies that have settled (sent-and-acked or failed). */
  replySeq(): number;
  /**
   * Resolve once the reply at index `n` has settled, with its outcome. A
   * failure outcome (or a drain that tears down before reply `n` arrives)
   * carries `ok: false` so the caller fails the turn rather than treating the
   * reply as sent.
   */
  waitForReplyAfter(n: number): Promise<WarmReplySettlement>;
}

/**
 * One warm agent the step-invoker reuses across messages. The
 * `eventSinkRef` is rewritten per message; the `eventForward` promise
 * settles when the agent's stream ends at `close()`. `replyDrive` is the
 * connector reply drain's per-turn barrier when the deployment drives
 * threaded replies, or `null` for a warm deployment with no connector state.
 */
interface WarmEntry {
  readonly agent: Agent;
  readonly eventSinkRef: WarmEventSinkRef;
  readonly eventForward: Promise<void>;
  readonly replyDrive: WarmReplyDrive | null;
}

/**
 * Per-address warm-agent cache, keyed by the step's stable identity so a
 * long-lived agent resolves to the same entry on every message.
 * Single-writer from the run-loop's perspective: the step-invoker
 * builds-or-reuses inside one step invocation, the run-loop evicts at
 * teardown.
 */
export interface WarmAgentCache {
  /** Return the warm agent for `key`, or `null` when none is built yet (the lazy first-message path); the caller builds and calls `store` on a miss. */
  acquire(key: string): Agent | null;
  /**
   * Cache a freshly-built warm agent under `key` (`eventSinkRef` is the
   * forwarder's mutable sink, `eventForward` its settle promise, `replyDrive`
   * the per-turn barrier or `null`). Throws if an entry already exists -- a
   * double-build is a step-invoker bug, not a silent overwrite that would
   * leak the prior agent's LSP subprocess.
   */
  store(
    key: string,
    agent: Agent,
    eventSinkRef: WarmEventSinkRef,
    eventForward: Promise<void>,
    replyDrive: WarmReplyDrive | null,
  ): void;
  /**
   * Return the connector reply drain's per-turn barrier for `key`, or `null`
   * when the deployment drives no threaded replies. Fetched on every message
   * (the drain is established once at build, but each send must snapshot and
   * await it). Throws when no entry exists -- a fetch before `store` is a
   * step-invoker sequencing bug.
   */
  getReplyDrive(key: string): WarmReplyDrive | null;
  /** Point the agent's stream forwarder at the active step's event sink before its send; throws when no entry exists. */
  setEventSink(key: string, onEvent: (event: InferenceEvent) => void): void;
  /** Clear the active sink after a send settles, so a stray event between messages is dropped rather than delivered to a torn-down channel. A missing entry is a no-op. */
  clearEventSink(key: string): void;
  /**
   * Apply a rotated inference-source list to every retained warm agent in
   * place via `Agent.setSources` (a single-step cache holds 0 or 1 entry, so
   * this rotates the one built agent or no-ops in the pre-first-build
   * window). The swap mutates the agent's shared active-source object in
   * place and takes effect on the reactor's next call; the single-threaded
   * control loop means no torn read against a concurrent send. `setSources`
   * validates the list, so a bad rotation surfaces rather than being
   * swallowed.
   */
  applySources(sources: InferenceSource[], defaultSource: string): void;
  /**
   * Tear down every cached warm agent: run the wrapped `agent.close()`
   * (disposing plugins, killing the LSP subprocess) and drain the stream
   * forwarder. Idempotent -- a second call on an empty cache is a no-op, so
   * the run-loop can evict on both shutdown and the exit-path `finally`
   * without double-closing. Resolves once every agent is closed and every
   * forwarder drained.
   */
  evictAll(reason: string): Promise<void>;
}

/** Construct an empty warm-agent cache; multi-step deployments construct none. */
export function createWarmAgentCache(): WarmAgentCache {
  const entries = new Map<string, WarmEntry>();

  function acquire(key: string): Agent | null {
    const entry = entries.get(key);
    return entry === undefined ? null : entry.agent;
  }

  function store(
    key: string,
    agent: Agent,
    eventSinkRef: WarmEventSinkRef,
    eventForward: Promise<void>,
    replyDrive: WarmReplyDrive | null,
  ): void {
    if (entries.has(key)) {
      throw new Error(
        `warm-agent cache: an entry already exists for ${key}; the step-invoker must reuse the cached agent rather than rebuild it`,
      );
    }
    entries.set(key, { agent, eventSinkRef, eventForward, replyDrive });
  }

  function getReplyDrive(key: string): WarmReplyDrive | null {
    const entry = entries.get(key);
    if (entry === undefined) {
      throw new Error(
        `warm-agent cache: getReplyDrive for ${key} with no cached entry; the step-invoker must store the warm agent before fetching its reply barrier`,
      );
    }
    return entry.replyDrive;
  }

  function setEventSink(
    key: string,
    onEvent: (event: InferenceEvent) => void,
  ): void {
    const entry = entries.get(key);
    if (entry === undefined) {
      throw new Error(
        `warm-agent cache: setEventSink for ${key} with no cached entry; the step-invoker must store the warm agent before wiring its per-message event sink`,
      );
    }
    entry.eventSinkRef.current = onEvent;
  }

  function clearEventSink(key: string): void {
    const entry = entries.get(key);
    if (entry === undefined) return;
    entry.eventSinkRef.current = null;
  }

  function applySources(
    sources: InferenceSource[],
    defaultSource: string,
  ): void {
    // Rotate every retained agent before surfacing any failure: one agent
    // rejecting the rotation must not skip the rest. Collect failures and
    // throw them together. (Single-step today, so the cache holds 0 or 1
    // entry; this stays honest if warm-keep ever spans steps.)
    const failures: unknown[] = [];
    for (const entry of entries.values()) {
      try {
        entry.agent.setSources(sources, defaultSource);
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`warm-agent rotation: setSources failed: ${message}`;
        failures.push(cause);
      }
    }
    if (failures.length > 0) {
      throw new AggregateError(
        failures,
        `warm-agent rotation: ${String(failures.length)} agent(s) rejected the source rotation`,
      );
    }
  }

  async function evictAll(reason: string): Promise<void> {
    if (entries.size === 0) return;
    const toEvict = [...entries.values()];
    entries.clear();
    // Close every entry before surfacing any failure: one entry's failure
    // must not strand the rest's teardown (leaking LSP subprocesses).
    // Collect failures and throw together once every agent is closed.
    const failures: unknown[] = [];
    for (const entry of toEvict) {
      // Clear the sink first so events emitted during the shutdown window
      // are dropped rather than delivered to a torn-down channel.
      entry.eventSinkRef.current = null;
      try {
        await entry.agent.close();
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`warm-agent eviction (${reason}): agent.close failed: ${message}`;
        failures.push(cause);
      } finally {
        // `agent.close()` terminates the stream iterator; await the
        // forwarder so none outlives the eviction.
        await entry.eventForward;
      }
    }
    if (failures.length > 0) {
      throw new AggregateError(
        failures,
        `warm-agent eviction (${reason}): ${String(failures.length)} agent(s) failed to close; an LSP subprocess may be leaked`,
      );
    }
  }

  return {
    acquire,
    store,
    getReplyDrive,
    setEventSink,
    clearEventSink,
    applySources,
    evictAll,
  };
}
