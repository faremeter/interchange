// Warm-agent cache for the workflow-process child (design §3b).
//
// A long-lived single-step agent is built once -- tools materialized,
// plugins instantiated, the LSP subprocess spawned -- and reused across
// every inbound message. Re-materializing tools and re-spawning the LSP
// per message is the instantiate-send-teardown cost the warm cache
// removes; keeping the agent alive across messages is also what
// preserves in-memory conversation continuity (durability across child
// respawns lands in §3c, a later sub-step).
//
// Ownership and lifetime. The cache lives in the child's address space,
// owned by the run-loop (`run-child.ts`), NOT the supervisor. The
// step-invoker consults it on every step invocation: a cache hit reuses
// the warm agent, a miss builds and stores one lazily. The cached agent
// is torn down -- the wrapped `agent.close()` runs, disposing plugins
// and killing the LSP subprocess -- only at the run-loop's eviction
// points (child shutdown, deployment undeploy, recycle, post-drain
// teardown), never between messages. On recycle the child process dies,
// killing the LSP grandchild regardless; the respawned child starts with
// an empty cache and re-warms lazily.
//
// Per-message event sink. The agent's `stream()` is consumed once, for
// the agent's whole life, by a single forwarder owned by the entry. The
// per-step `onEvent` sink the runtime threads in differs per message
// (it carries the run id in its error-log path), so the forwarder routes
// through a mutable reference the step-invoker rewrites before each
// `agent.send`. The forwarder loop ends only when the agent closes at an
// eviction point.
//
// Warm-keep is gated explicitly: the cache is constructed only when the
// deploy projection marks the deployment a warm candidate (the
// single-step launched agent). Multi-step deployments pass no cache and
// keep instantiate-send-teardown per step. The decision is never a
// silent default -- a multi-step agent is never warm-kept.

import { getLogger } from "@intx/log";
import type { Agent } from "@intx/agent";
import type { InferenceEvent } from "@intx/types/runtime";

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
 * One warm agent the step-invoker reuses across messages. The
 * `eventSinkRef` is rewritten per message; the `eventForward` promise
 * settles when the agent's stream ends at `close()`.
 */
interface WarmEntry {
  readonly agent: Agent;
  readonly eventSinkRef: WarmEventSinkRef;
  readonly eventForward: Promise<void>;
}

/**
 * Per-address warm-agent cache. Keyed by the step's stable identity (the
 * single step's id), so a long-lived agent resolves to the same entry on
 * every inbound message. The cache is single-writer from the run-loop's
 * perspective: the step-invoker builds-or-reuses inside one step
 * invocation, and the run-loop evicts at teardown.
 */
export interface WarmAgentCache {
  /**
   * Return the warm agent cached for `key`, or `null` when none is
   * built yet (the lazy first-message path). The caller builds the
   * agent and calls `store` on a miss.
   */
  acquire(key: string): Agent | null;
  /**
   * Cache a freshly-built warm agent under `key`. The `eventSinkRef` is
   * the mutable sink the agent's stream forwarder reads; `eventForward`
   * is the forwarder loop's settle promise. Throws if an entry already
   * exists for `key` -- a double-build is a step-invoker bug, not a
   * silent overwrite that would leak the prior agent's LSP subprocess.
   */
  store(
    key: string,
    agent: Agent,
    eventSinkRef: WarmEventSinkRef,
    eventForward: Promise<void>,
  ): void;
  /**
   * Point the warm agent's stream forwarder at the active step's event
   * sink before its `agent.send`. Throws when no entry exists for
   * `key` -- the step-invoker must `store` before it rewrites the sink.
   */
  setEventSink(key: string, onEvent: (event: InferenceEvent) => void): void;
  /**
   * Clear the active event sink for `key` after a step's `agent.send`
   * settles, so a stray event between messages is dropped rather than
   * delivered to a torn-down per-run channel. A missing entry is a
   * no-op: the agent may already have been evicted.
   */
  clearEventSink(key: string): void;
  /**
   * Tear down every cached warm agent: run the wrapped `agent.close()`
   * (disposing plugins and killing the LSP subprocess) and drain the
   * stream forwarder. Idempotent -- a second call after the cache is
   * empty is a no-op, so the run-loop can evict on both the shutdown
   * frame and the exit-path `finally` without double-closing. Resolves
   * once every agent is closed and every forwarder has drained, so no
   * LSP subprocess outlives the call.
   */
  evictAll(reason: string): Promise<void>;
}

/**
 * Construct an empty warm-agent cache. The run-loop builds one per
 * spawn when the deployment is a warm candidate and threads it into the
 * step-invoker; multi-step deployments construct none.
 */
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
  ): void {
    if (entries.has(key)) {
      throw new Error(
        `warm-agent cache: an entry already exists for ${key}; the step-invoker must reuse the cached agent rather than rebuild it`,
      );
    }
    entries.set(key, { agent, eventSinkRef, eventForward });
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

  async function evictAll(reason: string): Promise<void> {
    if (entries.size === 0) return;
    const toEvict = [...entries.values()];
    entries.clear();
    for (const entry of toEvict) {
      // Clear the sink first so any event emitted during the agent's
      // shutdown window is dropped rather than delivered to a per-run
      // channel the run-loop is tearing down.
      entry.eventSinkRef.current = null;
      try {
        // The wrapped close (see `createToolBearingAgentFactory`) runs
        // the agent's own close and then the plugin + tool-bundle
        // disposers, killing the LSP subprocess. A close failure must
        // surface, not be swallowed -- a leaked LSP subprocess is
        // exactly the failure warm-keep risks -- so it propagates after
        // we have drained what we can.
        await entry.agent.close();
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`warm-agent eviction (${reason}): agent.close failed: ${message}`;
        throw cause instanceof Error ? cause : new Error(message);
      } finally {
        // `agent.close()` terminates the stream iterator, so the
        // forwarder loop has ended (or is about to). Await it so no
        // forwarder outlives the eviction.
        await entry.eventForward;
      }
    }
  }

  return {
    acquire,
    store,
    setEventSink,
    clearEventSink,
    evictAll,
  };
}
