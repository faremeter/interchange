// @intx/harness composition layer.
//
// The harness imports `@intx/agent` and composes a mail-transport
// surface on top of `createAgent(def, env)`. The reactor is wrapped
// exactly once -- inside the agent harness in `@intx/agent`. This
// module owns transport subscription, the connector router and its
// state persistence, the INBOX watch loop, and the outbound side of
// `connector.reply` events.
//
// What this module does *not* own: reactor wrapping, audit accumulation
// or flushing, source-registry hot-swap. Those live in `@intx/agent`
// and are reached via `agent.deliver`, `agent.setSource`, and
// `agent.stream()` respectively.

import {
  createAgent,
  defineTool,
  type Agent,
  type AgentDefinition,
  type AnnotatedToolFactory,
  type BaseEnv,
  type ToolBundle,
} from "@intx/agent";
import { getLogger } from "@intx/log";
import type {
  BlobReader,
  ConnectorThreadState,
  ContextStore,
  InboundMessage,
  InferenceSource,
  MessageTransport,
  Unsubscribe,
} from "@intx/types/runtime";

import { createConnectorRouter, type RouteDecision } from "./connector-router";

const logger = getLogger(["interchange", "harness"]);

/**
 * Env extension the composition layer requires beyond `BaseEnv`. Tools
 * shipped by this package declare the matching `requires` so
 * `validateEnv` can blame either at the env entry point.
 *
 * `onReplySendFailed` is invoked when the reply drain catches a failure
 * from `connectorRouter.composeReply` or `transport.send` for an
 * outbound `connector.reply`. The reply is dropped and the router
 * state is not advanced; the callback is the only programmatic surface
 * a caller has to observe the loss. Production deployments that need
 * retry semantics layer them on top of this callback.
 *
 * The callback may be synchronous or async; the reply drain awaits its
 * resolution so an async callback's rejection is observed (and logged)
 * rather than surfacing as an unhandled promise rejection.
 *
 * `onReplyDrainTerminated` is invoked when the reply drain's `for await`
 * loop exits abnormally -- the only documented case is a
 * `StreamBackpressureError` thrown by the agent's event stream when the
 * drain's per-consumer buffer overruns `streamBufferMax`. After this
 * fires the harness is no longer forwarding `connector.reply` events to
 * the transport: in-process `agent.send()` callers still resolve, but
 * outbound replies are silently dropped until `close()`. Production
 * deployments that need to alert on this failure mode subscribe via
 * this callback; the harness only emits a `logger.warn` otherwise. The
 * callback may be synchronous or async and is awaited the same way
 * `onReplySendFailed` is, so an async rejection is observed (and
 * logged) rather than escaping as an unhandled rejection.
 */
export interface MailEnv extends BaseEnv {
  transport: MessageTransport;
  address: string;
  onConnectorStateChanged?: (state: ConnectorThreadState | null) => void;
  onReplySendFailed?: (cause: unknown) => void | Promise<void>;
  onReplyDrainTerminated?: (cause: unknown) => void | Promise<void>;
}

/**
 * Narrowed public surface returned by `createHarness`. `close` is the
 * only direct surface; everything else is a pass-through to the
 * underlying agent. `stream` is exposed so observability consumers can
 * subscribe to the reactor's event stream without having to grab the
 * agent reference.
 */
export interface Harness {
  close(): Promise<void>;
  deliver(message: InboundMessage): void;
  setSource(source: InferenceSource): void;
  stream: Agent["stream"];
  readonly blobReader: BlobReader;
}

/**
 * Mail-tool factory shape. The `createMailTools` constructor in
 * `@intx/tools-mail` builds a runner from a transport-bearing
 * capability set; the harness wraps that into a single `defineTool`
 * bundle whose `requires` names the env keys the wrapper touches.
 *
 * Callers (e.g. the sidecar) supply the wrapper as a tool factory on
 * their `AgentDefinition`. `createHarness` does not synthesize it
 * internally -- the caller is the layer that knows which mail-tool
 * implementation to use.
 */
export type MailToolWrapper = (
  transport: MessageTransport,
) => Omit<ToolBundle, "dispose">;

/**
 * Invoke the caller-supplied `onReplySendFailed` callback and absorb any
 * failure it raises. Extracted from the reply drain so the await-the-
 * callback contract is testable in isolation: a bare invocation would
 * compile (TypeScript admits `async () => void` as satisfying a `void`-
 * returning signature) but would let an async callback's rejection
 * escape as an unhandled promise rejection. Awaiting protects against
 * that; the helper exists so the protection is asserted by a test
 * rather than implied by inspection of the drain.
 *
 * Exported only for the regression test in this package; no external
 * consumer should call it.
 *
 * The export-and-mark-internal shape is the codebase's convention
 * for helpers that exist to make a production-code contract
 * testable in isolation. A separate `@intx/harness/testing`
 * entry-point was considered and rejected: the helper is one
 * try/catch wrapper around the production callback, tightly
 * coupled to the `MailEnv` callback type defined adjacent to it.
 * Moving it would either duplicate the production code in a test
 * module (defeating the point) or require a parallel entry-point
 * whose only export is a single function -- bundler ceremony for a
 * boundary TypeScript cannot enforce anyway, since deep imports
 * (`@intx/harness/src/harness`) reach the same module regardless
 * of what `index.ts` re-exports. The docstring convention is
 * load-bearing here: the marker is the contract.
 *
 * `invokeReplyDrainTerminated` (below) follows the same shape for
 * the same reason.
 */
export async function invokeReplySendFailed(
  callback: NonNullable<MailEnv["onReplySendFailed"]>,
  cause: unknown,
): Promise<void> {
  try {
    await callback(cause);
  } catch (callbackError) {
    logger.error`onReplySendFailed callback threw: ${callbackError}`;
  }
}

/**
 * Invoke the caller-supplied `onReplyDrainTerminated` callback and
 * absorb any failure it raises. Mirrors `invokeReplySendFailed`:
 * extracted from the reply drain so the await-the-callback contract
 * is testable in isolation, exported only for the regression test in
 * this package.
 */
export async function invokeReplyDrainTerminated(
  callback: NonNullable<MailEnv["onReplyDrainTerminated"]>,
  cause: unknown,
): Promise<void> {
  try {
    await callback(cause);
  } catch (callbackError) {
    logger.error`onReplyDrainTerminated callback threw: ${callbackError}`;
  }
}

/**
 * Build the `load` / `writeMetadata` overrides the harness layers onto
 * `env.storage`. Extracted from `createHarness` so the dirty-bit gating
 * on `load()` is directly testable -- the production path constructs
 * the overrides inline with the same arguments.
 *
 * The `isInMemoryStateAuthoritative` callback is read on every `load`
 * invocation. The harness sets the bit from the router's
 * `onStateChanged` callback so the gate flips on the same tick a
 * commit produces its first state change; subsequent loads (whether
 * driven by reactor recovery, mid-cycle, or anywhere else) leave the
 * router's in-memory snapshot intact rather than blanking it with the
 * pre-commit disk value.
 *
 * Exported for the regression test in this package; no external
 * consumer should call it. Same shape and rationale as
 * `invokeReplySendFailed` and `invokeReplyDrainTerminated` above --
 * the helper is tightly coupled to the dirty-bit gating semantics
 * that live in this module, and a separate testing entry-point
 * would buy bundler ceremony for a boundary TypeScript cannot
 * enforce. The docstring "internal" marker is the contract.
 */
export function createWrappedStorageOverrides(
  baseStorage: ContextStore,
  connectorRouter: ReturnType<typeof createConnectorRouter>,
  isInMemoryStateAuthoritative: () => boolean,
): Pick<ContextStore, "load" | "writeMetadata"> {
  return {
    async load(signal) {
      const loaded = await baseStorage.load(signal);
      if (!isInMemoryStateAuthoritative()) {
        connectorRouter.restore(loaded.connectorState);
      }
      return loaded;
    },
    async writeMetadata(metadata, signal) {
      baseStorage.setConnectorState(connectorRouter.snapshot());
      return baseStorage.writeMetadata(metadata, signal);
    },
  };
}

/**
 * Construct an `AnnotatedToolFactory` for a mail-tool bundle. The
 * factory binds `transport` from env at construction time and produces
 * a bundle whose lifetime is tied to the agent. Disposal of the
 * underlying mail tools is the caller's responsibility (the env is the
 * agent's dependency contract; the caller owns what it puts in env);
 * the agent itself does not call bundle disposers (see the
 * `ToolBundle` contract in `@intx/agent`). Callers that need to
 * dispose mail tools on shutdown retain a reference to the underlying
 * `MailToolWrapper`'s output and invoke its `dispose` directly --
 * routing disposal through the bundle the agent receives would still
 * not fire since the agent never holds it.
 *
 * The `requires: ["transport", "address"]` declaration captures the
 * env-key surface of the entire mail composition path -- the factory
 * body reads `transport`, and `createHarness` (which the caller pairs
 * this factory with) reads `env.address` to label rejected-message
 * log records identifying which agent's router refused the message.
 * No routing decision keys off `env.address` -- the connector router
 * routes on per-message thread state, not on the agent's own
 * address -- so the field is observability-only. It still belongs in
 * `requires` because the harness's log record assumes the field is
 * populated; declaring it here lets the agent's `validateEnv` blame a
 * missing `address` at construction time rather than letting the
 * watch loop discover it under operational load. Callers that hand-
 * build a `defineTool` factory for a different mail-tool runner must
 * remember to surface `address` on their own `requires` if their
 * `createHarness` consumes it -- the agent has no way to deduce
 * composition-layer env requirements from a factory body that does
 * not itself read the field.
 *
 * The `requires` set is fixed at the two keys above by design; this
 * helper is not the extension point for mail-tool runners that need
 * additional env keys. A mail tool that wants to read (say) a tenant
 * identifier from env should drop down to `defineTool` directly,
 * declare its own `requires` with the full surface, and call the
 * underlying mail-tool constructor inside that factory. Folding an
 * additional `requires` parameter into `defineMailTools` would push
 * the "what does the harness need vs. what does the tool runner
 * need" partition onto the caller, which is exactly the partition
 * this helper exists to hide.
 */
export function defineMailTools(
  wrapper: MailToolWrapper,
): AnnotatedToolFactory<MailEnv> {
  return defineTool<MailEnv>({
    id: "@intx/harness/mail",
    requires: ["transport", "address"],
    factory: (env) => {
      const bundle = wrapper(env.transport);
      return {
        definitions: bundle.definitions,
        run: (call, signal) => bundle.run(call, signal),
      };
    },
  });
}

/**
 * Construct a composition-layer agent: the underlying agent wrapped
 * with connector-state-aware storage, transport subscription, INBOX
 * watch, and connector-reply forwarding.
 *
 * The reactor is wrapped exactly once -- inside `createAgent`.
 * `createHarness` augments env.storage with connector-state load/save
 * and subscribes to the agent's event stream to intercept
 * `connector.reply` events for outbound transport sends.
 */
export async function createHarness<EnvReq extends MailEnv>(
  def: AgentDefinition<EnvReq>,
  env: EnvReq,
): Promise<Harness> {
  const transport = env.transport;

  // The wrappedStorage's load() needs to know whether the router's
  // in-memory state is "fresher" than disk. The dirty bit flips on the
  // first state change emitted by the router (commit() in the watch
  // loop, onReplySent() after a connector.reply) and never flips back.
  // Once dirty, the wrappedStorage refuses to restore from disk -- the
  // router's in-memory state is authoritative.
  //
  // The wrappedStorage subscribes to the router's onStateChanged so the
  // dirty bit is set the same tick commit() runs, even if a
  // contextStore.load() races behind it.
  let inMemoryStateAuthoritative = false;
  const userOnStateChanged = env.onConnectorStateChanged;
  const connectorRouter = createConnectorRouter({
    onStateChanged: (state) => {
      inMemoryStateAuthoritative = true;
      if (userOnStateChanged !== undefined) userOnStateChanged(state);
    },
  });

  // Wrap env.storage. The first load() restores connector state from
  // disk only if no router commit has happened yet -- once a commit
  // makes the router's state authoritative, subsequent loads return
  // the store's payload unchanged and leave the in-memory state
  // intact.
  //
  // The router's in-memory state diverges from disk between commit()
  // (in the watch callback) and the next writeMetadata (at the
  // reactor's per-cycle checkpoint). A load() landing in that window
  // must not clobber the in-memory state with the stale disk value --
  // doing so makes the harness's outbound connector.reply path drop
  // replies with NoActiveConnectorThreadError when composeReply() runs
  // after a mid-cycle reload.
  //
  // The wrapper is implemented as a Proxy over env.storage so adding a
  // new method to ContextStore does not require touching the harness:
  // any method not named in `overrides` forwards to env.storage with
  // its `this` bound to env.storage. The two overrides intercept
  // load (cold-boot restore) and writeMetadata (flush router snapshot
  // before delegate). `setConnectorState` is left to the default
  // Proxy fall-through path since the harness adds no behaviour beyond
  // delegation there.
  const overrides = createWrappedStorageOverrides(
    env.storage,
    connectorRouter,
    () => inMemoryStateAuthoritative,
  );

  const wrappedStorage: ContextStore = new Proxy(env.storage, {
    get(target, prop, _receiver) {
      if (prop === "load") return overrides.load;
      if (prop === "writeMetadata") return overrides.writeMetadata;
      const value = Reflect.get(target, prop, target);
      // Bind methods to the underlying store so isogit-style
      // closure-captured state and prototype-bound this both resolve
      // against the real store, not the proxy.
      return typeof value === "function" ? value.bind(target) : value;
    },
  });

  const agentEnv = { ...env, storage: wrappedStorage };

  const agent = await createAgent(def, agentEnv);

  // From here through the final `return`, the agent is constructed
  // and the workdir lock is held. Anything that throws -- the reply
  // drain's IIFE-construction expression, `transport.watch()`,
  // anything in the watch callback's synchronous registration -- has
  // to release the lock by closing the agent before re-raising; the
  // caller never sees the agent and cannot do it themselves.
  // `createAgent` covers its own internal failure paths via its
  // `succeeded`/`finally` shape; this is the matching coverage for
  // the harness's own construction tail.
  let harnessSucceeded = false;
  try {
    // Background drain of the agent's event stream. Intercepts
    // `connector.reply` to send the reply via transport; everything
    // else flows past unobserved. Other consumers can subscribe to the
    // exposed `stream()` method to see the same events.
    //
    // Reply sends are serialized through `replyChain` so two replies
    // fired in quick succession do not interleave their
    // composeReply / transport.send / onReplySent sequence -- the
    // second reply waits for the first's receipt to land in the router
    // before composing its own.
    let stopReplyDrain = false;
    let replyChain: Promise<void> = Promise.resolve();
    const replyDrainDone = (async () => {
      try {
        for await (const event of agent.stream()) {
          if (stopReplyDrain) break;
          if (event.type === "connector.reply") {
            const replyContent = event.data.content;
            replyChain = replyChain.then(async () => {
              try {
                const parts = connectorRouter.composeReply();
                const receipt = await transport.send({
                  ...parts,
                  content: replyContent,
                  type: "conversation.message",
                });
                connectorRouter.onReplySent(receipt);
              } catch (cause) {
                // The reply is dropped and the router state stays at
                // its pre-send value. Surface the loss to the caller's
                // optional onReplySendFailed callback in addition to
                // the operator-facing log so programmatic consumers
                // (retries, alerting) can observe what logger.error
                // alone hides.
                logger.error`Failed to send connector reply: ${cause}`;
                if (env.onReplySendFailed !== undefined) {
                  await invokeReplySendFailed(env.onReplySendFailed, cause);
                }
              }
            });
          }
        }
        // Drain any pending reply before the loop exits so close() sees
        // a settled state.
        await replyChain;
      } catch (cause) {
        // The agent's stream throws on backpressure violations; log and
        // exit the drain. The reply path stops working but the rest of
        // the harness keeps running until close() tears it down.
        // Surface the loss to the caller's optional
        // `onReplyDrainTerminated` callback so programmatic consumers
        // (alerting, watchdogs) can observe what `logger.warn` alone
        // hides.
        logger.warn`Reply-drain stream terminated: ${cause}`;
        if (env.onReplyDrainTerminated !== undefined) {
          await invokeReplyDrainTerminated(env.onReplyDrainTerminated, cause);
        }
      }
    })();

    // Delete a message from the INBOX after it has been delivered to the
    // reactor.
    //
    // A failure here is logged and swallowed: the router state has
    // already been committed and `agent.deliver` has accepted the
    // message, so re-raising would unwind a half-applied delivery. The
    // message stays in the INBOX and a future startup (or watch firing)
    // re-fetches it, re-routes it, and re-delivers it. The router's
    // persisted state makes that benign on the routing side: the sender
    // is already a thread participant, so `route()` returns either a
    // `continue` (which is a no-op state mutation since the sender is
    // unchanged) or a `passthrough` (no headers match). The agent's
    // director sees a duplicate `message.received`; idempotent
    // directors are unaffected, and the audit trail records the
    // duplicate for post-hoc reconciliation.
    async function consumeFromInbox(message: InboundMessage): Promise<void> {
      try {
        await transport.setFlags(message.ref, ["\\Deleted"]);
        await transport.expunge("INBOX");
      } catch (cause) {
        logger.warn`Failed to consume message uid=${message.ref.uid} from INBOX: ${cause}`;
      }
    }

    // INBOX watch loop. Subscribe before the agent's reactor is fully
    // settled so no message is missed in the window between subscription
    // and the first watch callback.
    let stopped = false;
    const unsubscribe: Unsubscribe = transport.watch("INBOX", (event) => {
      if (stopped) return;
      if (event.type !== "exists") return;

      const ref = { uid: event.uid, mailbox: "INBOX" };

      void (async () => {
        try {
          let message: InboundMessage;
          try {
            message = await transport.fetchFull(ref);
          } catch (cause) {
            logger.error`Failed to fetch message uid=${event.uid}: ${cause}`;
            return;
          }

          if (stopped) return;

          let decision: RouteDecision;
          try {
            decision = connectorRouter.route(message);
          } catch (cause) {
            // A router-rejected message (malformed headers, parse error
            // inside the router, etc.) is still surfaced to the agent
            // as an inbound `message.received`. The agent's director
            // decides what the message means and how to respond;
            // dropping it on the floor here would hide messages the
            // operator may want to see. The router's state is *not*
            // committed for the rejected message, so subsequent replies
            // compose against the pre-rejection thread state.
            logger.warn`Connector router rejected message uid=${message.ref.uid} for agent ${env.address}: ${cause instanceof Error ? cause.message : String(cause)}`;
            if (stopped) return;
            agent.deliver(message);
            return;
          }

          if (decision.kind === "passthrough") {
            if (stopped) return;
            agent.deliver(message);
            return;
          }

          // start or continue: commit router state synchronously before
          // any await so a concurrent watch callback observes the
          // updated state.
          connectorRouter.commit(decision);
          if (stopped) return;
          agent.deliver(message);
          await consumeFromInbox(message);
        } catch (cause) {
          // `agent.deliver` throws `AgentClosedError` synchronously when
          // called after the agent has closed. The `if (stopped) return`
          // guards above narrow the race window but cannot close it: a
          // `close()` call landing between the guard and the synchronous
          // throw still surfaces the rejection here. The fetched message
          // is dropped; close() is in progress and the harness is
          // tearing down, so the loss is expected. Without this catch
          // the rejection would escape the void-IIFE as an unhandled
          // promise rejection on the event loop.
          if (cause instanceof Error && cause.name === "AgentClosedError") {
            logger.warn`INBOX watch dropped uid=${event.uid} because the agent closed mid-delivery`;
            return;
          }
          logger.error`INBOX watch failed for uid=${event.uid}: ${cause}`;
        }
      })();
    });

    async function close(): Promise<void> {
      if (stopped) return;
      stopped = true;
      unsubscribe();
      stopReplyDrain = true;
      await agent.close();
      // The reply-drain loop exits once the underlying stream closes
      // (close() above terminates streamConsumers). Awaiting here makes
      // close idempotent and lets callers rely on a settled state.
      await replyDrainDone;
    }

    const harness: Harness = {
      close,
      deliver: (message) => agent.deliver(message),
      setSource: (source) => agent.setSource(source),
      stream: () => agent.stream(),
      blobReader: agent.blobReader,
    };
    harnessSucceeded = true;
    return harness;
  } finally {
    if (!harnessSucceeded) {
      // Close the agent without waiting on its shutdown timeout so a
      // synchronous post-`createAgent` throw does not stall the
      // caller's failure path. The `.catch` swallows any rejection
      // from the close: the caller is already receiving the original
      // throw, and a noisier-than-original close failure here would
      // mask it.
      void agent.close().catch(() => {
        // Swallow per the comment above.
      });
    }
  }
}
