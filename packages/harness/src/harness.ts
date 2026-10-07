// @intx/harness composition layer: composes a mail-transport surface
// on top of `createAgent(def, env)` and owns transport subscription,
// the connector router and its state persistence, the INBOX watch
// loop, and the outbound side of `connector.reply` events. Reactor
// wrapping, audit handling, and source-registry hot-swap stay in
// `@intx/agent`.

import {
  createAgent,
  defineTool,
  type Agent,
  type AgentDefinition,
  type AnnotatedToolFactory,
  type BaseEnv,
  type ToolBundle,
  type ToolDeclaration,
} from "@intx/agent";
import { getLogger } from "@intx/log";
import type {
  BlobReader,
  ConnectorThreadState,
  ContextStore,
  InboundMessage,
  InferenceSource,
  MessageRef,
  MessageTransport,
} from "@intx/types/runtime";

import { createConnectorRouter, type RouteDecision } from "./connector-router";
import { driveConnectorReplies } from "./reply-drain";

const logger = getLogger(["interchange", "harness"]);

/**
 * Keyword the INBOX watch puts on a message whose `transport.fetchFull`
 * threw. The message is left in the INBOX, not consumed, and the watch
 * skips it on later arrival events. Searchable via
 * `hasFlags: [MAIL_FETCH_FAILED_FLAG]`.
 */
export const MAIL_FETCH_FAILED_FLAG = "$FetchFailed";

/**
 * Env extension the composition layer requires beyond `BaseEnv`. Tools
 * shipped by this package declare the matching `requires` so
 * `validateEnv` can blame either at the env entry point.
 *
 * `onReplySendFailed` fires when the reply drain loses an outbound
 * `connector.reply` (compose or send failure): the reply is dropped,
 * the router state is not advanced, and the callback is the only
 * programmatic surface a caller has to observe the loss.
 *
 * `onReplyDrainTerminated` fires when the reply drain's loop exits
 * abnormally (a stream backpressure error); after it fires, outbound
 * `connector.reply` events are no longer forwarded.
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
 * only direct surface; everything else passes through to the
 * underlying agent.
 */
export interface Harness {
  close(): Promise<void>;
  deliver(message: InboundMessage): void;
  setSource(source: InferenceSource): void;
  setSources(sources: InferenceSource[], defaultSource: string): void;
  stream: Agent["stream"];
  readonly blobReader: BlobReader;
}

/**
 * Mail-tool factory shape: `createMailTools` in `@intx/tools-mail`
 * builds a runner from a transport-bearing capability set; the harness
 * wraps it into a single `defineTool` bundle. Callers supply the
 * wrapper on their `AgentDefinition`; `createHarness` does not
 * synthesize it internally.
 */
export type MailToolWrapper = (
  transport: MessageTransport,
) => Omit<ToolBundle, "dispose">;

/**
 * Build the `load` / `writeMetadata` overrides the harness layers onto
 * `env.storage`. Extracted from `createHarness` so the dirty-bit gating
 * on `load()` is directly testable.
 *
 * `load()` restores the router from disk only while
 * `isInMemoryStateAuthoritative()` is false; once a router commit has
 * produced a state change, the in-memory snapshot wins over the
 * pre-commit disk value.
 *
 * Exported for the regression test in this package; no external
 * consumer should call it.
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
 * factory binds `transport` from env at construction time. The
 * `requires: ["transport", "address"]` set captures the env keys the
 * harness's mail path reads: `transport` in the factory body,
 * `address` for the rejected-message log records. `definitions` is the
 * static declaration `defineTool` requires; the caller supplies it
 * because the wrapper binds `transport` from env and cannot run at
 * declaration time.
 */
export function defineMailTools(
  wrapper: MailToolWrapper,
  definitions: readonly ToolDeclaration[],
): AnnotatedToolFactory<MailEnv> {
  return defineTool<MailEnv>({
    id: "@intx/harness/mail",
    requires: ["transport", "address"],
    definitions,
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
 * watch, and connector-reply forwarding. The reactor is wrapped
 * exactly once, inside `createAgent`.
 */
export async function createHarness<EnvReq extends MailEnv>(
  def: AgentDefinition<EnvReq>,
  env: EnvReq,
): Promise<Harness> {
  const transport = env.transport;

  // The dirty bit flips on the router's first state change (commit()
  // in the watch loop, onReplySent() after a connector.reply) and never
  // flips back; once set, the wrappedStorage's load() refuses to
  // restore from disk. Subscribing to onStateChanged sets the bit the
  // same tick commit() runs, even if a load() races behind it.
  let inMemoryStateAuthoritative = false;
  const userOnStateChanged = env.onConnectorStateChanged;
  const connectorRouter = createConnectorRouter({
    onStateChanged: (state) => {
      inMemoryStateAuthoritative = true;
      if (userOnStateChanged !== undefined) userOnStateChanged(state);
    },
  });

  // Wrap env.storage with the two overrides. load() restores connector
  // state from disk only while the router has not committed; otherwise
  // it returns the store payload unchanged, so a mid-cycle load cannot
  // clobber the in-memory state with the stale disk value (which would
  // make composeReply() drop replies with NoActiveConnectorThreadError).
  // The Proxy forwards every other method to env.storage.
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

  // From here through the final `return` the workdir lock is held, so
  // any throw must close the agent before re-raising; the caller never
  // sees the agent and cannot do it themselves.
  let harnessSucceeded = false;
  try {
    // Background drain of the agent's event stream: intercepts
    // `connector.reply` to send via transport; everything else flows
    // past unobserved. The shared `driveConnectorReplies` helper owns
    // the loop (serialization, failure surfacing).
    const replyDrain = driveConnectorReplies({
      stream: agent.stream(),
      composeReply: () => connectorRouter.composeReply(),
      send: (message) => transport.send(message),
      onReplySent: (receipt) => connectorRouter.onReplySent(receipt),
      ...(env.onReplySendFailed !== undefined
        ? { onSendFailed: env.onReplySendFailed }
        : {}),
      ...(env.onReplyDrainTerminated !== undefined
        ? { onTerminated: env.onReplyDrainTerminated }
        : {}),
    });

    // A failure here is logged and swallowed: the router state is
    // already committed and the message delivered, so re-raising would
    // unwind a half-applied delivery.
    async function consumeFromInbox(ref: MessageRef): Promise<void> {
      try {
        await transport.setFlags(ref, ["\\Deleted"]);
        await transport.expunge("INBOX");
      } catch (cause) {
        logger.warn`Failed to consume message uid=${ref.uid} from INBOX: ${cause}`;
      }
    }

    // Whether an earlier attempt on this uid already failed its fetch.
    // The transport is asked rather than an in-process uid set because
    // the record lives on the message, which outlives the process.
    async function fetchAlreadyFailed(ref: MessageRef): Promise<boolean> {
      const flagged = await transport.search("INBOX", {
        hasFlags: [MAIL_FETCH_FAILED_FLAG],
      });
      return flagged.some((candidate) => candidate.uid === ref.uid);
    }

    // A flag write that fails leaves the message unflagged, so a later
    // arrival event retries the fetch -- the safe direction to fail in.
    async function markFetchFailed(ref: MessageRef): Promise<void> {
      try {
        await transport.setFlags(ref, [MAIL_FETCH_FAILED_FLAG]);
      } catch (cause) {
        logger.warn`Failed to flag unfetchable message uid=${ref.uid} in INBOX: ${cause}`;
      }
    }

    // INBOX watch loop. The transport registers the callback before
    // its promise resolves, so a message arriving during acceptance is
    // not missed. A refusal rejects inside this try; the finally
    // closes the agent.
    let stopped = false;
    const unsubscribe = await transport.watch("INBOX", (event) => {
      if (stopped) return;
      if (event.type !== "exists") return;

      const ref = { uid: event.uid, mailbox: "INBOX" };

      void (async () => {
        try {
          if (await fetchAlreadyFailed(ref)) {
            // An earlier attempt failed and flagged the message; leave
            // it in the INBOX and deliver nothing.
            logger.debug`Skipping message uid=${event.uid}: an earlier fetch failed and flagged it ${MAIL_FETCH_FAILED_FLAG}`;
            return;
          }
          if (stopped) return;

          let message: InboundMessage;
          try {
            message = await transport.fetchFull(ref);
          } catch (cause) {
            // The same throw covers an expunged uid (nothing left to
            // consume) and a faulting read (a later attempt may
            // succeed). Only the assembly case is input a peer chose,
            // and consuming on it would let a peer delete its own mail
            // out of the INBOX by malforming a header -- so the
            // message is flagged and left in place, and not delivered.
            logger.error`Failed to fetch message uid=${event.uid}; flagging it ${MAIL_FETCH_FAILED_FLAG} and leaving it in the INBOX: ${cause}`;
            if (stopped) return;
            await markFetchFailed(ref);
            return;
          }

          if (stopped) return;

          let decision: RouteDecision;
          try {
            decision = connectorRouter.route(message);
          } catch (cause) {
            // A router-rejected message is still surfaced to the agent
            // as `message.received`; the router state is *not*
            // committed, so subsequent replies compose against the
            // pre-rejection thread state.
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
          await consumeFromInbox(message.ref);
        } catch (cause) {
          // `agent.deliver` throws `AgentClosedError` synchronously
          // after close(); the guards above narrow the race but cannot
          // close it, so this catch keeps the rejection from escaping
          // the void-IIFE as an unhandled rejection. The message is
          // dropped; the harness is tearing down, so the loss is
          // expected.
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
      replyDrain.stop();
      await agent.close();
      // The drain loop exits once the underlying stream closes;
      // awaiting here makes close idempotent.
      await replyDrain.done;
    }

    const harness: Harness = {
      close,
      deliver: (message) => agent.deliver(message),
      setSource: (source) => agent.setSource(source),
      setSources: (sources, defaultSource) =>
        agent.setSources(sources, defaultSource),
      stream: () => agent.stream(),
      blobReader: agent.blobReader,
    };
    harnessSucceeded = true;
    return harness;
  } finally {
    if (!harnessSucceeded) {
      // Close without waiting on the shutdown timeout so a throw after
      // `createAgent` does not stall the caller's failure path. The
      // `.catch` swallows any close rejection -- the caller is already
      // receiving the original throw.
      void agent.close().catch(() => {
        // Swallow per the comment above.
      });
    }
  }
}
