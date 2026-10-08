// @intx/harness composition layer: a mail-transport surface on top of
// `createAgent(def, env)` -- transport subscription, connector router
// state, the INBOX watch loop, and outbound `connector.reply` events.
// Reactor wrapping, audit handling, and source hot-swap stay in
// @intx/agent.

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
 * Flag the INBOX watch sets on a message whose `transport.fetchFull`
 * threw; the message stays in the INBOX and is skipped on later
 * arrival events.
 */
export const MAIL_FETCH_FAILED_FLAG = "$FetchFailed";

/**
 * Env extension beyond `BaseEnv`; tools shipped by this package declare
 * the matching `requires` so `validateEnv` blames the right entry point.
 *
 * `onReplySendFailed` fires when the reply drain drops an outbound
 * `connector.reply` (compose or send failure): the reply is dropped and
 * the router state is not advanced. `onReplyDrainTerminated` fires when
 * the drain loop exits abnormally; after it, outbound replies are no
 * longer forwarded.
 */
export interface MailEnv extends BaseEnv {
  transport: MessageTransport;
  address: string;
  onConnectorStateChanged?: (state: ConnectorThreadState | null) => void;
  onReplySendFailed?: (cause: unknown) => void | Promise<void>;
  onReplyDrainTerminated?: (cause: unknown) => void | Promise<void>;
}

/** Public surface returned by `createHarness`; everything but `close` passes through to the underlying agent. */
export interface Harness {
  close(): Promise<void>;
  deliver(message: InboundMessage): void;
  setSource(source: InferenceSource): void;
  setSources(sources: InferenceSource[], defaultSource: string): void;
  stream: Agent["stream"];
  readonly blobReader: BlobReader;
}

/**
 * What `createMailTools` in `@intx/tools-mail` returns; the harness
 * wraps it into a single `defineTool` bundle. Callers supply it on
 * their `AgentDefinition`.
 */
export type MailToolWrapper = (
  transport: MessageTransport,
) => Omit<ToolBundle, "dispose">;

/**
 * The `load` / `writeMetadata` overrides layered onto `env.storage`;
 * extracted from `createHarness` so the dirty-bit gating on `load()` is
 * directly testable. `load()` restores the router from disk only while
 * no router commit has produced a state change; after that the
 * in-memory snapshot wins. Exported for this package's tests; no
 * external consumer should call it.
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
 * factory binds `transport` from env at construction time; `requires`
 * names the env keys the mail path reads. `definitions` is the static
 * declaration `defineTool` requires, supplied by the caller because the
 * wrapper cannot run at declaration time.
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

  // Set on the router's first state change (commit() or onReplySent())
  // and never cleared; once set, load() refuses to restore from disk.
  // onStateChanged fires in the same tick commit() runs, so the bit is
  // set before a racing load().
  let inMemoryStateAuthoritative = false;
  const userOnStateChanged = env.onConnectorStateChanged;
  const connectorRouter = createConnectorRouter({
    onStateChanged: (state) => {
      inMemoryStateAuthoritative = true;
      if (userOnStateChanged !== undefined) userOnStateChanged(state);
    },
  });

  // Wrap env.storage with the two overrides; the Proxy forwards every
  // other method to env.storage.
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
      // Bind methods so closure-captured state and prototype-bound this
      // resolve against the real store, not the proxy.
      return typeof value === "function" ? value.bind(target) : value;
    },
  });

  const agentEnv = { ...env, storage: wrappedStorage };

  const agent = await createAgent(def, agentEnv);

  // From here the workdir lock is held, so any throw must close the
  // agent before re-raising; the caller never sees it.
  let harnessSucceeded = false;
  try {
    // Drain `connector.reply` events out through the transport;
    // everything else flows past unobserved.
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

    // Failures are logged and swallowed: the router state is committed
    // and the message delivered, so re-raising would unwind a
    // half-applied delivery.
    async function consumeFromInbox(ref: MessageRef): Promise<void> {
      try {
        await transport.setFlags(ref, ["\\Deleted"]);
        await transport.expunge("INBOX");
      } catch (cause) {
        logger.warn`Failed to consume message uid=${ref.uid} from INBOX: ${cause}`;
      }
    }

    // Ask the transport rather than keeping an in-process uid set: the
    // flag record lives on the message, which outlives the process.
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

    // INBOX watch. The transport registers the callback before its
    // promise resolves, so a message arriving during acceptance is not
    // missed.
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
            // The same throw covers an expunged uid and a faulting
            // read. Consuming here would let a peer delete its own mail
            // out of the INBOX by malforming a header, so the message
            // is flagged and left in place, not delivered.
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
            // A router-rejected message still reaches the agent; the
            // router state is not committed, so replies compose against
            // the pre-rejection thread state.
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
          // after close(); the guards above narrow but cannot close the
          // race, so this catch keeps the rejection from escaping the
          // void-IIFE. The message is dropped; the harness is tearing
          // down, so the loss is expected.
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
      // `createAgent` does not stall the failure path; swallow any
      // close rejection -- the caller already has the original throw.
      void agent.close().catch(() => {
        // Swallow.
      });
    }
  }
}
