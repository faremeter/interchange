// Agent harness: supervisor, connector, and reactor wiring.
//
// The harness is the supervisor layer between the message transport and the
// reactor. It watches the agent's INBOX, routes messages by thread, and
// manages the connector lifecycle.
//
// Connector semantics:
//   - Messages in the active connector thread are fetched, delivered to the
//     reactor, and deleted from the INBOX (consumed).
//   - All other inbound messages (replies to agent sends, unsolicited
//     inter-agent mail) are delivered to the reactor and stay in the INBOX.
//   - Outbound replies are sent by the harness when the reactor emits a
//     connector.reply event, with correct threading headers.
//
// The INBOX is a delivery queue — the persistent conversation record lives in
// the context store (git), not the mailbox.
//
// (ARCHITECTURE.md § Agent Harness, INFERENCE.md § Relationship to Harness)

import { getLogger } from "@intx/log";
import { createReactorAssembly } from "@intx/inference";
import type { ReactorEmittedEvent } from "@intx/inference";
import {
  InferenceSource,
  applyInferenceSourceFields,
  type BlobReader,
  type ContextStore,
  type InboundMessage,
  type Unsubscribe,
  type ReactorDirector,
} from "@intx/types/runtime";

import type { ErrorRecord } from "@intx/types/audit";

import type { HarnessConfig } from "./config";
import { validateConfig } from "./config";
import {
  buildMailToolHandlers,
  buildCombinedRunner,
  getMailToolDefinitions,
} from "./tools";
import { createDefaultDirector } from "./director";
import { createConnectorRouter } from "./connector-router";
import { type } from "arktype";

const logger = getLogger(["interchange", "harness"]);

export type Harness = {
  /**
   * Begin watching the agent's INBOX and start the reactor event loop.
   * Must be called exactly once.
   */
  start(): void;

  /**
   * Initiate graceful shutdown: abort the reactor, unsubscribe from the
   * transport watch, and flush state to the context store.
   */
  stop(): void;

  /**
   * Inject an already-fetched inbound message directly into the reactor.
   * Useful for testing and for messages the harness receives through channels
   * other than the INBOX watch.
   */
  deliver(message: InboundMessage): void;

  /**
   * Hot-swap the active inference source. Takes effect on the next
   * inference call — in-flight calls continue with the previous source.
   */
  setSource(source: InferenceSource): void;

  /**
   * Read-only blob reader backed by this harness's context store. Pass it to
   * the tool factory (e.g. `createPosixTools({ blobReader })`) so the agent
   * can resolve `tool-output:///{callId}` URIs through the same store the
   * reactor commits to.
   */
  readonly blobReader: BlobReader;
};

export function createHarness(config: HarnessConfig): Harness {
  validateConfig(config);

  const { transport, storage, source, tools, onEvent } = config;

  const deployToolDefs = config.deployTools ?? [];

  let director: ReactorDirector;
  if (config.director !== undefined) {
    director = config.director;
  } else {
    director = createDefaultDirector(
      config.systemPrompt,
      [...getMailToolDefinitions(), ...deployToolDefs],
      config.directorPolicy ?? {},
    );
  }

  // Build mail tool handlers and the combined runner. Name collision
  // detection happens here at construction time — startup fails loudly.
  const mailHandlers = buildMailToolHandlers(transport);

  const combinedRunner = buildCombinedRunner(
    mailHandlers,
    tools,
    deployToolDefs,
  );

  const sessionId = crypto.randomUUID();

  const auditStore = config.auditStore;

  const accumulatedErrors: ErrorRecord[] = [];
  let errorSeq = 0;

  // -------------------------------------------------------------------------
  // Connector state: track which thread(s) this reactor owns.
  // -------------------------------------------------------------------------

  const connectorRouter = createConnectorRouter();

  // Wrap the context store so load() restores connector state and the reactor's
  // per-cycle writeMetadata picks up the live connector state via the underlying
  // store's setConnectorState buffer (Phase 4: connector state rides along with
  // metadata.json rather than being injected during commit).
  const wrappedStore: ContextStore = {
    async load(signal) {
      const loaded = await storage.load(signal);
      connectorRouter.restore(loaded.connectorState);
      return loaded;
    },
    setConnectorState(state) {
      storage.setConnectorState(state);
    },
    async commit(options, signal) {
      return storage.commit(options, signal);
    },
    async branch(name, signal) {
      return storage.branch(name, signal);
    },
    async log(limit, signal) {
      return storage.log(limit, signal);
    },
    async readAt(hash, signal) {
      return storage.readAt(hash, signal);
    },
    async writeBlob(key, bytes, contentType, signal) {
      return storage.writeBlob(key, bytes, contentType, signal);
    },
    async readBlob(key, signal) {
      return storage.readBlob(key, signal);
    },
    async writePrompt(turns, signal) {
      return storage.writePrompt(turns, signal);
    },
    async writeResponse(turn, signal) {
      return storage.writeResponse(turn, signal);
    },
    async writeManifest(records, signal) {
      return storage.writeManifest(records, signal);
    },
    async writeTurns(turns, signal) {
      return storage.writeTurns(turns, signal);
    },
    async writeMetadata(metadata, signal) {
      // Flush the current in-memory connector state into the wrapped store's
      // buffer so writeMetadata picks it up alongside pendingOperations and
      // tokenUsage. This is the reactor's per-cycle moment to durably record
      // connector thread state.
      storage.setConnectorState(connectorRouter.snapshot());
      return storage.writeMetadata(metadata, signal);
    },
    async readManifestHistory(limit, signal) {
      return storage.readManifestHistory(limit, signal);
    },
  };

  /**
   * Delete a message from the INBOX after it has been delivered to the reactor.
   */
  async function consumeFromInbox(message: InboundMessage): Promise<void> {
    try {
      await transport.setFlags(message.ref, ["\\Deleted"]);
      await transport.expunge("INBOX");
    } catch (cause) {
      logger.warn`Failed to consume message uid=${message.ref.uid} from INBOX: ${cause}`;
    }
  }

  // -------------------------------------------------------------------------
  // Event interception
  // -------------------------------------------------------------------------

  function handleEvent(event: ReactorEmittedEvent): void {
    // Handle connector.reply: send the reply via transport.
    if (event.type === "connector.reply") {
      const replyContent = event.data.content;

      void (async () => {
        try {
          const parts = connectorRouter.composeReply();
          const receipt = await transport.send({
            ...parts,
            content: replyContent,
            type: "conversation.message",
          });
          connectorRouter.onReplySent(receipt);
        } catch (cause) {
          logger.error`Failed to send connector reply: ${cause}`;
        }
      })();
    }

    // message.received is reactor-internal; do not forward to the caller.
    if (event.type === "message.received") return;

    if (event.type === "inference.error" && auditStore) {
      accumulatedErrors.push({
        source: "inference",
        category: event.data.error.category,
        message: event.data.error.message,
        fatal: false,
        timestamp: new Date().toISOString(),
        sessionId,
        seq: errorSeq++,
        ...(event.data.error.statusCode !== undefined
          ? { statusCode: event.data.error.statusCode }
          : {}),
      });
    }

    if (event.type === "reactor.error" && auditStore) {
      accumulatedErrors.push({
        source: "reactor",
        category: "reactor_error",
        message: event.data.error,
        fatal: event.data.fatal,
        timestamp: new Date().toISOString(),
        sessionId,
        seq: errorSeq++,
      });
    }

    onEvent(event);
  }

  // -------------------------------------------------------------------------
  // Reactor
  // -------------------------------------------------------------------------

  async function flushErrors(): Promise<void> {
    if (accumulatedErrors.length === 0) return;
    if (auditStore === undefined) return;
    const count = accumulatedErrors.length;
    await auditStore.commitErrors(accumulatedErrors.slice(0, count));
    accumulatedErrors.splice(0, count);
  }

  // activeSource is held as a single mutable object whose reference is
  // shared with the reactor's config (via the assembly helper). The reactor
  // reads the source lazily at each inference call, so mutating the
  // fields on this object hot-swaps credentials and model without
  // restarting.
  const activeSource: InferenceSource = { ...source };

  const { reactor, blobReader } = createReactorAssembly({
    sessionId,
    director,
    source: activeSource,
    toolRunner: combinedRunner,
    contextStore: wrappedStore,
    onEvent: handleEvent,
    ...(config.authorize !== undefined ? { authorize: config.authorize } : {}),
    ...(config.auditStore !== undefined
      ? { auditStore: config.auditStore }
      : {}),
    ...(config.beforeToolExtensions !== undefined
      ? { beforeToolExtensions: config.beforeToolExtensions }
      : {}),
    // flushErrors only runs when audit is wired — preserves today's
    // behavior where harness.ts only invokes flushErrors inside the
    // auditCollector branch.
    ...(config.auditStore !== undefined
      ? { afterCheckpoint: flushErrors, onShutdown: flushErrors }
      : {}),
  });

  let unsubscribe: Unsubscribe | null = null;
  let started = false;
  let stopped = false;

  function start(): void {
    if (started) {
      throw new Error("Harness is already started");
    }
    started = true;

    // Subscribe to the INBOX before starting the reactor so no messages are
    // missed in the window between subscription and first watch callback.
    unsubscribe = transport.watch("INBOX", (event) => {
      if (stopped) return;

      if (event.type !== "exists") {
        return;
      }

      const ref = { uid: event.uid, mailbox: "INBOX" };

      void (async () => {
        let message;
        try {
          message = await transport.fetchFull(ref);
        } catch (cause) {
          logger.error`Failed to fetch message uid=${event.uid}: ${cause}`;
          return;
        }

        if (stopped) return;

        // Only connector-thread messages are consumed from the INBOX.
        // Everything else is delivered to the reactor and stays in the
        // INBOX so message tools can access it.
        const decision = connectorRouter.route(message);
        if (decision.kind === "passthrough") {
          // Non-connector mail (replies to agent sends, unsolicited
          // inter-agent mail, etc.). Deliver to reactor for notification
          // but leave in INBOX for message tools.
          reactor.deliver(message);
          return;
        }

        // start or continue: commit router state synchronously before
        // any await so that a concurrent watch callback fired during
        // consumeFromInbox observes the updated state.
        connectorRouter.commit(decision);
        reactor.deliver(message);
        await consumeFromInbox(message);
      })();
    });

    reactor.start();
  }

  function stop(): void {
    if (stopped) return;
    stopped = true;

    reactor.abort("user_disconnect");

    if (unsubscribe !== null) {
      unsubscribe();
      unsubscribe = null;
    }
  }

  function deliver(message: InboundMessage): void {
    reactor.deliver(message);
  }

  function setSource(newSource: InferenceSource): void {
    const parsed = InferenceSource(newSource);
    if (parsed instanceof type.errors) {
      throw new Error(`Invalid InferenceSource: ${parsed.summary}`);
    }
    // Mutate the shared activeSource object in place so the reactor's
    // next inference call (which reads the source lazily through the
    // same reference held by the assembly helper) observes the new
    // fields. Defaults and capabilities rotate alongside the
    // credentials.
    applyInferenceSourceFields(activeSource, parsed);
  }

  return { start, stop, deliver, setSource, blobReader };
}
