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

import { getLogger } from "@interchange/log";
import { createReactorAssembly } from "@interchange/inference";
import type { ReactorEmittedEvent } from "@interchange/inference";
import {
  ProviderConfig,
  type BlobReader,
  type ContextStore,
  type ConnectorThreadState,
  type InboundMessage,
  type Unsubscribe,
  type ReactorDirector,
} from "@interchange/types/runtime";

import type { ErrorRecord } from "@interchange/types/audit";

import type { HarnessConfig } from "./config";
import { validateConfig } from "./config";
import {
  buildMailToolHandlers,
  buildCombinedRunner,
  getMailToolDefinitions,
} from "./tools";
import { createDefaultDirector } from "./director";
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
   * Hot-swap the provider configuration. Takes effect on the next inference
   * call — in-flight calls continue with the previous config.
   */
  setProviderConfig(config: ProviderConfig): void;

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

  const { transport, storage, provider, tools, onEvent } = config;

  const deployToolDefs = config.deployTools ?? [];

  let director: ReactorDirector;
  if (config.director !== undefined) {
    director = config.director;
  } else {
    if (provider.model === undefined) {
      throw new Error(
        "provider.model is required when using the default director",
      );
    }
    director = createDefaultDirector(
      provider.model,
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

  let connectorThreadRoot: string | undefined = undefined;
  let connectorLastMessageId: string | undefined = undefined;
  let connectorReplyTo: string | undefined = undefined;
  let connectorSubject: string | undefined = undefined;

  function currentConnectorState(): ConnectorThreadState | null {
    if (
      connectorThreadRoot === undefined ||
      connectorLastMessageId === undefined ||
      connectorReplyTo === undefined
    ) {
      return null;
    }
    return {
      threadRoot: connectorThreadRoot,
      lastMessageId: connectorLastMessageId,
      replyTo: connectorReplyTo,
      ...(connectorSubject !== undefined ? { subject: connectorSubject } : {}),
    };
  }

  function restoreConnectorState(state: ConnectorThreadState | null): void {
    if (state === null) {
      connectorThreadRoot = undefined;
      connectorLastMessageId = undefined;
      connectorReplyTo = undefined;
      connectorSubject = undefined;
    } else {
      connectorThreadRoot = state.threadRoot;
      connectorLastMessageId = state.lastMessageId;
      connectorReplyTo = state.replyTo;
      connectorSubject = state.subject;
    }
  }

  // Wrap the context store so load() restores connector state and the reactor's
  // per-cycle writeMetadata picks up the live connector state via the underlying
  // store's setConnectorState buffer (Phase 4: connector state rides along with
  // metadata.json rather than being injected during commit).
  const wrappedStore: ContextStore = {
    async load(signal) {
      const loaded = await storage.load(signal);
      restoreConnectorState(loaded.connectorState);
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
      storage.setConnectorState(currentConnectorState());
      return storage.writeMetadata(metadata, signal);
    },
    async readManifestHistory(limit, signal) {
      return storage.readManifestHistory(limit, signal);
    },
  };

  /**
   * Determine whether a message belongs to the active connector thread.
   */
  function isConnectorMessage(message: InboundMessage): boolean {
    if (connectorThreadRoot === undefined) return false;

    const { inReplyTo, references } = message.headers;

    if (references !== undefined && references.includes(connectorThreadRoot)) {
      return true;
    }

    if (
      inReplyTo !== undefined &&
      connectorLastMessageId !== undefined &&
      inReplyTo === connectorLastMessageId
    ) {
      return true;
    }

    return false;
  }

  /**
   * Start tracking a new connector thread from an initial inbound message.
   */
  function initConnectorThread(message: InboundMessage): void {
    connectorThreadRoot = message.headers.messageId;
    connectorLastMessageId = message.headers.messageId;
    connectorReplyTo = message.headers.from;
    connectorSubject = message.headers.subject;
  }

  /**
   * Update connector state when a new message arrives in the thread.
   */
  function advanceConnectorThread(message: InboundMessage): void {
    connectorLastMessageId = message.headers.messageId;
    connectorReplyTo = message.headers.from;
  }

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
    if (
      event.type === "connector.reply" &&
      connectorReplyTo !== undefined &&
      connectorLastMessageId !== undefined
    ) {
      const replyContent = event.data.content;
      const replyTo = connectorReplyTo;
      const inReplyTo = connectorLastMessageId;
      const subject = connectorSubject;

      void (async () => {
        try {
          const receipt = await transport.send({
            to: replyTo,
            content: replyContent,
            type: "conversation.message",
            inReplyTo,
            ...(subject !== undefined ? { subject } : {}),
          });
          connectorLastMessageId = receipt.messageId;
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

  // providerConfig is held as a single mutable object whose reference is
  // shared with the reactor's config (via the assembly helper). The reactor
  // reads providerConfig lazily at each inference call, so mutating the
  // fields on this object hot-swaps credentials without restarting.
  const providerConfig: ProviderConfig = { ...provider };

  const { reactor, blobReader } = createReactorAssembly({
    sessionId,
    director,
    providerConfig,
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
        if (connectorThreadRoot === undefined) {
          // No active conversation — this message starts one.
          initConnectorThread(message);
          reactor.deliver(message);
          await consumeFromInbox(message);
        } else if (isConnectorMessage(message)) {
          // Continues the active connector thread.
          advanceConnectorThread(message);
          reactor.deliver(message);
          await consumeFromInbox(message);
        } else {
          // Non-connector mail (replies to agent sends, unsolicited
          // inter-agent mail, etc.). Deliver to reactor for notification
          // but leave in INBOX for message tools.
          reactor.deliver(message);
        }
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

  function setProviderConfig(newConfig: ProviderConfig): void {
    const parsed = ProviderConfig(newConfig);
    if (parsed instanceof type.errors) {
      throw new Error(`Invalid ProviderConfig: ${parsed.summary}`);
    }
    // Mutate the shared providerConfig object in place so the reactor's
    // next inference call (which reads providerConfig lazily through the
    // same reference held by the assembly helper) observes the new fields.
    providerConfig.provider = parsed.provider;
    providerConfig.baseURL = parsed.baseURL;
    providerConfig.apiKey = parsed.apiKey;
    if (parsed.model !== undefined) {
      providerConfig.model = parsed.model;
    } else {
      delete providerConfig.model;
    }
  }

  return { start, stop, deliver, setProviderConfig, blobReader };
}
