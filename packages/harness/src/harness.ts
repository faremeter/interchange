// Agent harness: supervisor, connector, and reactor wiring.
//
// The harness is the supervisor layer between the message transport and the
// reactor. It watches the agent's INBOX, routes messages by thread, and
// manages the connector lifecycle.
//
// Connector semantics:
//   - Messages in the active connector thread are fetched, delivered to the
//     reactor, and deleted from the INBOX (consumed).
//   - Messages responding to agent-initiated outbound sends are also delivered
//     and consumed (tracked via outbound message IDs).
//   - Unsolicited messages (new threads, untracked threads) stay in the INBOX
//     for the agent to discover via message tools.
//   - Outbound replies are sent by the harness when the reactor emits a
//     connector.reply event, with correct threading headers.
//
// The INBOX is a delivery queue — the persistent conversation record lives in
// the context store (git), not the mailbox.
//
// (ARCHITECTURE.md § Agent Harness, INFERENCE.md § Relationship to Harness)

import { getLogger } from "@interchange/log";
import {
  createReactor,
  createAuditCollector,
  createAuthzExtension,
} from "@interchange/inference";
import type { Reactor, AuditCollector } from "@interchange/inference";
import type {
  InboundMessage,
  InferenceEvent,
  Unsubscribe,
  ReactorPlugin,
} from "@interchange/types/runtime";

import type { HarnessConfig } from "./config";
import { validateConfig } from "./config";
import {
  buildMessageToolHandlers,
  buildCombinedRunner,
  getMessageToolDefinitions,
} from "./tools";
import { createDefaultPlugin } from "./plugin";

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
};

export function createHarness(config: HarnessConfig): Harness {
  validateConfig(config);

  const { transport, storage, provider, tools, onEvent } = config;

  let plugin: ReactorPlugin;
  if (config.plugin !== undefined) {
    plugin = config.plugin;
  } else {
    if (provider.model === undefined) {
      throw new Error(
        "provider.model is required when using the default plugin",
      );
    }
    plugin = createDefaultPlugin(
      provider.model,
      config.systemPrompt,
      getMessageToolDefinitions(),
      config.pluginPolicy ?? {},
    );
  }

  // Build message tool handlers and the combined runner. Name collision
  // detection happens here at construction time — startup fails loudly.
  const messageHandlers = buildMessageToolHandlers(transport);

  const callerToolNames = (config as { toolNames?: string[] }).toolNames ?? [];

  const combinedRunner = buildCombinedRunner(
    messageHandlers,
    tools,
    callerToolNames,
  );

  const sessionId = crypto.randomUUID();

  // -------------------------------------------------------------------------
  // Audit collector: correlates tool events with authz decisions.
  // -------------------------------------------------------------------------

  const auditStore = config.auditStore;
  const auditCollector: AuditCollector | undefined =
    auditStore !== undefined ? createAuditCollector(sessionId) : undefined;

  const authzExtension =
    config.authorize !== undefined
      ? createAuthzExtension({
          authorize: config.authorize,
          onDecision: (d) => auditCollector?.onDecision(d),
        })
      : undefined;

  const beforeToolExtensions = [
    ...(authzExtension !== undefined ? [authzExtension] : []),
    ...(config.beforeToolExtensions ?? []),
  ];

  // -------------------------------------------------------------------------
  // Connector state: track which thread(s) this reactor owns.
  // -------------------------------------------------------------------------

  // The root Message-ID of the active connector conversation. Messages whose
  // References chain includes this ID are connector traffic.
  let connectorThreadRoot: string | undefined = undefined;

  // The Message-ID of the most recent message in the connector thread. Used
  // as In-Reply-To when sending the next reply.
  let connectorLastMessageId: string | undefined = undefined;

  // The address to reply to for the connector conversation.
  let connectorReplyTo: string | undefined = undefined;

  // The subject line of the connector conversation.
  let connectorSubject: string | undefined = undefined;

  // -------------------------------------------------------------------------
  // Outbound thread tracking: message IDs from agent-initiated sends.
  // -------------------------------------------------------------------------

  // Message-IDs of outbound messages sent by the agent via tools. When a
  // response arrives with In-Reply-To matching one of these, it belongs to
  // the reactor and gets delivered + consumed.
  const outboundMessageIds = new Set<string>();

  // Track in-flight tool calls that are message sends, so we can extract the
  // messageId from the result.
  const pendingSendCallIds = new Set<string>();

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
   * Determine whether a message is a response to an agent-initiated send.
   */
  function isAgentInitiatedResponse(message: InboundMessage): boolean {
    const { inReplyTo } = message.headers;
    if (inReplyTo === undefined) return false;
    return outboundMessageIds.has(inReplyTo);
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

  function handleEvent(event: InferenceEvent): void {
    if (auditCollector !== undefined) {
      auditCollector.onEvent(event);
    }

    // Track outbound sends: when a message.send or message.reply tool starts,
    // note the call ID. When it completes, extract the messageId.
    if (event.type === "tool.start") {
      const name = event.data.call.name;
      if (name === "message_send" || name === "message_reply") {
        pendingSendCallIds.add(event.data.call.id);
      }
    }

    if (event.type === "tool.done") {
      const callId = event.data.result.callId;
      if (pendingSendCallIds.has(callId)) {
        pendingSendCallIds.delete(callId);
        const content = event.data.result.content;
        if (
          typeof content === "object" &&
          content !== null &&
          "messageId" in content &&
          typeof (content as Record<string, unknown>)["messageId"] === "string"
        ) {
          outboundMessageIds.add(
            (content as Record<string, unknown>)["messageId"] as string,
          );
        }
      }
    }

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

    // Always forward the event to the caller.
    onEvent(event);
  }

  // -------------------------------------------------------------------------
  // Reactor
  // -------------------------------------------------------------------------

  async function flushAudit(): Promise<void> {
    if (auditCollector === undefined) return;
    if (auditStore === undefined) {
      throw new Error(
        "auditStore must be defined when auditCollector is present",
      );
    }
    const records = auditCollector.flush();
    if (records.length > 0) {
      await auditStore.commitAudit(records);
    }
  }

  const reactorConfig: Parameters<typeof createReactor>[0] = {
    sessionId,
    plugin,
    providerConfig: provider,
    toolRunner: combinedRunner,
    contextStore: storage,
    onEvent: handleEvent,
    beforeToolExtensions,
  };

  if (auditCollector !== undefined) {
    reactorConfig.afterCheckpoint = () => flushAudit();
    reactorConfig.onShutdown = async () => {
      const inflight = auditCollector.pending();
      if (inflight > 0) {
        logger.warn`${inflight} audit records in flight at shutdown, these tool calls will not be recorded`;
      }
      await flushAudit();
    };
  }

  const reactor: Reactor = createReactor(reactorConfig);

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

        // Route by thread: connector and agent-initiated traffic is consumed,
        // everything else stays in the INBOX.
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
        } else if (isAgentInitiatedResponse(message)) {
          // Response to an outbound message the agent sent via tools.
          reactor.deliver(message);
          await consumeFromInbox(message);
        }
        // else: unsolicited message — stays in the INBOX for the agent's
        // mail tools to discover.
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

  return { start, stop, deliver };
}
