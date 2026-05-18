// In-process agent runtime.
//
// `createAgent` resolves storage, builds the provider registry and tool
// dispatcher, wires `createReactorAssembly`, and exposes the public Agent
// surface. The agent owns the active provider config object so
// `setProvider` can mutate it in place and the reactor's lazy read at the
// start of each inference call picks up the swap.
//
// This module implements the baseline shape: `deliver`, `setProvider`,
// `history`, `checkpoints`, `readAt`, `blobReader`, and a single-in-flight
// `send`. The FIFO queue + AbortSignal on `send`, the bounded fan-out for
// `stream`, and the full `close` lifecycle land in subsequent commits.

import { createDefaultDirector } from "@interchange/harness";
import {
  createReactorAssembly,
  type AuthzExtensionOptions,
  type ReactorEmittedEvent,
} from "@interchange/inference";
import { createInboundMessage } from "@interchange/mime";
import { createIsogitStore } from "@interchange/storage-isogit";
import type {
  AssistantTurn,
  AuditStore,
  BlobReader,
  ContextCommit,
  ContextStore,
  ConversationTurn,
  InboundMessage,
  ProviderConfig,
  ReactorDirector,
} from "@interchange/types/runtime";

import { acquireContextDirLock, type ContextDirLock } from "./lock";
import { createProviderRegistry, type ProviderRegistry } from "./provider";
import { createToolRunner, type AgentTool, type AgentToolRunner } from "./tool";

const DEFAULT_SEND_FROM = "user@local";
const DEFAULT_SEND_TO = "agent@local";

export type AgentConfig = {
  /**
   * The conversation/history store. Exactly one of `contextStore` or
   * `contextDir` must be supplied. When `contextStore` is given the caller
   * owns the store's lifetime; the singleton-per-directory lock is not
   * acquired.
   */
  contextStore?: ContextStore;

  /**
   * Path to a directory the agent will manage as an isogit-backed
   * `ContextStore & AuditStore`. The singleton-per-directory lock is
   * acquired when this form is used.
   */
  contextDir?: string;

  /** Pre-configured providers. Must be non-empty. Each entry must have model. */
  providers: ProviderConfig[];
  /** Must match the `model` field of one of `providers`. */
  defaultModel: string;

  /** System prompt for the default director. */
  systemPrompt: string;
  /** Tools registered via `tool()` / `stringTool()`. */
  tools: AgentTool[];
  /** Director override. Defaults to `createDefaultDirector`. */
  director?: ReactorDirector;

  /** Audit store. When omitted with `contextDir`, the isogit store is used. */
  auditStore?: AuditStore;
  /** Authz extension hook. */
  authorize?: AuthzExtensionOptions["authorize"];
  /** Override the default 10k tool-result size cap. */
  sizeCapMaxChars?: number;

  /** Override the auto-generated session ID. */
  sessionId?: string;
};

export type SendOptions = {
  signal?: AbortSignal;
  /** Override the default "from" header on the synthetic inbound message. */
  from?: string;
};

export type SendResult = {
  /** Reply text emitted by the director's `reply` action. */
  reply: string;
  /**
   * Full-fidelity assistant turn that produced the reply. Captured from
   * the reactor's `inference.done` event preceding `connector.reply`.
   */
  turn: ConversationTurn;
};

export type Agent = {
  send(
    content: string | InboundMessage,
    opts?: SendOptions,
  ): Promise<SendResult>;
  stream(): AsyncIterable<ReactorEmittedEvent>;
  deliver(message: InboundMessage): void;
  close(): Promise<void>;
  setProvider(config: ProviderConfig): void;
  history(): Promise<ConversationTurn[]>;
  checkpoints(limit?: number): Promise<ContextCommit[]>;
  readAt(hash: string): Promise<ConversationTurn[]>;
  readonly blobReader: BlobReader;
};

export class AgentConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AgentConfigError";
  }
}

export class AgentClosedError extends Error {
  constructor() {
    super("agent is closed");
    this.name = "AgentClosedError";
  }
}

export class ConcurrentSendError extends Error {
  constructor() {
    super("send() is already in flight; queueing lands in a later change");
    this.name = "ConcurrentSendError";
  }
}

export async function createAgent(config: AgentConfig): Promise<Agent> {
  const hasStore = config.contextStore !== undefined;
  const hasDir = config.contextDir !== undefined;
  if (hasStore === hasDir) {
    throw new AgentConfigError(
      "exactly one of contextStore or contextDir is required",
    );
  }

  let contextStore: ContextStore;
  let auditStore: AuditStore | undefined;
  let lock: ContextDirLock | undefined;

  if (config.contextDir !== undefined) {
    lock = acquireContextDirLock(config.contextDir);
    try {
      const store = await createIsogitStore(config.contextDir);
      contextStore = store;
      auditStore = config.auditStore ?? store;
    } catch (cause) {
      lock.release();
      throw cause;
    }
  } else if (config.contextStore !== undefined) {
    contextStore = config.contextStore;
    auditStore = config.auditStore;
  } else {
    throw new AgentConfigError("unreachable: storage form validated above");
  }

  let providerRegistry: ProviderRegistry;
  let toolRunner: AgentToolRunner;
  try {
    providerRegistry = createProviderRegistry({
      providers: config.providers,
      defaultModel: config.defaultModel,
    });
    toolRunner = createToolRunner(config.tools);
  } catch (cause) {
    if (lock !== undefined) lock.release();
    throw cause;
  }

  let director: ReactorDirector;
  if (config.director !== undefined) {
    director = config.director;
  } else {
    if (providerRegistry.active.model === undefined) {
      if (lock !== undefined) lock.release();
      throw new AgentConfigError(
        "active provider must have model defined when using the default director",
      );
    }
    director = createDefaultDirector(
      providerRegistry.active.model,
      config.systemPrompt,
      [...toolRunner.definitions],
      {},
    );
  }

  const sessionId = config.sessionId ?? crypto.randomUUID();

  // Event listeners. Commits 7 and 8 replace these naive callback sets with
  // a FIFO send queue and a bounded per-consumer stream buffer.
  type EventListener = (event: ReactorEmittedEvent) => void;
  const streamConsumers = new Set<EventListener>();
  let activeSendListener: EventListener | undefined;

  function handleEvent(event: ReactorEmittedEvent): void {
    if (activeSendListener !== undefined) {
      activeSendListener(event);
    }
    for (const consumer of streamConsumers) {
      consumer(event);
    }
  }

  const { reactor, blobReader } = createReactorAssembly({
    sessionId,
    director,
    providerConfig: providerRegistry.active,
    toolRunner,
    contextStore,
    onEvent: handleEvent,
    ...(auditStore !== undefined ? { auditStore } : {}),
    ...(config.authorize !== undefined ? { authorize: config.authorize } : {}),
    ...(config.sizeCapMaxChars !== undefined
      ? { sizeCapMaxChars: config.sizeCapMaxChars }
      : {}),
  });

  reactor.start();

  let closed = false;
  let sendInFlight = false;

  function ensureOpen(): void {
    if (closed) throw new AgentClosedError();
  }

  function buildInboundMessage(
    content: string | InboundMessage,
    opts?: SendOptions,
  ): InboundMessage {
    if (typeof content !== "string") return content;
    // Conversation messages use `content` (a string); the mail-builder
    // rejects passing `payload` for conversation types.
    return createInboundMessage({
      from: opts?.from ?? DEFAULT_SEND_FROM,
      to: DEFAULT_SEND_TO,
      content,
      interchangeType: "conversation.message",
    });
  }

  function send(
    content: string | InboundMessage,
    opts?: SendOptions,
  ): Promise<SendResult> {
    ensureOpen();
    if (sendInFlight) {
      return Promise.reject(new ConcurrentSendError());
    }
    sendInFlight = true;

    const message = buildInboundMessage(content, opts);

    return new Promise<SendResult>((resolve, reject) => {
      let lastAssistantTurn: AssistantTurn | undefined;

      activeSendListener = (event) => {
        if (event.type === "inference.done") {
          lastAssistantTurn = event.data.turn;
          return;
        }
        if (event.type === "connector.reply") {
          activeSendListener = undefined;
          sendInFlight = false;
          const turn: ConversationTurn = lastAssistantTurn ?? {
            role: "assistant",
            content: [{ type: "text", text: event.data.content }],
            ...(providerRegistry.active.model !== undefined
              ? { model: providerRegistry.active.model }
              : {}),
            timestamp: Date.now(),
          };
          resolve({ reply: event.data.content, turn });
          return;
        }
        if (event.type === "reactor.error") {
          activeSendListener = undefined;
          sendInFlight = false;
          reject(new Error(`reactor error: ${event.data.error}`));
          return;
        }
        if (event.type === "reactor.done") {
          activeSendListener = undefined;
          sendInFlight = false;
          reject(new AgentClosedError());
          return;
        }
      };

      reactor.deliver(message);
    });
  }

  // eslint-disable-next-line require-yield -- placeholder iterator; commit 8 replaces with bounded fan-out
  async function* stream(): AsyncIterable<ReactorEmittedEvent> {
    ensureOpen();
    // Placeholder until commit 8 wires the bounded per-consumer buffer.
    // Returning an immediately-finishing iterator keeps the type honest
    // without yielding events.
    return;
  }

  function deliver(message: InboundMessage): void {
    ensureOpen();
    reactor.deliver(message);
  }

  function setProvider(cfg: ProviderConfig): void {
    ensureOpen();
    providerRegistry.setProvider(cfg);
  }

  async function history(): Promise<ConversationTurn[]> {
    const loaded = await contextStore.load();
    return loaded.turns;
  }

  async function checkpoints(limit?: number): Promise<ContextCommit[]> {
    return contextStore.log(limit);
  }

  async function readAt(hash: string): Promise<ConversationTurn[]> {
    return contextStore.readAt(hash);
  }

  async function close(): Promise<void> {
    if (closed) return;
    closed = true;
    reactor.abort("user_disconnect");
    streamConsumers.clear();
    activeSendListener = undefined;
    if (lock !== undefined) lock.release();
  }

  return {
    send,
    stream,
    deliver,
    close,
    setProvider,
    history,
    checkpoints,
    readAt,
    blobReader,
  };
}
