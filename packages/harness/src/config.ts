import type {
  MessageTransport,
  CryptoProvider,
  ConnectorThreadState,
  ContextStore,
  AuditStore,
  ToolRunner,
  ToolDefinition,
  InferenceSource,
  InferenceEvent,
  ReactorDirector,
  BeforeToolExtension,
} from "@intx/types/runtime";
import type { AuthzCallResult, DirectorPolicy } from "@intx/inference";

/**
 * Configuration passed to `createHarness`. All required fields must be
 * provided; none have defaults that silently mask missing values.
 */
export type HarnessConfig = {
  /** The agent's SMTP address, e.g. "agent@tenant.interchange.network". */
  address: string;

  /** System prompt for the agent's reasoning. */
  systemPrompt: string;

  /** Active inference source (id, provider, model, API key, etc.). */
  source: InferenceSource;

  /** Message transport implementation (SMTP/IMAP or in-memory). */
  transport: MessageTransport;

  /** Cryptographic provider for signing outbound messages. */
  crypto: CryptoProvider;

  /** Context store for persisting message history. */
  storage: ContextStore;

  /**
   * Caller-supplied tool runner (e.g. POSIX tools). The harness adds message
   * tools on top. Name collisions with message.* tools throw at startup.
   */
  tools: ToolRunner;

  /** Callback invoked for every inference event emitted by the reactor. */
  onEvent: (event: InferenceEvent) => void;

  /**
   * Optional callback invoked whenever the connector router's state changes
   * (commit of a start/continue decision, an outbound reply send advancing
   * lastMessageId, or load-time restore from the context store). Fires only
   * on a real state change, not on no-op operations. Used by the sidecar to
   * lift connector-state updates onto the hub-bound event channel.
   */
  onConnectorStateChanged?: (state: ConnectorThreadState | null) => void;

  /**
   * Optional custom director. When omitted, the default conversational director
   * is used (message.received → infer → execute_tools loop → reply → wait).
   */
  director?: ReactorDirector;

  /**
   * Policy overrides for the default director. Ignored when a custom director is
   * provided. Each field controls a specific decision point in the director's
   * event handling loop.
   */
  directorPolicy?: DirectorPolicy;

  /**
   * Extensions that run before each tool call. Return a string to block the
   * call (the string becomes the tool result), or undefined to allow it.
   * When using `authorize`, the harness creates and prepends an authz
   * extension automatically — do not also pass one here.
   */
  beforeToolExtensions?: BeforeToolExtension[];

  /**
   * Audit store for persisting tool invocation records. When provided,
   * the harness creates an audit collector and flushes completed records
   * at checkpoint boundaries and shutdown.
   *
   * Requires `authorize` to be set so the authz extension's onDecision
   * callback can feed governance decisions to the collector.
   */
  auditStore?: AuditStore;

  /**
   * Authorization function for tool calls. When provided, the harness
   * constructs an authz extension internally and prepends it to
   * `beforeToolExtensions`. Callers should not also pass a manually
   * constructed authz extension via `beforeToolExtensions`.
   */
  authorize?: (resource: string, action: string) => Promise<AuthzCallResult>;

  /**
   * Tool definitions from the deploy tree. These are checked for name
   * collisions with the harness's built-in message tools and included
   * in the director's tool list for inference calls.
   */
  deployTools?: ToolDefinition[];
};

export function validateConfig(config: HarnessConfig): void {
  if (config.address.trim() === "") {
    throw new Error("HarnessConfig.address must not be empty");
  }
  if (config.systemPrompt.trim() === "") {
    throw new Error("HarnessConfig.systemPrompt must not be empty");
  }
  if (config.source.id.trim() === "") {
    throw new Error("HarnessConfig.source.id must not be empty");
  }
  if (config.source.provider.trim() === "") {
    throw new Error("HarnessConfig.source.provider must not be empty");
  }
  if (config.source.model.trim() === "") {
    throw new Error("HarnessConfig.source.model must not be empty");
  }
  if (config.source.apiKey.trim() === "") {
    throw new Error("HarnessConfig.source.apiKey must not be empty");
  }
  if (config.source.baseURL.trim() === "") {
    throw new Error("HarnessConfig.source.baseURL must not be empty");
  }
  if (config.auditStore !== undefined && config.authorize === undefined) {
    throw new Error(
      "HarnessConfig.authorize is required when auditStore is provided",
    );
  }
}
