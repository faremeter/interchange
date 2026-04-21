import type {
  MessageTransport,
  CryptoProvider,
  ContextStore,
  AuditStore,
  ToolRunner,
  ToolDefinition,
  ProviderConfig,
  InferenceEvent,
  ReactorPlugin,
  BeforeToolExtension,
} from "@interchange/types/runtime";
import type { AuthzCallResult } from "@interchange/inference";

/**
 * Configuration passed to `createHarness`. All required fields must be
 * provided; none have defaults that silently mask missing values.
 */
export type HarnessConfig = {
  /** The agent's SMTP address, e.g. "agent@tenant.interchange.network". */
  address: string;

  /** System prompt for the agent's reasoning. */
  systemPrompt: string;

  /** Inference provider configuration (provider, model, API key, etc.). */
  provider: ProviderConfig;

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
   * Optional custom plugin. When omitted, the default conversational plugin
   * is used (message.received → infer → execute_tools loop → reply → wait).
   */
  plugin?: ReactorPlugin;

  /**
   * Policy overrides for the default plugin. Ignored when a custom plugin is
   * provided. Each field controls a specific decision point in the plugin's
   * event handling loop.
   */
  pluginPolicy?: PluginPolicy;

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
   * in the plugin's tool list for inference calls.
   */
  deployTools?: ToolDefinition[];
};

export type PluginPolicy = {
  /**
   * Controls the agent's behavior after inference completes.
   *
   *   "conversational" (default) — The standard agentic loop. After tools
   *     complete, re-infer so the model can reason about results, issue more
   *     tool calls, or compose a reply. When inference produces text without
   *     tool calls, send it as a connector reply.
   *
   *   "reactive" — The agent acts on each message by executing tools, then
   *     returns to the event loop to wait for the next inbound event. It does
   *     not re-infer after tools complete and does not send connector replies.
   *     Use this for agents that perform a single action per message.
   */
  mode?: "conversational" | "reactive";
};

export function validateConfig(config: HarnessConfig): void {
  if (config.address.trim() === "") {
    throw new Error("HarnessConfig.address must not be empty");
  }
  if (config.systemPrompt.trim() === "") {
    throw new Error("HarnessConfig.systemPrompt must not be empty");
  }
  if (config.provider.provider.trim() === "") {
    throw new Error("HarnessConfig.provider.provider must not be empty");
  }
  if (config.provider.apiKey.trim() === "") {
    throw new Error("HarnessConfig.provider.apiKey must not be empty");
  }
  if (config.provider.baseURL.trim() === "") {
    throw new Error("HarnessConfig.provider.baseURL must not be empty");
  }
  if (config.auditStore !== undefined && config.authorize === undefined) {
    throw new Error(
      "HarnessConfig.authorize is required when auditStore is provided",
    );
  }
}
