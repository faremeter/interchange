import type {
  MessageTransport,
  CryptoProvider,
  ContextStore,
  ToolRunner,
  ProviderConfig,
  InferenceEvent,
  ReactorPlugin,
} from "@interchange/types/runtime";

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
}
