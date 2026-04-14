// Agent harness: lifecycle, transport watch, reactor wiring.
//
// The harness is the integration layer between the message transport and the
// reactor. It watches the agent's INBOX, fetches full messages on arrival,
// and delivers them to the reactor. It manages the combined ToolRunner that
// merges message tools with caller-provided tools.
//
// (ARCHITECTURE.md § Agent Harness, INFERENCE.md § Relationship to Harness)

import { getLogger } from "@interchange/log";
import { createReactor } from "@interchange/inference";
import type { Reactor } from "@interchange/inference";
import type { Unsubscribe, ReactorPlugin } from "@interchange/types/runtime";

import type { HarnessConfig } from "./config";
import { validateConfig } from "./config";
import { buildMessageToolHandlers, buildCombinedRunner } from "./tools";
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
  deliver(message: import("@interchange/types/runtime").InboundMessage): void;
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
    plugin = createDefaultPlugin(provider.model, config.systemPrompt);
  }

  // Build message tool handlers and the combined runner. Name collision
  // detection happens here at construction time — startup fails loudly.
  const messageHandlers = buildMessageToolHandlers(transport);

  // We cannot enumerate the caller's ToolRunner tool names generically since
  // ToolRunner is an opaque interface. The caller should not register tools
  // with message.* names. To allow the collision check to work we require the
  // config to carry toolNames when provided tools implement known names.
  // For now we perform the check with an empty caller tool name list, which
  // means collisions that exist in callerTools are caught only if the caller
  // passes toolNames explicitly via the extended config.
  const callerToolNames = (config as { toolNames?: string[] }).toolNames ?? [];

  const combinedRunner = buildCombinedRunner(
    messageHandlers,
    tools,
    callerToolNames,
  );

  const sessionId = crypto.randomUUID();

  const reactor: Reactor = createReactor({
    sessionId,
    plugin,
    providerConfig: provider,
    toolRunner: combinedRunner,
    contextStore: storage,
    onEvent,
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
          // eslint-disable-next-line @typescript-eslint/no-unused-expressions
          logger.error`Failed to fetch message uid=${event.uid}: ${cause}`;
          return;
        }

        if (stopped) return;
        reactor.deliver(message);
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

  function deliver(
    message: import("@interchange/types/runtime").InboundMessage,
  ): void {
    reactor.deliver(message);
  }

  return { start, stop, deliver };
}
