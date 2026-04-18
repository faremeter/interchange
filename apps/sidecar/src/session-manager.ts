// Session manager: creates and manages harness instances per agent.
//
// Each agent session gets its own harness backed by a scoped view of the
// shared InMemoryTransport. The session manager handles session lifecycle
// (create, destroy, abort) in response to control frames from the hub.

import { getLogger } from "@interchange/log";
import { createHarness, type Harness } from "@interchange/harness";
import type { InMemoryTransport } from "@interchange/message-memory";
import type {
  CryptoProvider,
  InboundMessage,
  InferenceEvent,
  HarnessConfig as AgentConfig,
} from "@interchange/types/runtime";

import { createMemoryContextStore } from "./context-store";

const logger = getLogger(["interchange", "sidecar", "sessions"]);

export type AgentSession = {
  harness: Harness;
  agentAddress: string;
  agentId: string;
};

export type SessionEventSink = (
  agentAddress: string,
  sessionId: string,
  event: InferenceEvent,
) => void;

export type SessionManagerConfig = {
  transport: InMemoryTransport;
  crypto: CryptoProvider;
  onEvent: SessionEventSink;
};

export type SessionManager = {
  createSession(config: AgentConfig): string;
  destroySession(agentAddress: string): void;
  abortSession(agentAddress: string, reason: string): void;
  deliverMessage(agentAddress: string, message: InboundMessage): void;
  hasSession(agentAddress: string): boolean;
  getAddresses(): string[];
};

export function createSessionManager(
  config: SessionManagerConfig,
): SessionManager {
  const { transport, crypto, onEvent } = config;
  const sessions = new Map<string, AgentSession>();

  function createSession(agentConfig: AgentConfig): string {
    const { agentAddress } = agentConfig;

    if (sessions.has(agentAddress)) {
      throw new Error(`Session already exists for agent "${agentAddress}"`);
    }

    // Register the agent on the local transport so it can send/receive mail.
    transport.registerAgent(agentAddress, crypto);
    const agentTransport = transport.getTransportForAgent(agentAddress);

    const provider = agentConfig.providers[0];
    if (provider === undefined) {
      throw new Error(`No provider configured for agent "${agentAddress}"`);
    }

    const sessionId = agentConfig.sessionId;

    const harness = createHarness({
      address: agentAddress,
      systemPrompt: agentConfig.systemPrompt,
      provider: {
        ...provider,
        model: agentConfig.defaultModel,
      },
      transport: agentTransport,
      crypto,
      storage: createMemoryContextStore(),
      tools: {
        async run(call) {
          return {
            callId: call.id,
            content: `Tool "${call.name}" is not available on this sidecar`,
            isError: true,
          };
        },
      },
      onEvent(event: InferenceEvent) {
        onEvent(agentAddress, sessionId, event);
      },
    });

    sessions.set(agentAddress, {
      harness,
      agentAddress,
      agentId: agentConfig.agentId,
    });

    harness.start();

    logger.info`Created session for ${agentAddress} (session ${sessionId})`;
    return sessionId;
  }

  function destroySession(agentAddress: string): void {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    session.harness.stop();
    sessions.delete(agentAddress);
    logger.info`Destroyed session for ${agentAddress}`;
  }

  function abortSession(agentAddress: string, reason: string): void {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    session.harness.stop();
    sessions.delete(agentAddress);
    logger.info`Aborted session for ${agentAddress}: ${reason}`;
  }

  function deliverMessage(agentAddress: string, message: InboundMessage): void {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    session.harness.deliver(message);
  }

  function hasSession(agentAddress: string): boolean {
    return sessions.has(agentAddress);
  }

  function getAddresses(): string[] {
    return [...sessions.keys()];
  }

  return {
    createSession,
    destroySession,
    abortSession,
    deliverMessage,
    hasSession,
    getAddresses,
  };
}
