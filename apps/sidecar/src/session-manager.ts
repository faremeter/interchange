// Session manager: creates and manages harness instances per agent.
//
// Each agent session gets its own harness backed by a scoped view of the
// shared InMemoryTransport. The session manager handles session lifecycle
// (create, destroy, abort) in response to control frames from the hub.

import path from "node:path";
import { getLogger } from "@interchange/log";
import { createHarness, type Harness } from "@interchange/harness";
import { createIsogitStore } from "@interchange/storage-isogit";
import type { InMemoryTransport } from "@interchange/message-memory";
import type {
  CryptoProvider,
  InboundMessage,
  InferenceEvent,
  HarnessConfig as AgentConfig,
} from "@interchange/types/runtime";

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
  dataDir: string;
  onEvent: SessionEventSink;
};

// Sanitize an agent address into a safe directory name.
// Replaces `@` with `_at_` and any character outside [a-zA-Z0-9_-] with `_`.
export function sanitizeAddress(address: string): string {
  return address.replace(/@/g, "_at_").replace(/[^a-zA-Z0-9_-]/g, "_");
}

export type SessionManager = {
  createSession(config: AgentConfig): Promise<string>;
  destroySession(agentAddress: string): void;
  abortSession(agentAddress: string, reason: string): void;
  deliverMessage(agentAddress: string, message: InboundMessage): void;
  hasSession(agentAddress: string): boolean;
  getAddresses(): string[];
};

export function createSessionManager(
  config: SessionManagerConfig,
): SessionManager {
  const { transport, crypto, dataDir, onEvent } = config;
  const sessions = new Map<string, AgentSession>();
  const pending = new Set<string>();

  async function createSession(agentConfig: AgentConfig): Promise<string> {
    const { agentAddress } = agentConfig;

    if (sessions.has(agentAddress) || pending.has(agentAddress)) {
      throw new Error(`Session already exists for agent "${agentAddress}"`);
    }

    pending.add(agentAddress);

    try {
      // Register the agent on the local transport so it can send/receive mail.
      transport.registerAgent(agentAddress, crypto);
      const agentTransport = transport.getTransportForAgent(agentAddress);

      const provider = agentConfig.providers[0];
      if (provider === undefined) {
        throw new Error(`No provider configured for agent "${agentAddress}"`);
      }

      const sessionId = agentConfig.sessionId;

      const storeDir = path.join(dataDir, sanitizeAddress(agentAddress));
      const storage = await createIsogitStore(storeDir);

      const harness = createHarness({
        address: agentAddress,
        systemPrompt: agentConfig.systemPrompt,
        provider: {
          ...provider,
          model: agentConfig.defaultModel,
        },
        transport: agentTransport,
        crypto,
        storage,
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
    } finally {
      pending.delete(agentAddress);
    }
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
