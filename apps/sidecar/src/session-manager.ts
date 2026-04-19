// Agent manager: creates and manages harness instances per agent.
//
// Each agent gets its own harness backed by a scoped view of the shared
// InMemoryTransport. The manager handles agent lifecycle (deploy, undeploy,
// abort) in response to control frames from the hub.

import path from "node:path";
import { getLogger } from "@interchange/log";
import { createHarness, type Harness } from "@interchange/harness";
import { createNodeCrypto } from "@interchange/crypto-node";
import { createIsogitStore } from "@interchange/storage-isogit";
import type { InMemoryTransport } from "@interchange/message-memory";
import type {
  InboundMessage,
  InferenceEvent,
  KeyPair,
  HarnessConfig as AgentConfig,
} from "@interchange/types/runtime";

import { loadOrGenerateKeyPair, hexEncode } from "./key-store";

const logger = getLogger(["interchange", "sidecar", "agents"]);

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
  dataDir: string;
  onEvent: SessionEventSink;
};

// Sanitize an agent address into a safe directory name.
// Replaces `@` with `_at_` and any character outside [a-zA-Z0-9_-] with `_`.
export function sanitizeAddress(address: string): string {
  return address.replace(/@/g, "_at_").replace(/[^a-zA-Z0-9_-]/g, "_");
}

export type CreateSessionResult = {
  sessionId: string;
  publicKey: string;
  keyPair: KeyPair;
};

export type SessionManager = {
  createSession(config: AgentConfig): Promise<CreateSessionResult>;
  destroySession(agentAddress: string): void;
  abortSession(agentAddress: string, reason: string): void;
  deliverMessage(agentAddress: string, message: InboundMessage): void;
  hasSession(agentAddress: string): boolean;
  getAddresses(): string[];
};

export function createSessionManager(
  config: SessionManagerConfig,
): SessionManager {
  const { transport, dataDir, onEvent } = config;
  const sessions = new Map<string, AgentSession>();
  const pending = new Set<string>();

  async function createSession(
    agentConfig: AgentConfig,
  ): Promise<CreateSessionResult> {
    const { agentAddress } = agentConfig;

    if (sessions.has(agentAddress) || pending.has(agentAddress)) {
      throw new Error(`Session already exists for agent "${agentAddress}"`);
    }

    pending.add(agentAddress);

    try {
      // Generate or load the agent's per-agent key pair.
      const { keyPair, isNew } = await loadOrGenerateKeyPair(
        dataDir,
        agentAddress,
      );
      const crypto = createNodeCrypto(keyPair);

      if (isNew) {
        logger.info`Generated new key pair for ${agentAddress}`;
      }

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

      const publicKey = hexEncode(keyPair.publicKey);
      logger.info`Deployed agent ${agentAddress} (session ${sessionId})`;
      return { sessionId, publicKey, keyPair };
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
    logger.info`Undeployed agent ${agentAddress}`;
  }

  function abortSession(agentAddress: string, reason: string): void {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    session.harness.stop();
    sessions.delete(agentAddress);
    logger.info`Aborted agent ${agentAddress}: ${reason}`;
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
