export interface SidecarConfig {
  port: number;
  sidecarId: string;
  hubUrl: string;
  opencodePort: number;
  opencodePassword?: string;
}

export interface AgentSession {
  id: string;
  agentId: string;
  opencodeSessionId?: string;
  systemPrompt: string;
  skills: string[];
  status: "starting" | "running" | "stopped" | "error";
  createdAt: Date;
}

export interface StoredCredential {
  id: string;
  type: string;
  data: Record<string, string>;
}

export interface ToolInvocation {
  toolId: string;
  agentId: string;
  params: Record<string, unknown>;
}

export interface Env {
  Variables: {
    sidecar: Sidecar;
  };
}

export class Sidecar {
  config: SidecarConfig;
  agents: Map<string, AgentSession> = new Map<string, AgentSession>();
  credentials: Map<string, StoredCredential[]> = new Map<
    string,
    StoredCredential[]
  >();
  opencodeProcess?: ReturnType<typeof setInterval>;

  constructor(config: SidecarConfig) {
    this.config = config;
  }

  async startOpencode(): Promise<void> {
    // OpenCode is started via OpenCodeManager
  }

  async stopOpencode(): Promise<void> {
    // OpenCode is stopped via OpenCodeManager
  }

  async createSession(
    agentId: string,
    systemPrompt: string,
    skills: string[],
  ): Promise<AgentSession> {
    const session: AgentSession = {
      id: crypto.randomUUID(),
      agentId,
      systemPrompt,
      skills,
      status: "starting",
      createdAt: new Date(),
    };
    this.agents.set(session.id, session);
    return session;
  }

  async deleteSession(sessionId: string): Promise<void> {
    const session = this.agents.get(sessionId);
    if (session) {
      session.status = "stopped";
      this.agents.delete(sessionId);
    }
  }

  storeCredentials(agentId: string, credentials: StoredCredential[]): void {
    this.credentials.set(agentId, credentials);
  }

  getCredentials(agentId: string): StoredCredential[] {
    return this.credentials.get(agentId) || [];
  }
}
