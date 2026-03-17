import { spawn } from "child_process";
import { env } from "process";
import { mkdirSync } from "fs";
import { Sidecar } from "./types";
import { getLogger } from "@interchange/log";

const logger = getLogger(["sidecar", "opencode"]);

interface SessionResponse {
  id: string;
}

export class OpenCodeManager {
  private sidecar: Sidecar;
  private process: ReturnType<typeof spawn> | null = null;

  private sessions = new Map<
    string,
    {
      buffer: { data: string; timestamp: number }[];
      listeners: Set<(data: string) => void>;
    }
  >();

  private eventReader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectDelay = 1000;

  constructor(sidecar: Sidecar) {
    this.sidecar = sidecar;
  }

  private authHeaders(): Record<string, string> {
    const { opencodePassword } = this.sidecar.config;
    if (!opencodePassword) return {};
    return {
      Authorization: `Basic ${btoa(`opencode:${opencodePassword}`)}`,
    };
  }

  async start(): Promise<void> {
    const { opencodePort, opencodePassword } = this.sidecar.config;

    logger.info(`Starting OpenCode on port ${opencodePort}...`);

    const envVars: Record<string, string> = {};
    for (const [key, value] of Object.entries(env)) {
      if (value !== undefined) {
        envVars[key] = value;
      }
    }
    envVars.OPENCODE_PORT = String(opencodePort);

    if (opencodePassword) {
      envVars.OPENCODE_SERVER_PASSWORD = opencodePassword;
    }

    const workDir = "/tmp/opencode-sidecar";
    const opencodeHome = "/tmp/opencode-home";
    try {
      mkdirSync(workDir, { recursive: true });
      mkdirSync(opencodeHome, { recursive: true });
    } catch {
      // dir exists
    }

    envVars.HOME = opencodeHome;
    envVars.XDG_CONFIG_HOME = `${opencodeHome}/.config`;
    envVars.OPENCODE_DIR = opencodeHome;

    this.process = spawn("opencode", ["serve"], {
      stdio: ["ignore", "pipe", "pipe"],
      env: envVars,
      detached: false,
      cwd: workDir,
    });

    this.process.stdout?.on("data", (data) => {
      logger.info(`[OpenCode] ${data}`);
    });

    this.process.stderr?.on("data", (data) => {
      logger.error(`[OpenCode] ${data}`);
    });

    const maxAttempts = 15;
    const delayMs = 1000;
    let started = false;
    for (let i = 0; i < maxAttempts; i++) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      try {
        const response = await fetch(
          `http://localhost:${opencodePort}/global/health`,
        );
        if (response.ok) {
          logger.info("OpenCode started");
          started = true;
          break;
        }
      } catch {
        // OpenCode not ready yet
      }
    }
    if (!started) {
      logger.warn("OpenCode health check timed out, assuming started");
    }

    this.startEventListener().catch((err) => {
      logger.error(`Event listener startup error: ${err}`);
    });
  }

  async stop(): Promise<void> {
    if (this.process) {
      logger.info("Stopping OpenCode...");

      if (this.reconnectTimer !== null) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
      }

      if (this.eventReader) {
        try {
          await this.eventReader.cancel();
        } catch {
          // ignore cancel errors
        }
        this.eventReader = null;
      }

      this.process.kill();
      this.process = null;
    }
  }

  async createSession(
    systemPrompt: string,
    _skills: string[],
  ): Promise<{ id: string; initialResponse: string } | null> {
    logger.info(`Creating session with systemPrompt: "${systemPrompt}"`);
    const { opencodePort } = this.sidecar.config;
    const baseUrl = `http://localhost:${opencodePort}`;

    try {
      const response = await fetch(`${baseUrl}/session`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...this.authHeaders(),
        },
        body: JSON.stringify({}),
      });

      if (!response.ok) {
        logger.error(
          `Failed to create session: ${response.status} ${await response.text()}`,
        );
        return null;
      }

      const data = (await response.json()) as SessionResponse;
      const sessionId = data.id;

      // Fire-and-forget: send system prompt as boot message via prompt_async.
      // The system field sets the persona; the parts trigger the model to
      // produce an opening response. The response arrives asynchronously
      // via the OpenCode SSE event stream.
      if (systemPrompt) {
        fetch(`${baseUrl}/session/${sessionId}/prompt_async`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...this.authHeaders(),
          },
          body: JSON.stringify({
            system: systemPrompt,
            parts: [{ type: "text", text: systemPrompt }],
          }),
        }).catch((err) => {
          logger.error(`Failed to send boot prompt: ${err}`);
        });
      }

      return { id: sessionId, initialResponse: "" };
    } catch (error) {
      logger.error(`Error creating OpenCode session: ${error}`);
      return null;
    }
  }

  async sendMessage(
    sessionId: string,
    message: string,
  ): Promise<{ queued: boolean } | null> {
    const { opencodePort } = this.sidecar.config;

    try {
      const response = await fetch(
        `http://localhost:${opencodePort}/session/${sessionId}/prompt_async`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...this.authHeaders(),
          },
          body: JSON.stringify({
            parts: [{ type: "text", text: message }],
          }),
        },
      );

      if (!response.ok) {
        logger.error(`Failed to send message: ${response.status}`);
        return null;
      }

      // prompt_async returns 204 No Content — don't parse body
      return { queued: true };
    } catch (error) {
      logger.error(`Error sending message: ${error}`);
      return null;
    }
  }

  async deleteSession(sessionId: string): Promise<boolean> {
    const { opencodePort } = this.sidecar.config;

    try {
      const response = await fetch(
        `http://localhost:${opencodePort}/session/${sessionId}`,
        {
          method: "DELETE",
          headers: this.authHeaders(),
        },
      );

      return response.ok;
    } catch (error) {
      logger.error(`Error deleting session: ${error}`);
      return false;
    } finally {
      this.sessions.delete(sessionId);
    }
  }

  subscribe(sessionId: string, callback: (data: string) => void): () => void {
    const entry = this.getOrCreateSession(sessionId);

    const now = Date.now();
    for (const item of entry.buffer) {
      if (now - item.timestamp < 60_000) {
        callback(item.data);
      }
    }

    entry.listeners.add(callback);

    return () => {
      entry.listeners.delete(callback);
    };
  }

  private getOrCreateSession(sessionId: string): {
    buffer: { data: string; timestamp: number }[];
    listeners: Set<(data: string) => void>;
  } {
    const existing = this.sessions.get(sessionId);
    if (existing) return existing;
    const created = {
      buffer: [] as { data: string; timestamp: number }[],
      listeners: new Set<(data: string) => void>(),
    };
    this.sessions.set(sessionId, created);
    return created;
  }

  private fanout(sessionId: string, data: string): void {
    const entry = this.getOrCreateSession(sessionId);
    const now = Date.now();

    if (entry.listeners.size === 0) {
      entry.buffer = entry.buffer.filter(
        (item) => now - item.timestamp < 60_000,
      );
      if (entry.buffer.length >= 500) {
        entry.buffer.shift();
      }
      entry.buffer.push({ data, timestamp: now });
    } else {
      for (const cb of entry.listeners) {
        try {
          cb(data);
        } catch (err) {
          logger.error(`Event listener callback error: ${err}`);
          entry.listeners.delete(cb);
        }
      }
    }
  }

  private async startEventListener(): Promise<void> {
    try {
      const { opencodePort } = this.sidecar.config;
      const response = await fetch(`http://localhost:${opencodePort}/event`, {
        headers: this.authHeaders(),
      });

      if (!response.ok || !response.body) {
        throw new Error(`Event stream connect failed: ${response.status}`);
      }

      this.reconnectDelay = 1000;
      this.eventReader =
        response.body.getReader() as ReadableStreamDefaultReader<Uint8Array>;
      const decoder = new TextDecoder();
      let lineBuffer = "";

      while (true) {
        const { done, value } = await this.eventReader.read();
        if (done) break;

        if (value) {
          lineBuffer += decoder.decode(value, { stream: true });
        }
        const lines = lineBuffer.split("\n");
        lineBuffer = lines.pop() ?? "";

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const raw = line.slice(6).trim();
          if (!raw) continue;

          try {
            const event = JSON.parse(raw) as {
              type: string;
              properties: Record<string, unknown>;
            };
            const sessionId =
              ((event.properties as Record<string, unknown>)?.sessionID as
                | string
                | undefined) ??
              ((event.properties?.part as Record<string, unknown>)
                ?.sessionID as string | undefined) ??
              (event.properties as { info?: { sessionID?: string } })?.info
                ?.sessionID;
            if (sessionId) {
              this.fanout(sessionId, raw);
            }
          } catch {
            // malformed event, skip
          }
        }
      }
    } catch (err) {
      logger.error(`OpenCode event stream error: ${err}`);
    }

    if (this.process !== null) {
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        this.startEventListener().catch((err) => {
          logger.error(`Event listener reconnect error: ${err}`);
        });
      }, this.reconnectDelay);
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30_000);
    }
  }

  isRunning(): boolean {
    return this.process !== null;
  }
}
