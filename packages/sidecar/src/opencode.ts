import { spawn } from "child_process";
import { env } from "process";
import { mkdirSync } from "fs";
import { Sidecar } from "./types";
import { getLogger } from "@interchange/log";

const logger = getLogger(["sidecar", "opencode"]);

interface SessionResponse {
  id: string;
}

interface MessageResponse {
  info: { id: string; role: string };
  parts: { type: string; text?: string }[];
}

export class OpenCodeManager {
  private sidecar: Sidecar;
  private process: ReturnType<typeof spawn> | null = null;

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
    for (let i = 0; i < maxAttempts; i++) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      try {
        const response = await fetch(
          `http://localhost:${opencodePort}/global/health`,
        );
        if (response.ok) {
          logger.info("OpenCode started");
          return;
        }
      } catch {
        // OpenCode not ready yet
      }
    }
    logger.warn("OpenCode health check timed out, assuming started");
  }

  async stop(): Promise<void> {
    if (this.process) {
      logger.info("Stopping OpenCode...");
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
  ): Promise<{ text: string } | null> {
    const { opencodePort } = this.sidecar.config;

    try {
      const response = await fetch(
        `http://localhost:${opencodePort}/session/${sessionId}/message`,
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
        logger.error(
          `Failed to send message: ${response.status} ${await response.text()}`,
        );
        return null;
      }

      const data = (await response.json()) as MessageResponse;
      const textPart = data.parts.find((p) => p.type === "text");
      return { text: textPart?.text || "" };
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
    }
  }

  isRunning(): boolean {
    return this.process !== null;
  }
}
