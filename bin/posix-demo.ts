import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { setup, getLogger } from "@interchange/log";
import { createInMemoryTransport } from "@interchange/message-memory";
import { generateKeyPair, createNodeCrypto } from "@interchange/crypto-node";
import { createIsogitStore } from "@interchange/storage-isogit";
import { createPosixTools } from "@interchange/tools-posix";
import { createHarness } from "@interchange/harness";
import type {
  InferenceEvent,
  ProviderConfig,
} from "@interchange/types/runtime";

await setup({ dev: true });

const log = getLogger(["demo"]);

// ---------------------------------------------------------------------------
// Environment — per-agent provider config
// ---------------------------------------------------------------------------
//
// Each agent reads ALPHA_* / BETA_* env vars, falling back to shared defaults.
//
//   ALPHA_PROVIDER   (default: openai-compatible)
//   ALPHA_BASE_URL   (default: OPENAI_BASE_URL or http://localhost:4096/v1)
//   ALPHA_API_KEY    (default: OPENAI_API_KEY or OPENCODE_API_KEY)
//   ALPHA_MODEL      (default: OPENAI_MODEL)
//
// Same pattern for BETA_*. Set ANTHROPIC_API_KEY + BETA_PROVIDER=anthropic
// to point beta at Anthropic while alpha stays on OpenCode.

const ANTHROPIC_DEFAULTS = {
  baseURL: "https://api.anthropic.com",
  model: "claude-sonnet-4-20250514",
};

function readAgentProvider(prefix: string): ProviderConfig {
  const env = (key: string) => process.env[`${prefix}_${key}`];

  const provider = env("PROVIDER") ?? "openai-compatible";

  const baseURL =
    env("BASE_URL") ??
    (provider === "anthropic"
      ? ANTHROPIC_DEFAULTS.baseURL
      : (process.env["OPENAI_BASE_URL"] ?? "http://localhost:4096/v1"));

  const apiKey =
    env("API_KEY") ??
    (provider === "anthropic"
      ? (process.env["ANTHROPIC_API_KEY"] ?? "")
      : (process.env["OPENAI_API_KEY"] ??
        process.env["OPENCODE_API_KEY"] ??
        ""));

  const model =
    env("MODEL") ??
    (provider === "anthropic"
      ? ANTHROPIC_DEFAULTS.model
      : (process.env["OPENAI_MODEL"] ?? ""));

  if (!apiKey) {
    log.fatal("{prefix}: no API key found", { prefix });
    process.exit(1);
  }
  if (!model) {
    log.fatal("{prefix}: no model specified", { prefix });
    process.exit(1);
  }

  return { provider, baseURL, apiKey, model };
}

const alphaProvider = readAgentProvider("ALPHA");
const betaProvider = readAgentProvider("BETA");

const DEMO_SEED =
  process.env["DEMO_SEED"] ??
  "Use the mail_send tool to send a message to beta@local.interchange asking: What do you want to be when you grow up? Then tell me what Beta says.";

log.info("alpha: {provider} / {model}", {
  provider: alphaProvider.provider,
  model: alphaProvider.model,
});
log.info("beta: {provider} / {model}", {
  provider: betaProvider.provider,
  model: betaProvider.model,
});

// ---------------------------------------------------------------------------
// Working directories
// ---------------------------------------------------------------------------

const REPO_ROOT = fileURLToPath(new URL("..", import.meta.url));
const TMP_ROOT = join(REPO_ROOT, "tmp");

const alphaDir = join(TMP_ROOT, "agent-alpha");
const betaDir = join(TMP_ROOT, "agent-beta");

await mkdir(alphaDir, { recursive: true });
await mkdir(betaDir, { recursive: true });

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

const sharedTransport = createInMemoryTransport();

// ---------------------------------------------------------------------------
// Cryptographic identities
// ---------------------------------------------------------------------------

const [alphaKeyPair, betaKeyPair, userKeyPair] = await Promise.all([
  generateKeyPair(),
  generateKeyPair(),
  generateKeyPair(),
]);

const cryptoAlpha = createNodeCrypto(alphaKeyPair);
const cryptoBeta = createNodeCrypto(betaKeyPair);
const cryptoUser = createNodeCrypto(userKeyPair);

const ALPHA_ADDRESS = "alpha@local.interchange";
const BETA_ADDRESS = "beta@local.interchange";
const USER_ADDRESS = "user@local.interchange";

sharedTransport.registerAgent(ALPHA_ADDRESS, cryptoAlpha);
sharedTransport.registerAgent(BETA_ADDRESS, cryptoBeta);
sharedTransport.registerAgent(USER_ADDRESS, cryptoUser);

const transportAlpha = sharedTransport.getTransportForAgent(ALPHA_ADDRESS);
const transportBeta = sharedTransport.getTransportForAgent(BETA_ADDRESS);
const transportUser = sharedTransport.getTransportForAgent(USER_ADDRESS);

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

const [storageAlpha, storageBeta] = await Promise.all([
  createIsogitStore(alphaDir),
  createIsogitStore(betaDir),
]);

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

const toolsAlpha = createPosixTools();
const toolsBeta = createPosixTools();

// ---------------------------------------------------------------------------
// Shutdown coordination
// ---------------------------------------------------------------------------

let shutdownInitiated = false;

// Watch the user's INBOX for Alpha's final reply.
transportUser.watch("INBOX", (event) => {
  if (event.type !== "exists") return;

  void (async () => {
    const msg = await transportUser.fetchFull({
      uid: event.uid,
      mailbox: "INBOX",
    });
    log.info("=== Reply from Alpha to User ===");
    log.info("{content}", { content: msg.content ?? "(no content)" });
    log.info("================================");

    if (!shutdownInitiated) {
      shutdownInitiated = true;
      setImmediate(() => {
        harnessAlpha.stop();
        harnessBeta.stop();
        clearTimeout(safetyTimer);
      });
    }
  })();
});

// ---------------------------------------------------------------------------
// Event logging
// ---------------------------------------------------------------------------

function makeEventLogger(label: string): (event: InferenceEvent) => void {
  return (event) => {
    switch (event.type) {
      case "inference.start":
        log.info("[{label}] Thinking...", { label });
        break;

      case "inference.done":
        log.info("[{label}] Done (input: {input}, output: {output} tokens)", {
          label,
          input: event.data.usage.input,
          output: event.data.usage.output,
        });
        break;

      case "tool.start": {
        const args = JSON.stringify(event.data.call.arguments);
        log.info("[{label}] Tool: {name}({args})", {
          label,
          name: event.data.call.name,
          args: args.length > 200 ? args.slice(0, 200) + "..." : args,
        });
        break;
      }

      case "tool.done": {
        const summary =
          typeof event.data.result.content === "string"
            ? event.data.result.content.slice(0, 80)
            : JSON.stringify(event.data.result.content).slice(0, 80);
        log.info("[{label}] Tool result: {summary}", { label, summary });
        break;
      }

      case "message.received": {
        const from = event.data.message.headers.from;
        log.info("[{label}] Received message from {from}", { label, from });
        break;
      }

      case "connector.reply": {
        const content = event.data.content;
        log.info("[{label}] Connector reply: {content}", {
          label,
          content:
            content.length > 120 ? content.slice(0, 120) + "..." : content,
        });
        break;
      }

      case "inference.error":
        log.error(
          "[{label}] Inference error: {message} (category: {category}, status: {status})",
          {
            label,
            message: event.data.error.message,
            category: event.data.error.category,
            status: event.data.error.statusCode ?? "n/a",
          },
        );
        if (event.data.error.raw !== undefined) {
          log.error("[{label}] Raw error body: {raw}", {
            label,
            raw: JSON.stringify(event.data.error.raw),
          });
        }
        break;

      default:
        break;
    }
  };
}

// ---------------------------------------------------------------------------
// Harnesses
// ---------------------------------------------------------------------------

const harnessAlpha = createHarness({
  address: ALPHA_ADDRESS,
  systemPrompt: `You are Alpha, an agent that relays messages between the user and other agents.

IMPORTANT: When the user mentions another agent by name, you MUST immediately call the mail_send tool to contact that agent. Do NOT reply to the user asking what to send. Do NOT introduce yourself. Just call the tool.

Example: If the user says "Ask Beta what his favorite color is", you call mail_send with to="${BETA_ADDRESS}" and content="What is your favorite color?"

Known agents:
  Beta: ${BETA_ADDRESS}

After you receive a response from an agent, tell the user what they said.`,
  provider: alphaProvider,
  transport: transportAlpha,
  crypto: cryptoAlpha,
  storage: storageAlpha,
  tools: toolsAlpha,
  onEvent: makeEventLogger("alpha"),
});

const harnessBeta = createHarness({
  address: BETA_ADDRESS,
  systemPrompt:
    "You are Beta, a thoughtful agent. When someone asks you a question, answer honestly and with personality. Be concise but interesting.",
  provider: betaProvider,
  transport: transportBeta,
  crypto: cryptoBeta,
  storage: storageBeta,
  tools: toolsBeta,
  onEvent: makeEventLogger("beta"),
});

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

function shutdown(signal: string): void {
  if (shutdownInitiated) return;
  shutdownInitiated = true;
  log.info("Received {signal}, shutting down", { signal });
  harnessAlpha.stop();
  harnessBeta.stop();
  clearTimeout(safetyTimer);
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));

// Safety timeout — don't hang forever.
const safetyTimer = setTimeout(() => {
  if (!shutdownInitiated) {
    log.info("Safety timeout reached, shutting down");
    shutdown("timeout");
  }
}, 120_000);

// ---------------------------------------------------------------------------
// Start harnesses and seed conversation
// ---------------------------------------------------------------------------

harnessAlpha.start();
harnessBeta.start();

log.info("Sending to Alpha: {seed}", { seed: DEMO_SEED });

// Send from the user to Alpha's connector.
await transportUser.send({
  to: ALPHA_ADDRESS,
  type: "conversation.message",
  content: DEMO_SEED,
  subject: "User request",
});

log.info("Demo running — waiting for Alpha to respond to user");
