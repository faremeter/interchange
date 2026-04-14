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

const DEMO_TURNS = Number(process.env["DEMO_TURNS"] ?? "10");
const DEMO_SEED =
  process.env["DEMO_SEED"] ??
  "Hello Beta, I'm Alpha. Let's collaborate on something interesting. What should we work on?";

log.info("alpha: {provider} / {model}", {
  provider: alphaProvider.provider,
  model: alphaProvider.model,
});
log.info("beta: {provider} / {model}", {
  provider: betaProvider.provider,
  model: betaProvider.model,
});
log.info("Starting posix-demo with {turns} max turns", { turns: DEMO_TURNS });

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

const [alphaKeyPair, betaKeyPair] = await Promise.all([
  generateKeyPair(),
  generateKeyPair(),
]);

const cryptoAlpha = createNodeCrypto(alphaKeyPair);
const cryptoBeta = createNodeCrypto(betaKeyPair);

const ALPHA_ADDRESS = "alpha@local.interchange";
const BETA_ADDRESS = "beta@local.interchange";

sharedTransport.registerAgent(ALPHA_ADDRESS, cryptoAlpha);
sharedTransport.registerAgent(BETA_ADDRESS, cryptoBeta);

const transportAlpha = sharedTransport.getTransportForAgent(ALPHA_ADDRESS);
const transportBeta = sharedTransport.getTransportForAgent(BETA_ADDRESS);

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
// Turn counting
// ---------------------------------------------------------------------------

let turnsRemaining = DEMO_TURNS;
let shutdownInitiated = false;

function checkTurnLimit(): void {
  if (turnsRemaining <= 0 && !shutdownInitiated) {
    shutdownInitiated = true;
    log.info("Turn limit reached, shutting down");
    // Defer shutdown slightly so the current handler finishes
    setImmediate(() => {
      harnessAlpha.stop();
      harnessBeta.stop();
    });
  }
}

// ---------------------------------------------------------------------------
// Event logging
// ---------------------------------------------------------------------------

function makeEventLogger(label: string): (event: InferenceEvent) => void {
  return (event) => {
    switch (event.type) {
      case "inference.start":
        log.info("[{label}] Thinking...", { label });
        break;

      case "inference.text.delta":
        log.debug("[{label}] > {token}", { label, token: event.data.token });
        break;

      case "inference.done":
        log.info("[{label}] Done (input: {input}, output: {output} tokens)", {
          label,
          input: event.data.usage.input,
          output: event.data.usage.output,
        });
        break;

      case "tool.start":
        log.info("[{label}] Tool: {name}", {
          label,
          name: event.data.call.name,
        });
        break;

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
        turnsRemaining -= 1;
        log.info("Turns remaining: {remaining}", {
          remaining: turnsRemaining,
        });
        checkTurnLimit();
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

      case "reactor.gate.blocked":
        log.info("[{label}] Waiting: {reason}", {
          label,
          reason: event.data.reason,
        });
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
  systemPrompt:
    "You are Alpha, a collaborative agent. You work with Beta to solve problems. Be concise.",
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
    "You are Beta, a collaborative agent. You work with Alpha to solve problems. Be concise.",
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
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));

// ---------------------------------------------------------------------------
// Start harnesses and seed conversation
// ---------------------------------------------------------------------------

harnessAlpha.start();
harnessBeta.start();

log.info("Seeding conversation: {seed}", { seed: DEMO_SEED });

// Send the seed from Beta to Alpha so Alpha's reply goes to Beta,
// starting the A↔B conversation loop.
await transportBeta.send({
  to: ALPHA_ADDRESS,
  type: "conversation.message",
  content: DEMO_SEED,
  subject: "Demo conversation",
});

log.info("Demo running — waiting for conversation to complete");
