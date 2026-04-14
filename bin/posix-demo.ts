import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { setup, getLogger } from "@interchange/log";
import { createInMemoryTransport } from "@interchange/message-memory";
import { generateKeyPair, createNodeCrypto } from "@interchange/crypto-node";
import { createIsogitStore } from "@interchange/storage-isogit";
import { createPosixTools } from "@interchange/tools-posix";
import { createHarness } from "@interchange/harness";
import type { InferenceEvent } from "@interchange/types/runtime";

await setup({ dev: true });

const log = getLogger(["demo"]);

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const ANTHROPIC_API_KEY = process.env["ANTHROPIC_API_KEY"];
if (!ANTHROPIC_API_KEY) {
  log.fatal("ANTHROPIC_API_KEY is required but not set");
  process.exit(1);
}

const OPENAI_BASE_URL =
  process.env["OPENAI_BASE_URL"] ?? "http://localhost:4096/v1";
const OPENAI_API_KEY =
  process.env["OPENAI_API_KEY"] ?? process.env["OPENCODE_API_KEY"] ?? "";
const OPENAI_MODEL = process.env["OPENAI_MODEL"] ?? "";
const DEMO_TURNS = Number(process.env["DEMO_TURNS"] ?? "10");
const DEMO_SEED =
  process.env["DEMO_SEED"] ??
  "Hello Beta, I'm Alpha. Let's collaborate on something interesting. What should we work on?";

if (!OPENAI_API_KEY) {
  log.fatal("OPENAI_API_KEY (or OPENCODE_API_KEY) is required but not set");
  process.exit(1);
}

if (!OPENAI_MODEL) {
  log.fatal("OPENAI_MODEL is required but not set");
  process.exit(1);
}

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

const [alphaKeyPair, betaKeyPair, systemKeyPair] = await Promise.all([
  generateKeyPair(),
  generateKeyPair(),
  generateKeyPair(),
]);

const cryptoAlpha = createNodeCrypto(alphaKeyPair);
const cryptoBeta = createNodeCrypto(betaKeyPair);
const cryptoSystem = createNodeCrypto(systemKeyPair);

const ALPHA_ADDRESS = "alpha@local.interchange";
const BETA_ADDRESS = "beta@local.interchange";
const SYSTEM_ADDRESS = "system@local.interchange";

sharedTransport.registerAgent(ALPHA_ADDRESS, cryptoAlpha);
sharedTransport.registerAgent(BETA_ADDRESS, cryptoBeta);
sharedTransport.registerAgent(SYSTEM_ADDRESS, cryptoSystem);

const transportAlpha = sharedTransport.getTransportForAgent(ALPHA_ADDRESS);
const transportBeta = sharedTransport.getTransportForAgent(BETA_ADDRESS);
const transportSystem = sharedTransport.getTransportForAgent(SYSTEM_ADDRESS);

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
  provider: {
    provider: "openai-compatible",
    baseURL: OPENAI_BASE_URL,
    apiKey: OPENAI_API_KEY,
    model: OPENAI_MODEL,
  },
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
  provider: {
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    apiKey: ANTHROPIC_API_KEY,
    model: "claude-sonnet-4-20250514",
  },
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

await transportSystem.send({
  to: ALPHA_ADDRESS,
  type: "conversation.message",
  content: DEMO_SEED,
  subject: "Demo conversation",
});

log.info("Demo running — waiting for conversation to complete");
