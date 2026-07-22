import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  createDefaultDirectorRegistry,
  defineAgent,
  defineTool,
} from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import type { ReactorEmittedEvent } from "@intx/inference";
import { setup, getLogger } from "@intx/log";
import { createInMemoryTransport } from "@intx/mail-memory";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import { createIsogitStore } from "@intx/storage-isogit";
import { createPosixTools } from "@intx/tools-posix";
import { createMailTools } from "@intx/tools-mail";
import {
  createHarness,
  createHarnessRuntimeCapabilities,
  defineMailTools,
  type Harness,
  type MailEnv,
} from "@intx/harness";
import type { InferenceSource } from "@intx/types/runtime";

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
  model: "claude-sonnet-5",
};

function readAgentSource(prefix: string): InferenceSource {
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

  return {
    id: `${provider}:${model}`,
    provider,
    baseURL,
    apiKey,
    model,
  };
}

const alphaSource = readAgentSource("ALPHA");
const betaSource = readAgentSource("BETA");

const DEMO_SEED =
  process.env["DEMO_SEED"] ??
  "Use the mail_send tool to send a message to beta@local.interchange asking: What do you want to be when you grow up? Then tell me what Beta says.";

log.info("alpha: {provider} / {model}", {
  provider: alphaSource.provider,
  model: alphaSource.model,
});
log.info("beta: {provider} / {model}", {
  provider: betaSource.provider,
  model: betaSource.model,
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

const cryptoAlpha = createEd25519Crypto(alphaKeyPair);
const cryptoBeta = createEd25519Crypto(betaKeyPair);
const cryptoUser = createEd25519Crypto(userKeyPair);

const ALPHA_ADDRESS = "alpha@local.interchange";
const BETA_ADDRESS = "beta@local.interchange";
const USER_ADDRESS = "user@local.interchange";

sharedTransport.register(ALPHA_ADDRESS, cryptoAlpha);
sharedTransport.register(BETA_ADDRESS, cryptoBeta);
sharedTransport.register(USER_ADDRESS, cryptoUser);

const transportAlpha = sharedTransport.getTransportFor(ALPHA_ADDRESS);
const transportBeta = sharedTransport.getTransportFor(BETA_ADDRESS);
const transportUser = sharedTransport.getTransportFor(USER_ADDRESS);

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

// Keep per-package references so the shutdown path can dispose each
// runner. Each per-agent createMailTools() is constructed against
// that agent's transport; the harness wraps it as a defineTool bundle
// factory via defineMailTools().
const toolsAlphaMail = createMailTools({
  capabilities: createHarnessRuntimeCapabilities({
    transport: transportAlpha,
  }),
});
const toolsAlphaPosix = createPosixTools({ cwd: process.cwd() });

const toolsBetaMail = createMailTools({
  capabilities: createHarnessRuntimeCapabilities({
    transport: transportBeta,
  }),
});
const toolsBetaPosix = createPosixTools({ cwd: process.cwd() });

function posixFactoryFor(posixTools: typeof toolsAlphaPosix) {
  return defineTool({
    id: "@interchange-demo/posix-demo/posix",
    factory: () => ({
      definitions: posixTools.definitions,
      run: (call, signal) => posixTools.run(call, signal),
    }),
  });
}

function mailFactoryFor(mailTools: typeof toolsAlphaMail) {
  return defineMailTools(() => ({
    definitions: mailTools.definitions,
    run: (call, signal) => mailTools.run(call, signal),
  }));
}

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
      setImmediate(() => shutdown("alpha-reply"));
    }
  })();
});

// ---------------------------------------------------------------------------
// Event logging
// ---------------------------------------------------------------------------

function makeEventLogger(label: string): (event: ReactorEmittedEvent) => void {
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

const ALPHA_SYSTEM_PROMPT = `You are Alpha, an agent that relays messages between the user and other agents.

IMPORTANT: When the user mentions another agent by name, you MUST immediately call the mail_send tool to contact that agent. Do NOT reply to the user asking what to send. Do NOT introduce yourself. Just call the tool.

Example: If the user says "Ask Beta what his favorite color is", you call mail_send with to="${BETA_ADDRESS}" and content="What is your favorite color?"

Known agents:
  Beta: ${BETA_ADDRESS}

After you receive a response from an agent, tell the user what they said.`;

const BETA_SYSTEM_PROMPT =
  "You are Beta, a thoughtful agent. When someone asks you a question, answer honestly and with personality. Be concise but interesting.";

const alphaDef = defineAgent({
  id: ALPHA_ADDRESS,
  systemPrompt: ALPHA_SYSTEM_PROMPT,
  tools: [mailFactoryFor(toolsAlphaMail), posixFactoryFor(toolsAlphaPosix)],
  capabilities: [],
  inference: {
    sources: [{ provider: alphaSource.provider, model: alphaSource.model }],
  },
});

const betaDef = defineAgent({
  id: BETA_ADDRESS,
  systemPrompt: BETA_SYSTEM_PROMPT,
  tools: [mailFactoryFor(toolsBetaMail), posixFactoryFor(toolsBetaPosix)],
  capabilities: [],
  inference: {
    sources: [{ provider: betaSource.provider, model: betaSource.model }],
  },
});

const alphaEnv: MailEnv = {
  sources: [alphaSource],
  defaultSource: alphaSource.id,
  storage: storageAlpha,
  workdir: alphaDir,
  audit: noopAuditStore(),
  authorize: permissiveAuthorize(),
  directors: createDefaultDirectorRegistry(),
  transport: transportAlpha,
  address: ALPHA_ADDRESS,
};

const betaEnv: MailEnv = {
  sources: [betaSource],
  defaultSource: betaSource.id,
  storage: storageBeta,
  workdir: betaDir,
  audit: noopAuditStore(),
  authorize: permissiveAuthorize(),
  directors: createDefaultDirectorRegistry(),
  transport: transportBeta,
  address: BETA_ADDRESS,
};

const harnessAlpha: Harness = await createHarness(alphaDef, alphaEnv);
const harnessBeta: Harness = await createHarness(betaDef, betaEnv);

// Drain each harness's event stream in the background and route to the
// per-agent logger. The createHarness composition layer no longer
// takes an `onEvent` callback; observability consumers subscribe via
// stream() instead.
function startEventLogger(harness: Harness, label: string): () => void {
  const logger = makeEventLogger(label);
  // Register the StreamConsumer synchronously so events emitted in
  // the window before the IIFE's for-await loop starts are buffered
  // rather than dropped.
  const events = harness.stream();
  let stop = false;
  void (async () => {
    try {
      for await (const event of events) {
        if (stop) break;
        logger(event);
      }
    } catch {
      // Stream may close during shutdown; swallow.
    }
  })();
  return () => {
    stop = true;
  };
}

const stopAlphaLogger = startEventLogger(harnessAlpha, "alpha");
const stopBetaLogger = startEventLogger(harnessBeta, "beta");

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

function shutdown(signal: string): void {
  if (shutdownInitiated) return;
  shutdownInitiated = true;
  log.info("Received {signal}, shutting down", { signal });
  stopAlphaLogger();
  stopBetaLogger();
  clearTimeout(safetyTimer);
  // Each per-agent tool runner owns its own resources (LSP child
  // processes via the posix tool runner's plugins, future per-package
  // teardown via the mail tool runner). Dispose them so the demo exits
  // cleanly. createHarness does not aggregate dispose -- the demo
  // built the per-package runners and is responsible for disposing
  // them after the harness closes.
  void (async () => {
    await harnessAlpha.close();
    await harnessBeta.close();
    await Promise.all([
      toolsAlphaMail.dispose(),
      toolsAlphaPosix.dispose(),
      toolsBetaMail.dispose(),
      toolsBetaPosix.dispose(),
    ]);
  })();
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
// Seed conversation
// ---------------------------------------------------------------------------
//
// The composition-layer harness is started by createHarness above; no
// separate start() step is needed.

log.info("Sending to Alpha: {seed}", { seed: DEMO_SEED });

// Send from the user to Alpha's connector.
await transportUser.send({
  to: ALPHA_ADDRESS,
  type: "conversation.message",
  content: DEMO_SEED,
  subject: "User request",
});

log.info("Demo running — waiting for Alpha to respond to user");
