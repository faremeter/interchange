import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { setup, getLogger } from "@intx/log";
import { createInMemoryTransport } from "@intx/mail-memory";
import { generateKeyPair, createNodeCrypto } from "@intx/crypto-node";
import { createIsogitStore } from "@intx/storage-isogit";
import { createPosixTools } from "@intx/tools-posix";
import { createHarness } from "@intx/harness";
import type { InferenceEvent, InferenceSource } from "@intx/types/runtime";

await setup({ dev: true });

const log = getLogger(["ring-demo"]);

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const ANTHROPIC_DEFAULTS = {
  baseURL: "https://api.anthropic.com",
  model: "claude-sonnet-4-6",
};

function readSource(): InferenceSource {
  const provider = process.env["RING_PROVIDER"] ?? "openai-compatible";

  const baseURL =
    process.env["RING_BASE_URL"] ??
    (provider === "anthropic"
      ? ANTHROPIC_DEFAULTS.baseURL
      : (process.env["OPENAI_BASE_URL"] ?? "http://localhost:4096/v1"));

  const apiKey =
    process.env["RING_API_KEY"] ??
    (provider === "anthropic"
      ? (process.env["ANTHROPIC_API_KEY"] ?? "")
      : (process.env["OPENAI_API_KEY"] ??
        process.env["OPENCODE_API_KEY"] ??
        ""));

  const model =
    process.env["RING_MODEL"] ??
    (provider === "anthropic"
      ? ANTHROPIC_DEFAULTS.model
      : (process.env["OPENAI_MODEL"] ?? ""));

  if (!apiKey) {
    log.fatal("No API key found");
    process.exit(1);
  }
  if (!model) {
    log.fatal("No model specified");
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

const source = readSource();

log.info("Provider: {provider} / {model}", {
  provider: source.provider,
  model: source.model,
});

// ---------------------------------------------------------------------------
// Ring topology
// ---------------------------------------------------------------------------

const AGENT_NAMES = ["alpha", "bravo", "charlie", "delta", "echo"];

const SEED_PROMPT =
  process.env["RING_PROMPT"] ??
  "Should we build our own authentication system or use a third-party provider?";

function agentAddress(name: string): string {
  return `${name}@local.interchange`;
}

// ---------------------------------------------------------------------------
// Working directories
// ---------------------------------------------------------------------------

const REPO_ROOT = fileURLToPath(new URL("..", import.meta.url));
const TMP_ROOT = join(REPO_ROOT, "tmp", "ring");

for (const name of AGENT_NAMES) {
  await mkdir(join(TMP_ROOT, name), { recursive: true });
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

const sharedTransport = createInMemoryTransport();

// ---------------------------------------------------------------------------
// Cryptographic identities and registration
// ---------------------------------------------------------------------------

const USER_ADDRESS = "user@local.interchange";

const keyPairs = await Promise.all(
  [...AGENT_NAMES, "user"].map(() => generateKeyPair()),
);

const cryptoInstances = keyPairs.map((kp) => createNodeCrypto(kp));

// Register all agents and the user.
for (const [i, name] of AGENT_NAMES.entries()) {
  const crypto = cryptoInstances[i];
  if (crypto === undefined) {
    throw new Error(`Missing crypto for agent index ${String(i)}`);
  }
  sharedTransport.register(agentAddress(name), crypto);
}
const userCrypto = cryptoInstances.at(-1);
if (userCrypto === undefined) {
  throw new Error("Missing crypto for user");
}
sharedTransport.register(USER_ADDRESS, userCrypto);

// Per-agent transports.
const transports = AGENT_NAMES.map((name) =>
  sharedTransport.getTransportFor(agentAddress(name)),
);
const transportUser = sharedTransport.getTransportFor(USER_ADDRESS);

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

const stores = await Promise.all(
  AGENT_NAMES.map((name) => createIsogitStore(join(TMP_ROOT, name))),
);

// ---------------------------------------------------------------------------
// Shutdown coordination
// ---------------------------------------------------------------------------

let shutdownInitiated = false;
const harnesses: ReturnType<typeof createHarness>[] = [];

transportUser.watch("INBOX", (event) => {
  if (event.type !== "exists") return;

  void (async () => {
    const msg = await transportUser.fetchFull({
      uid: event.uid,
      mailbox: "INBOX",
    });
    log.info("=== Braintrust Recommendation ===");
    log.info("{content}", { content: msg.content ?? "(no content)" });
    log.info("=================================");

    if (!shutdownInitiated) {
      shutdownInitiated = true;
      setImmediate(() => {
        for (const h of harnesses) h.stop();
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
        log.info(
          "[{label}] Inference complete (input: {input}, output: {output} tokens)",
          {
            label,
            input: event.data.usage.input,
            output: event.data.usage.output,
          },
        );
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
            content.length > 200 ? content.slice(0, 200) + "..." : content,
        });
        break;
      }

      case "inference.error":
        log.error(
          "[{label}] Inference error: {message} (category: {category})",
          {
            label,
            message: event.data.error.message,
            category: event.data.error.category,
          },
        );
        break;

      default:
        break;
    }
  };
}

// ---------------------------------------------------------------------------
// Braintrust roles
// ---------------------------------------------------------------------------

const ROLES: Record<string, { title: string; perspective: string }> = {
  alpha: {
    title: "Facilitator",
    perspective:
      "You frame problems clearly, identify the core tension, and synthesize diverse viewpoints into actionable recommendations.",
  },
  bravo: {
    title: "Devil's Advocate",
    perspective:
      "You challenge assumptions and poke holes in arguments. You ask 'what if we're wrong about X?' and surface hidden risks others overlook.",
  },
  charlie: {
    title: "Technical Architect",
    perspective:
      "You evaluate technical feasibility, system design trade-offs, scalability concerns, and implementation complexity.",
  },
  delta: {
    title: "User Experience Lead",
    perspective:
      "You advocate for the end user. You consider usability, accessibility, onboarding friction, and how decisions affect the people who use the product.",
  },
  echo: {
    title: "Synthesizer",
    perspective:
      "You distill the discussion into its strongest threads. You identify where the group agrees, where they diverge, and what the key open questions are.",
  },
};

// ---------------------------------------------------------------------------
// System prompts
// ---------------------------------------------------------------------------

function buildRingPrompt(name: string, index: number): string {
  const role = ROLES[name];
  if (role === undefined) {
    throw new Error(`No role defined for ${name}`);
  }
  const nextIndex = (index + 1) % AGENT_NAMES.length;
  const nextName = AGENT_NAMES[nextIndex];
  if (nextName === undefined) {
    throw new Error(`No agent name at index ${String(nextIndex)}`);
  }
  const nextAddress = agentAddress(nextName);
  const nextRole = ROLES[nextName];
  if (nextRole === undefined) {
    throw new Error(`No role defined for ${nextName}`);
  }

  if (index === 0) {
    const lastName = AGENT_NAMES.at(-1);
    if (lastName === undefined) {
      throw new Error("AGENT_NAMES is empty");
    }
    const lastAddress = agentAddress(lastName);
    return `You are ${name}, the ${role.title} of a braintrust of ${AGENT_NAMES.length} advisors.

${role.perspective}

When the user poses a question or problem:
1. Write a brief framing of the problem (2-3 sentences) that identifies the core tension.
2. Use mail_send to send the user's original question along with your framing to ${nextAddress} (${nextRole.title}).
3. Use mail_wait with {"from": "${lastAddress}", "timeout": 300} to wait for the discussion to complete the ring. This will block until ${lastName} sends you the final analysis.
4. Synthesize the entire braintrust's input into a clear recommendation for the user.

Your message to ${nextName} should include the original question and your framing, clearly labeled. The braintrust will build on it as it circulates.`;
  }

  return `You are ${name}, the ${role.title} on a braintrust of advisors.

${role.perspective}

When you receive a message, it contains a question being discussed by the braintrust along with analysis from previous advisors. Your job:
1. Read what's been said so far.
2. Add your perspective as ${role.title} in 2-4 sentences. Be specific and direct. Disagree with earlier points if warranted.
3. Use mail_send to forward the entire discussion (previous analysis plus your addition) to ${nextAddress} (${nextRole.title}).

Format your addition as:
**${name} (${role.title}):** [your analysis]

After the tool call succeeds, stop immediately. Do not write any reply text.`;
}

// ---------------------------------------------------------------------------
// Harnesses
// ---------------------------------------------------------------------------

for (const [i, name] of AGENT_NAMES.entries()) {
  const transport = transports[i];
  const crypto = cryptoInstances[i];
  const storage = stores[i];
  if (
    transport === undefined ||
    crypto === undefined ||
    storage === undefined
  ) {
    throw new Error(`Missing per-agent dependency for index ${String(i)}`);
  }
  const h = createHarness({
    address: agentAddress(name),
    systemPrompt: buildRingPrompt(name, i),
    source,
    transport,
    crypto,
    storage,
    tools: createPosixTools({ cwd: process.cwd() }),
    onEvent: makeEventLogger(name),
    defaultDirectorPolicy: i !== 0 ? { mode: "reactive" } : {},
  });
  harnesses.push(h);
}

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

function shutdown(signal: string): void {
  if (shutdownInitiated) return;
  shutdownInitiated = true;
  log.info("Received {signal}, shutting down", { signal });
  for (const h of harnesses) h.stop();
  clearTimeout(safetyTimer);
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));

const safetyTimer = setTimeout(() => {
  if (!shutdownInitiated) {
    log.info("Safety timeout reached, shutting down");
    shutdown("timeout");
  }
}, 300_000);

// ---------------------------------------------------------------------------
// Start and seed
// ---------------------------------------------------------------------------

for (const h of harnesses) h.start();

log.info("Sending prompt to alpha: {prompt}", { prompt: SEED_PROMPT });

await transportUser.send({
  to: agentAddress("alpha"),
  type: "conversation.message",
  content: SEED_PROMPT,
  subject: "Braintrust",
});

log.info("Braintrust running — waiting for the ring to complete");
