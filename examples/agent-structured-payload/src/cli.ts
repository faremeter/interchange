// agent-structured-payload: build an `InboundMessage` carrying a
// typed `InterchangeType` payload (here `offering.request`), deliver
// it to an agent, and confirm via the reactor's event stream that
// the typed envelope landed intact.
//
// The mail-shaped message model is the feature this example
// surfaces: a payload envelope with a stable `type`, `version`, and
// `body` flows through the same `agent.deliver()` surface a
// conversation message uses, and the reactor preserves the typed
// data verbatim for audit and downstream consumers.
//
// What the example does NOT do, and why: the default director in
// `@interchange/harness` does not render structured payloads into
// the model's prompt (`createInboundTurn` in
// packages/inference/src/turns.ts returns null when
// `message.content` is empty, which is the case for structured
// payloads). Making the model react to a payload requires a custom
// director or context transform that maps the payload's body into a
// user turn. The README links the pattern; this example focuses on
// the delivery half, which is the part the mail-builder API
// directly governs.

import {
  openExampleAgent,
  optional,
  resolveAgentProvider,
  resolveStdio,
  type SingleProviderMainOptions,
} from "@interchange/example-agent-common";
import type { ReactorEmittedEvent } from "@interchange/inference";
import { createInboundMessage } from "@interchange/mime";
import type { InboundMessage } from "@interchange/types/runtime";

const EXAMPLE_NAME = "agent-structured-payload";

export type OfferingRequestArgs = {
  offeringId: string;
  description: string;
  priceCents: number;
  currency: string;
};

export type MainOptions = SingleProviderMainOptions & {
  /** Provide pre-built args; overrides any argv parsing. */
  offering?: OfferingRequestArgs;
  /**
   * Bound on how long the CLI waits for the reactor to surface a
   * `message.received` event for the delivered payload. Defaults to
   * 30 seconds so a stuck reactor does not hang the caller.
   */
  receivedTimeoutMs?: number;
};

const DEFAULT_RECEIVED_TIMEOUT_MS = 30_000;

const DEFAULT_OFFERING: OfferingRequestArgs = {
  offeringId: "demo-offering-001",
  description: "Premium widget — limited release",
  priceCents: 1999,
  currency: "USD",
};

export function buildOfferingRequest(
  args: OfferingRequestArgs,
  opts?: { from?: string; to?: string; correlationId?: string },
): InboundMessage {
  return createInboundMessage({
    from: opts?.from ?? "merchant@local",
    to: opts?.to ?? "agent@local",
    payload: {
      type: "offering.request",
      body: {
        offeringId: args.offeringId,
        description: args.description,
        priceCents: args.priceCents,
        currency: args.currency,
      },
    },
    ...optional("correlationId", opts?.correlationId),
    offeringId: args.offeringId,
  });
}

function parseArgs(
  argv: string[],
  stderr: (s: string) => void,
): OfferingRequestArgs | undefined {
  const out: OfferingRequestArgs = { ...DEFAULT_OFFERING };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    const value = argv[i + 1];
    if (value === undefined) {
      stderr(
        `agent-structured-payload: flag ${String(key)} is missing a value\n`,
      );
      return undefined;
    }
    i++;
    switch (key) {
      case "--offering-id":
        out.offeringId = value;
        break;
      case "--description":
        out.description = value;
        break;
      case "--price-cents": {
        const n = Number(value);
        if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0) {
          stderr(
            `agent-structured-payload: --price-cents must be a non-negative integer; got ${value}\n`,
          );
          return undefined;
        }
        out.priceCents = n;
        break;
      }
      case "--currency":
        out.currency = value;
        break;
      default:
        stderr(
          `agent-structured-payload: unknown flag ${String(key)}; recognised flags are --offering-id, --description, --price-cents, --currency\n`,
        );
        return undefined;
    }
  }
  return out;
}

export async function main(
  argv: string[],
  env: NodeJS.ProcessEnv,
  opts: MainOptions = {},
): Promise<number> {
  const { stdout, stderr } = resolveStdio(opts);

  const offering = opts.offering ?? parseArgs(argv, stderr);
  if (offering === undefined) return 1;

  const resolved = resolveAgentProvider(opts, env, EXAMPLE_NAME, stderr);
  if (resolved === null) return 1;

  const message = buildOfferingRequest(offering);

  const agent = await openExampleAgent(opts, {
    exampleName: EXAMPLE_NAME,
    systemPrompt:
      "You are an offering-aware assistant. The default director does not surface structured payloads to you; a custom director would render them as user turns. This example demonstrates the delivery half of the contract.",
    tools: [],
    providers: [resolved.provider],
    defaultModel: resolved.model,
  });

  const timeoutMs = opts.receivedTimeoutMs ?? DEFAULT_RECEIVED_TIMEOUT_MS;

  try {
    // Subscribe before delivering so the message.received event
    // cannot fire before our consumer is attached.
    const received = waitForReceived(agent.stream(), timeoutMs);

    stdout("delivering offering.request:\n");
    stdout(`  offeringId:  ${offering.offeringId}\n`);
    stdout(`  description: ${offering.description}\n`);
    stdout(
      `  price:       ${formatPrice(offering.priceCents, offering.currency)}\n`,
    );
    stdout(`  type:        ${message.headers.interchangeType ?? "(unset)"}\n`);
    stdout(`  messageId:   ${message.headers.messageId}\n`);
    stdout("\n");

    agent.deliver(message);

    const outcome = await received;
    if (outcome === "timeout") {
      stderr(
        `did not observe message.received within ${String(timeoutMs)}ms\n`,
      );
      return 2;
    }
    if (outcome === undefined) {
      stderr("reactor closed before message.received fired\n");
      return 2;
    }

    const delivered = outcome.data.message;
    stdout("reactor received:\n");
    stdout(
      `  type:        ${delivered.headers.interchangeType ?? "(unset)"}\n`,
    );
    stdout(`  from:        ${delivered.headers.from}\n`);
    stdout(`  messageId:   ${delivered.headers.messageId}\n`);
    stdout(`  payload.type:    ${delivered.payload?.type ?? "(no payload)"}\n`);
    stdout(
      `  payload.version: ${delivered.payload?.version ?? "(no payload)"}\n`,
    );
    if (delivered.payload !== undefined) {
      stdout(`  payload.body:    ${JSON.stringify(delivered.payload.body)}\n`);
    }
    return 0;
  } finally {
    await agent.close();
  }
}

/**
 * Watch a reactor event stream for a `message.received` event,
 * returning the event when it fires, `undefined` if the reactor
 * shuts down first, or `"timeout"` if `timeoutMs` elapses before
 * either.
 */
async function waitForReceived(
  events: AsyncIterable<ReactorEmittedEvent>,
  timeoutMs: number,
): Promise<
  | Extract<ReactorEmittedEvent, { type: "message.received" }>
  | undefined
  | "timeout"
> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<"timeout">((resolve) => {
    timer = setTimeout(() => resolve("timeout"), timeoutMs);
  });
  // The IIFE keeps running in the background when `timeout` wins
  // the race; `.catch` swallows any rejection from the stream
  // consumer's `close()` path so it does not surface as an unhandled
  // promise rejection after the timeout has already been reported.
  // The example deliberately does not depend on the precise
  // termination shape of `agent.stream()` — graceful end or thrown
  // error both produce `undefined` here.
  const found = (async (): Promise<
    Extract<ReactorEmittedEvent, { type: "message.received" }> | undefined
  > => {
    for await (const event of events) {
      if (event.type === "message.received") return event;
      if (event.type === "reactor.done") return undefined;
    }
    return undefined;
  })().catch((): undefined => undefined);
  try {
    return await Promise.race([found, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

function formatPrice(cents: number, currency: string): string {
  const major = (cents / 100).toFixed(2);
  return `${currency} ${major}`;
}

if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
