// Mail-participating agent fixture for the reactor-once tests.
//
// The fixture's definition has a tool factory that declares
// `requires: ["transport", "address"]`. A bare `BaseEnv` is short on
// those keys; instantiation must fail with `AgentEnvError`. A
// transport-bearing env satisfies the requirement and instantiation
// succeeds; the reactor-once assertion lives in `mail.test.ts`.
//
// The fixture does not import `@intx/harness` -- the composition
// layer's reactor-once invariant is exercised by the harness's own
// tests against its own surface; cross-importing harness here would
// introduce a `@intx/agent <-> @intx/harness` cycle.

import type { ContextStore, InferenceSource } from "@intx/types/runtime";

import { defineAgent, type AgentDefinition } from "../definition";
import { createDefaultDirectorRegistry } from "../director-registry";
import { type BaseEnv } from "../env";
import { noopAuditStore } from "../testing/audit-noop";
import { permissiveAuthorize } from "../testing/authorize-allow";
import { defineTool } from "../tool";

export const MAIL_SOURCE: InferenceSource = {
  id: "anthropic:claude-opus-4-6",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-mail",
  model: "claude-opus-4-6",
};

export const MAIL_ADDRESS = "support@fixture.local";

/**
 * Env extension declared by the fixture's mail tool. Production code
 * uses the `MailEnv` from `@intx/harness`; the fixture redeclares the
 * shape locally to avoid the cross-package import.
 */
export interface FixtureMailEnv extends BaseEnv {
  transport: unknown;
  address: string;
}

/**
 * A no-op mail tool factory declaring `requires: ["transport",
 * "address"]`. The bundle's definitions are empty -- the fixture's
 * tests do not invoke any tool; they verify env validation behaviour.
 */
export const fixtureMailFactory = defineTool<FixtureMailEnv>({
  id: "@intx-fixtures/mail/bundle",
  requires: ["transport", "address"],
  factory: () => ({
    definitions: [],
    async run(call) {
      return { callId: call.id, content: "" };
    },
  }),
});

export const mailAgentDefinition: AgentDefinition<FixtureMailEnv> = defineAgent(
  {
    id: "mail-fixture",
    description: "Mail-participating fixture agent",
    systemPrompt: "You handle mail.",
    tools: [fixtureMailFactory],
    capabilities: [],
    inference: {
      sources: [{ provider: MAIL_SOURCE.provider, model: MAIL_SOURCE.model }],
    },
  },
);

/**
 * Build a bare `BaseEnv` lacking `transport` and `address`. The mail
 * factory's `requires` makes this env short; `validateEnv` throws
 * `AgentEnvError` blaming the factory.
 */
export function createBareMailEnv(opts: {
  storage: ContextStore;
  workdir: string;
}): BaseEnv {
  return {
    source: MAIL_SOURCE,
    storage: opts.storage,
    workdir: opts.workdir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
  };
}

/**
 * Build a transport-bearing `FixtureMailEnv` for the success path.
 * `transport` is opaque to the fixture (the mail factory's run is a
 * no-op) so the test does not need to supply a real transport.
 */
export function createTransportMailEnv(opts: {
  storage: ContextStore;
  workdir: string;
}): FixtureMailEnv {
  return {
    source: MAIL_SOURCE,
    storage: opts.storage,
    workdir: opts.workdir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
    transport: { kind: "fake-transport" },
    address: MAIL_ADDRESS,
  };
}
