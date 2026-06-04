// Reactor-once + env-validation assertions for the mail fixture.
//
// Two cases:
//   1. Bare `BaseEnv` (no transport, no address): `createAgent` throws
//      `AgentEnvError` whose `missing` includes "transport" and
//      "address", and whose `contributors` includes the mail factory id.
//   2. Transport-bearing env: `createAgent` succeeds and the reactor
//      is wrapped exactly once (counted via a director-factory proxy,
//      same approach as `planner.test.ts`).
//
// The third spec-listed case -- composition-layer instantiation
// through `@intx/harness` -- is exercised by the harness package's own
// tests; importing `@intx/harness` here would cycle the
// `@intx/agent <-> @intx/harness` workspace dep, so the cross-check
// lives there instead.

import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type } from "arktype";

import { createIsogitStore } from "@intx/storage-isogit";

import { createAgent } from "../agent";
import { defaultDirectorFactory } from "../default-director";
import { defineDirector } from "../director";
import { createDirectorRegistry } from "../director-registry";
import { AgentEnvError } from "../env";
import {
  createBareMailEnv,
  createTransportMailEnv,
  fixtureMailFactory,
  mailAgentDefinition,
} from "./mail";

let factoryCallCount = 0;

const countingDefault = defineDirector({
  id: "@intx-fixtures/mail/counting-default",
  configSchema: type({}),
  factory: (_config, env, agent) => {
    factoryCallCount += 1;
    return defaultDirectorFactory({}, env, agent);
  },
});

function countingRegistry() {
  return createDirectorRegistry({
    factories: [countingDefault.factory],
    defaultId: countingDefault.factory.id,
  });
}

describe("mail fixture: env validation and reactor-once", () => {
  test("bare BaseEnv triggers AgentEnvError blaming the mail factory", async () => {
    const workdir = mkdtempSync(join(tmpdir(), "fixture-mail-bare-"));
    try {
      const storage = await createIsogitStore(workdir);
      const env = createBareMailEnv({ storage, workdir });

      let caught: unknown;
      try {
        await createAgent(
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- exercise runtime validateEnv against intentionally mis-typed pairing
          mailAgentDefinition as unknown as Parameters<
            typeof createAgent<typeof env>
          >[0],
          env,
        );
      } catch (err) {
        caught = err;
      }
      if (!(caught instanceof AgentEnvError)) {
        throw new Error("expected AgentEnvError, got " + String(caught));
      }
      expect(caught.missing).toContain("transport");
      expect(caught.missing).toContain("address");
      expect(caught.contributors).toContain(`tool:${fixtureMailFactory.id}`);
    } finally {
      rmSync(workdir, { recursive: true, force: true });
    }
  });

  test("transport-bearing env succeeds; reactor wrapped exactly once", async () => {
    const workdir = mkdtempSync(join(tmpdir(), "fixture-mail-ok-"));
    try {
      const storage = await createIsogitStore(workdir);
      const before = factoryCallCount;

      const env = {
        ...createTransportMailEnv({ storage, workdir }),
        directors: countingRegistry(),
      };

      const agent = await createAgent(mailAgentDefinition, env);
      try {
        const after = factoryCallCount;
        expect(after - before).toBe(1);
      } finally {
        await agent.close();
      }
    } finally {
      rmSync(workdir, { recursive: true, force: true });
    }
  });
});
