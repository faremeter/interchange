// Reactor-once assertion for the planner fixture.
//
// The agent's `createAgent` resolves the env's director registry,
// invokes the resolved factory exactly once, and passes the resulting
// `ReactorDirector` into `createReactorAssembly`. Counting director-
// factory invocations is therefore a precise proxy for counting
// reactor wraps: one invocation per `createAgent` call, every time.
//
// The counter lives inside this file (not shared with `mail.test.ts`)
// so module-load ordering between test files cannot smear the count.

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
import type { BaseEnv } from "../env";
import { PLANNER_SOURCE, plannerAgentDefinition } from "./planner";
import { noopAuditStore } from "../testing/audit-noop";
import { permissiveAuthorize } from "../testing/authorize-allow";

let factoryCallCount = 0;

const countingDefault = defineDirector({
  id: "@intx-fixtures/planner/counting-default",
  configSchema: type({}),
  factory: (_config, env, agent) => {
    factoryCallCount += 1;
    // Delegate to the real default director so the agent's reactor
    // gets a working loop controller.
    return defaultDirectorFactory({}, env, agent);
  },
});

function countingRegistry() {
  return createDirectorRegistry({
    factories: [countingDefault.factory],
    defaultId: countingDefault.factory.id,
  });
}

describe("planner fixture: reactor wrapped exactly once", () => {
  test("createAgent invokes the director factory once per instantiation", async () => {
    const workdir = mkdtempSync(join(tmpdir(), "fixture-planner-"));
    try {
      const before = factoryCallCount;
      const storage = await createIsogitStore(workdir);

      const env: BaseEnv = {
        source: PLANNER_SOURCE,
        storage,
        workdir,
        audit: noopAuditStore(),
        authorize: permissiveAuthorize(),
        directors: countingRegistry(),
      };

      const agent = await createAgent(plannerAgentDefinition, env);
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
