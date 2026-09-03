// Regression guard for INTR-170's design: the posix tool working
// directory (`env.toolCwd`) is independent of the agent's singleton lock
// boundary (`env.workdir`). Two agents may share one `toolCwd` while
// holding distinct `workdir` values, because the lock guards `workdir`
// and not the tool tree. These tests pin all three invariants the design
// commits to: the shared-toolCwd coexistence, the lock key staying on
// `workdir`, and the posix bundle requiring `toolCwd` through env-DI.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  AgentContextLockError,
  AgentEnvError,
  createAgent,
  createDefaultDirectorRegistry,
  createStaticCredentialResolver,
  defineAgent,
  type Agent,
  type AgentDefinition,
  type BaseEnv,
} from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { setupHarness, type Harness } from "@intx/inference-testing";
import { createIsogitStore } from "@intx/storage-isogit/node";
import { posix, type PosixToolEnv } from "@intx/tools-posix/sidecar-bundle";
import type { InferenceSource } from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-test-posix-toolcwd",
  model: "claude-3-5-sonnet",
};

function definition(): AgentDefinition<PosixToolEnv> {
  return defineAgent({
    id: "posix-toolcwd-test",
    systemPrompt: "posix toolCwd test",
    tools: [posix],
    capabilities: [],
    inference: {
      sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
    },
  });
}

async function envFor(
  workdir: string,
  toolCwd: string,
  harness: Harness,
): Promise<PosixToolEnv> {
  const storage = await createIsogitStore(workdir);
  return {
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    storage,
    workdir,
    toolCwd,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
    // The mock adapter never sends the secret, but the inference path
    // resolves the source's credential before dispatch, so a resolver
    // must answer for SOURCE.credentialId or the call fails closed.
    readCurrentMaterial: createStaticCredentialResolver({
      [SOURCE.credentialId]: "sk-test-posix-toolcwd-secret",
    }),
    deps: harness.deps,
  };
}

// Drive one write_file tool call through the agent, letting the reactor
// close the cycle with a follow-up text reply. permissiveAuthorize
// returns { effect: "allow" }, so the gated write runs inline with no
// approval suspension. The path MUST stay relative: an absolute path
// bypasses toolCwd resolution entirely, so an absolute path here would
// make the test pass without exercising the toolCwd wiring at all.
async function driveWrite(
  agent: Agent,
  harness: Harness,
  relPath: string,
  content: string,
): Promise<void> {
  harness.scenario.replyOnce("anthropic", {
    toolCalls: [
      {
        callId: `write-${relPath}`,
        name: "write_file",
        argsJSON: JSON.stringify({ path: relPath, content }),
      },
    ],
  });
  harness.scenario.replyOnce("anthropic", { text: "done" });
  const sendPromise = agent.send(`write ${relPath}`);
  await harness.run();
  await sendPromise;
}

describe("@intx/agent posix toolCwd env-DI", () => {
  let root: string;
  let harness: Harness;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), "agent-posix-toolcwd-"));
    harness = setupHarness();
  });

  afterEach(() => {
    harness.dispose();
    rmSync(root, { recursive: true, force: true });
  });

  test("two agents share a toolCwd while holding distinct workdirs", async () => {
    const toolCwd = mkdtempSync(join(root, "shared-tool-"));
    const agentA = await createAgent(
      definition(),
      await envFor(join(root, "wd-a"), toolCwd, harness),
    );
    try {
      const agentB = await createAgent(
        definition(),
        await envFor(join(root, "wd-b"), toolCwd, harness),
      );
      try {
        await driveWrite(agentA, harness, "a.txt", "from agent a");
        await driveWrite(agentB, harness, "b.txt", "from agent b");

        expect(readFileSync(join(toolCwd, "a.txt"), "utf8")).toBe(
          "from agent a",
        );
        expect(readFileSync(join(toolCwd, "b.txt"), "utf8")).toBe(
          "from agent b",
        );
      } finally {
        await agentB.close();
      }
    } finally {
      await agentA.close();
    }
  });

  test("the singleton lock still keys on workdir, not toolCwd", async () => {
    const workdir = join(root, "shared-wd");
    // Distinct toolCwd values so the shared workdir is the sole cause of
    // the rejection -- otherwise this would merely restate the existing
    // same-workdir lock test in lifecycle.test.ts.
    const first = await createAgent(
      definition(),
      await envFor(workdir, mkdtempSync(join(root, "tool-a-")), harness),
    );
    try {
      await expect(
        createAgent(
          definition(),
          await envFor(workdir, mkdtempSync(join(root, "tool-b-")), harness),
        ),
      ).rejects.toBeInstanceOf(AgentContextLockError);
    } finally {
      await first.close();
    }
  });

  test("the posix bundle requires toolCwd through env-DI", async () => {
    const storage = await createIsogitStore(join(root, "wd-missing"));
    const envWithoutToolCwd: BaseEnv = {
      sources: [SOURCE],
      defaultSource: SOURCE.id,
      storage,
      workdir: join(root, "wd-missing"),
      audit: noopAuditStore(),
      authorize: permissiveAuthorize(),
      directors: createDefaultDirectorRegistry(),
      deps: harness.deps,
    };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- bypass the type-level toolCwd requirement to exercise runtime env-DI validation
    const looseDef = definition() as unknown as AgentDefinition<BaseEnv>;
    try {
      await createAgent(looseDef, envWithoutToolCwd);
      throw new Error("expected createAgent to reject the toolCwd-less env");
    } catch (err) {
      if (!(err instanceof AgentEnvError)) throw err;
      expect(err.missing).toContain("toolCwd");
      expect(err.contributors).toContain(
        "tool:@intx/tools-posix/sidecar-bundle",
      );
    }
  });
});
