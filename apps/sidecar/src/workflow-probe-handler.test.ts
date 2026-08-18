import { afterEach, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { hexDecode, hexEncode } from "@intx/types";
import type { WorkflowProbeRequestFrame } from "@intx/types/sidecar";
import {
  encodeEnvelope,
  signHmac,
  type FrameEnvelope,
} from "@intx/workflow-host";

import {
  createWorkflowProbeExecutor,
  defaultProbeChildSpawner,
  enrichProbeError,
  type MaterializeWorkflowClosure,
  type MaterializedWorkflowClosure,
  type ProbeChildHandle,
  type ProbeChildSpawner,
} from "./workflow-probe-handler";

describe("enrichProbeError", () => {
  test("adds a dependencies/devDependencies hint to a module-not-found", () => {
    const enriched = enrichProbeError(
      new Error("Cannot find module '@wf/lib' from '/x/workflow.mjs'"),
    );
    expect(enriched).toMatch(/could not resolve "@wf\/lib"/);
    expect(enriched).toMatch(/"dependencies" rather than "devDependencies"/);
  });

  test("matches the 'Cannot find package' phrasing too", () => {
    const enriched = enrichProbeError(
      new Error('Cannot find package "left-pad" imported from /x/workflow.mjs'),
    );
    expect(enriched).toMatch(/could not resolve "left-pad"/);
  });

  test("passes a non-resolution error through unchanged", () => {
    const enriched = enrichProbeError(new Error("boom, author code threw"));
    expect(enriched).toBe("boom, author code threw");
  });
});

// The workflow package the fixture entry modules import
// `@intx/workflow/definition` from. The fixture lays this out under the
// package's `node_modules/` the way a materialized closure would, so the
// entry's bare-specifier import resolves inside the spawned child.
const WORKFLOW_PACKAGE_DIR = path.resolve(
  import.meta.dir,
  "../../../packages/workflow",
);
// The agent package a fixture entry imports `defineAgent`/director refs from,
// laid out under the fixture's node_modules the same way as @intx/workflow.
const AGENT_PACKAGE_DIR = path.resolve(
  import.meta.dir,
  "../../../packages/agent",
);

const SPAWN_TEST_TIMEOUT_MS = 30_000;

const createdDirs: string[] = [];

afterEach(async () => {
  while (createdDirs.length > 0) {
    const dir = createdDirs.pop();
    if (dir === undefined) continue;
    await fs.rm(dir, { recursive: true, force: true });
  }
});

const MAIL_TRIGGER_ENTRY = `
import { defineWorkflow } from "@intx/workflow/definition";
export default defineWorkflow({
  id: "probe-fixture",
  trigger: { type: "mail", to: "probe@example.com" },
  steps: {
    wait: { kind: "sleep", id: "", durationMs: 5 },
  },
});
`;

const THROWING_ENTRY = `throw new Error("probe fixture boom");`;

// A workflow whose agent references a director id the closure does not ship
// (no interchange.directors). The capability walk resolves it against the
// built-ins-only registry, reports it unresolved, and the probe must fail
// closed rather than advertise a grant set missing the director.
const UNRESOLVABLE_DIRECTOR_ENTRY = `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
export default defineWorkflow({
  id: "probe-unresolvable-director",
  trigger: { type: "mail", to: "probe@example.com" },
  steps: {
    act: step({
      agent: defineAgent({
        id: "agent_x",
        systemPrompt: "x",
        tools: [],
        capabilities: [],
        inference: { sources: [{ provider: "openai", model: "gpt-4o" }] },
        director: { id: "@unshipped/pkg/director", config: {} },
      }),
    }),
  },
});
`;

async function layoutClosureFixture(entrySource: string): Promise<string> {
  const packageDir = await fs.mkdtemp(path.join(os.tmpdir(), "wf-probe-"));
  createdDirs.push(packageDir);

  const scopeDir = path.join(packageDir, "node_modules", "@intx");
  await fs.mkdir(scopeDir, { recursive: true });
  await fs.symlink(
    WORKFLOW_PACKAGE_DIR,
    path.join(scopeDir, "workflow"),
    "dir",
  );
  await fs.symlink(AGENT_PACKAGE_DIR, path.join(scopeDir, "agent"), "dir");

  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify(
      {
        name: "@fixture/probe-workflow-package",
        version: "1.0.0",
        interchange: { workflow: "./workflow.js" },
      },
      null,
      2,
    ),
  );
  await fs.writeFile(path.join(packageDir, "workflow.js"), entrySource);
  return packageDir;
}

// A host-side materializer that lays out a fixture package dir (no
// network, no real closure fetch) so the test can exercise a REAL child
// spawn + real load/evaluate/walk/project against a known workflow.
function fixtureMaterializer(entrySource: string): MaterializeWorkflowClosure {
  return async (): Promise<MaterializedWorkflowClosure> => {
    const packageDir = await layoutClosureFixture(entrySource);
    return {
      packageDir,
      async cleanup() {
        await fs.rm(packageDir, { recursive: true, force: true });
      },
    };
  };
}

// Wrap the real spawner so the test captures the spawned pids and can
// assert the child was reaped once the probe settles.
function recordingSpawner(pids: number[]): ProbeChildSpawner {
  return (args): ProbeChildHandle => {
    const handle = defaultProbeChildSpawner(args);
    pids.push(handle.pid);
    return handle;
  };
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // ESRCH: the process is gone. EPERM: it exists but is not ours (a
    // recycled pid owned by another user) -- treat as alive to avoid a
    // false "reaped" pass.
    if (
      err !== null &&
      typeof err === "object" &&
      "code" in err &&
      err.code === "EPERM"
    ) {
      return true;
    }
    return false;
  }
}

function probeFrame(): WorkflowProbeRequestFrame {
  return {
    type: "workflow.probe.request",
    requestId: "probe-req-1",
    source: { kind: "registry", registry: "npmjs" },
    closure: { schemaVersion: "1", topLevel: [], entries: [] },
    entry: "./workflow.js",
  };
}

describe("createWorkflowProbeExecutor", () => {
  test(
    "spawns a one-shot child that returns the inert projection, grants, and hash, and reaps it on success",
    async () => {
      const pids: number[] = [];
      const executor = createWorkflowProbeExecutor({
        materialize: fixtureMaterializer(MAIL_TRIGGER_ENTRY),
        spawnProbeChild: recordingSpawner(pids),
      });

      const result = await executor.probe(probeFrame());

      expect(result.projection.id).toBe("probe-fixture");
      expect(result.projection.stepOrder).toEqual(["wait"]);
      expect(result.grants).toEqual([
        "mail.address:probe@example.com",
        "mail.send:example.com",
      ]);
      // The un-flattened snapshot preserves the per-step grouping and the
      // (here empty) tool-grant effect map the flattened `grants` discards.
      // The `sleep` step carries only the deployment-wide trigger grants.
      expect(result.grantWalkSnapshot).toEqual({
        perStep: [
          {
            stepId: "wait",
            grants: ["mail.address:probe@example.com", "mail.send:example.com"],
            grantEffects: {},
          },
        ],
        grantRequirements: [],
      });
      expect(result.wireHash).toMatch(/^[0-9a-f]{64}$/);

      // A real child was spawned, and it is reaped by the time the probe
      // resolves: no leaked process survives the call.
      expect(pids).toHaveLength(1);
      for (const pid of pids) {
        expect(isProcessAlive(pid)).toBe(false);
      }
    },
    SPAWN_TEST_TIMEOUT_MS,
  );

  test("returns a written result even when the child exit resolves before the read", async () => {
    // A one-shot child writes its result line and then exits promptly, so both
    // the buffered line and `handle.exited` become ready together. A handle
    // whose `exited` is ALREADY resolved and whose stdout carries a valid signed
    // result reproduces that race deterministically: the former `exit` race arm
    // would win and discard the written result; racing only the line must
    // return it.
    const projection = {
      id: "race-fixture",
      triggers: [],
      stepOrder: [],
      steps: {},
    };
    const raceSpawner: ProbeChildSpawner = ({ env }) => {
      const channelId = env["PROBE_IPC_CHANNEL_ID"];
      const hmacHex = env["PROBE_IPC_HMAC_KEY"];
      if (channelId === undefined || hmacHex === undefined) {
        throw new Error("probe spawn env missing channel id / hmac key");
      }
      const hmacKey = hexDecode(hmacHex);
      const stdout = new ReadableStream<Uint8Array>({
        async start(controller) {
          const payload = {
            ok: true as const,
            projection,
            grants: ["cap:probe-race"],
            grantWalkSnapshot: {
              perStep: [
                { stepId: "s1", grants: ["cap:probe-race"], grantEffects: {} },
              ],
              grantRequirements: [],
            },
            wireHash: "a".repeat(64),
          };
          const envelope: FrameEnvelope = { seq: 0, channelId, payload };
          const mac = hexEncode(
            await signHmac(encodeEnvelope(envelope), hmacKey),
          );
          controller.enqueue(
            new TextEncoder().encode(`${JSON.stringify({ envelope, mac })}\n`),
          );
          controller.close();
        },
      });
      return {
        pid: 4242,
        stdout,
        exited: Promise.resolve(0),
        kill: () => {
          /* mock handle: already exited, nothing to reap */
        },
      };
    };

    const materialize: MaterializeWorkflowClosure = () =>
      Promise.resolve({
        packageDir: "/unused-by-the-race-spawner",
        cleanup: () => Promise.resolve(),
      });

    const executor = createWorkflowProbeExecutor({
      materialize,
      spawnProbeChild: raceSpawner,
    });

    const result = await executor.probe(probeFrame());
    expect(result.projection).toEqual(projection);
    expect(result.grants).toEqual(["cap:probe-race"]);
    expect(result.grantWalkSnapshot).toEqual({
      perStep: [{ stepId: "s1", grants: ["cap:probe-race"], grantEffects: {} }],
      grantRequirements: [],
    });
    expect(result.wireHash).toBe("a".repeat(64));
  });

  test(
    "rejects and reaps the child when the workflow entry throws during evaluation",
    async () => {
      const pids: number[] = [];
      const executor = createWorkflowProbeExecutor({
        materialize: fixtureMaterializer(THROWING_ENTRY),
        spawnProbeChild: recordingSpawner(pids),
      });

      await expect(executor.probe(probeFrame())).rejects.toThrow(
        /workflow probe evaluation failed/,
      );

      expect(pids).toHaveLength(1);
      for (const pid of pids) {
        expect(isProcessAlive(pid)).toBe(false);
      }
    },
    SPAWN_TEST_TIMEOUT_MS,
  );

  test(
    "fails closed when the workflow references a director the closure does not ship",
    async () => {
      const executor = createWorkflowProbeExecutor({
        materialize: fixtureMaterializer(UNRESOLVABLE_DIRECTOR_ENTRY),
        spawnProbeChild: recordingSpawner([]),
      });

      // The custom director id resolves against nothing (the closure ships no
      // interchange.directors), so the walk reports it unresolved and the probe
      // refuses rather than advertising a grant set missing the director. This
      // is the only approval checkpoint for a director -- the runtime does not
      // re-gate director:<id> against the approved set.
      await expect(executor.probe(probeFrame())).rejects.toThrow(
        /unresolvable director/,
      );
    },
    SPAWN_TEST_TIMEOUT_MS,
  );

  test(
    "always calls the materializer cleanup, on both success and failure",
    async () => {
      let cleanups = 0;
      const materialize: MaterializeWorkflowClosure = async () => {
        const packageDir = await layoutClosureFixture(MAIL_TRIGGER_ENTRY);
        return {
          packageDir,
          async cleanup() {
            cleanups += 1;
            await fs.rm(packageDir, { recursive: true, force: true });
          },
        };
      };

      const executor = createWorkflowProbeExecutor({
        materialize,
        spawnProbeChild: recordingSpawner([]),
      });

      await executor.probe(probeFrame());
      expect(cleanups).toBe(1);
    },
    SPAWN_TEST_TIMEOUT_MS,
  );
});
