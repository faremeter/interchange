import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto-node";
import { collectReachableObjects } from "@intx/storage-isogit";
import type { KeyPair } from "@intx/types/runtime";
import {
  workflowRunKindHandler,
  workflowRunAuthorize,
  enqueueInbox,
  dequeueToProcessing,
  markConsumed,
  replayProcessingToInbox,
  WORKFLOW_RUN_GITIGNORE_PATH,
  WORKFLOW_RUN_RUNS_PREFIX,
  WORKFLOW_RUN_ADDRESSES_PREFIX,
  WORKFLOW_RUN_INBOX_DIR,
  WORKFLOW_RUN_PROCESSING_DIR,
  WORKFLOW_RUN_CONSUMED_DIR,
  WORKFLOW_RUN_BLOBS_DIR,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  WORKFLOW_RUN_WATERMARK_FILE,
} from "./workflow-run-kind";
import { createRepoStore } from "./repo-store";
import type { KindHandler, Principal, RepoId } from "./repo-store";

const REF = "refs/heads/events";

function makeReadBlob(
  files: Record<string, string>,
): (path: string) => Promise<Uint8Array> {
  return async (path) => {
    const body = files[path];
    if (body === undefined) {
      throw new Error(`readBlob: ${path} not found`);
    }
    return new TextEncoder().encode(body);
  };
}

function makeListDir(
  files: Record<string, string>,
): (path: string) => Promise<string[]> {
  return async (path) => {
    const prefix = path === "" ? "" : `${path}/`;
    const names = new Set<string>();
    for (const p of Object.keys(files)) {
      if (prefix !== "" && !p.startsWith(prefix)) continue;
      const rest = p.slice(prefix.length);
      if (rest.length === 0) continue;
      const slash = rest.indexOf("/");
      names.add(slash === -1 ? rest : rest.substring(0, slash));
    }
    return Array.from(names);
  };
}

function topLevels(files: Record<string, string>): string[] {
  const names = new Set<string>();
  for (const p of Object.keys(files)) {
    const slash = p.indexOf("/");
    names.add(slash === -1 ? p : p.substring(0, slash));
  }
  return Array.from(names);
}

function uniqueRepoId(prefix: string): RepoId {
  const id = `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
  return { kind: "workflow-run", id };
}

function eventBody(
  seq: number,
  type: string,
  extras: Record<string, unknown> = {},
): string {
  return JSON.stringify({ seq, type, ...extras });
}

const HUB_PRINCIPAL: Principal = { kind: "hub" };
const SUPERVISOR_PRINCIPAL: Principal = { kind: "supervisor" };
const WORKFLOW_PROCESS_PRINCIPAL_SHAPE = {
  kind: "workflow-process",
  deploymentId: "test-deployment",
};
const WORKFLOW_PROCESS_PRINCIPAL: Principal = WORKFLOW_PROCESS_PRINCIPAL_SHAPE;
const noPriorBlob = async (): Promise<Uint8Array | null> => null;
const noPriorDir = async (): Promise<string[]> => [];

type ValidateOpts = {
  ref?: string;
  principal?: Principal;
  priorFiles?: Record<string, string>;
  changedPathPrefixes?: ReadonlySet<string>;
};

async function validate(
  files: Record<string, string>,
  opts: ValidateOpts = {},
) {
  const repoId = uniqueRepoId("wfr");
  const ref = opts.ref ?? REF;
  const principal = opts.principal ?? HUB_PRINCIPAL;
  const priorReadBlob =
    opts.priorFiles === undefined
      ? noPriorBlob
      : makePriorReadBlob(opts.priorFiles);
  const priorListDir =
    opts.priorFiles === undefined ? noPriorDir : makeListDir(opts.priorFiles);
  return workflowRunKindHandler.validatePush({
    repoId,
    ref,
    principal,
    topLevelTreePaths: topLevels(files),
    readBlob: makeReadBlob(files),
    listDir: makeListDir(files),
    priorReadBlob,
    priorListDir,
    changedPathPrefixes: opts.changedPathPrefixes,
  });
}

function makePriorReadBlob(
  files: Record<string, string>,
): (path: string) => Promise<Uint8Array | null> {
  return async (path) => {
    const body = files[path];
    if (body === undefined) return null;
    return new TextEncoder().encode(body);
  };
}

describe("workflowRunKindHandler metadata", () => {
  test("declares the workflow-run kind and workflow-runs directory prefix", () => {
    expect(workflowRunKindHandler.kind).toBe("workflow-run");
    expect(workflowRunKindHandler.directoryPrefix).toBe("workflow-runs");
  });
});

describe("workflowRunKindHandler.validatePush — accepts", () => {
  test("accepts a .gitignore-only genesis tree", async () => {
    const r = await validate({ [WORKFLOW_RUN_GITIGNORE_PATH]: "" });
    expect(r.ok).toBe(true);
  });

  test("accepts a tree with a single run and a single non-terminal event", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    });
    expect(r.ok).toBe(true);
  });

  test("accepts a tree with multiple runs and many ordered events", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "StepStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/2.json`]: eventBody(
        2,
        "StepCompleted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    });
    expect(r.ok).toBe(true);
  });

  test("accepts a terminal event as the last event in the run", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCompleted",
      ),
    });
    expect(r.ok).toBe(true);
  });

  test("accepts a CancelRequested event whose origin matches the signing principal kind", async () => {
    const matrix: { origin: string; principal: Principal }[] = [
      { origin: "self", principal: SUPERVISOR_PRINCIPAL },
      { origin: "supervisor-drain", principal: SUPERVISOR_PRINCIPAL },
      { origin: "supervisor-operator", principal: SUPERVISOR_PRINCIPAL },
      { origin: "hub-admin", principal: HUB_PRINCIPAL },
    ];
    for (const { origin, principal } of matrix) {
      const r = await validate(
        {
          [WORKFLOW_RUN_GITIGNORE_PATH]: "",
          [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
            0,
            "RunStarted",
          ),
          [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
            1,
            "CancelRequested",
            { origin, reason: "operator pressed stop" },
          ),
        },
        { principal },
      );
      expect(r.ok).toBe(true);
    }
  });

  test("accepts a per-agent conversation snapshot under agent-state/", async () => {
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_AGENT_STATE_PREFIX}/step-1/conversation.json`]:
          JSON.stringify({ turns: [], connectorState: null }),
      },
      { principal: WORKFLOW_PROCESS_PRINCIPAL },
    );
    expect(r.ok).toBe(true);
  });

  test("accepts the two-tier WAL + checkpoint agent-state layout (Phase D1)", async () => {
    // The durable conversation store no longer writes a single
    // `conversation.json`; it writes a compacted `checkpoint.json` plus
    // bucket-sharded `wal/<bucket>/<seq>.json` delta blobs. The validator
    // enforces only that every `agent-state/<segment>` is a non-empty
    // directory (not a dangling blob) and round-trips URL-encoding -- it
    // says nothing about the files INSIDE, so the nested WAL layout must
    // pass unchanged with no validator loosening.
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_AGENT_STATE_PREFIX}/step-1/checkpoint.json`]:
          JSON.stringify({
            turns: [],
            pendingOperations: [],
            tokenUsage: {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              thinking: 0,
            },
            connectorState: null,
          }),
        [`${WORKFLOW_RUN_AGENT_STATE_PREFIX}/step-1/checkpoint.meta.json`]:
          JSON.stringify({
            checkpointSeq: 0,
            turnCount: 0,
            pendingOperations: [],
            tokenUsage: {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              thinking: 0,
            },
            connectorState: null,
          }),
        [`${WORKFLOW_RUN_AGENT_STATE_PREFIX}/step-1/wal/0/0.json`]:
          JSON.stringify({
            seq: 0,
            turns: [{ role: "user", content: [], timestamp: 0 }],
            metadata: {
              pendingOperations: [],
              tokenUsage: {
                input: 0,
                output: 0,
                cacheRead: 0,
                cacheWrite: 0,
                thinking: 0,
              },
              connectorState: null,
            },
          }),
      },
      { principal: WORKFLOW_PROCESS_PRINCIPAL },
    );
    expect(r.ok).toBe(true);
  });

  test("accepts a mutated agent-state snapshot (subtree is mutable, not append-only)", async () => {
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [`${WORKFLOW_RUN_AGENT_STATE_PREFIX}/step-1/conversation.json`]:
          JSON.stringify({ turns: ["second"], connectorState: null }),
      },
      {
        principal: WORKFLOW_PROCESS_PRINCIPAL,
        priorFiles: {
          [WORKFLOW_RUN_GITIGNORE_PATH]: "",
          [`${WORKFLOW_RUN_AGENT_STATE_PREFIX}/step-1/conversation.json`]:
            JSON.stringify({ turns: ["first"], connectorState: null }),
        },
      },
    );
    expect(r.ok).toBe(true);
  });

  test("rejects an agent-state segment that does not round-trip URL-encoding", async () => {
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [`${WORKFLOW_RUN_AGENT_STATE_PREFIX}/bad%2segment/conversation.json`]:
          "{}",
      },
      { principal: WORKFLOW_PROCESS_PRINCIPAL },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/agent-state segment/);
  });

  test("rejects a blob dangling directly under agent-state/", async () => {
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        // No `<agentKey>/` directory layer: the blob sits directly under
        // the prefix, so it is not keyed by any agent.
        [`${WORKFLOW_RUN_AGENT_STATE_PREFIX}/conversation.json`]: "{}",
      },
      { principal: WORKFLOW_PROCESS_PRINCIPAL },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/blob directly under/);
  });
});

describe("workflowRunKindHandler.validatePush — newly-terminal signal", () => {
  test("reports a run whose terminal event is newly added by this commit", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCompleted",
      ),
    });
    if (!r.ok) throw new Error(`expected ok, got: ${r.reason}`);
    expect(r.newlyTerminalRuns).toEqual([
      { runId: "run-a", terminalEventJson: eventBody(1, "RunCompleted") },
    ]);
  });

  test("reports no terminal run for a commit that adds only non-terminal events", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    });
    if (!r.ok) throw new Error(`expected ok, got: ${r.reason}`);
    expect(r.newlyTerminalRuns ?? []).toEqual([]);
  });

  test("does not re-report a terminal event already present in the prior tree", async () => {
    const files = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCompleted",
      ),
    };
    // The prior tree already carries the terminal event, so a no-op
    // re-validation -- and a later compaction commit that folds the
    // events forward -- must not re-fire the signal.
    const r = await validate(files, { priorFiles: files });
    if (!r.ok) throw new Error(`expected ok, got: ${r.reason}`);
    expect(r.newlyTerminalRuns ?? []).toEqual([]);
  });

  test("reports every run whose terminal event is newly added in one commit", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCompleted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/1.json`]: eventBody(
        1,
        "RunCancelled",
      ),
    });
    if (!r.ok) throw new Error(`expected ok, got: ${r.reason}`);
    expect(r.newlyTerminalRuns).toEqual(
      expect.arrayContaining([
        { runId: "run-a", terminalEventJson: eventBody(1, "RunCompleted") },
        { runId: "run-b", terminalEventJson: eventBody(1, "RunCancelled") },
      ]),
    );
    expect(r.newlyTerminalRuns ?? []).toHaveLength(2);
  });

  test("reports only the run whose terminal event is new when another is carried forward", async () => {
    const carried = {
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCompleted",
      ),
    };
    // run-a's terminal event is already in the prior tree (carried
    // forward unchanged); only run-b's newly added terminal must fire.
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        ...carried,
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/1.json`]: eventBody(
          1,
          "RunCompleted",
        ),
      },
      { priorFiles: { [WORKFLOW_RUN_GITIGNORE_PATH]: "", ...carried } },
    );
    if (!r.ok) throw new Error(`expected ok, got: ${r.reason}`);
    expect(r.newlyTerminalRuns).toEqual([
      { runId: "run-b", terminalEventJson: eventBody(1, "RunCompleted") },
    ]);
  });
});

describe("workflowRunKindHandler.validatePush — compaction (events.jsonl)", () => {
  const RUN = "run-a";
  const eventsDir = `${WORKFLOW_RUN_RUNS_PREFIX}/${RUN}/events`;
  const combinedPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${RUN}/events.jsonl`;
  const e0 = eventBody(0, "RunStarted");
  const e1 = eventBody(1, "RunCompleted");
  const perEventPrior: Record<string, string> = {
    [WORKFLOW_RUN_GITIGNORE_PATH]: "",
    [`${eventsDir}/0.json`]: e0,
    [`${eventsDir}/1.json`]: e1,
  };
  const fold = (...lines: string[]) => lines.join("\n") + "\n";

  test("accepts a faithful byte-for-byte fold of the prior per-event files", async () => {
    const r = await validate(
      { [WORKFLOW_RUN_GITIGNORE_PATH]: "", [combinedPath]: fold(e0, e1) },
      { priorFiles: perEventPrior },
    );
    expect(r.ok).toBe(true);
  });

  test("rejects a fold that mutates a historical event's bytes", async () => {
    const tampered = fold(e0.replace("}", ',"tampered":1}'), e1);
    const r = await validate(
      { [WORKFLOW_RUN_GITIGNORE_PATH]: "", [combinedPath]: tampered },
      { priorFiles: perEventPrior },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/does not fold its prior events verbatim/);
  });

  test("rejects a fold that drops a prior event", async () => {
    const r = await validate(
      { [WORKFLOW_RUN_GITIGNORE_PATH]: "", [combinedPath]: fold(e1) },
      { priorFiles: perEventPrior },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/does not fold its prior events verbatim/);
  });

  test("rejects a fold that adds an event not in the prior tree", async () => {
    const extra = fold(
      e0,
      eventBody(1, "StepStarted"),
      eventBody(2, "RunCompleted"),
    );
    const r = await validate(
      { [WORKFLOW_RUN_GITIGNORE_PATH]: "", [combinedPath]: extra },
      { priorFiles: perEventPrior },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/does not fold its prior events verbatim/);
  });

  test("rejects a run carrying both a combined file and a per-event directory", async () => {
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [combinedPath]: fold(e0, e1),
        [`${eventsDir}/0.json`]: e0,
        [`${eventsDir}/1.json`]: e1,
      },
      { priorFiles: perEventPrior },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/carries both/);
  });

  test("a re-pushed sealed run is immutable", async () => {
    const sealedPrior = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [combinedPath]: fold(e0, e1),
    };
    const unchanged = await validate(
      { [WORKFLOW_RUN_GITIGNORE_PATH]: "", [combinedPath]: fold(e0, e1) },
      { priorFiles: sealedPrior },
    );
    expect(unchanged.ok).toBe(true);
    const mutated = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [combinedPath]: fold(e0.replace("}", ',"x":1}'), e1),
      },
      { priorFiles: sealedPrior },
    );
    expect(mutated.ok).toBe(false);
  });

  test("rejects a freshly-sealed run with no terminal event", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [combinedPath]: fold(
        eventBody(0, "RunStarted"),
        eventBody(1, "StepStarted"),
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/no terminal event/);
  });

  test("the compaction commit reports no newly-terminal run", async () => {
    const r = await validate(
      { [WORKFLOW_RUN_GITIGNORE_PATH]: "", [combinedPath]: fold(e0, e1) },
      { priorFiles: perEventPrior },
    );
    if (!r.ok) throw new Error(`expected ok, got: ${r.reason}`);
    expect(r.newlyTerminalRuns ?? []).toEqual([]);
  });

  test("rejects a fold whose bytes differ from the prior blobs even if it decodes to the same content", async () => {
    // The fold gate is BYTE equality, not decoded-string equality: each
    // event is signed over its own bytes, so a sealed file that merely
    // decodes to the prior content (here, a UTF-8 BOM prepended to the true
    // fold) must be rejected. The string-based `validate` harness cannot
    // express this, so drive the handler with raw bytes.
    const enc = new TextEncoder();
    const prospectiveKeys: Record<string, string> = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [combinedPath]: "",
    };
    const priorKeys: Record<string, string> = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${eventsDir}/0.json`]: "",
      [`${eventsDir}/1.json`]: "",
    };
    const priorBytes: Record<string, Uint8Array> = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: enc.encode(""),
      [`${eventsDir}/0.json`]: enc.encode(e0),
      [`${eventsDir}/1.json`]: enc.encode(e1),
    };
    const trueFold = enc.encode(`${e0}\n${e1}\n`);
    const bomFold = new Uint8Array([0xef, 0xbb, 0xbf, ...trueFold]);
    const prospectiveBytes: Record<string, Uint8Array> = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: enc.encode(""),
      [combinedPath]: bomFold,
    };
    const r = await workflowRunKindHandler.validatePush({
      repoId: uniqueRepoId("wfr"),
      ref: REF,
      principal: HUB_PRINCIPAL,
      topLevelTreePaths: topLevels(prospectiveKeys),
      readBlob: async (p) => {
        const b = prospectiveBytes[p];
        if (b === undefined) throw new Error(`readBlob: ${p} not found`);
        return b;
      },
      listDir: makeListDir(prospectiveKeys),
      priorReadBlob: async (p) => priorBytes[p] ?? null,
      priorListDir: makeListDir(priorKeys),
      changedPathPrefixes: undefined,
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/does not fold its prior events verbatim/);
  });
});

describe("workflowRunKindHandler.validatePush — rejects top-level shape", () => {
  test("rejects any path under control/ (unsupported subtree)", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "control/policy.json": "{}",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/unsupported .*control\//);
  });

  test("rejects an arbitrary disallowed top-level entry and names agent-state in the allowed list", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "stray.txt": "nope",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/unexpected top-level entry/);
    // The allowed-list in the rejection message must enumerate every
    // accepted top-level, including agent-state/ (Phase 4.5).
    expect(r.reason).toContain(WORKFLOW_RUN_AGENT_STATE_PREFIX);
  });
});

describe("workflowRunKindHandler.validatePush — rejects event shape", () => {
  test("rejects when a run directory has no events subdirectory", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/notes.txt`]: "stray",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/contains unexpected entry/);
  });

  test("rejects an event file whose name is not <seq>.json", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/oops.json`]: eventBody(
        0,
        "RunStarted",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/does not match <seq>\.json/);
  });

  test("rejects an event whose body is not valid JSON", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: "{not-json",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/is not valid JSON/);
  });

  test("rejects an event missing the required seq and type envelope", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: JSON.stringify({
        data: "no envelope",
      }),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/envelope invalid/);
  });

  test("rejects an event whose body.seq does not match its filename seq", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        7,
        "RunStarted",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/body\.seq .* does not match filename seq/);
  });
});

describe("workflowRunKindHandler.validatePush — terminal-phase lock", () => {
  test("rejects an event whose seq is strictly greater than a prior terminal event's", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCompleted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/2.json`]: eventBody(
        2,
        "StepStarted",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/after terminal RunCompleted/);
  });

  test("rejects events after a RunFailed terminal event", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunFailed",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/2.json`]: eventBody(
        2,
        "StepStarted",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/after terminal RunFailed/);
  });

  test("rejects events after a RunCancelled terminal event", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCancelled",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/2.json`]: eventBody(
        2,
        "StepStarted",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/after terminal RunCancelled/);
  });

  test("treats terminal lock per-run: another run is unaffected", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "RunCompleted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/1.json`]: eventBody(
        1,
        "StepStarted",
      ),
    });
    expect(r.ok).toBe(true);
  });
});

describe("workflowRunKindHandler.validatePush — CancelRequested origin", () => {
  test("rejects a CancelRequested whose origin is not a CancelOrigin", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "CancelRequested",
        { origin: "rogue-actor", reason: "spoof" },
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/origin .* is not a recognised CancelOrigin/);
  });

  test("rejects a CancelRequested missing the origin field", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "CancelRequested",
        { reason: "no origin field" },
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/CancelRequested payload invalid/);
  });

  test("rejects a CancelRequested with an empty reason", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "CancelRequested",
        { origin: "self", reason: "" },
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/CancelRequested payload invalid/);
  });
});

describe("workflowRunKindHandler.validatePush — CancelRequested principal-vs-origin", () => {
  // Only a `hub` principal may mint a `hub-admin` origin; only a
  // `supervisor` principal may mint `self`, `supervisor-drain`, or
  // `supervisor-operator`. The handler enforces the principal-vs-
  // origin pairing at `validatePush`; these cases pin the boundary.

  function cancelTree(origin: string): Record<string, string> {
    return {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "CancelRequested",
        { origin, reason: "cancel" },
      ),
    };
  }

  test("rejects CancelRequested{origin:hub-admin} signed by a workflow-process principal", async () => {
    const r = await validate(cancelTree("hub-admin"), {
      principal: WORKFLOW_PROCESS_PRINCIPAL,
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /CancelRequested origin "hub-admin" requires principal\.kind="hub"/,
    );
    expect(r.reason).toMatch(/principal\.kind="workflow-process"/);
  });

  test("rejects CancelRequested{origin:hub-admin} signed by a supervisor principal", async () => {
    const r = await validate(cancelTree("hub-admin"), {
      principal: SUPERVISOR_PRINCIPAL,
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/requires principal\.kind="hub"/);
    expect(r.reason).toMatch(/principal\.kind="supervisor"/);
  });

  test("rejects CancelRequested{origin:self} signed by a hub principal", async () => {
    const r = await validate(cancelTree("self"), { principal: HUB_PRINCIPAL });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/requires principal\.kind="supervisor"/);
    expect(r.reason).toMatch(/principal\.kind="hub"/);
  });

  test("rejects CancelRequested{origin:supervisor-drain} signed by a hub principal", async () => {
    const r = await validate(cancelTree("supervisor-drain"), {
      principal: HUB_PRINCIPAL,
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/requires principal\.kind="supervisor"/);
  });

  test("rejects CancelRequested{origin:supervisor-operator} signed by a workflow-process principal", async () => {
    const r = await validate(cancelTree("supervisor-operator"), {
      principal: WORKFLOW_PROCESS_PRINCIPAL,
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/requires principal\.kind="supervisor"/);
  });

  test("accepts CancelRequested{origin:hub-admin} signed by a hub principal", async () => {
    const r = await validate(cancelTree("hub-admin"), {
      principal: HUB_PRINCIPAL,
    });
    expect(r.ok).toBe(true);
  });

  test("accepts CancelRequested{origin:self} signed by a supervisor principal", async () => {
    const r = await validate(cancelTree("self"), {
      principal: SUPERVISOR_PRINCIPAL,
    });
    expect(r.ok).toBe(true);
  });
});

describe("workflowRunKindHandler.validatePush — append-only via prior-tree", () => {
  // The handler reads the parent commit's tree via `priorReadBlob`
  // and rejects any event path whose prospective bytes diverge from
  // the prior bytes. These cases pin that append-only boundary at
  // `validatePush`.

  test("rejects a prospective tree that mutates the bytes of an event present in the prior tree", async () => {
    const prior = {
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: eventBody(
        0,
        "RunStarted",
        { original: true },
      ),
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: eventBody(
        0,
        "RunStarted",
        { original: false },
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/1.json`]: eventBody(
        1,
        "StepStarted",
      ),
    };
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /event runs\/run-x\/events\/0\.json bytes diverge from the prior tree/,
    );
  });

  test("accepts a prospective tree that appends a new event while preserving prior bytes", async () => {
    const seq0 = eventBody(0, "RunStarted");
    const prior = {
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: seq0,
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: seq0,
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/1.json`]: eventBody(
        1,
        "StepStarted",
      ),
    };
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(true);
  });

  test("accepts the first push (no prior tree) as inherently append-only", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    });
    expect(r.ok).toBe(true);
  });

  test("rejects a prospective tree that truncates an existing event blob", async () => {
    const prior = {
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: eventBody(
        0,
        "RunStarted",
        { padding: "x".repeat(64) },
      ),
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-x/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    };
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/bytes diverge from the prior tree/);
  });
});

describe("workflowRunKindHandler.validatePush — blobs subtree", () => {
  // The production `BlobSubstrate` adapter spills any output whose
  // JSON-stringified form exceeds 1 MiB to
  // `runs/<runId>/blobs/<sha256-hex>`. The key is a lowercase
  // 64-character sha256 hex string. The blob value is opaque bytes;
  // immutability is enforced by prior-tree byte-equality (mirroring
  // the consumed-entry discipline in the claim-check subtree).
  //
  // The regression fixture below mirrors what the BlobSubstrate adapter
  // commits when `recordOutput` is called with a value whose
  // JSON-stringified length exceeds the inline threshold: a blob keyed
  // by the sha256 of the payload bytes, sized comfortably above 1 MiB.

  const BLOB_KEY_A =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  const BLOB_KEY_B =
    "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
  const LARGE_BLOB_BYTES = "x".repeat(1_500_000);

  function blobsTree(
    runId: string,
    blobs: Record<string, string>,
  ): Record<string, string> {
    const tree: Record<string, string> = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    };
    for (const [key, body] of Object.entries(blobs)) {
      tree[
        `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/${WORKFLOW_RUN_BLOBS_DIR}/${key}`
      ] = body;
    }
    return tree;
  }

  test("regression: accepts a 1.5 MiB blob committed under runs/<runId>/blobs/<sha256>", async () => {
    const tree = blobsTree("run-spill", { [BLOB_KEY_A]: LARGE_BLOB_BYTES });
    const r = await validate(tree);
    expect(r.ok).toBe(true);
  });

  test("accepts a new blob whose key is a valid sha256 hex string", async () => {
    const tree = blobsTree("run-a", { [BLOB_KEY_A]: "payload-bytes" });
    const r = await validate(tree);
    expect(r.ok).toBe(true);
  });

  test("rejects a blob whose key is not a 64-char lowercase sha256 hex string", async () => {
    const tree = blobsTree("run-a", { "not-a-hash.bin": "payload-bytes" });
    const r = await validate(tree);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/blob filename .* does not match/);
  });

  test("rejects a blob whose key has uppercase hex (non-canonical sha256)", async () => {
    const upper = BLOB_KEY_A.toUpperCase();
    const tree = blobsTree("run-a", { [upper]: "payload-bytes" });
    const r = await validate(tree);
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/blob filename .* does not match/);
  });

  test("rejects a mutated blob whose prior-tree bytes differ", async () => {
    const prior = blobsTree("run-a", { [BLOB_KEY_A]: "original-bytes" });
    const prospective = blobsTree("run-a", { [BLOB_KEY_A]: "mutated-bytes!" });
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /blob runs\/run-a\/blobs\/.* bytes diverge from the prior tree/,
    );
  });

  test("accepts an idempotent re-write of an existing blob with identical bytes", async () => {
    const bytes = "stable-bytes";
    const prior = blobsTree("run-a", { [BLOB_KEY_A]: bytes });
    const prospective = blobsTree("run-a", { [BLOB_KEY_A]: bytes });
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(true);
  });

  test("accepts appending a new blob alongside an existing immutable blob", async () => {
    const prior = blobsTree("run-a", { [BLOB_KEY_A]: "first-bytes" });
    const prospective = blobsTree("run-a", {
      [BLOB_KEY_A]: "first-bytes",
      [BLOB_KEY_B]: "second-bytes",
    });
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(true);
  });
});

describe("workflowRunAuthorize — repoId guard", () => {
  test("rejects calls when repoId.kind is not workflow-run", () => {
    const r = workflowRunAuthorize(
      { kind: "hub" } as Principal,
      { kind: "agent-state", id: "dep-1" } as RepoId,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/non-workflow-run repo/);
  });
});

describe("workflowRunAuthorize — hub principal", () => {
  test("allowed for every action", () => {
    const repo: RepoId = { kind: "workflow-run", id: "dep-1" };
    for (const action of [
      "init",
      "writeTree",
      "receivePack",
      "createPack",
      "resolveRef",
    ] as const) {
      const r = workflowRunAuthorize(
        { kind: "hub" } as Principal,
        repo,
        REF,
        action,
      );
      expect(r.allowed).toBe(true);
    }
  });
});

describe("workflowRunAuthorize — workflow-process principal", () => {
  const REPO: RepoId = { kind: "workflow-run", id: "dep-1" };

  test("allowed for full read+write on its own deployment", () => {
    const principal = {
      kind: "workflow-process",
      deploymentId: "dep-1",
      runId: "run-a",
    } as Principal;
    for (const action of [
      "init",
      "writeTree",
      "receivePack",
      "createPack",
      "resolveRef",
    ] as const) {
      const r = workflowRunAuthorize(principal, REPO, REF, action);
      expect(r.allowed).toBe(true);
    }
  });

  test("allowed without runId field (the per-call runId is optional)", () => {
    const principal = {
      kind: "workflow-process",
      deploymentId: "dep-1",
    } as Principal;
    const r = workflowRunAuthorize(principal, REPO, REF, "writeTree");
    expect(r.allowed).toBe(true);
  });

  test("denied when targeting another deployment's repo", () => {
    const principal = {
      kind: "workflow-process",
      deploymentId: "dep-other",
    } as Principal;
    const r = workflowRunAuthorize(principal, REPO, REF, "writeTree");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/cannot access workflow-run/);
  });

  test("malformed workflow-process principal is denied", () => {
    const principal = { kind: "workflow-process" } as Principal;
    const r = workflowRunAuthorize(principal, REPO, REF, "writeTree");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/workflow-process principal is malformed/);
  });
});

describe("workflowRunKindHandler.validatePush — workflow-process path-scope fail-closed", () => {
  // The production substrate is wired against `workflowRunAuthorize`,
  // which rejects malformed workflow-process principals at
  // `gateAccess` BEFORE `validatePush` runs. A substrate wired with a
  // permissive authorize (e.g. test harnesses using `allowAll`) can
  // let a malformed principal reach `validatePush`; the path-scope
  // helper must fail closed there instead of silently waving the
  // principal through, since the runId scoping below depends on the
  // parsed principal carrying a valid `deploymentId`.
  test("a malformed workflow-process principal reaching validatePush rejects with a structured reason", async () => {
    const principal: Principal = { kind: "workflow-process" };
    const events = {
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
        { consumedMessageId: "msg-1" },
      ),
    };
    const result = await workflowRunKindHandler.validatePush({
      repoId: uniqueRepoId("wfr"),
      ref: REF,
      principal,
      topLevelTreePaths: topLevels(events),
      readBlob: makeReadBlob(events),
      listDir: makeListDir(events),
      priorReadBlob: noPriorBlob,
      priorListDir: noPriorDir,
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/workflow-process principal is malformed/);
  });
});

describe("workflowRunAuthorize — supervisor principal", () => {
  const REPO: RepoId = { kind: "workflow-run", id: "dep-1" };

  test("allowed for full read+write on its own deployment", () => {
    const principal = {
      kind: "supervisor",
      deploymentId: "dep-1",
    } as Principal;
    for (const action of [
      "init",
      "writeTree",
      "receivePack",
      "createPack",
      "resolveRef",
    ] as const) {
      const r = workflowRunAuthorize(principal, REPO, REF, action);
      expect(r.allowed).toBe(true);
    }
  });

  test("denied when targeting another deployment's repo", () => {
    const principal = {
      kind: "supervisor",
      deploymentId: "dep-other",
    } as Principal;
    const r = workflowRunAuthorize(principal, REPO, REF, "writeTree");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/cannot access workflow-run/);
  });

  test("malformed supervisor principal is denied", () => {
    const principal = { kind: "supervisor" } as Principal;
    const r = workflowRunAuthorize(principal, REPO, REF, "writeTree");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/supervisor principal is malformed/);
  });
});

describe("workflowRunAuthorize — sidecar principal", () => {
  const REPO: RepoId = { kind: "workflow-run", id: "dep-1" };

  test("createPack / resolveRef allowed", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    expect(workflowRunAuthorize(sidecar, REPO, REF, "createPack").allowed).toBe(
      true,
    );
    expect(workflowRunAuthorize(sidecar, REPO, REF, "resolveRef").allowed).toBe(
      true,
    );
  });

  test("writeTree / receivePack / init denied", () => {
    const sidecar = { kind: "sidecar", agentId: "agent-1" } as Principal;
    for (const action of ["init", "writeTree", "receivePack"] as const) {
      const r = workflowRunAuthorize(sidecar, REPO, REF, action);
      expect(r.allowed).toBe(false);
      if (r.allowed) throw new Error("unreachable");
      expect(r.reason).toMatch(/sidecars may only read workflow-run/);
    }
  });

  test("malformed sidecar principal is denied", () => {
    const malformed = { kind: "sidecar" } as Principal;
    const r = workflowRunAuthorize(malformed, REPO, REF, "createPack");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/sidecar principal is malformed/);
  });
});

describe("workflowRunAuthorize — user principal", () => {
  const REPO: RepoId = { kind: "workflow-run", id: "dep-1" };

  function farFuture(): number {
    return Date.now() + 60_000;
  }

  function userPrincipal(
    overrides: {
      effect?: "allow" | "deny";
      resource?: string;
      grantVerb?: string;
      refPattern?: string;
      actions?: string[];
      expiresAt?: number;
    } = {},
  ): Principal {
    return {
      kind: "user",
      principalId: "user-1",
      tenantId: "tenant-1",
      authz: {
        effect: overrides.effect ?? "allow",
        resource: overrides.resource ?? "workflow-run:dep-1",
        grantVerb: overrides.grantVerb ?? "read",
      },
      tokenClaims: {
        refPattern: overrides.refPattern ?? "refs/heads/**",
        actions: overrides.actions ?? ["createPack", "resolveRef"],
        expiresAt: overrides.expiresAt ?? farFuture(),
      },
    } as Principal;
  }

  test("allowed when claims and verdict agree", () => {
    const r = workflowRunAuthorize(userPrincipal(), REPO, REF, "createPack");
    expect(r.allowed).toBe(true);
  });

  test("bulk read uses '*' ref and bypasses refPattern check", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ refPattern: "refs/heads/release-*" }),
      REPO,
      "*",
      "resolveRef",
    );
    expect(r.allowed).toBe(true);
  });

  test("malformed user principal is denied", () => {
    const badPrincipal = { kind: "user", principalId: "u" } as Principal;
    const r = workflowRunAuthorize(badPrincipal, REPO, REF, "createPack");
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/user principal is malformed/);
  });

  test("denied when tokenClaims.actions does not include the requested action", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ actions: ["resolveRef"] }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/token does not grant action createPack/);
  });

  test("denied when refPattern does not match the requested ref", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ refPattern: "refs/heads/release-*" }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/refPattern .* does not match/);
  });

  test("denied when the token is expired", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ expiresAt: Date.now() - 1 }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/token expired/);
  });

  test("denied when verdict.resource targets a different workflow-run id", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ resource: "workflow-run:dep-other" }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict resource .* does not match/);
  });

  test("denied when verdict.resource has the wrong kind prefix", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ resource: "workflow:dep-1" }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict resource .* does not match/);
  });

  test("denied when verdict.grantVerb does not match the action's verb", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ grantVerb: "write" }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict grantVerb .* does not match/);
  });

  test("denied when verdict effect is deny even though all sanity checks pass", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ effect: "deny" }),
      REPO,
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/authz verdict denied/);
  });

  test("write action requires write grantVerb and matching claims", () => {
    const r = workflowRunAuthorize(
      userPrincipal({ actions: ["receivePack"], grantVerb: "write" }),
      REPO,
      REF,
      "receivePack",
    );
    expect(r.allowed).toBe(true);
  });
});

describe("workflowRunAuthorize — unknown principal", () => {
  test("denied with a generic reason", () => {
    const r = workflowRunAuthorize(
      { kind: "robot" } as Principal,
      { kind: "workflow-run", id: "dep-1" },
      REF,
      "createPack",
    );
    expect(r.allowed).toBe(false);
    if (r.allowed) throw new Error("unreachable");
    expect(r.reason).toMatch(/unknown principal kind/);
  });
});

// ---------------------------------------------------------------------
// Claim-check substrate tests.

const ADDRESS = "alice@example.com";
const ADDRESS_SEG = encodeURIComponent(ADDRESS);

function inboxPathFor(seg: string, receivedAt: number, messageId: string) {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${seg}/${WORKFLOW_RUN_INBOX_DIR}/${String(receivedAt)}-${messageId}.json`;
}

function processingPathFor(seg: string, receivedAt: number, messageId: string) {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${seg}/${WORKFLOW_RUN_PROCESSING_DIR}/${String(receivedAt)}-${messageId}.json`;
}

function consumedPathFor(seg: string, messageId: string) {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${seg}/${WORKFLOW_RUN_CONSUMED_DIR}/${messageId}.json`;
}

function watermarkPathFor(seg: string) {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${seg}/${WORKFLOW_RUN_WATERMARK_FILE}`;
}

function watermarkBody(watermark: number): string {
  return JSON.stringify({ watermark });
}

function inboxBody(
  messageId: string,
  receivedAt: number,
  address = ADDRESS,
): string {
  return JSON.stringify({
    messageId,
    receivedAt,
    address,
    mailAuditRef: { store: "audit", path: `mail/${messageId}` },
  });
}

function consumedBody(
  messageId: string,
  receivedAt: number,
  runId: string,
  consumedAt: number,
  address = ADDRESS,
): string {
  return JSON.stringify({
    messageId,
    receivedAt,
    address,
    runId,
    consumedAt,
    mailAuditRef: { store: "audit", path: `mail/${messageId}` },
  });
}

describe("workflowRunKindHandler.validatePush — claim-check subtree shape", () => {
  test("accepts a single inbox entry with a well-formed envelope", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [inboxPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 100),
    });
    expect(r.ok).toBe(true);
  });

  test("accepts inbox + a future processing entry only when prior tree carried the inbox", async () => {
    const inboxEntry = inboxBody("msg-1", 100);
    const prior = {
      [inboxPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxEntry,
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [processingPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxEntry,
    };
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(true);
  });

  test("rejects an address segment that does not round-trip URL-encoding", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_ADDRESSES_PREFIX}/raw@addr/${WORKFLOW_RUN_INBOX_DIR}/100-msg-1.json`]:
        inboxBody("msg-1", 100, "raw@addr"),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/does not round-trip URL-encoding/);
  });

  test("rejects an unexpected subdirectory under an address", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_ADDRESSES_PREFIX}/${ADDRESS_SEG}/stray/x.json`]: "{}",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/contains unexpected entry "stray"/);
  });

  test("rejects an inbox filename that does not match <receivedAt>-<messageId>.json", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_ADDRESSES_PREFIX}/${ADDRESS_SEG}/${WORKFLOW_RUN_INBOX_DIR}/no-receivedat.json`]:
        inboxBody("msg-1", 100),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /inbox filename .* does not match <receivedAt>-<messageId>\.json/,
    );
  });

  test("rejects an inbox body whose receivedAt does not match its filename", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [inboxPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 999),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/body\.receivedAt .* does not match filename/);
  });

  test("rejects an inbox body whose messageId does not match its filename", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [inboxPathFor(ADDRESS_SEG, 100, "msg-A")]: inboxBody("msg-B", 100),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/body\.messageId .* does not match filename/);
  });

  test("rejects an inbox body whose address does not match the decoded segment", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [inboxPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody(
        "msg-1",
        100,
        "different@example.com",
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/body\.address .* does not match decoded address/);
  });

  test("rejects a consumed filename that is not <messageId>.json shape", async () => {
    // Lay out a valid processing entry in the prior tree so we can
    // exercise the consumed-only filename check rather than the
    // transition check.
    const prior = {
      [processingPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 100),
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_ADDRESSES_PREFIX}/${ADDRESS_SEG}/${WORKFLOW_RUN_CONSUMED_DIR}/no.dot.json.bogus`]:
        consumedBody("msg-1", 100, "run-1", 200),
    };
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    // The malformed filename either trips the filename regex (no `.json`
    // suffix) or trips the messageId-mismatch — either is a structural
    // rejection at the consumed boundary.
    expect(r.reason).toMatch(/consumed filename|consumed .* does not match/);
  });

  test("rejects a tree where the same messageId appears in inbox and processing", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [inboxPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 100),
      [processingPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 100),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/appears in multiple queue states/);
  });

  test("rejects a tree where the same messageId appears in inbox and consumed", async () => {
    // Need a prior processing entry so the consumed entry passes the
    // transition check long enough to fail the atomicity check.
    const prior = {
      [processingPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 100),
    };
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [inboxPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 100),
        [consumedPathFor(ADDRESS_SEG, "msg-1")]: consumedBody(
          "msg-1",
          100,
          "run-1",
          200,
        ),
      },
      { priorFiles: prior },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/appears in multiple queue states/);
  });

  test("rejects a processing entry that has no matching prior-tree inbox entry", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [processingPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 100),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /processing .* prior tree has no matching inbox entry/,
    );
  });

  test("rejects a consumed entry that has no matching prior-tree processing entry", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [consumedPathFor(ADDRESS_SEG, "msg-1")]: consumedBody(
        "msg-1",
        100,
        "run-1",
        200,
      ),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /consumed .* prior tree has no matching processing entry/,
    );
  });

  test("rejects a consumed envelope whose receivedAt diverges from the prior processing entry", async () => {
    const prior = {
      [processingPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 100),
    };
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [consumedPathFor(ADDRESS_SEG, "msg-1")]: consumedBody(
          "msg-1",
          999,
          "run-1",
          200,
        ),
      },
      { priorFiles: prior },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /body\.receivedAt .* does not match the prior processing entry's receivedAt/,
    );
  });

  test("rejects a mutation to a consumed entry that already exists in the prior tree", async () => {
    const prior = {
      [consumedPathFor(ADDRESS_SEG, "msg-1")]: consumedBody(
        "msg-1",
        100,
        "run-1",
        200,
      ),
    };
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [consumedPathFor(ADDRESS_SEG, "msg-1")]: consumedBody(
          "msg-1",
          100,
          "run-2",
          200,
        ),
      },
      { priorFiles: prior },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/consumed .* bytes diverge from the prior tree/);
  });

  test("accepts a clean inbox→processing transition (atomic move via prospective tree)", async () => {
    const inboxEntry = inboxBody("msg-1", 100);
    const prior = {
      [inboxPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxEntry,
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [processingPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxEntry,
    };
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(true);
  });

  test("accepts a clean processing→consumed transition with matching receivedAt", async () => {
    const prior = {
      [processingPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 100),
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [consumedPathFor(ADDRESS_SEG, "msg-1")]: consumedBody(
        "msg-1",
        100,
        "run-1",
        200,
      ),
    };
    const r = await validate(prospective, { priorFiles: prior });
    expect(r.ok).toBe(true);
  });
});

// ---------------------------------------------------------------------
// End-to-end claim-check API tests against a real on-disk RepoStore.

const claimCheckTempDirs: string[] = [];

async function makeClaimCheckTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  claimCheckTempDirs.push(d);
  return d;
}

let claimCheckSigningKey: KeyPair;

beforeAll(async () => {
  claimCheckSigningKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of claimCheckTempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch((_e) => {
      /* best effort cleanup */
    });
  }
});

async function makeClaimCheckStore(prefix: string): Promise<{
  store: ReturnType<typeof createRepoStore>;
  repoId: RepoId;
  principal: Principal;
}> {
  const dataDir = await makeClaimCheckTempDir(prefix);
  const store = createRepoStore({
    dataDir,
    signingKey: claimCheckSigningKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: () => ({ allowed: true }),
  });
  const repoId: RepoId = {
    kind: "workflow-run",
    id: `dep-${Math.random().toString(36).slice(2, 10)}`,
  };
  await store.initRepo(repoId);
  return { store, repoId, principal: HUB_PRINCIPAL };
}

describe("claim-check API — enqueueInbox", () => {
  test("writes a single inbox entry with the expected filename and envelope", async () => {
    const { store, repoId, principal } = await makeClaimCheckStore("cc-enq-");
    const result = await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-1",
      receivedAt: 100,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    expect(result.inboxKey).toBe("100-msg-1");
    expect(result.envelope.messageId).toBe("msg-1");

    const repoDir = store.getRepoDir(repoId);
    const blob = await fs.promises.readFile(
      path.join(repoDir, inboxPathFor(ADDRESS_SEG, 100, "msg-1")),
      "utf-8",
    );
    const parsed: unknown = JSON.parse(blob);
    if (
      parsed === null ||
      typeof parsed !== "object" ||
      !("messageId" in parsed) ||
      !("address" in parsed) ||
      !("receivedAt" in parsed)
    ) {
      throw new Error("unexpected inbox envelope shape");
    }
    expect(parsed.messageId).toBe("msg-1");
    expect(parsed.address).toBe(ADDRESS);
    expect(parsed.receivedAt).toBe(100);
  });

  test("two enqueueInbox calls coexist in the inbox subtree", async () => {
    const { store, repoId, principal } = await makeClaimCheckStore("cc-enq2-");
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-1",
      receivedAt: 100,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-2",
      receivedAt: 200,
      mailAuditRef: { store: "audit", path: "mail/msg-2" },
    });
    const repoDir = store.getRepoDir(repoId);
    const inboxDir = path.join(
      repoDir,
      `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${ADDRESS_SEG}/${WORKFLOW_RUN_INBOX_DIR}`,
    );
    const entries = await fs.promises.readdir(inboxDir);
    expect(entries.sort()).toEqual(["100-msg-1.json", "200-msg-2.json"]);
  });

  test("rejects an enqueue against a messageId already in processing", async () => {
    const { store, repoId, principal } =
      await makeClaimCheckStore("cc-enq-dup-");
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-1",
      receivedAt: 100,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    await dequeueToProcessing(store, principal, repoId, ADDRESS);
    await expect(
      enqueueInbox(store, principal, repoId, {
        address: ADDRESS,
        messageId: "msg-1",
        receivedAt: 300,
        mailAuditRef: { store: "audit", path: "mail/msg-1" },
      }),
    ).rejects.toThrow(/claim_check_already_processing/);
  });
});

describe("claim-check API — markConsumed", () => {
  test("atomic move from processing to consumed preserves originating receivedAt", async () => {
    const { store, repoId, principal } = await makeClaimCheckStore("cc-mark-");
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-1",
      receivedAt: 100,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    await dequeueToProcessing(store, principal, repoId, ADDRESS);
    const result = await markConsumed(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-1",
      runId: "run-X",
      consumedAt: 500,
    });
    expect(result.envelope.runId).toBe("run-X");
    expect(result.envelope.receivedAt).toBe(100);

    const repoDir = store.getRepoDir(repoId);
    const processingDir = path.join(
      repoDir,
      `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${ADDRESS_SEG}/${WORKFLOW_RUN_PROCESSING_DIR}`,
    );
    const remaining: string[] = await fs.promises
      .readdir(processingDir)
      .catch((): string[] => []);
    expect(remaining).toEqual([]);

    const consumedPath = path.join(
      repoDir,
      consumedPathFor(ADDRESS_SEG, "msg-1"),
    );
    await fs.promises.access(consumedPath);
  });

  test("rejects a consume without a matching processing entry", async () => {
    const { store, repoId, principal } =
      await makeClaimCheckStore("cc-mark-bad-");
    await expect(
      markConsumed(store, principal, repoId, {
        address: ADDRESS,
        messageId: "absent",
        runId: "run-X",
        consumedAt: 500,
      }),
    ).rejects.toThrow(/claim_check_processing_not_found/);
  });
});

// ---------------------------------------------------------------------
// Substrate-level FIFO unit test — validation criterion 4 (substrate
// half). Two messages enqueued in order, dequeued twice, then a
// mid-FIFO "crash" leaves a processing entry behind;
// `replayProcessingToInbox` must restore it under its original key so
// the next dequeue picks the same entry that the crashed worker had
// claimed.

describe("claim-check substrate FIFO invariant", () => {
  test("dequeues two messages in receivedAt order and re-dequeues after crash replay with the original key", async () => {
    const { store, repoId, principal } = await makeClaimCheckStore("cc-fifo-");

    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-1",
      receivedAt: 100,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-2",
      receivedAt: 200,
      mailAuditRef: { store: "audit", path: "mail/msg-2" },
    });

    const first = await dequeueToProcessing(store, principal, repoId, ADDRESS);
    expect(first).not.toBeNull();
    if (first === null) throw new Error("unreachable");
    expect(first.envelope.messageId).toBe("msg-1");
    expect(first.key).toBe("100-msg-1");

    await markConsumed(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-1",
      runId: "run-1",
      consumedAt: 150,
    });

    const second = await dequeueToProcessing(store, principal, repoId, ADDRESS);
    expect(second).not.toBeNull();
    if (second === null) throw new Error("unreachable");
    expect(second.envelope.messageId).toBe("msg-2");
    expect(second.key).toBe("200-msg-2");

    // Mid-FIFO crash: msg-2 stays in processing, no consumed entry
    // landed. The worker process is gone. The recovery path moves
    // processing entries back to inbox preserving the filename key.
    const replay = await replayProcessingToInbox(
      store,
      principal,
      repoId,
      ADDRESS,
    );
    expect(replay.replayedKeys).toEqual(["200-msg-2"]);

    const reDequeue = await dequeueToProcessing(
      store,
      principal,
      repoId,
      ADDRESS,
    );
    expect(reDequeue).not.toBeNull();
    if (reDequeue === null) throw new Error("unreachable");
    expect(reDequeue.envelope.messageId).toBe("msg-2");
    expect(reDequeue.key).toBe("200-msg-2");

    await markConsumed(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-2",
      runId: "run-2",
      consumedAt: 300,
    });

    const finalDequeue = await dequeueToProcessing(
      store,
      principal,
      repoId,
      ADDRESS,
    );
    expect(finalDequeue).toBeNull();
  });

  test("replayProcessingToInbox is a no-op when processing is empty", async () => {
    const { store, repoId, principal } =
      await makeClaimCheckStore("cc-replay-noop-");
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-1",
      receivedAt: 100,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    const replay = await replayProcessingToInbox(
      store,
      principal,
      repoId,
      ADDRESS,
    );
    expect(replay.replayedKeys).toEqual([]);
    // The inbox entry is untouched.
    const next = await dequeueToProcessing(store, principal, repoId, ADDRESS);
    expect(next).not.toBeNull();
    if (next === null) throw new Error("unreachable");
    expect(next.envelope.messageId).toBe("msg-1");
  });

  // Regression: lexicographic-sort FIFO bug. Before the fix,
  // dequeueToProcessing sorted inbox filenames as strings, so a
  // later-received message with a longer receivedAt prefix dequeued
  // ahead of an earlier-received message with a shorter prefix
  // (e.g. "100-msg-B" < "99-msg-A" because '1' < '9'). After the
  // fix the substrate sorts by parsed numeric receivedAt and the
  // earlier message wins.
  test("non-uniform receivedAt widths still respect FIFO (msg-A at 99 dequeues before msg-B at 100)", async () => {
    const { store, repoId, principal } =
      await makeClaimCheckStore("cc-fifo-width-");
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-A",
      receivedAt: 99,
      mailAuditRef: { store: "audit", path: "mail/A" },
    });
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-B",
      receivedAt: 100,
      mailAuditRef: { store: "audit", path: "mail/B" },
    });
    const first = await dequeueToProcessing(store, principal, repoId, ADDRESS);
    expect(first).not.toBeNull();
    if (first === null) throw new Error("unreachable");
    expect(first.envelope.messageId).toBe("msg-A");
    expect(first.key).toBe("99-msg-A");
    const second = await dequeueToProcessing(store, principal, repoId, ADDRESS);
    expect(second).not.toBeNull();
    if (second === null) throw new Error("unreachable");
    expect(second.envelope.messageId).toBe("msg-B");
    expect(second.key).toBe("100-msg-B");
  });
});

// ---------------------------------------------------------------------
// Regression: per-messageId atomicity gap. Before the fix,
// enqueueInbox only rejected a same-receivedAt collision and a
// processing/consumed scan for the messageId — it did NOT scan the
// inbox prefix for a same-messageId-at-different-receivedAt match.
// The validatePush atomicity Set was keyed by kind, so two inbox
// entries with the same messageId and different receivedAt produced
// a single-element {"inbox"} Set and did not trip the check.
// After the fix, the second enqueue throws claim_check_already_inbox
// and the inbox directory holds exactly one entry.

describe("claim-check API — enqueueInbox per-messageId atomicity in inbox", () => {
  test("rejects a second enqueue for the same messageId at a different receivedAt", async () => {
    const { store, repoId, principal } =
      await makeClaimCheckStore("cc-enq-dup-inbox-");
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-X",
      receivedAt: 100,
      mailAuditRef: { store: "audit", path: "mail/X" },
    });
    await expect(
      enqueueInbox(store, principal, repoId, {
        address: ADDRESS,
        messageId: "msg-X",
        receivedAt: 200,
        mailAuditRef: { store: "audit", path: "mail/X" },
      }),
    ).rejects.toThrow(/claim_check_already_inbox/);
    const repoDir = store.getRepoDir(repoId);
    const inboxDir = path.join(
      repoDir,
      `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${ADDRESS_SEG}/${WORKFLOW_RUN_INBOX_DIR}`,
    );
    const entries = await fs.promises.readdir(inboxDir);
    expect(entries.sort()).toEqual(["100-msg-X.json"]);
  });
});

// Regression at the validatePush layer for the same intra-state
// atomicity gap. A prospective tree carrying two inbox entries for
// the same messageId at distinct receivedAt values is structurally
// invalid; the rejection lands on the same code path that catches
// inbox+processing collisions.
describe("workflowRunKindHandler.validatePush — claim-check intra-state atomicity", () => {
  test("rejects a tree with two inbox entries sharing a messageId at different receivedAt", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [inboxPathFor(ADDRESS_SEG, 100, "msg-X")]: inboxBody("msg-X", 100),
      [inboxPathFor(ADDRESS_SEG, 200, "msg-X")]: inboxBody("msg-X", 200),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/appears at multiple inbox positions/);
  });
});

// Regression for per-commit-walk pack validation. A single pack
// carrying [enqueue, dequeue] commits — produced by the supervisor's
// first-mail bootstrap — must validate cleanly against a fresh target
// repo. Before the substrate walked per-commit, both commits were
// validated against the ref's pre-pack tip (here: just the genesis),
// so the dequeue commit's "newly added" processing entry had no
// matching prior inbox entry and tripped a path_violation. After the
// fix the dequeue's prior tree is the enqueue commit's tree, so the
// inbox→processing transition lands inside the validator's
// well-formed branch.
describe("workflow-run substrate — per-commit pack validation", () => {
  test("single pack with enqueue + dequeue validates cleanly on a fresh target", async () => {
    const sourceDataDir = await makeClaimCheckTempDir("wfr-percommit-src-");
    const sourceStore = createRepoStore({
      dataDir: sourceDataDir,
      signingKey: claimCheckSigningKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: () => ({ allowed: true }),
    });
    const repoId: RepoId = {
      kind: "workflow-run",
      id: `dep-${Math.random().toString(36).slice(2, 10)}`,
    };
    await sourceStore.initRepo(repoId);
    const enqueueResult = await enqueueInbox(
      sourceStore,
      HUB_PRINCIPAL,
      repoId,
      {
        address: ADDRESS,
        messageId: "msg-1",
        receivedAt: 100,
        mailAuditRef: { store: "audit", path: "mail/msg-1" },
      },
    );
    const dequeued = await dequeueToProcessing(
      sourceStore,
      HUB_PRINCIPAL,
      repoId,
      ADDRESS,
    );
    if (dequeued === null) {
      throw new Error("expected dequeue to find the enqueued entry");
    }

    const ref = "refs/heads/events";
    const sourceDir = sourceStore.getRepoDir(repoId);
    const tipSha = await sourceStore.resolveRef(HUB_PRINCIPAL, repoId, ref);
    if (tipSha === null) {
      throw new Error("expected source ref to resolve");
    }
    // Build a pack carrying BOTH the enqueue and the dequeue commits.
    // `collectReachableObjects` walks one commit's tree (not its
    // ancestor commits), so to feed the per-commit walker on the
    // target a pack with every parent it needs, also include the
    // genesis commit `initRepo` produced -- both the enqueue and
    // the dequeue commits chain back to it. Mirrors the supervisor
    // bootstrap shape the per-commit walker has to handle.
    const genesisSha = await git.resolveRef({
      fs,
      dir: sourceDir,
      ref: "HEAD",
    });
    const enqueueObjects = await collectReachableObjects(
      sourceDir,
      enqueueResult.commitSha,
    );
    const dequeueObjects = await collectReachableObjects(
      sourceDir,
      dequeued.commitSha,
    );
    const genesisObjects = await collectReachableObjects(sourceDir, genesisSha);
    const oids = Array.from(
      new Set([...genesisObjects, ...enqueueObjects, ...dequeueObjects]),
    );
    const packResult = await git.packObjects({
      fs,
      dir: sourceDir,
      oids,
      write: false,
    });
    if (packResult.packfile === undefined) {
      throw new Error("git.packObjects returned no packfile");
    }
    const pack = packResult.packfile;

    const targetDataDir = await makeClaimCheckTempDir("wfr-percommit-tgt-");
    const targetStore = createRepoStore({
      dataDir: targetDataDir,
      signingKey: claimCheckSigningKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: () => ({ allowed: true }),
    });
    await targetStore.initRepo(repoId);
    await targetStore.receivePack(
      HUB_PRINCIPAL,
      repoId,
      ref,
      pack,
      dequeued.commitSha,
      null,
    );

    const targetTip = await targetStore.resolveRef(HUB_PRINCIPAL, repoId, ref);
    expect(targetTip).toBe(dequeued.commitSha);
  });
});

// B3.3 per-run validatePush scoping. The substrate bounds a
// prefix-preserving commit's change set via `changedPathPrefixes`; the
// handler scopes its per-run event/blob walks to the runs under those
// prefixes. These cases pin two properties: (1) scoping does not change
// the verdict for the touched run -- every per-run violation still
// rejects when the violating run is in scope; (2) the prospective tree a
// scoped run-event commit produces validates identically whether the
// substrate bounds the change set or not, so the commit the substrate
// signs is byte-identical either way.
describe("workflowRunKindHandler.validatePush — per-run scoping", () => {
  const TWO_RUN_TREE: Record<string, string> = {
    [WORKFLOW_RUN_GITIGNORE_PATH]: "",
    [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
      0,
      "RunStarted",
    ),
    [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
      1,
      "StepCompleted",
    ),
    [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
      0,
      "RunStarted",
    ),
  };

  test("a valid two-run tree is accepted under undefined and scoped change sets alike", async () => {
    const unscoped = await validate(TWO_RUN_TREE);
    expect(unscoped.ok).toBe(true);

    const scopedToA = await validate(TWO_RUN_TREE, {
      priorFiles: {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
      },
      changedPathPrefixes: new Set([
        `${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/`,
      ]),
    });
    expect(scopedToA.ok).toBe(true);
  });

  test("an append-only overwrite in the TOUCHED run is still rejected under scoping", async () => {
    const prior = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
        { tampered: true },
      ),
    };
    const r = await validate(prospective, {
      priorFiles: prior,
      changedPathPrefixes: new Set([
        `${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/`,
      ]),
    });
    expect(r.ok).toBe(false);
  });

  test("a sequence gap in the TOUCHED run is still rejected under scoping", async () => {
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/2.json`]: eventBody(
          2,
          "StepCompleted",
        ),
      },
      {
        changedPathPrefixes: new Set([
          `${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/`,
        ]),
      },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain("sequence gap");
  });

  test("a post-terminal event in the TOUCHED run is still rejected under scoping", async () => {
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
          0,
          "RunCompleted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
          1,
          "StepStarted",
        ),
      },
      {
        changedPathPrefixes: new Set([
          `${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/`,
        ]),
      },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain("after terminal");
  });

  test("dropping a prior event blob in the TOUCHED run is still rejected under scoping", async () => {
    const prior = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/1.json`]: eventBody(
        1,
        "StepCompleted",
      ),
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
    };
    const r = await validate(prospective, {
      priorFiles: prior,
      changedPathPrefixes: new Set([
        `${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/`,
      ]),
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain("append-only");
  });

  test("a mutated content-addressed blob in the TOUCHED run is still rejected under scoping", async () => {
    const sha = "a".repeat(64);
    const prior = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/${WORKFLOW_RUN_BLOBS_DIR}/${sha}`]:
        "original",
    };
    const prospective = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
        0,
        "RunStarted",
      ),
      [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/${WORKFLOW_RUN_BLOBS_DIR}/${sha}`]:
        "mutated",
    };
    const r = await validate(prospective, {
      priorFiles: prior,
      changedPathPrefixes: new Set([
        `${WORKFLOW_RUN_RUNS_PREFIX}/run-a/${WORKFLOW_RUN_BLOBS_DIR}/`,
      ]),
    });
    expect(r.ok).toBe(false);
  });

  test("a bare runs/ change prefix falls back to validating every run", async () => {
    // When the substrate can only say "runs/ changed" without naming the
    // run, the scope must widen to validate-all so a violation in any run
    // is still caught.
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/2.json`]: eventBody(
          2,
          "StepCompleted",
        ),
      },
      { changedPathPrefixes: new Set([`${WORKFLOW_RUN_RUNS_PREFIX}/`]) },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toContain("sequence gap");
  });

  test("a claim-check-only change set scopes the run walk to nothing", async () => {
    // A commit whose change prefix is entirely under addresses/ touches
    // no run; the per-run walk legitimately validates nothing while the
    // pre-existing runs are carried forward by the substrate.
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
      },
      {
        priorFiles: {
          [WORKFLOW_RUN_GITIGNORE_PATH]: "",
          [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
            0,
            "RunStarted",
          ),
        },
        changedPathPrefixes: new Set([
          `${WORKFLOW_RUN_ADDRESSES_PREFIX}/some-address/`,
        ]),
      },
    );
    expect(r.ok).toBe(true);
  });
});

// B3.3 byte-identical-commit equivalence. validatePush only accepts or
// rejects -- it never alters the tree git.commit builds -- so any commit
// the scoped handler accepts is byte-identical to the same commit the
// validate-all handler accepts. This drives the real substrate end to
// end: an identical multi-run, multi-commit run-event sequence is
// replayed into two stores, one running the production (scoped) handler
// and one running a handler whose `changedPathPrefixes` is forced to
// `undefined` (validate-all), and asserts every commit's tree object id
// matches. A divergence would mean the scoping changed which writes were
// accepted, hence the committed history -- the failure mode the gate
// guards against.
describe("workflowRunKindHandler — scoped vs validate-all byte-identity", () => {
  const equivTempDirs: string[] = [];
  let equivKey: KeyPair;

  beforeAll(async () => {
    equivKey = await generateKeyPair();
  });
  afterAll(async () => {
    for (const d of equivTempDirs.splice(0)) {
      await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
        /* best effort */
      });
    }
  });

  const validateAllHandler: KindHandler = {
    ...workflowRunKindHandler,
    validatePush(args) {
      // Force the validate-all path regardless of what the substrate
      // computed, so this store is the un-scoped reference.
      return workflowRunKindHandler.validatePush({
        ...args,
        changedPathPrefixes: undefined,
      });
    },
  };

  async function makeStore(
    handler: KindHandler,
  ): Promise<{ store: ReturnType<typeof createRepoStore>; dir: string }> {
    const dataDir = await fs.promises.mkdtemp(
      path.join(os.tmpdir(), "wfr-equiv-"),
    );
    equivTempDirs.push(dataDir);
    const store = createRepoStore({
      dataDir,
      signingKey: equivKey,
      handlers: { "workflow-run": handler },
      authorize: () => ({ allowed: true }),
    });
    return { store, dir: path.join(dataDir, handler.directoryPrefix, "") };
  }

  test("identical run-event sequences across two runs produce identical commit trees", async () => {
    const repoId: RepoId = { kind: "workflow-run", id: "equiv-dep" };
    const ref = REF;

    // Each entry is one run-event bracket commit: the run whose
    // `events/` prefix is preserved, plus the full event set for that
    // run at that point. Interleaving run-a and run-b exercises the
    // scoping deciding "this commit touched only run-X".
    const commits: { runId: string; files: Record<string, string> }[] = [
      {
        runId: "run-a",
        files: { "runs/run-a/events/0.json": eventBody(0, "RunStarted") },
      },
      {
        runId: "run-b",
        files: { "runs/run-b/events/0.json": eventBody(0, "RunStarted") },
      },
      {
        runId: "run-a",
        files: {
          "runs/run-a/events/0.json": eventBody(0, "RunStarted"),
          "runs/run-a/events/1.json": eventBody(1, "StepCompleted"),
        },
      },
      {
        runId: "run-b",
        files: {
          "runs/run-b/events/0.json": eventBody(0, "RunStarted"),
          "runs/run-b/events/1.json": eventBody(1, "RunCompleted"),
        },
      },
      {
        runId: "run-a",
        files: {
          "runs/run-a/events/0.json": eventBody(0, "RunStarted"),
          "runs/run-a/events/1.json": eventBody(1, "StepCompleted"),
          "runs/run-a/events/2.json": eventBody(2, "RunCompleted"),
        },
      },
    ];

    const scoped = await makeStore(workflowRunKindHandler);
    const reference = await makeStore(validateAllHandler);
    await scoped.store.initRepo(repoId);
    await reference.store.initRepo(repoId);

    const scopedDir = path.join(scoped.dir, repoId.id);
    const refDir = path.join(reference.dir, repoId.id);

    for (const c of commits) {
      const content = {
        files: c.files,
        clearPrefix: `runs/${c.runId}/events/`,
        message: `bracket ${c.runId}`,
      };
      const s = await scoped.store.writeTree(
        HUB_PRINCIPAL,
        repoId,
        ref,
        content,
      );
      const r = await reference.store.writeTree(
        HUB_PRINCIPAL,
        repoId,
        ref,
        content,
      );
      const { commit: sCommit } = await git.readCommit({
        fs,
        dir: scopedDir,
        oid: s.commitSha,
      });
      const { commit: rCommit } = await git.readCommit({
        fs,
        dir: refDir,
        oid: r.commitSha,
      });
      // The tree object id is the content hash of the whole tree; equal
      // tree oids means byte-identical trees.
      expect(sCommit.tree).toBe(rCommit.tree);
    }
  });
});

// B3.3 pack-path per-run scope completeness. The byte-identity test
// above drives single-run writeTree commits; production writes one run
// per commit today, so the multi-run *pack* path -- where
// computeChangedPathPrefixes derives the touched-run SET from a tree
// OID diff (design 56c/61) -- is the load-bearing derivation site that
// no other test exercises. These author commits directly into a source
// git repo, pack them, and receivePack into the real scoped handler.
describe("workflow-run substrate — pack-path per-run scope completeness", () => {
  // Author a commit whose tree adds `files` on top of the index left by
  // `parent`, parented on `parent`. receivePack validates the tree via
  // validatePush, not the commit signature, so an unsigned source commit
  // is sufficient.
  async function authorCommit(
    dir: string,
    files: Record<string, string>,
    parent: string[],
    message: string,
  ): Promise<string> {
    for (const [rel, body] of Object.entries(files)) {
      const full = path.join(dir, rel);
      await fs.promises.mkdir(path.dirname(full), { recursive: true });
      await fs.promises.writeFile(full, body);
      await git.add({ fs, dir, filepath: rel });
    }
    return git.commit({
      fs,
      dir,
      message,
      author: { name: "probe", email: "probe@example.com" },
      parent,
      ref: REF,
    });
  }

  // Author a commit whose tree is EXACTLY `files`: every currently
  // tracked path is removed first, so paths absent from `files` are
  // dropped. This is how a run deletion is expressed.
  async function authorExactTree(
    dir: string,
    files: Record<string, string>,
    parent: string[],
    message: string,
  ): Promise<string> {
    const tracked = await git.listFiles({ fs, dir });
    for (const rel of tracked) {
      await git.remove({ fs, dir, filepath: rel });
      await fs.promises.rm(path.join(dir, rel), { force: true });
    }
    for (const [rel, body] of Object.entries(files)) {
      const full = path.join(dir, rel);
      await fs.promises.mkdir(path.dirname(full), { recursive: true });
      await fs.promises.writeFile(full, body);
      await git.add({ fs, dir, filepath: rel });
    }
    return git.commit({
      fs,
      dir,
      message,
      author: { name: "probe", email: "probe@example.com" },
      parent,
      ref: REF,
    });
  }

  async function packOids(dir: string, shas: string[]): Promise<Uint8Array> {
    const oids = Array.from(
      new Set(
        (
          await Promise.all(shas.map((s) => collectReachableObjects(dir, s)))
        ).flat(),
      ),
    );
    const packResult = await git.packObjects({ fs, dir, oids, write: false });
    if (packResult.packfile === undefined) {
      throw new Error("git.packObjects returned no packfile");
    }
    return packResult.packfile;
  }

  function makeTargetStore() {
    const repoId: RepoId = {
      kind: "workflow-run",
      id: `dep-${Math.random().toString(36).slice(2, 10)}`,
    };
    return { repoId };
  }

  test("a seq gap in the SECOND run of a two-run pack commit is rejected", async () => {
    // If computeChangedPathPrefixes under-reported run-b (the second
    // changed run), the scoped walk would skip it and accept the gap.
    const srcDir = await makeClaimCheckTempDir("wfr-multirun-src-");
    await git.init({ fs, dir: srcDir, defaultBranch: "events" });
    const genesis = await authorCommit(srcDir, { ".gitignore": "" }, [], "g");
    const tip = await authorCommit(
      srcDir,
      {
        ".gitignore": "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/2.json`]: eventBody(
          2,
          "StepCompleted",
        ),
      },
      [genesis],
      "two runs, gap in run-b",
    );
    const pack = await packOids(srcDir, [genesis, tip]);

    const tgtDataDir = await makeClaimCheckTempDir("wfr-multirun-tgt-");
    const store = createRepoStore({
      dataDir: tgtDataDir,
      signingKey: claimCheckSigningKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: () => ({ allowed: true }),
    });
    const { repoId } = makeTargetStore();
    await store.initRepo(repoId);

    let reason = "";
    try {
      await store.receivePack(HUB_PRINCIPAL, repoId, REF, pack, tip, null);
      throw new Error("expected receivePack to reject the seq gap in run-b");
    } catch (err) {
      reason = err instanceof Error ? err.message : String(err);
    }
    expect(reason).toContain("sequence gap");
    // The ref must not advance.
    const tipAfter = await store.resolveRef(HUB_PRINCIPAL, repoId, REF);
    expect(tipAfter).toBe(null);
  });

  test("a valid two-run pack commit is accepted (scope does not over-reject)", async () => {
    const srcDir = await makeClaimCheckTempDir("wfr-multirun-ok-src-");
    await git.init({ fs, dir: srcDir, defaultBranch: "events" });
    const genesis = await authorCommit(srcDir, { ".gitignore": "" }, [], "g");
    const tip = await authorCommit(
      srcDir,
      {
        ".gitignore": "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/1.json`]: eventBody(
          1,
          "StepCompleted",
        ),
      },
      [genesis],
      "two valid runs",
    );
    const pack = await packOids(srcDir, [genesis, tip]);

    const tgtDataDir = await makeClaimCheckTempDir("wfr-multirun-ok-tgt-");
    const store = createRepoStore({
      dataDir: tgtDataDir,
      signingKey: claimCheckSigningKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: () => ({ allowed: true }),
    });
    const { repoId } = makeTargetStore();
    await store.initRepo(repoId);
    await store.receivePack(HUB_PRINCIPAL, repoId, REF, pack, tip, null);
    expect(await store.resolveRef(HUB_PRINCIPAL, repoId, REF)).toBe(tip);
  });

  test("dropping a prior run while adding to another is rejected with the clean append-only reason", async () => {
    // computeChangedPathPrefixes flags run-a (its subtree OID went
    // present->absent), putting it in scope so the deletion-direction
    // guard fires. The scoped prospective walk lists the now-absent
    // run-a as empty -- via buildCommitTreeClosures.listDir returning []
    // for a missing dir -- and skips it, so the rejection surfaces as the
    // clean append-only path_violation rather than a raw substrate throw.
    const srcDir = await makeClaimCheckTempDir("wfr-drop-src-");
    await git.init({ fs, dir: srcDir, defaultBranch: "events" });
    const parent = await authorExactTree(
      srcDir,
      {
        ".gitignore": "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-a/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
      },
      [],
      "two runs",
    );
    const tip = await authorExactTree(
      srcDir,
      {
        ".gitignore": "",
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/0.json`]: eventBody(
          0,
          "RunStarted",
        ),
        [`${WORKFLOW_RUN_RUNS_PREFIX}/run-b/events/1.json`]: eventBody(
          1,
          "StepCompleted",
        ),
      },
      [parent],
      "drop run-a, grow run-b",
    );

    const tgtDataDir = await makeClaimCheckTempDir("wfr-drop-tgt-");
    const store = createRepoStore({
      dataDir: tgtDataDir,
      signingKey: claimCheckSigningKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: () => ({ allowed: true }),
    });
    const { repoId } = makeTargetStore();
    await store.initRepo(repoId);

    // Land the valid two-run parent first.
    const parentPack = await packOids(srcDir, [parent]);
    await store.receivePack(
      HUB_PRINCIPAL,
      repoId,
      REF,
      parentPack,
      parent,
      null,
    );

    // Push the tip that drops run-a. It must be rejected, fail-closed,
    // with the clean append-only reason (not a raw listDir throw).
    const tipPack = await packOids(srcDir, [tip]);
    let reason = "";
    try {
      await store.receivePack(HUB_PRINCIPAL, repoId, REF, tipPack, tip, parent);
      throw new Error("expected receivePack to reject the dropped run-a");
    } catch (err) {
      reason = err instanceof Error ? err.message : String(err);
    }
    // The ref must not advance past the parent.
    expect(await store.resolveRef(HUB_PRINCIPAL, repoId, REF)).toBe(parent);
    // The clean contract reason, not a raw "is not a directory" throw.
    expect(reason).toContain("append-only");
    expect(reason).not.toContain("is not a directory");
  });
});

// =====================================================================
// P2 — retention watermark: bound the consumed/ dedup index.
//
// The watermark is a per-address monotonic receivedAt horizon. A
// markConsumed commit advances it and prunes consumed entries below it
// (the oldest tail only); enqueueInbox refuses any inbound below it as
// definitively-stale. These tests prove the gate items: the structural
// contract relaxation (validate-level: suffix-only prune, monotonic
// watermark, retained-floor) and the end-to-end exactly-once + bounded
// behaviour against a real on-disk store.

describe("workflowRunKindHandler.validatePush — retention watermark contract", () => {
  // Gate 3: a watermark regression is rejected.
  test("rejects a watermark that moves backward", async () => {
    const prior = {
      [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(500),
      [consumedPathFor(ADDRESS_SEG, "msg-1")]: consumedBody(
        "msg-1",
        600,
        "run-1",
        700,
      ),
    };
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(400),
        [consumedPathFor(ADDRESS_SEG, "msg-1")]: consumedBody(
          "msg-1",
          600,
          "run-1",
          700,
        ),
      },
      { priorFiles: prior },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/watermark regressed/);
  });

  // Gate 2: a non-suffix deletion (drop a recent consumed entry while
  // keeping an older one) is rejected. Dropping msg-recent (receivedAt
  // 200) while retaining msg-old (receivedAt 100) is not a suffix of
  // the age-ordered set -- the suffix guard fires (max dropped 200 >
  // min retained 100), regardless of where the watermark sits.
  test("rejects dropping a recent consumed entry while keeping an older one", async () => {
    const prior = {
      [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(0),
      [consumedPathFor(ADDRESS_SEG, "msg-old")]: consumedBody(
        "msg-old",
        100,
        "run-old",
        150,
      ),
      [consumedPathFor(ADDRESS_SEG, "msg-recent")]: consumedBody(
        "msg-recent",
        200,
        "run-recent",
        250,
      ),
    };
    // Drop msg-recent (the younger one), keep msg-old (the older one).
    // To "permit" the drop the writer must claim a watermark above 200
    // (so the dropped 200 is below it); the retained 100 is then newer
    // than nothing dropped above it, but the dropped 200 is newer than
    // the retained 100 -- a non-suffix prune.
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(201),
        [consumedPathFor(ADDRESS_SEG, "msg-old")]: consumedBody(
          "msg-old",
          100,
          "run-old",
          150,
        ),
      },
      { priorFiles: prior },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/prune is not a suffix/);
  });

  // A dropped entry that the watermark has NOT passed is rejected (you
  // may only prune what the watermark cleared).
  test("rejects pruning a consumed entry the watermark has not passed", async () => {
    const prior = {
      [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(0),
      [consumedPathFor(ADDRESS_SEG, "msg-1")]: consumedBody(
        "msg-1",
        100,
        "run-1",
        150,
      ),
    };
    // Watermark stays at 0; dropping msg-1 (receivedAt 100 >= 0) is not
    // a watermark-passed prune.
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(0),
      },
      { priorFiles: prior },
    );
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(
      /may be pruned only once the watermark has passed/,
    );
  });

  // The suffix prune IS permitted: drop the oldest tail (below the
  // advanced watermark), keep the rest at-or-above it.
  test("accepts pruning the oldest consumed tail below the advanced watermark", async () => {
    const prior = {
      [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(0),
      [consumedPathFor(ADDRESS_SEG, "msg-old")]: consumedBody(
        "msg-old",
        100,
        "run-old",
        150,
      ),
      [consumedPathFor(ADDRESS_SEG, "msg-new")]: consumedBody(
        "msg-new",
        300,
        "run-new",
        350,
      ),
    };
    // Advance watermark to 200: prune msg-old (100 < 200), retain
    // msg-new (300 >= 200).
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(200),
        [consumedPathFor(ADDRESS_SEG, "msg-new")]: consumedBody(
          "msg-new",
          300,
          "run-new",
          350,
        ),
      },
      { priorFiles: prior },
    );
    expect(r.ok).toBe(true);
  });

  // The freshly-written consumed entry is exempt from the retained
  // floor: a message consumed long after receipt may land below an
  // already-advanced watermark (it is pruned on the next commit).
  test("accepts a newly-added consumed entry below the watermark (slow-consumed message)", async () => {
    const prior = {
      [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(500),
      [processingPathFor(ADDRESS_SEG, 100, "msg-slow")]: inboxBody(
        "msg-slow",
        100,
      ),
    };
    // processing -> consumed for msg-slow whose receivedAt (100) is
    // below the prior watermark (500). The transition is legal and the
    // new entry is exempt from the floor.
    const r = await validate(
      {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(500),
        [consumedPathFor(ADDRESS_SEG, "msg-slow")]: consumedBody(
          "msg-slow",
          100,
          "run-slow",
          600,
        ),
      },
      { priorFiles: prior },
    );
    expect(r.ok).toBe(true);
  });

  // The address may carry a watermark.json file without tripping the
  // unexpected-entry guard.
  test("accepts a watermark.json file as a permitted address child", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [watermarkPathFor(ADDRESS_SEG)]: watermarkBody(0),
      [inboxPathFor(ADDRESS_SEG, 100, "msg-1")]: inboxBody("msg-1", 100),
    });
    expect(r.ok).toBe(true);
  });
});

// End-to-end retention against a real on-disk store.
function isEnoent(cause: unknown): boolean {
  return cause instanceof Error && "code" in cause && cause.code === "ENOENT";
}

async function consumedCount(repoDir: string, seg: string): Promise<number> {
  const dir = path.join(
    repoDir,
    WORKFLOW_RUN_ADDRESSES_PREFIX,
    seg,
    WORKFLOW_RUN_CONSUMED_DIR,
  );
  try {
    const names = await fs.promises.readdir(dir);
    return names.filter((n) => n.endsWith(".json")).length;
  } catch (cause) {
    if (isEnoent(cause)) return 0;
    throw cause;
  }
}

async function readWatermarkOnDisk(
  repoDir: string,
  seg: string,
): Promise<number | null> {
  const file = path.join(
    repoDir,
    WORKFLOW_RUN_ADDRESSES_PREFIX,
    seg,
    WORKFLOW_RUN_WATERMARK_FILE,
  );
  let raw: string;
  try {
    raw = await fs.promises.readFile(file, "utf8");
  } catch (cause) {
    if (isEnoent(cause)) return null;
    throw cause;
  }
  const parsed: unknown = JSON.parse(raw);
  if (
    typeof parsed === "object" &&
    parsed !== null &&
    "watermark" in parsed &&
    typeof parsed.watermark === "number"
  ) {
    return parsed.watermark;
  }
  throw new Error("watermark.json shape invalid");
}

describe("claim-check API — retention watermark exactly-once + bounded", () => {
  // Gate 1(a): a duplicate WITHIN the window is still deduped (the
  // consumed/ entry is retained, so a re-enqueue is rejected).
  test("a duplicate within the retention window is deduped at enqueue", async () => {
    const { store, repoId, principal } =
      await makeClaimCheckStore("cc-dup-window-");
    const horizon = 10_000;
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-1",
      receivedAt: 1000,
      mailAuditRef: { store: "audit", path: "mail/msg-1" },
    });
    await dequeueToProcessing(store, principal, repoId, ADDRESS);
    await markConsumed(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-1",
      runId: "run-1",
      consumedAt: 2000,
      retentionHorizonMs: horizon,
    });
    // Re-submit the same messageId still within the window (its
    // consumed/ entry is retained). receivedAt is a fresh, later value
    // but >= watermark, so the stale-reject does NOT fire; the
    // consumed-dedup does.
    await expect(
      enqueueInbox(store, principal, repoId, {
        address: ADDRESS,
        messageId: "msg-1",
        receivedAt: 3000,
        mailAuditRef: { store: "audit", path: "mail/msg-1" },
      }),
    ).rejects.toThrow(/claim_check_already_consumed/);
  });

  // Gate 1(b): a message whose receivedAt is below the watermark (its
  // dedup entry may have been pruned) is rejected at enqueue as stale,
  // NOT silently reprocessed.
  test("a message below the watermark is rejected at enqueue as stale", async () => {
    const { store, repoId, principal } = await makeClaimCheckStore("cc-stale-");
    const horizon = 1000;
    // Drive a message at a late time so the watermark advances well
    // past an old receivedAt.
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-driver",
      receivedAt: 100_000,
      mailAuditRef: { store: "audit", path: "mail/msg-driver" },
    });
    await dequeueToProcessing(store, principal, repoId, ADDRESS);
    const consumed = await markConsumed(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-driver",
      runId: "run-driver",
      consumedAt: 100_000,
      retentionHorizonMs: horizon,
    });
    // Watermark advanced to consumedAt - horizon = 99_000.
    expect(consumed.watermark).toBe(99_000);
    // A stale message arriving with an old receivedAt (50_000 < 99_000)
    // is refused loudly -- never reprocessed.
    await expect(
      enqueueInbox(store, principal, repoId, {
        address: ADDRESS,
        messageId: "msg-stale",
        receivedAt: 50_000,
        mailAuditRef: { store: "audit", path: "mail/msg-stale" },
      }),
    ).rejects.toThrow(/claim_check_stale_enqueue/);
  });

  // Gate 1(c): a brand-new message with receivedAt >= watermark is
  // accepted normally.
  test("a fresh message at or above the watermark is accepted", async () => {
    const { store, repoId, principal } = await makeClaimCheckStore("cc-fresh-");
    const horizon = 1000;
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-driver",
      receivedAt: 100_000,
      mailAuditRef: { store: "audit", path: "mail/msg-driver" },
    });
    await dequeueToProcessing(store, principal, repoId, ADDRESS);
    await markConsumed(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-driver",
      runId: "run-driver",
      consumedAt: 100_000,
      retentionHorizonMs: horizon,
    });
    // watermark = 99_000; a new message at 100_500 is fine.
    const r = await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "msg-fresh",
      receivedAt: 100_500,
      mailAuditRef: { store: "audit", path: "mail/msg-fresh" },
    });
    expect(r.inboxKey).toBe("100500-msg-fresh");
  });

  // Gate 4: after N >> horizon-worth of messages with advancing time,
  // consumed/ holds ~one horizon's worth, not N.
  test("consumed/ stays bounded under many messages with advancing time", async () => {
    const { store, repoId, principal } =
      await makeClaimCheckStore("cc-bounded-");
    const repoDir = store.getRepoDir(repoId);
    const seg = ADDRESS_SEG;
    const horizon = 1000; // retain ~1000ms worth of entries
    const step = 100; // a message every 100ms
    const n = 60; // 60 messages span 6000ms >> horizon
    for (let i = 0; i < n; i++) {
      const t = 10_000 + i * step;
      const messageId = `m-${String(i)}`;
      await enqueueInbox(store, principal, repoId, {
        address: ADDRESS,
        messageId,
        receivedAt: t,
        mailAuditRef: { store: "audit", path: `mail/${messageId}` },
      });
      await dequeueToProcessing(store, principal, repoId, ADDRESS);
      await markConsumed(store, principal, repoId, {
        address: ADDRESS,
        messageId,
        runId: `r-${String(i)}`,
        consumedAt: t,
        retentionHorizonMs: horizon,
      });
    }
    const count = await consumedCount(repoDir, seg);
    // Bounded: at most ceil(horizon/step)+1 entries are retained
    // (entries within [watermark, now]). It must NOT be ~N.
    const bound = Math.ceil(horizon / step) + 2;
    expect(count).toBeLessThanOrEqual(bound);
    expect(count).toBeLessThan(n);
    // The watermark advanced and tracks the prune boundary.
    const wm = await readWatermarkOnDisk(repoDir, seg);
    expect(wm).not.toBeNull();
    if (wm === null) throw new Error("unreachable");
    expect(wm).toBeGreaterThan(10_000);
  });

  // The watermark is monotonic across real commits: a later commit
  // with an earlier consumedAt does not move it backward.
  test("the watermark never regresses across markConsumed commits", async () => {
    const { store, repoId, principal } = await makeClaimCheckStore("cc-mono-");
    const repoDir = store.getRepoDir(repoId);
    const horizon = 1000;
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "a",
      receivedAt: 50_000,
      mailAuditRef: { store: "audit", path: "mail/a" },
    });
    await dequeueToProcessing(store, principal, repoId, ADDRESS);
    await markConsumed(store, principal, repoId, {
      address: ADDRESS,
      messageId: "a",
      runId: "ra",
      consumedAt: 50_000,
      retentionHorizonMs: horizon,
    });
    const wmAfterA = await readWatermarkOnDisk(repoDir, ADDRESS_SEG);
    expect(wmAfterA).toBe(49_000);
    // A second message that arrived earlier (clock skew) and is
    // consumed with an earlier consumedAt must not drag the watermark
    // back. Its receivedAt (49_500) is >= the current watermark so the
    // enqueue is accepted.
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "b",
      receivedAt: 49_500,
      mailAuditRef: { store: "audit", path: "mail/b" },
    });
    await dequeueToProcessing(store, principal, repoId, ADDRESS);
    const consumedB = await markConsumed(store, principal, repoId, {
      address: ADDRESS,
      messageId: "b",
      runId: "rb",
      consumedAt: 49_600,
      retentionHorizonMs: horizon,
    });
    // horizonBoundary for b = 48_600 < priorWatermark 49_000, so the
    // watermark holds at 49_000.
    expect(consumedB.watermark).toBe(49_000);
    const wmAfterB = await readWatermarkOnDisk(repoDir, ADDRESS_SEG);
    expect(wmAfterB).toBe(49_000);
  });

  // Replay-vs-watermark regression (the path that becomes a silent
  // message-loss bug if someone "tightens" replay with a watermark
  // stale-check). A message that is already in processing/ -- past
  // dedup -- whose receivedAt has fallen BELOW an advanced watermark
  // must be re-admitted to inbox/ by replayProcessingToInbox (NOT
  // rejected as stale), then dequeued and consumed exactly once (not
  // lost, not double-processed).
  test("replay re-admits a below-watermark in-flight message and it completes exactly once", async () => {
    const { store, repoId, principal } =
      await makeClaimCheckStore("cc-replay-wm-");
    const repoDir = store.getRepoDir(repoId);
    const horizon = 1000;

    // 1. An in-flight message: enqueued at receivedAt 10_000 and moved
    //    to processing (past dedup), left there to simulate a crash
    //    mid-handling.
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "inflight",
      receivedAt: 10_000,
      mailAuditRef: { store: "audit", path: "mail/inflight" },
    });
    const inflightDequeue = await dequeueToProcessing(
      store,
      principal,
      repoId,
      ADDRESS,
    );
    expect(inflightDequeue?.envelope.messageId).toBe("inflight");

    // 2. Advance the watermark well past 10_000 by consuming a much
    //    newer message. consumedAt 100_000, horizon 1000 -> watermark
    //    99_000. The in-flight processing entry (10_000) is now below
    //    the watermark.
    await enqueueInbox(store, principal, repoId, {
      address: ADDRESS,
      messageId: "newer",
      receivedAt: 100_000,
      mailAuditRef: { store: "audit", path: "mail/newer" },
    });
    await dequeueToProcessing(store, principal, repoId, ADDRESS);
    const newerConsumed = await markConsumed(store, principal, repoId, {
      address: ADDRESS,
      messageId: "newer",
      runId: "r-newer",
      consumedAt: 100_000,
      retentionHorizonMs: horizon,
    });
    expect(newerConsumed.watermark).toBe(99_000);
    const wm = await readWatermarkOnDisk(repoDir, ADDRESS_SEG);
    expect(wm).toBe(99_000);

    // 3. Replay must re-admit the below-watermark in-flight entry to
    //    inbox -- NOT reject it as stale. (A fresh enqueue at 10_000
    //    WOULD be refused; replay is intentionally exempt.)
    const replay = await replayProcessingToInbox(
      store,
      principal,
      repoId,
      ADDRESS,
    );
    expect(replay.replayedKeys).toContain("10000-inflight");
    // The entry is back in inbox at its original filename key.
    const inboxDir = path.join(
      repoDir,
      WORKFLOW_RUN_ADDRESSES_PREFIX,
      ADDRESS_SEG,
      WORKFLOW_RUN_INBOX_DIR,
    );
    const inboxEntries = await fs.promises.readdir(inboxDir);
    expect(inboxEntries).toContain("10000-inflight.json");

    // 4. It can be dequeued and consumed exactly once.
    const reDequeue = await dequeueToProcessing(
      store,
      principal,
      repoId,
      ADDRESS,
    );
    expect(reDequeue?.envelope.messageId).toBe("inflight");
    await markConsumed(store, principal, repoId, {
      address: ADDRESS,
      messageId: "inflight",
      runId: "r-inflight",
      consumedAt: 101_000,
      retentionHorizonMs: horizon,
    });
    const consumedPath = path.join(
      repoDir,
      WORKFLOW_RUN_ADDRESSES_PREFIX,
      ADDRESS_SEG,
      WORKFLOW_RUN_CONSUMED_DIR,
      "inflight.json",
    );
    await fs.promises.access(consumedPath);

    // 5. No double-process: nothing remains to dequeue, and a
    //    re-enqueue of the same content (fresh receivedAt >= watermark)
    //    is now deduped by the retained consumed entry.
    const drained = await dequeueToProcessing(
      store,
      principal,
      repoId,
      ADDRESS,
    );
    expect(drained).toBeNull();
    await expect(
      enqueueInbox(store, principal, repoId, {
        address: ADDRESS,
        messageId: "inflight",
        receivedAt: 102_000,
        mailAuditRef: { store: "audit", path: "mail/inflight" },
      }),
    ).rejects.toThrow(/claim_check_already_consumed/);
  });
});
