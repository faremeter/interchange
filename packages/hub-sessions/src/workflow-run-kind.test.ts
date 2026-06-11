import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { generateKeyPair } from "@intx/crypto-node";
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
} from "./workflow-run-kind";
import { createRepoStore } from "./repo-store";
import type { Principal, RepoId } from "./repo-store";

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
const WORKFLOW_PROCESS_PRINCIPAL: Principal = { kind: "workflow-process" };
const noPriorBlob = async (): Promise<Uint8Array | null> => null;
const noPriorDir = async (): Promise<string[]> => [];

type ValidateOpts = {
  ref?: string;
  principal?: Principal;
  priorFiles?: Record<string, string>;
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

  test("rejects an arbitrary disallowed top-level entry", async () => {
    const r = await validate({
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      "stray.txt": "nope",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.reason).toMatch(/unexpected top-level entry/);
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
