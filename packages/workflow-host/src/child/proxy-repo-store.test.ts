// A delta write through the child's proxy substrate. The child reads the
// parent commit from its own store over the shared data dir, and the IPC
// carries only the parent commit down and the new files up. A minimal
// supervisor stand-in answers the bridge's frames with the real store's
// `writeTreeDelta`, the way the supervisor's handler does.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { base64Decode } from "@intx/types/base64";
import {
  createAgentRepoStore,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";
import type {
  RepoId,
  RepoStore,
  WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";

import type {
  ControlChannelSender,
  ControlPayload,
} from "../ipc/control-channel";
import { createWorkflowRunRepoStore } from "../adapters/repo-store";
import { createProxyWorkflowRunRepoStore } from "./proxy-repo-store";
import {
  createChildSubstrateWriteBridge,
  type ChildSubstrateWriteBridge,
} from "./substrate-write-bridge";

const REF = "refs/heads/main";
const DEPLOYMENT = "deployment-proxy";
const REPO_ID: RepoId = { kind: "workflow-run", id: DEPLOYMENT };
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: DEPLOYMENT,
};
const RUN_ID = "run-1";

const tempDirs: string[] = [];
let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

type DeltaResponse = Extract<
  ControlPayload,
  { type: "substrate.delta.response" }
>["data"];

// Answer the bridge's upstream frames with `store`. Every frame the
// child sends is kept so a test can inspect what crossed the IPC.
function createSupervisorStandIn(store: RepoStore): {
  sender: ControlChannelSender;
  sent: ControlPayload[];
  attach: (bridge: ChildSubstrateWriteBridge) => void;
} {
  const sent: ControlPayload[] = [];
  const deltas = new Map<string, (data: DeltaResponse) => void>();
  let bridge: ChildSubstrateWriteBridge | undefined;
  async function runDeltaWrite(
    data: Extract<
      ControlPayload,
      { type: "substrate.write-delta.request" }
    >["data"],
  ): Promise<void> {
    if (bridge === undefined) throw new Error("bridge not attached");
    const child = bridge;
    try {
      const { commitSha } = await store.writeTreeDelta(
        PRINCIPAL,
        REPO_ID,
        data.ref,
        {
          changedPathPrefixes:
            data.changedPathPrefixes === null
              ? undefined
              : new Set(data.changedPathPrefixes),
          message: data.message,
          computeDelta: async (parentCommitSha) => {
            const response = new Promise<DeltaResponse>((resolve) => {
              deltas.set(data.requestId, resolve);
            });
            child.handleDeltaRequest({
              requestId: data.requestId,
              parentCommitSha,
            });
            const { result } = await response;
            if (!result.ok) throw new Error(result.reason);
            const puts: Record<string, Uint8Array> = {};
            for (const put of result.puts) {
              puts[put.path] = base64Decode(put.contentBase64);
            }
            return { puts, deletes: result.deletes };
          },
        },
      );
      child.handleWriteResponse({
        requestId: data.requestId,
        result: { ok: true, commitSha },
      });
    } catch (cause) {
      child.handleWriteResponse({
        requestId: data.requestId,
        result: {
          ok: false,
          reason: cause instanceof Error ? cause.message : String(cause),
        },
      });
    }
  }
  return {
    sent,
    attach(b) {
      bridge = b;
    },
    sender: {
      get seq() {
        return sent.length;
      },
      async send(payload) {
        sent.push(payload);
        if (payload.type === "substrate.write-delta.request") {
          void runDeltaWrite(payload.data);
        } else if (payload.type === "substrate.delta.response") {
          deltas.get(payload.data.requestId)?.(payload.data);
        }
      },
    },
  };
}

async function setup() {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "proxy-repo-store-"),
  );
  tempDirs.push(dataDir);
  // Two stores over one data dir, as the supervisor and child processes
  // each open their own.
  const supervisorStore = createAgentRepoStore({
    dataDir,
    signingKey,
  }).repoStore;
  const bareStore = createAgentRepoStore({ dataDir, signingKey }).repoStore;
  await supervisorStore.initRepo(REPO_ID);
  await supervisorStore.writeTree({ kind: "hub" }, REPO_ID, REF, {
    files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
    message: "genesis",
  });
  const standIn = createSupervisorStandIn(supervisorStore);
  const bridge = createChildSubstrateWriteBridge({
    upstreamSender: standIn.sender,
  });
  standIn.attach(bridge);
  const proxy = createProxyWorkflowRunRepoStore({
    bareStore,
    bridge,
    workflowRunRepoId: REPO_ID,
  });
  const adapter = createWorkflowRunRepoStore({
    substrate: proxy,
    principal: PRINCIPAL,
    repoId: REPO_ID,
    ref: REF,
  });
  return { adapter, sent: standIn.sent };
}

const AT = new Date(0).toISOString();

describe("child proxy writeTreeDelta", () => {
  test("an append sends only its new event and lands in the supervisor's store", async () => {
    const { adapter, sent } = await setup();
    await adapter.append(RUN_ID, {
      kind: "RunStarted",
      seq: 1,
      at: AT,
      runId: RUN_ID,
      definitionHash: "definition-hash",
      trigger: { type: "manual", payload: {} },
    });
    sent.length = 0;
    await adapter.append(RUN_ID, {
      kind: "StepStarted",
      seq: 2,
      at: AT,
      stepId: "step-a",
      attempt: 1,
      input: { ref: "inline:{}" },
    });

    expect(sent.map((p) => p.type)).toEqual([
      "substrate.write-delta.request",
      "substrate.delta.response",
    ]);
    const response = sent[1];
    if (response?.type !== "substrate.delta.response") {
      throw new Error("no delta response");
    }
    if (!response.data.result.ok) throw new Error(response.data.result.reason);
    expect(response.data.result.puts.map((p) => p.path)).toEqual([
      `runs/${RUN_ID}/events/2.json`,
    ]);
    expect(response.data.result.deletes).toEqual([]);

    const events = await adapter.read(RUN_ID);
    expect(events.map((e) => [e.kind, e.seq])).toEqual([
      ["RunStarted", 1],
      ["StepStarted", 2],
    ]);
  });

  test("an append whose seq the parent tree contradicts fails as a seq conflict", async () => {
    const { adapter } = await setup();
    await adapter.append(RUN_ID, {
      kind: "RunStarted",
      seq: 1,
      at: AT,
      runId: RUN_ID,
      definitionHash: "definition-hash",
      trigger: { type: "manual", payload: {} },
    });
    await expect(
      adapter.append(RUN_ID, {
        kind: "StepStarted",
        seq: 5,
        at: AT,
        stepId: "step-a",
        attempt: 1,
        input: { ref: "inline:{}" },
      }),
    ).rejects.toThrow(/seq conflict on append to run-1.*expected seq 2/);
  });
});
