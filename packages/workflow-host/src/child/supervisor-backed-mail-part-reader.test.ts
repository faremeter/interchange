import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  createDefaultDirectorRegistry,
  defineAgent,
  type Agent,
  type AgentDefinition,
  type BaseEnv,
  type SendResult,
} from "@intx/agent";
import { noopAuditStore } from "@intx/agent/testing";
import { generateKeyPair } from "@intx/crypto";
import {
  createRepoStore,
  workflowRunAuthorize,
  workflowRunKindHandler,
} from "@intx/hub-sessions";
import type { Principal, RepoId } from "@intx/hub-sessions";
import { base64Encode } from "@intx/types";
import type {
  BlobReader,
  ContextStore,
  InferenceSource,
  InboundMessage,
  MailPartReader,
  MessageHeaders,
  MessagePart,
} from "@intx/types/runtime";
import type {
  AuthorizeContext,
  StepInvokeRequest,
  WorkflowAuthorizeFn,
} from "@intx/workflow";

import { commitMail, createMailPartReader } from "../adapters/mail-part-store";
import {
  createWorkflowStepInvoker,
  type StepEnvBase,
} from "../adapters/step-invoker";
import type {
  ChildMailboxCallBridge,
  MailboxCall,
  MailboxCallSuccess,
} from "./mailbox-call-bridge";
import { createSupervisorBackedMailPartReader } from "./supervisor-backed-mail-part-reader";

const REF = "refs/heads/main";
const RUN_ID = "run-1";
const tempDirs: string[] = [];

const STUB_SOURCE: InferenceSource = {
  id: "anthropic:stub",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-stub",
  model: "stub-model",
};

let signingKey: Awaited<ReturnType<typeof generateKeyPair>>;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const dir of tempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

function bridgeThat(
  respond: (call: MailboxCall) => Promise<MailboxCallSuccess>,
): ChildMailboxCallBridge & { readonly calls: MailboxCall[] } {
  const calls: MailboxCall[] = [];
  return {
    calls,
    async submit(call) {
      calls.push(call);
      return respond(call);
    },
    handleResult() {
      /* the reader does not receive responses through this path */
    },
    cancelAll() {
      /* nothing pending inside the stub */
    },
    pendingCount: 0,
  };
}

function headers(): MessageHeaders {
  return {
    from: "sender@example.com",
    to: ["run@deployment.example.com"],
    date: "2026-01-02T03:04:05Z",
    messageId: "<msg-part-reader@example.com>",
  };
}

function part(
  contentType: string,
  content: string,
  extra: Partial<MessagePart> = {},
): MessagePart {
  return {
    contentType,
    content: new TextEncoder().encode(content),
    ...extra,
  };
}

function stubContextStore(): ContextStore {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; never invoked on the adapter path
  return {} as ContextStore;
}

function stubBlobReader(): BlobReader {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; never read on the adapter path
  return {} as BlobReader;
}

function stubBuildEnv(): StepEnvBase {
  return {
    sources: [STUB_SOURCE],
    defaultSource: STUB_SOURCE.id,
    storage: stubContextStore(),
    workdir: "/tmp/supervisor-backed-mail-part-reader",
    audit: noopAuditStore(),
    directors: createDefaultDirectorRegistry(),
  };
}

function stubDef(): AgentDefinition<BaseEnv> {
  return defineAgent({
    id: "part-reader-stub",
    systemPrompt: "stub",
    tools: [],
    capabilities: [],
    inference: {
      sources: [{ provider: STUB_SOURCE.provider, model: STUB_SOURCE.model }],
    },
  });
}

function buildRequest(input: unknown): StepInvokeRequest {
  const authzContext: AuthorizeContext = {
    stepId: "step-1",
    attempt: 1,
    runId: RUN_ID,
  };
  return {
    agent: stubDef(),
    input,
    authzContext,
    signal: new AbortController().signal,
  };
}

describe("createSupervisorBackedMailPartReader", () => {
  test("a mismatched op rejects before the bytes are decoded", async () => {
    const bridge = bridgeThat(() =>
      Promise.resolve({
        requestId: "mc-1",
        ok: true,
        op: "watch",
      }),
    );
    const reader = createSupervisorBackedMailPartReader({
      callBridge: bridge,
      runId: RUN_ID,
    });

    await expect(reader.read("mail-part:///dep/seg/0-a.bin")).rejects.toThrow(
      /does not match/,
    );
    expect(bridge.calls).toEqual([
      {
        op: "readMailPart",
        runId: RUN_ID,
        partRef: "mail-part:///dep/seg/0-a.bin",
      },
    ]);
  });

  test("a bridge rejection reaches the caller unchanged", async () => {
    const failure = new Error(
      "mail part reader: no committed part at runs/dep/parts/seg/0-a.bin",
    );
    const bridge = bridgeThat(() => Promise.reject(failure));
    const reader = createSupervisorBackedMailPartReader({
      callBridge: bridge,
      runId: RUN_ID,
    });

    await expect(reader.read("mail-part:///dep/seg/0-a.bin")).rejects.toBe(
      failure,
    );
  });

  test("a committed part is delivered as the step's attachment", async () => {
    const dataDir = await fs.promises.mkdtemp(
      path.join(os.tmpdir(), "mail-part-reader-"),
    );
    tempDirs.push(dataDir);
    const repoId: RepoId = { kind: "workflow-run", id: "dep-part" };
    const principalShape = {
      kind: "workflow-process" as const,
      anchorRunId: "dep-part",
    };
    const principal: Principal = principalShape;
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: workflowRunAuthorize,
    });
    const stored = await commitMail(
      {
        substrate,
        repoId,
        principal,
        runId: "dep-part",
        ref: REF,
      },
      "<msg-part-reader@example.com>",
      {
        headers: headers(),
        rawHeaders: {},
        parts: [
          part("text/plain", "look at this"),
          part("image/png", "png-bytes", {
            filename: "photo.png",
            disposition: "attachment",
          }),
        ],
      },
    );
    const image = stored.parts.find(
      (candidate) => candidate.filename === "photo.png",
    );
    if (image === undefined) throw new Error("expected the image part");
    const storeReader = createMailPartReader({
      substrate,
      repoId,
      principal,
      ref: REF,
    });
    const bridge = bridgeThat(async (call) => {
      if (call.op !== "readMailPart") throw new Error(call.op);
      const bytes = await storeReader.read(call.partRef);
      return {
        requestId: "mc-bytes",
        ok: true,
        op: "readMailPart",
        value: { contentBase64: base64Encode(bytes) },
      };
    });
    const reader: MailPartReader = createSupervisorBackedMailPartReader({
      callBridge: bridge,
      runId: RUN_ID,
    });
    const captured: { message: string | InboundMessage | undefined } = {
      message: undefined,
    };
    const agent: Agent = {
      async send(content): Promise<SendResult> {
        captured.message = content;
        return {
          type: "reply",
          reply: "ok",
          turn: {
            role: "assistant",
            content: [{ type: "text", text: "ok" }],
            model: STUB_SOURCE.model,
            timestamp: 0,
          },
        };
      },
      stream() {
        throw new Error("stub stream() not used");
      },
      deliver() {
        throw new Error("stub deliver() not used");
      },
      async close() {
        /* no-op */
      },
      setSource() {
        throw new Error("stub setSource() not used");
      },
      setSources() {
        throw new Error("stub setSources() not used");
      },
      async history() {
        return [];
      },
      async checkpoints() {
        return [];
      },
      async readAt() {
        return [];
      },
      blobReader: stubBlobReader(),
    };
    const allowAll: WorkflowAuthorizeFn = async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    });
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: allowAll,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      mailPartReader: reader,
    });

    await invoker(buildRequest(stored));

    const message = captured.message;
    if (typeof message === "string" || message === undefined) {
      throw new Error("expected an InboundMessage");
    }
    expect(message.content).toBe("look at this");
    expect(message.attachments).toEqual([
      {
        name: "photo.png",
        contentType: "image/png",
        data: new TextEncoder().encode("png-bytes"),
      },
    ]);
    expect(bridge.calls).toEqual([
      { op: "readMailPart", runId: RUN_ID, partRef: image.ref },
    ]);
  });
});
