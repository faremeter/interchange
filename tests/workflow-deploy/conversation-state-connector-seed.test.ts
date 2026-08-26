// Unit tests for the warm-agent durable conversation store's connector-seed
// hook (design §3c threading). Seeding drives the store's connector router
// from each mail-derived inbound message so the warm agent's reply path has
// thread state, and the seeded state persists to the workflow-run substrate
// so it survives a child respawn.
//
// The tests drive a REAL `createRepoStore` workflow-run substrate and a REAL
// isogit local store (the production path), so the connector-state flush, the
// change-driven + run-boundary mirrors, and the working-tree reconstruction
// are exercised end to end -- not mocked. This mirrors the harness in
// conversation-state-wal.test.ts; the substrate dependency is why these live
// under tests/ rather than co-located in apps/sidecar/src (co-location would
// force a workflow-run-substrate dependency cycle).

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair, createSSHSignature } from "@intx/crypto";
import { createInboundMessage } from "@intx/mime";
import type { KeyPair, InboundMessage } from "@intx/types/runtime";
import type {
  RepoId,
  RepoStore,
  WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import {
  createRepoStore,
  workflowRunKindHandler,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";
import {
  createDurableConversationStore,
  reconstructDurableConversation,
  type DurableConversationStore,
} from "@intx/sidecar-app/src/conversation-state";

const WORKFLOW_RUN_REF = "refs/heads/main";
const AGENT_KEY = "step-1";

const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: "connector-seed-unit",
};

interface Harness {
  baseDir: string;
  substrate: RepoStore;
  workflowRunRepoId: RepoId;
  signer: (payload: string) => Promise<string>;
  agentStateDir: string;
}

async function makeHarness(): Promise<Harness> {
  const baseDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "connector-seed-unit-"),
  );
  const signingKey: KeyPair = await generateKeyPair();
  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: "connector-seed-unit",
  };
  const substrate = createRepoStore({
    dataDir: baseDir,
    signingKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: () => ({ allowed: true }),
  });
  await substrate.writeTree(
    { kind: "hub" },
    workflowRunRepoId,
    WORKFLOW_RUN_REF,
    {
      files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
      message: "genesis",
    },
  );
  const signer = (payload: string): Promise<string> =>
    Promise.resolve(
      createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey),
    );
  const agentStateDir = path.join(
    substrate.getRepoDir(workflowRunRepoId),
    WORKFLOW_RUN_AGENT_STATE_PREFIX,
    encodeURIComponent(AGENT_KEY),
  );
  return { baseDir, substrate, workflowRunRepoId, signer, agentStateDir };
}

async function makeStore(
  h: Harness,
  localDir: string,
): Promise<DurableConversationStore> {
  return createDurableConversationStore({
    localStoreDir: localDir,
    signer: h.signer,
    substrate: h.substrate,
    workflowRunRepoId: h.workflowRunRepoId,
    workflowRunRef: WORKFLOW_RUN_REF,
    principal: PRINCIPAL,
    agentKey: AGENT_KEY,
  });
}

/** Build a mail-derived inbound message the way the step invoker delivers it. */
function inbound(opts: {
  from: string;
  messageId: string;
  inReplyTo?: string;
  references?: string[];
  subject?: string;
}): InboundMessage {
  return createInboundMessage({
    from: opts.from,
    to: "agent@example.com",
    content: "hello",
    messageId: opts.messageId,
    interchangeType: "conversation.message",
    ...(opts.inReplyTo !== undefined ? { inReplyTo: opts.inReplyTo } : {}),
    ...(opts.references !== undefined ? { references: opts.references } : {}),
    ...(opts.subject !== undefined ? { subject: opts.subject } : {}),
  });
}

describe("durable conversation store connector seed (design §3c)", () => {
  let h: Harness;
  let localDir: string;

  beforeEach(async () => {
    h = await makeHarness();
    localDir = path.join(h.baseDir, "local");
  });

  afterEach(async () => {
    await fs.promises.rm(h.baseDir, { recursive: true, force: true });
  });

  test("composeReply throws before any inbound has been seeded", async () => {
    const store = await makeStore(h, localDir);
    expect(() => store.composeReply()).toThrow(/no active connector thread/);
  });

  test("seeding an inbound starts a thread that composeReply targets at its sender", async () => {
    const store = await makeStore(h, localDir);
    await store.seedInbound(
      inbound({
        from: "alice@example.com",
        messageId: "<a@example.com>",
        subject: "hi there",
      }),
    );
    // Drain the change-driven mirror the seed enqueued so it does not outlive
    // the test's substrate.
    await store.mirrorToSubstrate();

    const reply = store.composeReply();
    // The first inbound starts the thread: reply to its sender, in-reply-to
    // its own message id, no other participants yet.
    expect(reply.to).toBe("alice@example.com");
    expect(reply.inReplyTo).toBe("<a@example.com>");
    expect(reply.cc).toEqual([]);
    expect(reply.subject).toBe("hi there");
  });

  test("seeding a reply-shaped continuation advances the thread and carries prior speakers to cc", async () => {
    const store = await makeStore(h, localDir);
    await store.seedInbound(
      inbound({ from: "alice@example.com", messageId: "<a@example.com>" }),
    );
    // Bob replies to Alice's message: In-Reply-To matches the thread's last
    // message id, so this is a continuation.
    await store.seedInbound(
      inbound({
        from: "bob@example.com",
        messageId: "<b@example.com>",
        inReplyTo: "<a@example.com>",
      }),
    );
    await store.mirrorToSubstrate();

    const reply = store.composeReply();
    // Bob is now the most recent speaker (the primary recipient); Alice moves
    // into cc; the reply threads onto Bob's message.
    expect(reply.to).toBe("bob@example.com");
    expect(reply.cc).toEqual(["alice@example.com"]);
    expect(reply.inReplyTo).toBe("<b@example.com>");
  });

  test("an inbound matching no active thread is a passthrough and leaves the thread unchanged", async () => {
    const store = await makeStore(h, localDir);
    await store.seedInbound(
      inbound({ from: "alice@example.com", messageId: "<a@example.com>" }),
    );
    // Carol's message references nothing on the thread, so on an already-active
    // thread it routes as a passthrough: the thread stays pinned to Alice.
    await store.seedInbound(
      inbound({ from: "carol@example.com", messageId: "<c@example.com>" }),
    );
    // Only the start seed enqueued a change-driven mirror (the passthrough
    // advanced nothing); drain it so it does not outlive the substrate.
    await store.mirrorToSubstrate();

    const reply = store.composeReply();
    expect(reply.to).toBe("alice@example.com");
    expect(reply.inReplyTo).toBe("<a@example.com>");
    expect(reply.cc).toEqual([]);
  });

  test("seeded connector state persists to the substrate and restores into a fresh store", async () => {
    const store = await makeStore(h, localDir);
    // Two messages, each a seed followed by its run-boundary mirror -- the
    // production per-message shape (seed before the send, mirror after it).
    await store.seedInbound(
      inbound({
        from: "alice@example.com",
        messageId: "<a@example.com>",
        subject: "durable thread",
      }),
    );
    await store.mirrorToSubstrate();
    await store.seedInbound(
      inbound({
        from: "bob@example.com",
        messageId: "<b@example.com>",
        inReplyTo: "<a@example.com>",
      }),
    );
    await store.mirrorToSubstrate();

    // The substrate carries the advanced connector thread, not a stale null.
    const reconstructed = await reconstructDurableConversation(
      h.agentStateDir,
      AGENT_KEY,
    );
    if (reconstructed === null) throw new Error("expected a reconstruction");
    expect(reconstructed.connectorState).toEqual({
      threadRoot: "<a@example.com>",
      lastMessageId: "<b@example.com>",
      replyTo: "bob@example.com",
      cc: ["alice@example.com"],
      subject: "durable thread",
    });

    // A fresh store (a respawn with an empty local FS) restores the seeded
    // thread and composes the same reply -- the cross-respawn continuity the
    // warm mail loop depends on.
    const freshLocalDir = path.join(h.baseDir, "respawn-local");
    const fresh = await makeStore(h, freshLocalDir);
    expect(await fresh.restoreFromSubstrate()).toBe(true);
    // Restoring a non-null connector state fires the router's change-driven
    // mirror; drain it so it does not outlive the test's substrate.
    await fresh.mirrorToSubstrate();

    const reply = fresh.composeReply();
    expect(reply.to).toBe("bob@example.com");
    expect(reply.cc).toEqual(["alice@example.com"]);
    expect(reply.inReplyTo).toBe("<b@example.com>");
    expect(reply.subject).toBe("durable thread");
  });
});
