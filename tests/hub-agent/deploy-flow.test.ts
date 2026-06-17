// Integration test: full deploy lifecycle through SessionService.
//
// Spins up a real WS server (hub side), spawns a real sidecar subprocess,
// and exercises the complete agent lifecycle orchestrated by SessionService:
//
//   launchSession (write → pack → provision → deliver → start)
//     → message → sync → endSession
//
// The gap this test fills: nobody else tests that AgentRepoStore.writeDeployTree
// → createDeployPack → sendPack produces a packfile the sidecar actually
// accepts and materializes correctly.
//
// Inference is mocked by a tiny HTTP server that echoes the tools it receives,
// so we can assert the model saw the correct tool definitions without calling
// a real LLM.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@intx/mime";
import { generateKeyPair, createNodeCrypto } from "@intx/crypto-node";
import { parseAgentId } from "@intx/hub-sessions";
import { createAgentRepoStore as createSidecarRepoStore } from "@intx/hub-agent";
import type { HarnessConfig } from "@intx/types/runtime";
import git from "isomorphic-git";

import {
  AGENT_ADDRESS,
  AGENT_ID,
  SESSION_ID,
  SIDECAR_ID,
  startDeployFlowEnv,
  waitFor,
  type DeployFlowEnv,
} from "./lib/deploy-flow-env";

let env: DeployFlowEnv;
let setupInferenceRequestIndex: number;
let setupAgentEventsBeforeMessage: number;

// The downstream tests (send-message side-effect assertions,
// sync-request, endSession) each depend on prior lifecycle stages
// having run. Bun runs tests within a `describe` in source order, so
// the chain works when the file runs end-to-end, but `bun test -t
// <name>` would skip the prerequisite stages and a filtered run of
// `sync request` would fail with `No sidecar connected for agent` or
// `tree must include at least one state-bearing top-level entry`.
// Running the prerequisite stages here -- launchSession AND the first
// message routing, both of which populate the state the sync test
// then asserts against -- codifies the preconditions once so each
// individual test can run on its own under a `-t` filter. The tests
// below assert against the state these setup steps produced.
beforeAll(async () => {
  env = await startDeployFlowEnv();

  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: AGENT_ID,
    tenantId: "tenant-1",
    principalId: "prin_integration-1",
    agentAddress: AGENT_ADDRESS,
    systemPrompt: "Fallback prompt (should be overridden by deploy tree)",
    tools: [],
    grants: [],
    sources: [
      {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${env.inference.server.port}`,
        apiKey: "sk-mock",
        model: "mock-model",
      },
    ],
    defaultSource: "anthropic:mock-model",
  };

  await env.hub.sessionService.launchSession({
    agentAddress: AGENT_ADDRESS,
    agentId: AGENT_ID,
    instanceId: AGENT_ID,
    config,
    deployContent: {
      systemPrompt: "You are an integration test agent.",
    },
    // The hub is wired with a `workspace-builtins` package-registry
    // asset containing a synthetic `@intx/tools-mail@0.1.2` tarball;
    // pinning here exercises the resolver → asset-pack fan-out →
    // sidecar loader path end-to-end. The model-facing tool surfaces
    // in the message test below as
    // `@intx/tools-mail/sidecar-bundle:mail_send`.
    toolPackagePins: [{ name: "@intx/tools-mail", version: "0.1.2" }],
  });

  // Record the indices before the setup message lands so the
  // message-side-effect test can locate the resulting inference
  // request and the inference.start event by absolute position
  // rather than by delta-from-an-earlier-mutation.
  setupInferenceRequestIndex = env.inference.requests.length;
  setupAgentEventsBeforeMessage = env.hub.agentEvents.length;

  const messageKeyPair = await generateKeyPair();
  const messageCrypto = createNodeCrypto(messageKeyPair);
  const messageHeaders: MessageHeaders = {
    from: "user@integration.interchange",
    to: [AGENT_ADDRESS],
    cc: undefined,
    date: new Date(),
    messageId: "<test-msg-1@integration.interchange>",
    subject: undefined,
    inReplyTo: undefined,
    references: undefined,
    mimeVersion: "1.0",
    interchangeType: "conversation.message",
    interchangeCorrelationId: undefined,
    interchangeTenantId: undefined,
    interchangeAgentId: undefined,
    interchangeSessionId: SESSION_ID,
    interchangeOfferingId: undefined,
    interchangeSchemaVersion: undefined,
    traceparent: undefined,
    tracestate: undefined,
  };
  const messageSignedContent = assembleSignedContent({
    kind: "conversation",
    text: "Hello.",
  });
  const messageSignature = await createDetachedSignatureFromProvider(
    messageSignedContent,
    messageCrypto,
  );
  const messageRaw = assembleMessage(
    messageHeaders,
    messageSignedContent,
    messageSignature,
  );
  env.hub.router.routeMail(
    AGENT_ADDRESS,
    Buffer.from(messageRaw).toString("base64"),
  );

  // Wait for the FULL message lifecycle to land so the downstream
  // sync test sees populated agent state on disk. The inference
  // request arriving is not enough -- it only signals the reactor
  // dispatched -- so we also wait for the `message.run.ended`
  // event, which fires after the reactor's context-store write
  // completes. Without this, the sync test races the state file
  // appearing on disk and the supervisor pushes an empty pack
  // (rejected by the kind handler with `tree must include at
  // least one state-bearing top-level entry`).
  function setupHasEventType(event: unknown, type: string): boolean {
    return (
      typeof event === "object" &&
      event !== null &&
      "type" in event &&
      event.type === type
    );
  }
  await waitFor(
    () => env.inference.requests.length > setupInferenceRequestIndex,
    { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
  );
  await waitFor(
    () =>
      env.hub.agentEvents
        .slice(setupAgentEventsBeforeMessage)
        .some((e) => setupHasEventType(e.event, "message.run.ended")),
    { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
  );
});

afterAll(async () => {
  await env.teardown();
});

describe("deploy flow integration", () => {
  test("sidecar registers with hub", () => {
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("launchSession writes, packs, provisions, delivers, and starts", async () => {
    // The launchSession call lives in `beforeAll` so the downstream
    // lifecycle tests can run under a `-t` filter; this test asserts
    // the side effects the call produced.

    // The deploy ack should have arrived (provision phase completed).
    const publicKey = env.hub.deployAcks.get(AGENT_ADDRESS);
    expect(publicKey).toBeDefined();
    if (publicKey === undefined) throw new Error("unreachable");
    expect(publicKey.length).toBeGreaterThan(0);

    // The agent should now be routable (session start completed).
    expect(env.hub.router.getRoutableAddresses()).toContain(AGENT_ADDRESS);

    // The deploy tree should have landed on the sidecar's disk.
    const agentDir = createSidecarRepoStore({
      dataDir: env.sidecar.dataDir,
    }).getAgentDir(AGENT_ADDRESS);

    await waitFor(
      async () => {
        try {
          await fs.promises.access(path.join(agentDir, "deploy", "prompt.md"));
          return true;
        } catch {
          return false;
        }
      },
      { diagnostics: env.sidecarDiagnostics },
    );

    const prompt = await fs.promises.readFile(
      path.join(agentDir, "deploy", "prompt.md"),
      "utf-8",
    );
    expect(prompt).toContain("integration test agent");
  });

  test("send message and inference receives the asset-backed mail tool", async () => {
    // The setup message landed in `beforeAll`; this test asserts the
    // resulting inference request and event chain. Reading by the
    // recorded indices keeps the assertions correct regardless of
    // whether prior tests in the suite also bumped the counters.
    const req = env.inference.requests[setupInferenceRequestIndex];
    if (req === undefined) throw new Error("unreachable");
    const tools = req.tools ?? [];
    const toolNames = tools.map((t) => t.name);
    // The synthetic tarball seeded into the workspace-builtins
    // package-registry asset publishes a single `mail_send`
    // definition under the `@intx/tools-mail/sidecar-bundle` factory
    // id; the loader prefixes the definition name with the factory id
    // to yield the qualified tool name the model sees.
    expect(toolNames).toContain("@intx/tools-mail/sidecar-bundle:mail_send");

    // reactor.start may or may not have arrived before
    // `setupAgentEventsBeforeMessage` was captured (it depends on how
    // fast contextStore.load() resolves), so wait until we see an
    // inference.start event rather than assuming it is the very first
    // new event.
    function hasEventType(
      event: unknown,
      type: string,
    ): event is { type: string } {
      return (
        typeof event === "object" &&
        event !== null &&
        "type" in event &&
        event.type === type
      );
    }

    await waitFor(
      () =>
        env.hub.agentEvents
          .slice(setupAgentEventsBeforeMessage)
          .some((e) => hasEventType(e.event, "inference.start")),
      { diagnostics: env.sidecarDiagnostics },
    );

    const inferenceStartEvent = env.hub.agentEvents
      .slice(setupAgentEventsBeforeMessage)
      .find((e) => hasEventType(e.event, "inference.start"));
    if (inferenceStartEvent === undefined) throw new Error("unreachable");
    expect(inferenceStartEvent.addr).toBe(AGENT_ADDRESS);
    expect(inferenceStartEvent.sid).toBe(SESSION_ID);
  });

  test("sync request triggers state push to hub repo", async () => {
    const packCountBefore = env.hub.statePacks.length;
    env.hub.router.sendSyncRequest(AGENT_ADDRESS);

    // 30s headroom: the state-pack push travels the
    // sidecar -> hub pack-push pipeline and the default
    // `waitFor` 10s budget flaked ~10% of runs on busier
    // machines.
    await waitFor(() => env.hub.statePacks.length > packCountBefore, {
      timeoutMs: 30_000,
      diagnostics: env.sidecarDiagnostics,
    });

    const last = env.hub.statePacks[env.hub.statePacks.length - 1];
    if (last === undefined) throw new Error("unreachable");
    expect(last.agentAddress).toBe(AGENT_ADDRESS);
    expect(last.ref).toMatch(/^refs\//);
    expect(last.commitSha).toMatch(/^[0-9a-f]{40}$/);
    expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);

    // Verify the pack was actually persisted in the hub's git repo.
    const hubAgentDir = path.join(
      env.hub.hubDataDir,
      "agents",
      parseAgentId(AGENT_ADDRESS),
    );
    const resolvedSha = await git.resolveRef({
      fs,
      dir: hubAgentDir,
      ref: last.ref,
    });
    expect(resolvedSha).toBe(last.commitSha);

    // Verify the commit object is readable (pack was properly indexed).
    const { commit } = await git.readCommit({
      fs,
      dir: hubAgentDir,
      oid: last.commitSha,
    });
    expect(commit.tree).toMatch(/^[0-9a-f]{40}$/);
  });

  test("endSession undeploys agent and cleans up sidecar", async () => {
    await env.hub.sessionService.endSession(AGENT_ADDRESS, "test_complete");

    // Agent should no longer be routable after ack.
    expect(env.hub.router.getRoutableAddresses()).not.toContain(AGENT_ADDRESS);

    // The ack is sent after deleteAgentDir completes, so the directory
    // is already gone by the time the promise resolves.
    const agentDir = createSidecarRepoStore({
      dataDir: env.sidecar.dataDir,
    }).getAgentDir(AGENT_ADDRESS);
    const dirExists = await fs.promises
      .access(agentDir)
      .then(() => true)
      .catch(() => false);
    expect(dirExists).toBe(false);
  });
});
