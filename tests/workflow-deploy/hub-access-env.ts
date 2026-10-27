// The environment the hub-access tests share: the real hub REST app served on a
// real port, a sidecar env whose router delivers each run's hub tokens pinned
// to that port, and a caller who deploys and triggers runs through the real
// routes. A run's mediated fetch reaches the app over HTTP; the test's own
// requests go to the app in process, so every request on the port came from a
// run.

import { type } from "arktype";

import { createNoopCredentialCipher } from "@intx/crypto";
import { createGrantStore, createRunTokenStore } from "@intx/db";
import { tenant as tenantTable } from "@intx/db/schema";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createAssetService,
  createRunCredentialsLoader,
  type EventCollectorRegistry,
} from "@intx/hub-sessions";
import type { GrantRequirement } from "@intx/types";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
import { createTestDb, type TestDb } from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedGrant,
  seedPrincipal,
  seedPrincipalKey,
  seedTenants,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type StartDeployFlowEnvOpts,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";

export const DEPLOYMENT_DOMAIN = "integration.interchange";

const HubCallOutcomes = type({
  "[string]": {
    status: "number",
    id: "string | null",
    message: "string | null",
  },
});

/** The hub call outcomes one fixture action step returned, by call name. */
export function hubCallOutcomes(
  events: readonly WorkflowRunEvent[],
  stepId: string,
) {
  const completed = events.find(
    (event) =>
      event.type === "StepCompleted" && event.body["stepId"] === stepId,
  );
  if (completed === undefined) {
    throw new Error(`no StepCompleted event for step ${stepId}`);
  }
  const ref = type({ ref: "string" }).assert(completed.body["output"]).ref;
  if (!ref.startsWith("inline:")) {
    throw new Error(`step ${stepId} output is not an inline ref: ${ref}`);
  }
  return HubCallOutcomes.assert(JSON.parse(ref.slice("inline:".length)));
}

/** A grant requirement for the run to read itself, from its creator. */
export const SELF_READ: GrantRequirement = {
  resource: "workflow-run:self",
  action: "read",
  source: "creator",
};

// The refusal each check gives, so a 403 shows which check refused a call.
export const NO_GRANT = "You do not have permission to perform this action";
export const OFF_LIST = "A workflow run token cannot use this route";
export const WRONG_TENANT = "Token is not valid for this tenant";

// The test's own requests carry this header. A request without it has no
// session, so only a run token can authenticate a call from a run.
const SESSION_HEADER = "x-test-session";

const MOCK_SOURCE_ID = "anthropic:mock-model";

/** A request that reached the hub's port, so a request from a run. */
export type ReceivedRequest = {
  path: string;
  authorization: string | null;
  status: number;
};

export type HubAccessEnv = {
  h: TestDb;
  env: DeployFlowEnv;
  /** The origin every delivered hub credential is pinned to. */
  hubOrigin: string;
  tenantId: string;
  callerPrincipalId: string;
  /** The `/api/tenants/:tenantId/workflows/runs` path of the tenant. */
  runsPath: string;
  received: ReceivedRequest[];
  /** A request from the caller's session, not from a run. */
  asCaller(path: string, init?: RequestInit): Promise<Response>;
  /** Seed the tenant, another tenant and the caller, and grant the caller. */
  seedTenancy(otherTenantId: string): Promise<void>;
  /** Grant the caller one more action on one resource. */
  grantCaller(resource: string, action: string): Promise<void>;
  /**
   * Deploy a source entry by source-ref under its own anchor and asset, and
   * return the deployment's public signing key.
   */
  deploy(args: {
    anchorRunId: string;
    entryModule: (mailAddress: string) => string;
    /** The principal the launch spec records as deployer; the caller when omitted. */
    deployerPrincipalId?: string;
    /** The `interchange.actions` module, for a workflow with actions. */
    actions?: string;
    /** The `interchange.loops` module, for a workflow with a loop. */
    loops?: string;
    /** The step that runs an agent, for a workflow with an agent step. */
    agentStepId?: string;
  }): Promise<{ publicKey: string }>;
  /**
   * Trigger a run through the real route, which mints the run's hub token,
   * and return without waiting for the run. The run id is the anchor id.
   */
  trigger(anchorRunId: string): Promise<void>;
  /** Trigger a run through the real route and wait for it to complete. */
  runOnce(anchorRunId: string): Promise<readonly WorkflowRunEvent[]>;
  teardown(): Promise<void>;
};

function sessionFor(userId: string): GetSession {
  const now = new Date("2026-01-01");
  return async (headers) =>
    headers.get(SESSION_HEADER) === null
      ? null
      : {
          user: {
            id: userId,
            email: "caller@example.com",
            emailVerified: true,
            name: "Caller",
            createdAt: now,
            updatedAt: now,
          },
          session: {
            id: "session_hub_access",
            userId,
            token: "tok_hub_access",
            expiresAt: new Date("2999-01-01"),
            createdAt: now,
            updatedAt: now,
          },
        };
}

function notImpl(name: string): never {
  throw new Error(`hub-access mock: ${name} not implemented`);
}

function createMockEventCollectors(): EventCollectorRegistry {
  return {
    create: () => notImpl("create"),
    dispatch: () => notImpl("dispatch"),
    abandon: () => notImpl("abandon"),
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

export async function startHubAccessEnv(args: {
  tenantId: string;
  callerUserId: string;
  callerPrincipalId: string;
  envOpts?: Omit<StartDeployFlowEnvOpts, "loadRunCredentials">;
}): Promise<HubAccessEnv> {
  const { tenantId, callerUserId, callerPrincipalId } = args;
  // One cipher seals the run tokens and unseals them for the app and the
  // loader. The deploy also decrypts the mock inference key through it, and
  // that key is plaintext, so the cipher is the no-op one.
  const cipher = createNoopCredentialCipher();
  const h = await createTestDb();
  const received: ReceivedRequest[] = [];
  const built: { app?: ReturnType<typeof createApp> } = {};
  const requireApp = () => {
    if (built.app === undefined) {
      throw new Error("the hub app is not built yet");
    }
    return built.app;
  };

  // The hub's origin goes into each delivered credential, so the server
  // starts before the sidecar env; the app it serves is built after.
  const hub = Bun.serve({
    port: 0,
    fetch: async (request) => {
      const response = await requireApp().fetch(request);
      received.push({
        path: new URL(request.url).pathname,
        authorization: request.headers.get("authorization"),
        status: response.status,
      });
      return response;
    },
  });
  const env = await startDeployFlowEnv({
    ...args.envOpts,
    loadRunCredentials: createRunCredentialsLoader({
      db: h.db,
      runTokenStore: createRunTokenStore({ db: h.db, cipher }),
      hubApiOrigin: hub.url.origin,
    }),
  });
  built.app = createApp({
    getSession: sessionFor(callerUserId),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createGrantStore(h.db),
    credentialCipher: cipher,
    sidecarRouter: env.hub.router,
    sessionService: env.hub.sessionService,
    eventCollectors: createMockEventCollectors(),
    assetService: createAssetService({
      db: h.db,
      repoStore: env.hub.agentRepoStore.repoStore,
    }),
    repoStore: env.hub.agentRepoStore.repoStore,
    maxTarballBytes: 10_000_000,
  });

  const asCaller = async (path: string, init: RequestInit = {}) => {
    const headers = new Headers(init.headers);
    headers.set("content-type", "application/json");
    headers.set(SESSION_HEADER, "1");
    return await requireApp().request(path, { ...init, headers });
  };

  const grantCaller = async (resource: string, action: string) => {
    await seedGrant(h.db, {
      id: `grant-caller-${action}-${resource}`,
      tenantId,
      resource,
      action,
      effect: "allow",
      origin: "system",
      principalId: callerPrincipalId,
    });
  };

  const source: InferenceSource = {
    id: MOCK_SOURCE_ID,
    provider: "anthropic",
    baseURL: `http://localhost:${String(env.inference.server.port)}`,
    credentialId: "sk-mock",
    model: "mock-model",
  };

  const trigger = async (anchorRunId: string) => {
    const res = await asCaller(
      `/api/tenants/${tenantId}/workflows/${anchorRunId}/mail`,
      { method: "POST", body: JSON.stringify({ content: "call the hub" }) },
    );
    if (res.status !== 202) {
      throw new Error(
        `expected 202 from /mail, got ${String(res.status)}: ${await res.text()}\n${env.sidecarDiagnostics()}`,
      );
    }
  };

  return {
    h,
    env,
    hubOrigin: hub.url.origin,
    tenantId,
    callerPrincipalId,
    runsPath: `/api/tenants/${tenantId}/workflows/runs`,
    received,
    asCaller,
    grantCaller,

    async seedTenancy(otherTenantId) {
      await h.reset();
      await h.db.insert(tenantTable).values({
        id: tenantId,
        name: tenantId,
        slug: tenantSlugFromId(tenantId),
        domain: DEPLOYMENT_DOMAIN,
        parentId: null,
      });
      await seedTenants(h.db, [{ id: otherTenantId }]);
      await seedPrincipal(h.db, {
        id: callerPrincipalId,
        tenantId,
        kind: "user",
        refId: callerUserId,
        status: "active",
      });
      await seedPrincipalKey(h.db, callerPrincipalId);
      // The creator can read every run. A run's `self` requirement asks only
      // for its own, so a run that reads another run would show the
      // requirement copied the creator's grant instead of binding `self`.
      await grantCaller("workflow-run:*", "read");
    },

    async deploy(deployArgs) {
      const { anchorRunId } = deployArgs;
      const definitionAssetId = `ast_${anchorRunId.replaceAll("-", "_")}`;
      await seedAsset(h.db, {
        id: definitionAssetId,
        tenantId,
        kind: "workflow",
        name: definitionAssetId,
        creatorPrincipalId: callerPrincipalId,
      });
      await grantCaller(`workflow-run:${anchorRunId}`, "manage");
      const mailAddress = deriveRunAddress({
        runId: anchorRunId,
        domain: DEPLOYMENT_DOMAIN,
      });
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: anchorRunId,
        tenantId: "tenant-1",
        principalId: `prin_${anchorRunId}`,
        agentAddress: mailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [source],
        defaultSource: MOCK_SOURCE_ID,
      };
      const handle = await deployWorkflowSourceForTest(env, {
        entryModule: deployArgs.entryModule(mailAddress),
        ...(deployArgs.actions !== undefined
          ? { actions: deployArgs.actions }
          : {}),
        ...(deployArgs.loops !== undefined ? { loops: deployArgs.loops } : {}),
        ...(deployArgs.agentStepId !== undefined
          ? { sources: { [deployArgs.agentStepId]: [source] } }
          : {}),
        db: h.db,
        tenantId,
        definitionAssetId,
        anchorRunId,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: mailAddress,
        approvals: "approve-probed",
        // The launch spec records the deployer and carries the approved grant
        // snapshot the trigger reads.
        sourceAuthorityPrincipalId:
          deployArgs.deployerPrincipalId ?? callerPrincipalId,
        config,
        credentialCipher: cipher,
      });
      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(mailAddress),
        { diagnostics: env.sidecarDiagnostics },
      );
      return { publicKey: handle.publicKey };
    },

    trigger,

    async runOnce(anchorRunId) {
      await trigger(anchorRunId);
      // The trigger runs the anchor itself, so the run id is the anchor id.
      const terminal = await waitForWorkflowRunComplete(
        env,
        anchorRunId,
        anchorRunId,
        { diagnostics: env.sidecarDiagnostics },
      );
      if (terminal.type !== "RunCompleted") {
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      return await readWorkflowRunEvents(env, anchorRunId, anchorRunId);
    },

    async teardown() {
      await env.teardown();
      await hub.stop(true);
      await h.close();
    },
  };
}
