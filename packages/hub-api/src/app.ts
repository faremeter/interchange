import type { Handler, MiddlewareHandler } from "hono";
import { Hono } from "hono";
import { openAPIRouteHandler } from "hono-openapi";

import { honoLogger, type HonoContext } from "@intx/log/hono";
import { timeWindowEvaluator } from "@intx/authz";
import { createNoopCredentialCipher } from "@intx/crypto";
import { getLogger } from "@intx/log";
import type { CredentialCipher } from "@intx/types";
import {
  type DB,
  type ApprovalStore,
  type SignalCorrelationStore,
  createGrantStore,
  createApprovalStore,
  createSignalCorrelationStore,
} from "@intx/db";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";

import type { AppEnv } from "./context";
import { createSessionMiddleware } from "./middleware/session";
import { createRequireGrant, type RequireGrant } from "./middleware/grant";
import { createResolveTenant, requireAuth } from "./middleware/tenant";
import type { GetSession } from "./session";
import type {
  AssetService,
  EventCollectorRegistry,
  RepoStore,
  SessionService,
  SidecarRouter,
  WorkflowAllocationService,
  WorkflowDispatchService,
} from "@intx/hub-sessions";

import { createMeRoutes } from "./routes/me";
import { createTenantRoutes } from "./routes/tenants";
import { createTenantFederationRoutes } from "./routes/tenant-federation";
import { createPrincipalRoutes, createInviteRoutes } from "./routes/principals";
import { createRoleRoutes, createRoleAssignRoutes } from "./routes/roles";
import { createGrantRoutes, createEvaluateRoutes } from "./routes/grants";
import { createRunRoutes } from "./routes/runs";
import { createWorkflowRoutes } from "./routes/workflows";
import { createWorkflowDefinitionRoutes } from "./routes/workflow-definitions";
import {
  createApprovalRoutes,
  type ReadRunLifecycles,
} from "./routes/approvals";
import { readDurableWorkflowRunLifecycles } from "./workflow-run-lifecycle";
import { createWalletRoutes } from "./routes/wallets";
import { createProviderRoutes } from "./routes/providers";
import { createOAuthClientRoutes } from "./routes/oauth-clients";
import { createCredentialRoutes } from "./routes/credentials";
import { createOfferingRoutes } from "./routes/offerings";
import {
  createModelCatalogRoutes,
  createModelDiscoveryRoutes,
} from "./routes/models";
import { createModelProviderRoutes } from "./routes/model-providers";
import { createModelOfferingRoutes } from "./routes/model-offerings";
import { createObservabilityRoutes } from "./routes/observability";
import { createAgentDataRoutes } from "./routes/agent-data";
import {
  createMeGitTokenRoutes,
  createTenantGitTokenRoutes,
} from "./routes/git-tokens";
import {
  ASSET_OPENAPI_EXCLUDE_GLOBS,
  createAssetRoutes,
} from "./routes/assets";
import {
  AGENT_STATE_OPENAPI_EXCLUDE_GLOBS,
  createAgentStateRunGitRoutes,
  createAgentStateReceivePackDeny,
} from "./routes/agent-state-git";
import { createGitTokenAuth } from "./middleware/git-token-auth";

const log = getLogger(["hub", "app"]);

/**
 * Resolve the credential cipher, falling back to the noop cipher when none is
 * provided. The fallback is expected in tests and local development; it warns so
 * a production deployment that forgot to configure a cipher is not silently
 * storing secrets unencrypted. The composition root (`apps/hub`) always supplies
 * a real env-key cipher, gated by a required `CREDENTIAL_ENCRYPTION_KEY` at boot.
 */
function resolveCredentialCipher(
  provided: CredentialCipher | undefined,
): CredentialCipher {
  if (provided) return provided;
  log.warn(
    "No credentialCipher configured; credential secrets will NOT be encrypted at rest. Expected in tests/local dev but MUST NOT happen in production.",
  );
  return createNoopCredentialCipher();
}

export type CreateHubContextMiddlewareDeps = {
  getSession: GetSession;
};

/**
 * Builds the per-request context middleware that resolves the
 * authenticated user and session from the incoming request and
 * exposes them via the Hono variable bag.
 */
export function createHubContextMiddleware({
  getSession,
}: CreateHubContextMiddlewareDeps): MiddlewareHandler<AppEnv> {
  return createSessionMiddleware(getSession);
}

export type MountHubRoutesDeps = {
  db: DB["db"];
  sidecarRouter: SidecarRouter;
  sessionService: SessionService;
  workflowAllocationService?: WorkflowAllocationService;
  workflowDispatchService?: WorkflowDispatchService;
  eventCollectors: EventCollectorRegistry;
  /**
   * Encrypts credential secrets at rest on the credential/oauth write paths.
   * Optional: when omitted, a noop cipher is used and a warning is logged.
   * Production supplies a real cipher; tests that do not exercise credential
   * secrets can omit it.
   */
  credentialCipher?: CredentialCipher;
  grantStore?: GrantStore;
  conditionRegistry?: ConditionRegistry;
  approvalStore?: ApprovalStore;
  signalCorrelationStore?: SignalCorrelationStore;
  readRunLifecycles?: ReadRunLifecycles;
  sidecarWsHandler?: Handler<AppEnv>;
  /**
   * The asset REST endpoint and smart-HTTP route group mount under
   * `/api/tenants/:tenantId/assets` when both are supplied. Tests
   * that have no reason to exercise the asset surface MUST pass
   * `null` for both to opt out explicitly; passing only one is a
   * wiring bug and throws at construction.
   */
  assetService: AssetService | null;
  repoStore: RepoStore | null;
  /**
   * Maximum tarball payload accepted by the package-registry PUT
   * endpoint. The hub edge owns the value; tests that exercise the
   * asset surface supply their own cap.
   */
  maxTarballBytes: number;
};

/**
 * Mounts every hub route group, middleware, and supporting endpoint
 * onto the provided Hono application. The caller is responsible for
 * having mounted the request logger and the context middleware first,
 * and for wiring their own auth handler at the path of their choice.
 *
 * `grantStore` and `conditionRegistry` default to the hub's standard
 * choices (a database-backed grant store and the time-window condition
 * evaluator) when not supplied.
 */
export function mountHubRoutes(
  app: Hono<AppEnv>,
  opts: MountHubRoutesDeps,
): void {
  const credentialCipher = resolveCredentialCipher(opts.credentialCipher);
  const {
    db,
    sidecarRouter,
    sessionService,
    workflowAllocationService,
    workflowDispatchService,
    eventCollectors,
    sidecarWsHandler,
    assetService,
    repoStore,
    readRunLifecycles,
    maxTarballBytes,
  } = opts;
  if ((assetService === null) !== (repoStore === null)) {
    throw new Error(
      "mountHubRoutes: assetService and repoStore must be provided together or both omitted",
    );
  }
  const grantStore = opts.grantStore ?? createGrantStore(db);
  const conditionRegistry: ConditionRegistry = opts.conditionRegistry ?? {
    time_window: timeWindowEvaluator,
  };
  const requireGrant: RequireGrant = createRequireGrant({
    grantStore,
    conditionRegistry,
  });
  const approvalStore = opts.approvalStore ?? createApprovalStore(db);
  const signalCorrelationStore =
    opts.signalCorrelationStore ?? createSignalCorrelationStore(db);
  const resolveTenant = createResolveTenant({ db });

  app.get("/status", (c) => c.json({ status: "ok" }));

  // User-scoped (cross-tenant) -- requires auth but not tenant membership
  app.use("/api/me/*", requireAuth);
  app.route("/api/me", createMeRoutes({ db }));

  // The git-tokens mint surface mounts under the same gate as the
  // smart-HTTP route groups: tokens are only useful when at least one
  // smart-HTTP route consumes them. Both deps null = no smart-HTTP
  // anywhere = no token-mint endpoints.
  if (repoStore !== null) {
    app.route("/api/me/git-tokens", createMeGitTokenRoutes({ db }));
  }

  // Smart-HTTP asset routes use bearer authentication instead of
  // session+tenant resolution. The bearer middleware mounts ahead of
  // resolveTenant so it populates `principal` + `tenant` first; the
  // tenant resolver short-circuits when both are already set, which
  // lets bearer-only requests bypass the session-required path.
  //
  // The gate is `repoStore !== null` rather than the two-dep check;
  // the XOR throw above already guarantees the deps move as a unit,
  // so checking either one is equivalent. Keeping a single shape
  // across every gate site makes the contract obvious to a reader.
  if (repoStore !== null) {
    // Constrain `:nameDotGit` to the `.git` suffix so the bearer
    // middleware does not capture the REST tarball routes that share
    // the `/api/tenants/:tenantId/assets/...` prefix.
    app.use(
      "/api/tenants/:tenantId/assets/:kind/:nameDotGit{[^/]+\\.git}/*",
      createGitTokenAuth({ db }),
    );
  }

  // Agent-state smart-HTTP read routes also use bearer auth. The
  // receive-pack denial middleware mounts BEFORE bearer auth so an
  // unauthenticated `git push -v` parses the pkt-line ERR record
  // rather than a generic 401. The bearer middleware then gates the
  // upload-pack half (advertise + POST) on a valid token.
  if (repoStore !== null) {
    app.use(
      "/api/tenants/:tenantId/workflows/runs/:runId/state.git/*",
      createAgentStateReceivePackDeny(),
    );
    app.use(
      "/api/tenants/:tenantId/workflows/runs/:runId/state.git/*",
      createGitTokenAuth({ db }),
    );
  }

  // Tenant-scoped middleware -- require auth + tenant membership for any
  // path under /api/tenants/:tenantId/*. Must be registered before routes
  // so Hono includes it in the middleware chain.
  app.use("/api/tenants/:tenantId/*", resolveTenant);

  // Global tenant routes (create needs auth, detail/update handle auth inline)
  app.route("/api/tenants", createTenantRoutes({ db }));

  // Tenant-scoped routes
  app.route(
    "/api/tenants/:tenantId/principals",
    createPrincipalRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/members/invite",
    createInviteRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/roles",
    createRoleRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/principals/:principalId/roles",
    createRoleAssignRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/grants",
    createGrantRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/principals/:principalId/evaluate",
    createEvaluateRoutes({ db, grantStore, conditionRegistry }),
  );
  // The run management surface -- list, observe, stop, mail a single run.
  // Mounted as `/workflows/runs` (tenant-wide runs) before the `/workflows`
  // deploy router below, and its literal `runs` segment out-prioritizes that
  // router's `:runId`, so `/workflows/runs` never resolves as a
  // deployment id.
  app.route(
    "/api/tenants/:tenantId/workflows/runs",
    createRunRoutes({
      db,
      sessionService,
      sidecarRouter,
      eventCollectors,
      repoStore,
      ...(workflowDispatchService !== undefined
        ? { workflowDispatchService }
        : {}),
      grantStore,
      conditionRegistry,
      requireGrant,
      approvalStore,
    }),
  );

  // Definition version/rollback management needs neither the asset service
  // nor the repo store, so it mounts unconditionally -- definition management
  // stays available even when the gated `/workflows` deploy surface is off.
  // Registered before that surface as a defensive measure: the concrete
  // `/workflows/definitions/...` paths do not overlap the deploy router's
  // `/:runId` patterns, so this ordering is belt-and-suspenders.
  app.route(
    "/api/tenants/:tenantId/workflows/definitions",
    createWorkflowDefinitionRoutes({ db, requireGrant }),
  );

  // The workflow deploy + signal + listing surface reads the workflow-run
  // repo through the repo store (its run-observe routes and the mail-send
  // trigger's terminal-state read). Gate on the repo store being present.
  if (repoStore !== null) {
    app.route(
      "/api/tenants/:tenantId/workflows",
      createWorkflowRoutes({
        db,
        ...(workflowAllocationService !== undefined
          ? { workflowAllocationService }
          : {}),
        ...(workflowDispatchService !== undefined
          ? { workflowDispatchService }
          : {}),
        sidecarRouter,
        repoStore,
        grantStore,
        requireGrant,
      }),
    );
  }

  app.route(
    "/api/tenants/:tenantId/approvals",
    createApprovalRoutes({
      db,
      sidecarRouter,
      ...(workflowDispatchService !== undefined
        ? { workflowDispatchService }
        : {}),
      ...(readRunLifecycles !== undefined
        ? { readRunLifecycles }
        : repoStore !== null
          ? {
              readRunLifecycles: async (
                agentAddress: string,
                topLevelRunId: string,
                targetRunId: string,
              ) => {
                const lifecycles = await readDurableWorkflowRunLifecycles(
                  repoStore,
                  agentAddress,
                  [topLevelRunId, targetRunId],
                );
                return {
                  topLevel: lifecycles.get(topLevelRunId) ?? "absent",
                  target: lifecycles.get(targetRunId) ?? "absent",
                };
              },
            }
          : {}),
      grantStore,
      conditionRegistry,
      approvalStore,
      signalCorrelationStore,
    }),
  );
  app.route(
    "/api/tenants/:tenantId/wallets",
    createWalletRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/providers",
    createProviderRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/oauth-clients",
    createOAuthClientRoutes({ db, requireGrant, credentialCipher }),
  );
  app.route(
    "/api/tenants/:tenantId/credentials",
    createCredentialRoutes({
      db,
      sidecarRouter,
      requireGrant,
      credentialCipher,
    }),
  );
  app.route(
    "/api/tenants/:tenantId/offerings",
    createOfferingRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/catalog/models",
    createModelCatalogRoutes({
      db,
      sidecarRouter,
      requireGrant,
      credentialCipher,
    }),
  );
  app.route(
    "/api/tenants/:tenantId/catalog/providers",
    createModelProviderRoutes({
      db,
      sidecarRouter,
      requireGrant,
      credentialCipher,
    }),
  );
  app.route(
    "/api/tenants/:tenantId/catalog/offerings",
    createModelOfferingRoutes({
      db,
      sidecarRouter,
      requireGrant,
      credentialCipher,
    }),
  );
  app.route(
    "/api/tenants/:tenantId/models",
    createModelDiscoveryRoutes({ db, requireGrant }),
  );
  if (repoStore !== null) {
    app.route(
      "/api/tenants/:tenantId/git-tokens",
      createTenantGitTokenRoutes({ db, requireGrant }),
    );
  }
  // Default-deny the observability and agent-data stub surfaces. Both are 501
  // stubs that carry no requireGrant of their own, so without this any active
  // tenant member would reach them; a mount-level grant check makes them fail
  // closed instead. No grants are minted for these resources today, so this is
  // a pure deny until the features ship. When they do, REFINE the resource and
  // action per route -- agent-data's history restore is a write, not a read,
  // and observability's agent logs/metrics belong under an observability
  // resource rather than the agent-data one this shared prefix applies -- do
  // not remove the gate. These two prefixes are stub-only; no live route
  // resolves under them.
  app.use(
    "/api/tenants/:tenantId/agents/:agentId/*",
    requireGrant("agent-data:*", "read"),
  );
  app.use(
    "/api/tenants/:tenantId/traces/*",
    requireGrant("observability:*", "read"),
  );
  app.route("/api/tenants/:tenantId", createObservabilityRoutes());
  app.route(
    "/api/tenants/:tenantId/federation",
    createTenantFederationRoutes({ db }),
  );
  app.route("/api/tenants/:tenantId/agents/:agentId", createAgentDataRoutes());

  if (assetService !== null && repoStore !== null) {
    app.route(
      "/api/tenants/:tenantId/assets",
      createAssetRoutes({
        db,
        assetService,
        repoStore,
        grantStore,
        conditionRegistry,
        requireGrant,
        maxTarballBytes,
      }),
    );
  }

  if (repoStore !== null) {
    // The folded run's agent-state clone surface. Mounts at `/workflows/runs`
    // alongside the run-management routes; the git sub-paths (`:runId/state.git`)
    // are disjoint from the run routes, and the literal `runs` segment
    // out-ranks the `/workflows/:runId` deploy router.
    app.route(
      "/api/tenants/:tenantId/workflows/runs",
      createAgentStateRunGitRoutes({
        db,
        repoStore,
        grantStore,
        conditionRegistry,
      }),
    );
  }

  if (sidecarWsHandler) {
    app.get("/api/sidecars/ws", sidecarWsHandler);
  }
}

export type CreateAppOpts = {
  getSession: GetSession;
  authHandler: Handler<AppEnv>;
  db: DB["db"];
  sidecarRouter: SidecarRouter;
  sessionService: SessionService;
  workflowAllocationService?: WorkflowAllocationService;
  workflowDispatchService?: WorkflowDispatchService;
  eventCollectors: EventCollectorRegistry;
  /**
   * Encrypts credential secrets at rest on the credential/oauth write paths.
   * Optional: when omitted, a noop cipher is used and a warning is logged.
   * Production supplies a real cipher; tests that do not exercise credential
   * secrets can omit it.
   */
  credentialCipher?: CredentialCipher;
  grantStore?: GrantStore;
  approvalStore?: ApprovalStore;
  signalCorrelationStore?: SignalCorrelationStore;
  readRunLifecycles?: ReadRunLifecycles;
  sidecarWsHandler?: Handler<AppEnv>;
  assetService: AssetService | null;
  repoStore: RepoStore | null;
  /**
   * Maximum tarball payload accepted by the package-registry PUT
   * endpoint. The hub edge resolves this from `HUB_MAX_TARBALL_BYTES`
   * (or its config default) and supplies a concrete value.
   */
  maxTarballBytes: number;
};

export function createApp({
  getSession,
  authHandler,
  db,
  sidecarRouter,
  sessionService,
  workflowAllocationService,
  workflowDispatchService,
  eventCollectors,
  credentialCipher,
  grantStore,
  approvalStore,
  signalCorrelationStore,
  readRunLifecycles,
  sidecarWsHandler,
  assetService,
  repoStore,
  maxTarballBytes,
}: CreateAppOpts) {
  const app = new Hono<AppEnv>();

  app.use(
    honoLogger({
      category: ["hub", "requests"],
      skip: (c: HonoContext) => c.req.path === "/status",
    }),
  );

  app.use(createHubContextMiddleware({ getSession }));

  app.all("/api/auth/*", authHandler);

  mountHubRoutes(app, {
    db,
    sidecarRouter,
    sessionService,
    ...(workflowAllocationService !== undefined
      ? { workflowAllocationService }
      : {}),
    ...(workflowDispatchService !== undefined
      ? { workflowDispatchService }
      : {}),
    eventCollectors,
    ...(credentialCipher ? { credentialCipher } : {}),
    assetService,
    repoStore,
    maxTarballBytes,
    ...(grantStore ? { grantStore } : {}),
    ...(approvalStore ? { approvalStore } : {}),
    ...(signalCorrelationStore ? { signalCorrelationStore } : {}),
    ...(readRunLifecycles ? { readRunLifecycles } : {}),
    ...(sidecarWsHandler ? { sidecarWsHandler } : {}),
  });

  app.get(
    "/openapi.json",
    openAPIRouteHandler(app, {
      documentation: {
        info: {
          title: "Interchange Hub",
          version: "0.0.0",
        },
      },
      exclude: [
        "/openapi.json",
        "/status",
        "/api/auth/**",
        ...ASSET_OPENAPI_EXCLUDE_GLOBS,
        ...AGENT_STATE_OPENAPI_EXCLUDE_GLOBS,
      ],
    }),
  );

  return app;
}

export type App = ReturnType<typeof createApp>;
