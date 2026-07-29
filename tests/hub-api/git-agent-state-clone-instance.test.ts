// End-to-end clone of an agent-state per-run repo against the
// real `/usr/bin/git`.
//
// The per-run repo is materialised lazily by the sidecar's
// first state pack. These tests cover the read path BEFORE any pack
// has landed: the route layer resolves the folded run by id, the
// advertise layer emits the `capabilities^{}` empty-repo record,
// and `git clone` succeeds with an empty working tree.
//
// We bypass the API-driven launch flow (which requires a connected
// sidecar + a resolvable credential requirement) and insert the folded
// `workflow_run` row + the creator-seed agent-state grant directly
// against the hub's schema. The schema is the same one the spawned
// hub runs against; both processes share the postgres database, so
// the inserts are visible to the running hub immediately.
//
// Note on the wire-format gaps documented at the dispatch level:
//   - Shallow clone is not advertised; `git clone --depth=1` returns
//     empty. Not exercised here.

import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import postgres from "postgres";

import { generateId } from "@intx/hub-common";

import {
  harnessHubEnvAvailable,
  runGit,
  startHub,
  type HubHandle,
} from "./lib/git-harness";
import { loadHarnessDbConfig } from "@intx/test-harness/db-harness";
import {
  createTenant,
  mintTenantGitToken,
  seedAgentDefinition,
  signUpUser,
  tokenEnv,
  type CreatedTenant,
  type SignedUpUser,
} from "./lib/git-asset-fixtures";

const stops: (() => Promise<void>)[] = [];
const tempDirs: string[] = [];

afterEach(async () => {
  for (const stop of stops.splice(0)) {
    await stop();
  }
  await Promise.all(
    tempDirs.splice(0).map((d) => fs.rm(d, { recursive: true, force: true })),
  );
});

async function mkTemp(prefix: string): Promise<string> {
  const d = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

async function startHubTracked(): Promise<HubHandle> {
  const hub = await startHub();
  stops.push(hub.stop);
  return hub;
}

/**
 * Direct insert of a folded `workflow_run` row (instance-shaped: a routing
 * address and no deployment) + the `agent-state:<runId>` creator-read grant.
 * Bypasses the launch endpoint which depends on a connected sidecar + a
 * resolvable credential requirement.
 *
 * Returns the synthetic run id; the caller uses this id in the smart-HTTP URL.
 */
async function seedRunRow(
  schema: string,
  user: SignedUpUser,
  tenant: CreatedTenant,
  definitionId: string,
): Promise<{ runId: string; creatorPrincipalId: string }> {
  const dbConfig = loadHarnessDbConfig();
  const sql = postgres({
    host: dbConfig.host,
    port: dbConfig.port,
    user: dbConfig.user,
    password: dbConfig.password,
    database: dbConfig.database,
    max: 1,
    connection: { search_path: `"${schema.replace(/"/g, '""')}"` },
  });
  try {
    // Look up the creator's tenant principal (created when the
    // tenant was provisioned).
    const principalRows = await sql<
      { id: string }[]
    >`select id from principal where tenant_id = ${tenant.tenantId} and kind = 'user' and ref_id = ${user.userId}`;
    const creatorPrincipal = principalRows[0];
    if (creatorPrincipal === undefined) {
      throw new Error(
        `seedRunRow: no principal for user ${user.userId} in tenant ${tenant.tenantId}`,
      );
    }
    const creatorPrincipalId = creatorPrincipal.id;

    const runId = generateId("instance");

    // Tenant domain controls the run address; lookup the row.
    const tenantRows = await sql<
      { domain: string }[]
    >`select domain from tenant where id = ${tenant.tenantId}`;
    const tenantDomainRow = tenantRows[0];
    if (tenantDomainRow === undefined) {
      throw new Error(`seedRunRow: no tenant row for ${tenant.tenantId}`);
    }
    const address = `${runId}@${tenantDomainRow.domain}`;

    // A folded launch run: `deployment_id` NULL and a routing address, so the
    // route's instance-shape gate resolves it. `principal_id` is the invoker;
    // the route does not consult it, verifying only tenant binding and the
    // bearer-token principal's grant.
    await sql`insert into workflow_run (id, tenant_id, definition_id, principal_id, address, status)
              values (${runId}, ${tenant.tenantId}, ${definitionId}, ${creatorPrincipalId}, ${address}, 'running')`;

    // Mirror the seed grant that the launch path writes: creator reads the
    // per-run agent-state repo. `grant` is a reserved SQL keyword so the table
    // identifier is quoted.
    const grantId = generateId("grant");
    await sql`insert into "grant" (id, tenant_id, principal_id, resource, action, effect, origin)
              values (${grantId}, ${tenant.tenantId}, ${creatorPrincipalId}, ${`agent-state:${runId}`}, 'read', 'allow', 'creator')`;

    return { runId, creatorPrincipalId };
  } finally {
    await sql.end();
  }
}

function runStateGitUrl(
  hubUrl: string,
  tenantId: string,
  runId: string,
): string {
  return `${hubUrl}/api/tenants/${tenantId}/workflows/runs/${runId}/state.git`;
}

describe.skipIf(!harnessHubEnvAvailable())("agent-state per-run clone", () => {
  test("creator clones an empty per-run repo", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const agent = await seedAgentDefinition(
      hub.schema,
      user,
      tenant,
      "clone-creator-agent",
    );
    const run = await seedRunRow(hub.schema, user, tenant, agent.definitionId);

    const token = await mintTenantGitToken(hub.url, user, tenant, {
      resource: `agent-state:${run.runId}`,
      refPattern: "**",
      actions: ["can_read"],
    });

    const env = await tokenEnv(token.secret);
    const cwd = await mkTemp("agent-state-creator-clone-");
    const cloneTarget = path.join(cwd, "repo");
    const remote = runStateGitUrl(hub.url, tenant.tenantId, run.runId);
    const result = await runGit(
      ["-c", "credential.helper=", "clone", remote, cloneTarget],
      { cwd, env },
    );
    if (result.status !== 0) {
      throw new Error(
        `git clone exited ${result.status}: ${result.stderr}\nstdout: ${result.stdout}`,
      );
    }
    // Empty repo: clone succeeds, working tree exists, no refs.
    const verify = await runGit(["rev-parse", "--is-inside-work-tree"], {
      cwd: cloneTarget,
    });
    if (verify.status !== 0) {
      throw new Error(`rev-parse failed: ${verify.stderr}`);
    }
    expect(verify.stdout.trim()).toBe("true");
    const refs = await runGit(["for-each-ref"], { cwd: cloneTarget });
    expect(refs.status).toBe(0);
    expect(refs.stdout.trim()).toBe("");
  }, 90_000);

  test("admin (tenant owner *:*) clones the per-run repo", async () => {
    // The tenant owner role is granted `*:*`; the route layer
    // authz check passes on `agent-state:<id>` `read` via that
    // catch-all. This scenario exercises the admin grant path
    // distinctly from the creator seed grant by minting a token
    // whose only authority is the owner's `*:*` grant — but in
    // this fixture the creator IS the owner, so the two paths
    // overlap. A separate admin-only flow without owner status
    // would require additional tenant member orchestration; here we
    // confirm the admin-grant code path returns 200 by exercising
    // the same `*:*` chain.
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const agent = await seedAgentDefinition(
      hub.schema,
      user,
      tenant,
      "clone-admin-agent",
    );
    const run = await seedRunRow(hub.schema, user, tenant, agent.definitionId);
    const token = await mintTenantGitToken(hub.url, user, tenant, {
      resource: `agent-state:${run.runId}`,
      refPattern: "**",
      actions: ["can_read"],
    });
    const env = await tokenEnv(token.secret);
    const cwd = await mkTemp("agent-state-admin-clone-");
    const remote = runStateGitUrl(hub.url, tenant.tenantId, run.runId);
    const result = await runGit(
      ["-c", "credential.helper=", "clone", remote, path.join(cwd, "repo")],
      { cwd, env },
    );
    if (result.status !== 0) {
      throw new Error(
        `git clone exited ${result.status}: ${result.stderr}\nstdout: ${result.stdout}`,
      );
    }
  }, 90_000);

  test("non-tenant token is denied at advertise", async () => {
    // A second user signs up and creates their own tenant; the
    // bearer middleware binds the token to that other tenant. Used
    // against the original tenant's instance URL, the middleware
    // rejects with 403 tenant_mismatch — the cleanest "this
    // principal has no business reading that repo" surface this
    // test file can produce without orchestrating fine-grained
    // member roles. Stock `git clone` surfaces 403 as
    // `http 403`/`forbidden` in stderr.
    const hub = await startHubTracked();
    const userA = await signUpUser(hub.url);
    const tenantA = await createTenant(hub.url, userA);
    const agent = await seedAgentDefinition(
      hub.schema,
      userA,
      tenantA,
      "clone-denied-agent",
    );
    const run = await seedRunRow(
      hub.schema,
      userA,
      tenantA,
      agent.definitionId,
    );

    const userB = await signUpUser(hub.url);
    const tenantB = await createTenant(hub.url, userB);
    const tokenB = await mintTenantGitToken(hub.url, userB, tenantB, {
      resource: `agent-state:${run.runId}`,
      refPattern: "**",
      actions: ["can_read"],
    });

    const advertiseUrl = `${runStateGitUrl(
      hub.url,
      tenantA.tenantId,
      run.runId,
    )}/info/refs?service=git-upload-pack`;
    const res = await fetch(advertiseUrl, {
      headers: {
        Authorization: `Bearer ${tokenB.secret}`,
      },
    });
    expect(res.status).toBe(403);
  }, 90_000);
});
