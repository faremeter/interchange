// End-to-end clone of an agent-state per-instance repo against the
// real `/usr/bin/git`.
//
// The per-instance repo is materialised lazily by the sidecar's
// first state pack. These tests cover the read path BEFORE any pack
// has landed: the route layer resolves the instance row by id, the
// advertise layer emits the `capabilities^{}` empty-repo record,
// and `git clone` succeeds with an empty working tree.
//
// We bypass the API-driven instance launch flow (which requires a
// connected sidecar + a resolvable credential requirement) and
// insert the `agent_instance` row + supporting principal /
// agent_session rows + the creator-seed agent-state grant directly
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
  loadHarnessDbConfig,
  runGit,
  startHub,
  type HubHandle,
} from "./lib/git-harness";
import {
  apiCall,
  createTenant,
  mintTenantGitToken,
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

type CreatedAgent = { agentId: string };

async function createAgentDefinition(
  hubUrl: string,
  user: SignedUpUser,
  tenantId: string,
  name: string,
): Promise<CreatedAgent> {
  const res = await apiCall(
    hubUrl,
    "POST",
    `/api/tenants/${tenantId}/agents/definitions`,
    {
      name,
      systemPrompt: "you are a test agent",
    },
    user.cookies,
  );
  if (res.status !== 201) {
    throw new Error(
      `create agent failed: status=${res.status} body=${JSON.stringify(res.data)}`,
    );
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the route returns AgentResponse; only `id` is needed downstream
  const data = res.data as { id: string };
  return { agentId: data.id };
}

/**
 * Direct insert of an `agent_instance` row + the supporting
 * `agent_session` row + the `agent-state:<instanceId>` creator-read
 * grant. Bypasses the launch endpoint which depends on a connected
 * sidecar + a resolvable credential requirement.
 *
 * Returns the synthetic instance id; the caller uses this id in the
 * smart-HTTP URL.
 */
async function seedInstanceRow(
  schema: string,
  user: SignedUpUser,
  tenant: CreatedTenant,
  agentId: string,
): Promise<{ instanceId: string; creatorPrincipalId: string }> {
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
        `seedInstanceRow: no principal for user ${user.userId} in tenant ${tenant.tenantId}`,
      );
    }
    const creatorPrincipalId = creatorPrincipal.id;

    const instanceId = generateId("instance");
    const sessionId = generateId("session");

    // Tenant domain controls the agent address; lookup the row.
    const tenantRows = await sql<
      { domain: string }[]
    >`select domain from tenant where id = ${tenant.tenantId}`;
    const tenantDomainRow = tenantRows[0];
    if (tenantDomainRow === undefined) {
      throw new Error(`seedInstanceRow: no tenant row for ${tenant.tenantId}`);
    }
    const address = `${instanceId}@${tenantDomainRow.domain}`;

    // The session row carries the invoker's principal id.
    await sql`insert into agent_session (id, tenant_id, agent_id, principal_id, status)
              values (${sessionId}, ${tenant.tenantId}, ${agentId}, ${creatorPrincipalId}, 'active')`;

    // The instance row points back to the invoker principal for
    // `principal_id` so we don't have to mint a new agent principal
    // to satisfy the FK. The route layer does not consult
    // `principal_id` on the instance row; it only verifies tenant
    // binding and resolves the bearer-token principal separately.
    await sql`insert into agent_instance (id, agent_id, tenant_id, principal_id, address, session_id, status)
              values (${instanceId}, ${agentId}, ${tenant.tenantId}, ${creatorPrincipalId}, ${address}, ${sessionId}, 'deployed')`;

    // Mirror the seed grant that the launch path would have written:
    // creator reads the per-instance agent-state repo. `grant` is a
    // reserved SQL keyword so the table identifier is quoted.
    const grantId = generateId("grant");
    await sql`insert into "grant" (id, tenant_id, principal_id, resource, action, effect, origin)
              values (${grantId}, ${tenant.tenantId}, ${creatorPrincipalId}, ${`agent-state:${instanceId}`}, 'read', 'allow', 'creator')`;

    return { instanceId, creatorPrincipalId };
  } finally {
    await sql.end();
  }
}

function instanceStateGitUrl(
  hubUrl: string,
  tenantId: string,
  instanceId: string,
): string {
  return `${hubUrl}/api/tenants/${tenantId}/agents/instances/${instanceId}/state.git`;
}

describe("agent-state per-instance clone", () => {
  test("creator clones an empty per-instance repo", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);
    const agent = await createAgentDefinition(
      hub.url,
      user,
      tenant.tenantId,
      "clone-creator-agent",
    );
    const instance = await seedInstanceRow(
      hub.schema,
      user,
      tenant,
      agent.agentId,
    );

    const token = await mintTenantGitToken(hub.url, user, tenant, {
      resource: `agent-state:${instance.instanceId}`,
      refPattern: "**",
      actions: ["can_read"],
    });

    const env = await tokenEnv(token.secret);
    const cwd = await mkTemp("agent-state-creator-clone-");
    const cloneTarget = path.join(cwd, "repo");
    const remote = instanceStateGitUrl(
      hub.url,
      tenant.tenantId,
      instance.instanceId,
    );
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

  test("admin (tenant owner *:*) clones the per-instance repo", async () => {
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
    const agent = await createAgentDefinition(
      hub.url,
      user,
      tenant.tenantId,
      "clone-admin-agent",
    );
    const instance = await seedInstanceRow(
      hub.schema,
      user,
      tenant,
      agent.agentId,
    );
    const token = await mintTenantGitToken(hub.url, user, tenant, {
      resource: `agent-state:${instance.instanceId}`,
      refPattern: "**",
      actions: ["can_read"],
    });
    const env = await tokenEnv(token.secret);
    const cwd = await mkTemp("agent-state-admin-clone-");
    const remote = instanceStateGitUrl(
      hub.url,
      tenant.tenantId,
      instance.instanceId,
    );
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
    const agent = await createAgentDefinition(
      hub.url,
      userA,
      tenantA.tenantId,
      "clone-denied-agent",
    );
    const instance = await seedInstanceRow(
      hub.schema,
      userA,
      tenantA,
      agent.agentId,
    );

    const userB = await signUpUser(hub.url);
    const tenantB = await createTenant(hub.url, userB);
    const tokenB = await mintTenantGitToken(hub.url, userB, tenantB, {
      resource: `agent-state:${instance.instanceId}`,
      refPattern: "**",
      actions: ["can_read"],
    });

    const advertiseUrl = `${instanceStateGitUrl(
      hub.url,
      tenantA.tenantId,
      instance.instanceId,
    )}/info/refs?service=git-upload-pack`;
    const res = await fetch(advertiseUrl, {
      headers: {
        Authorization: `Bearer ${tokenB.secret}`,
      },
    });
    expect(res.status).toBe(403);
  }, 90_000);
});
