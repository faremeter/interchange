// The cross-tenant "my agents" list (`GET /api/me/agents`) is served from
// `workflow_definition`: agents are folded onto it. The endpoint lists the
// folded population only (origin_agent_id not null), so a native
// workflow-origin definition -- owned by the workflow surface -- must not
// appear here even though it lives in the same table.

import { describe, test, expect, afterEach } from "bun:test";
import { type } from "arktype";
import postgres from "postgres";

import { generateId } from "@intx/hub-common";
import { loadHarnessDbConfig } from "@intx/test-harness/db-harness";

import {
  harnessHubEnvAvailable,
  startHub,
  type HubHandle,
} from "./lib/git-harness";
import {
  apiCall,
  createTenant,
  seedAgentDefinition,
  signUpUser,
} from "./lib/git-asset-fixtures";

const AgentListResponse = type({
  data: type({
    name: "string",
    tenantId: "string",
    status: "string",
    "+": "ignore",
  }).array(),
  "+": "ignore",
});

const stops: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const stop of stops.splice(0)) {
    await stop();
  }
});

async function startHubTracked(): Promise<HubHandle> {
  const hub = await startHub();
  stops.push(hub.stop);
  return hub;
}

// Insert a native workflow-origin definition (origin_agent_id null) directly,
// so the test can prove it is excluded from the agent list.
async function seedNativeDefinition(
  schema: string,
  tenantId: string,
  name: string,
): Promise<void> {
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
    await sql`insert into workflow_definition (id, tenant_id, name)
              values (${generateId("workflowDefinition")}, ${tenantId}, ${name})`;
  } finally {
    await sql.end();
  }
}

// Rename a folded definition so its name diverges from the mirrored agent row
// the fold produced it from. The list must then show the definition's name,
// which proves it reads workflow_definition rather than the agent table.
async function renameDefinition(
  schema: string,
  definitionId: string,
  name: string,
): Promise<void> {
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
    await sql`update workflow_definition set name = ${name}
              where id = ${definitionId}`;
  } finally {
    await sql.end();
  }
}

describe.skipIf(!harnessHubEnvAvailable())("GET /api/me/agents", () => {
  test("lists folded agents from the definition and excludes native ones", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);
    const tenant = await createTenant(hub.url, user);

    const folded = await seedAgentDefinition(
      hub.schema,
      user,
      tenant,
      "folded-agent",
    );
    // Diverge the definition's name from the agent row it was folded from, so
    // the list showing "folded-def" (not "folded-agent") proves it reads
    // workflow_definition, not the agent table.
    await renameDefinition(hub.schema, folded.definitionId, "folded-def");
    await seedNativeDefinition(hub.schema, tenant.tenantId, "native-workflow");

    const res = await apiCall(
      hub.url,
      "GET",
      "/api/me/agents",
      undefined,
      user.cookies,
    );
    expect(res.status).toBe(200);
    const body = AgentListResponse(res.data);
    if (body instanceof type.errors) {
      throw new Error(`unexpected /api/me/agents response: ${body.summary}`);
    }
    const names = body.data.map((a) => a.name);
    // The definition's name, not the agent row's -- proves the source table.
    expect(names).toContain("folded-def");
    expect(names).not.toContain("folded-agent");
    // The native workflow-origin definition is excluded (folded-only).
    expect(names).not.toContain("native-workflow");

    const item = body.data.find((a) => a.name === "folded-def");
    expect(item?.tenantId).toBe(tenant.tenantId);
    expect(item?.status).toBe("deployed");
  });
});
