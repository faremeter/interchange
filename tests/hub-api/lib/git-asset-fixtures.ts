// Shared fixtures for the asset smart-HTTP integration tests.
//
// These helpers compose on top of `git-harness`: they sign up a user
// via the betterAuth REST endpoint, create a tenant (which makes the
// signing user the tenant owner), create an asset via the REST
// endpoint, and mint a git token with the requested ref pattern and
// actions. The tests then drive `git clone` / `git fetch` /
// `git push` against the resulting smart-HTTP URL using the token in
// an askpass shim.
//
// All helpers fail loudly. If a setup step returns an unexpected
// status the helper throws with the server's response body so the
// test failure is actionable.

import { type } from "arktype";
import postgres from "postgres";

import { hexEncode } from "@intx/types";
import type { RepoAction } from "@intx/types/sidecar";
import { generateId } from "@intx/hub-common";
import { loadHarnessDbConfig } from "@intx/test-harness/db-harness";

import { tokenAskpassEnv } from "./git-harness";

export type SignedUpUser = {
  userId: string;
  email: string;
  password: string;
  cookies: string[];
};

export type CreatedTenant = {
  tenantId: string;
  slug: string;
};

export type CreatedAsset = {
  assetId: string;
  tenantId: string;
  kind: "skill" | "agent-state";
  name: string;
};

export type MintedGitToken = {
  tokenId: string;
  secret: string;
};

export type TokenAuthEnv = Record<string, string>;

type ApiResult = {
  status: number;
  data: unknown;
  cookies: string[];
};

function mergeCookies(existing: string[], setCookies: string[]): string[] {
  const out = [...existing];
  for (const sc of setCookies) {
    const name = sc.split("=")[0];
    if (name === undefined || name === "") continue;
    const value = sc.split(";")[0];
    if (value === undefined || value === "") continue;
    const idx = out.findIndex((c) => c.startsWith(`${name}=`));
    if (idx >= 0) {
      out[idx] = value;
    } else {
      out.push(value);
    }
  }
  return out;
}

export async function apiCall(
  base: string,
  method: string,
  path: string,
  body?: unknown,
  cookies: string[] = [],
): Promise<ApiResult> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (cookies.length > 0) {
    headers["Cookie"] = cookies.join("; ");
  }
  const res = await fetch(`${base}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    redirect: "manual",
  });
  const setCookies = res.headers.getSetCookie();
  const newCookies = mergeCookies(cookies, setCookies);
  let data: unknown = null;
  const ct = res.headers.get("content-type") ?? "";
  if (ct.includes("json")) {
    data = await res.json();
  }
  return { status: res.status, data, cookies: newCookies };
}

function randomSuffix(): string {
  return hexEncode(crypto.getRandomValues(new Uint8Array(4)));
}

function jsonSummary(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

const StringField = type({ id: "string" });

const SignUpResponse = type({
  "user?": { id: "string" },
});

const MintResponse = type({
  id: "string",
  secret: "string",
});

function extractIdField(data: unknown): string {
  const parsed = StringField(data);
  if (parsed instanceof type.errors) {
    throw new Error(
      `expected an id field in response, got ${jsonSummary(data)}: ${parsed.summary}`,
    );
  }
  return parsed.id;
}

export async function signUpUser(
  hubUrl: string,
  opts?: { emailPrefix?: string },
): Promise<SignedUpUser> {
  const suffix = randomSuffix();
  const email = `${opts?.emailPrefix ?? "tester"}-${suffix}@example.invalid`;
  const password = `password-${suffix}-AbcdEfgh`;
  const res = await apiCall(hubUrl, "POST", "/api/auth/sign-up/email", {
    name: `Test ${suffix}`,
    email,
    password,
  });
  if (res.cookies.length === 0) {
    throw new Error(
      `sign-up did not yield a session cookie: status=${res.status} body=${jsonSummary(res.data)}`,
    );
  }
  const parsed = SignUpResponse(res.data);
  if (parsed instanceof type.errors) {
    throw new Error(
      `sign-up response did not validate: status=${res.status} body=${jsonSummary(res.data)}: ${parsed.summary}`,
    );
  }
  const userId = parsed.user?.id;
  if (userId === undefined || userId === "") {
    throw new Error(
      `sign-up did not return a user.id: status=${res.status} body=${jsonSummary(res.data)}`,
    );
  }
  return {
    userId,
    email,
    password,
    cookies: res.cookies,
  };
}

export async function createTenant(
  hubUrl: string,
  user: SignedUpUser,
  opts?: { slugPrefix?: string },
): Promise<CreatedTenant> {
  const suffix = randomSuffix();
  const slug = `${opts?.slugPrefix ?? "t"}${suffix}`;
  const name = `Tenant ${suffix}`;
  const res = await apiCall(
    hubUrl,
    "POST",
    "/api/tenants",
    { name, slug },
    user.cookies,
  );
  if (res.status !== 201) {
    throw new Error(
      `create tenant failed: status=${res.status} body=${jsonSummary(res.data)}`,
    );
  }
  const tenantId = extractIdField(res.data);
  return { tenantId, slug };
}

// Insert an `agent` row and its folded `workflow_definition` directly,
// bypassing any HTTP surface -- every agent is folded, so the two always
// coexist. The agent-state definition-mode git route resolves the repo by
// looking the definition up by (origin_agent_id, tenantId), so a real folded
// definition must exist for the smart-HTTP request to pass tenant binding
// rather than 404. Returns both ids: the definition-mode repo keys on the
// agent id, while any session/run row keys on the definition id. Runs against
// the hub's per-test schema via search_path.
export async function seedAgentDefinition(
  schema: string,
  user: SignedUpUser,
  tenant: CreatedTenant,
  name: string,
): Promise<{ agentId: string; definitionId: string }> {
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
    const principalRows = await sql<
      { id: string }[]
    >`select id from principal where tenant_id = ${tenant.tenantId} and kind = 'user' and ref_id = ${user.userId}`;
    const creatorPrincipal = principalRows[0];
    if (creatorPrincipal === undefined) {
      throw new Error(
        `seedAgentDefinition: no principal for user ${user.userId} in tenant ${tenant.tenantId}`,
      );
    }
    const agentId = generateId("agent");
    const definitionId = generateId("workflowDefinition");
    await sql`insert into agent (id, tenant_id, creator_principal_id, name)
              values (${agentId}, ${tenant.tenantId}, ${creatorPrincipal.id}, ${name})`;
    // The folded definition the definition-mode route resolves by
    // origin_agent_id, and that any session/run row keys on.
    await sql`insert into workflow_definition
                (id, tenant_id, creator_principal_id, origin_agent_id, name)
              values (${definitionId}, ${tenant.tenantId},
                ${creatorPrincipal.id}, ${agentId}, ${name})`;
    return { agentId, definitionId };
  } finally {
    await sql.end();
  }
}

export async function createAsset(
  hubUrl: string,
  user: SignedUpUser,
  tenant: CreatedTenant,
  opts: { kind?: "skill" | "agent-state"; name?: string } = {},
): Promise<CreatedAsset> {
  const kind = opts.kind ?? "skill";
  const name = opts.name ?? `asset-${randomSuffix()}`;
  const res = await apiCall(
    hubUrl,
    "POST",
    `/api/tenants/${tenant.tenantId}/assets`,
    { kind, name },
    user.cookies,
  );
  if (res.status !== 201) {
    throw new Error(
      `create asset failed: status=${res.status} body=${jsonSummary(res.data)}`,
    );
  }
  const assetId = extractIdField(res.data);
  return { assetId, tenantId: tenant.tenantId, kind, name };
}

export type MintTokenOpts = {
  name?: string;
  resource?: string;
  refPattern: string;
  actions: (RepoAction | "can_read" | "can_push")[];
  lifetimeMs?: number;
};

export async function mintTenantGitToken(
  hubUrl: string,
  user: SignedUpUser,
  tenant: CreatedTenant,
  opts: MintTokenOpts,
): Promise<MintedGitToken> {
  const lifetimeMs = opts.lifetimeMs ?? 5 * 60 * 1000;
  const expiresAt = new Date(Date.now() + lifetimeMs).toISOString();
  const res = await apiCall(
    hubUrl,
    "POST",
    `/api/tenants/${tenant.tenantId}/git-tokens`,
    {
      name: opts.name ?? `token-${randomSuffix()}`,
      resource: opts.resource ?? "asset:*",
      refPattern: opts.refPattern,
      actions: opts.actions,
      expiresAt,
    },
    user.cookies,
  );
  if (res.status !== 201) {
    throw new Error(
      `mint git token failed: status=${res.status} body=${jsonSummary(res.data)}`,
    );
  }
  const parsed = MintResponse(res.data);
  if (parsed instanceof type.errors) {
    throw new Error(
      `mint git token response did not validate: status=${res.status} body=${jsonSummary(res.data)}: ${parsed.summary}`,
    );
  }
  return { tokenId: parsed.id, secret: parsed.secret };
}

export function assetSmartHttpUrl(hubUrl: string, asset: CreatedAsset): string {
  return `${hubUrl}/api/tenants/${asset.tenantId}/assets/${asset.kind}/${asset.name}.git`;
}

export async function tokenEnv(token: string): Promise<TokenAuthEnv> {
  return await tokenAskpassEnv(token);
}
