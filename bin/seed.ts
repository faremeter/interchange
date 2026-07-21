#!/usr/bin/env bun
/* eslint-disable no-console */

// Seed script for the local development database.
//
// This module is Node-bound: it spawns child processes via
// `node:child_process`, so it cannot run under a non-Node runtime
// regardless of whether it references Buffer.

import { spawn } from "node:child_process";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { type, type Type } from "arktype";
import {
  type Capability,
  TenantResponse,
  AgentResponse,
  AssetResponse,
  AssetWithOriginResponse,
  PrincipalResponse,
  PrincipalSummary,
  AgentSummary,
  RoleResponse,
  ProviderResponse,
  ModelResponse,
  ModelProviderResponse,
  ModelOfferingResponse,
  GrantResponse,
  paginatedSchema,
} from "@intx/types";
import {
  WORKSPACE_BUILTINS_REGISTRY,
  WORKFLOW_JSON_PATH,
} from "@intx/hub-sessions";
import { extractTarballPackageJSON } from "@intx/tool-packaging";
import { catalogCapabilitiesFor } from "@intx/inference-discovery/catalog";

import {
  buildWorkflowJson,
  WORKFLOW_FIXTURE_ASSET_NAME,
  WORKFLOW_FIXTURE_SIGNAL_NAME,
  WORKFLOW_RUN_GRANT_ACTION,
  WORKFLOW_RUN_GRANT_RESOURCE,
} from "./workflow-fixture";
import {
  catalogModels,
  catalogProviders,
  type CatalogOfferingSpec,
} from "./lib/catalog-seed-data";

// Resolve an offering's advertised capabilities: the wire capabilities the
// discovery matrix proved for its `discoverySource` tuple (empty when the tuple
// has not been probed), plus the hand-curated model capabilities the matrix
// cannot prove. Reading the matrix through the helper here keeps the wire set
// from ever drifting from what discovery captured.
function offeringCapabilities(offering: CatalogOfferingSpec): Capability[] {
  const wire = offering.discoverySource
    ? catalogCapabilitiesFor(
        offering.discoverySource.provider,
        offering.discoverySource.model,
      )
    : [];
  return [...wire, ...offering.curatedCapabilities];
}

const AuthResponse = type({ "user?": { id: "string" } });

const SEED_ROOT = resolve(import.meta.dirname ?? ".", "..");
const BUILTINS_DIR = resolve(SEED_ROOT, "dist", "builtins");

// Read every tarball under `dist/builtins/` and pin `(name, version)`
// using the bytes the publisher will actually upload. Reading the
// source packages' `package.json` instead would silently desynchronize
// from the on-disk tarballs whenever a version is bumped without
// re-running `make builtins`: the publish step would upload a tarball
// whose filename still encodes the old version, but the seed would
// pin the new version and the sidecar would fail with `tarball.missing`
// at launch time. Going through the artifacts directly keeps the seed
// honest about what was built.
async function readBuiltinPins(): Promise<{ name: string; version: string }[]> {
  let entries: string[];
  try {
    entries = readdirSync(BUILTINS_DIR);
  } catch (cause) {
    throw new Error(
      `seed: cannot read ${BUILTINS_DIR}; run \`make builtins\` first`,
      { cause },
    );
  }
  const tarballs = entries.filter((f) => f.endsWith(".tgz")).sort();
  if (tarballs.length === 0) {
    throw new Error(
      `seed: ${BUILTINS_DIR} contains no *.tgz files; run \`make builtins\` first`,
    );
  }
  const pins: { name: string; version: string }[] = [];
  for (const filename of tarballs) {
    const abs = resolve(BUILTINS_DIR, filename);
    const bytes = readFileSync(abs);
    const outcome = await extractTarballPackageJSON(new Uint8Array(bytes));
    if (outcome.kind !== "ok") {
      throw new Error(
        `seed: ${abs} did not yield a usable package.json (${outcome.kind})`,
      );
    }
    pins.push({
      name: outcome.parsed.name,
      version: outcome.parsed.version,
    });
  }
  return pins;
}

function parse<T extends Type>(
  schema: T,
  data: unknown,
  label: string,
): T["infer"] {
  const result = schema(data);
  if (result instanceof type.errors) {
    console.error(`Seed validation failed for ${label}: ${result.summary}`);
    process.exit(1);
  }
  return result;
}

const BASE = process.env["HUB_URL"] ?? "http://localhost:3000";

// Built-in tool-package pins shared by the three workspace agents. The
// matching tarballs are published into the `workspace-builtins`
// package-registry asset by `bin/publish-tool-packages.ts`; the hub's
// session-service scope-routing config maps the `@intx` scope onto
// that asset. Pins are read directly from the artifacts under
// `dist/builtins/` so the seed cannot pin a version that the build
// step never produced — the previous behavior (consulting each
// source package.json) silently desynchronized whenever a version
// was bumped without re-running `make builtins`.
const BUILTIN_TOOL_PACKAGES = await readBuiltinPins();

type CookieJar = string[];

async function api(
  method: string,
  path: string,
  body?: unknown,
  cookies: CookieJar = [],
): Promise<{ status: number; data: unknown; cookies: CookieJar }> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (cookies.length > 0) {
    headers["Cookie"] = cookies.join("; ");
  }

  const res = await fetch(`${BASE}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
    redirect: "manual",
  });

  const newCookies = [...cookies];
  const setCookies = res.headers.getSetCookie();
  for (const sc of setCookies) {
    const name = sc.split("=")[0];
    if (!name) continue;
    const idx = newCookies.findIndex((c) => c.startsWith(`${name}=`));
    const value = sc.split(";")[0];
    if (!value) continue;
    if (idx >= 0) {
      newCookies[idx] = value;
    } else {
      newCookies.push(value);
    }
  }

  let data: unknown = null;
  const ct = res.headers.get("content-type") ?? "";
  if (ct.includes("json")) {
    data = await res.json();
  }

  return { status: res.status, data, cookies: newCookies };
}

function log(msg: string) {
  process.stdout.write(`[seed] ${msg}\n`);
}

function check(label: string, status: number, expected: number, data: unknown) {
  if (status !== expected) {
    process.stderr.write(
      `[seed] FAIL ${label}: expected ${expected}, got ${status}\n`,
    );
    process.stderr.write(`[seed]   ${JSON.stringify(data)}\n`);
    process.exit(1);
  }
  log(`  OK ${label} (${status})`);
}

function checkOrSkip(
  label: string,
  status: number,
  expected: number,
  data: unknown,
): boolean {
  if (status === expected) {
    log(`  OK ${label} (${status})`);
    return true;
  }
  if (status === 409) {
    log(`  SKIP ${label} (already exists)`);
    return false;
  }
  process.stderr.write(
    `[seed] FAIL ${label}: expected ${expected}, got ${status}\n`,
  );
  process.stderr.write(`[seed]   ${JSON.stringify(data)}\n`);
  process.exit(1);
}

// A row that a create returned 201 or 409 for must appear in the subsequent
// list. When it does not, the seed is in an inconsistent state (a create that
// did not persist, a list that truncated), so fail loudly rather than skip
// and leave dangling rows behind.
function fatalMissing(label: string): never {
  process.stderr.write(`[seed] FAIL ${label}: not found after create\n`);
  process.exit(1);
}

// -- Authenticate users (sign up, or sign in if they already exist) --

async function authenticate(
  name: string,
  email: string,
  password: string,
): Promise<{ cookies: CookieJar; userId: string }> {
  const signUp = await api("POST", "/api/auth/sign-up/email", {
    name,
    email,
    password,
  });

  // better-auth's sign-up returns 200 on success and 422
  // UNPROCESSABLE_ENTITY when the address is already registered.
  // Anything else is a hub-side fault that must surface instead of
  // being papered over by the sign-in fallback.
  if (signUp.status === 200) {
    const userId =
      parse(AuthResponse, signUp.data, "sign-up response").user?.id ?? "";
    return { cookies: signUp.cookies, userId };
  }

  if (signUp.status !== 422) {
    process.stderr.write(
      `[seed] FATAL: unexpected sign-up response for ${email}: ${String(signUp.status)} ${JSON.stringify(signUp.data)}\n`,
    );
    process.exit(1);
  }

  const signIn = await api("POST", "/api/auth/sign-in/email", {
    email,
    password,
  });

  if (signIn.status !== 200) {
    process.stderr.write(`[seed] FATAL: could not authenticate ${email}\n`);
    process.stderr.write(
      `[seed]   sign-up: ${String(signUp.status)} ${JSON.stringify(signUp.data)}\n`,
    );
    process.stderr.write(
      `[seed]   sign-in: ${String(signIn.status)} ${JSON.stringify(signIn.data)}\n`,
    );
    process.exit(1);
  }

  const userId =
    parse(AuthResponse, signIn.data, "sign-in response").user?.id ?? "";
  return { cookies: signIn.cookies, userId };
}

log("Authenticating users...");

const alice = await authenticate(
  "Alice Admin",
  "alice@example.com",
  "password123",
);
const aliceCookies = alice.cookies;
log(`  Alice ID: ${alice.userId}`);

const bob = await authenticate("Bob Builder", "bob@example.com", "password123");
const bobCookies = bob.cookies;
log(`  Bob ID: ${bob.userId}`);

const carol = await authenticate(
  "Carol Creator",
  "carol@example.com",
  "password123",
);
const carolCookies = carol.cookies;
log(`  Carol ID: ${carol.userId}`);

// -- Create tenants (as Alice) --

log("Creating tenants...");

async function ensureTenant(
  name: string,
  slug: string,
  cookies: CookieJar,
): Promise<string> {
  const { status, data } = await api(
    "POST",
    "/api/tenants",
    { name, slug },
    cookies,
  );

  if (status === 201) {
    log(`  Created ${slug} tenant`);
    return parse(TenantResponse, data, "tenant response").id;
  }

  // Already exists -- look up via /me/principals
  const { data: principals } = await api(
    "GET",
    "/api/me/principals",
    undefined,
    cookies,
  );
  const principalList = parse(
    paginatedSchema(PrincipalSummary),
    principals,
    "me/principals response",
  ).data;
  const match = principalList.find((p) => p.tenantSlug === slug);

  if (match) {
    log(`  ${slug} tenant already exists`);
    return match.tenantId;
  }

  process.stderr.write(`[seed] FATAL: could not resolve tenant ${slug}\n`);
  process.exit(1);
}

const acmeTenantId = await ensureTenant("Acme Corp", "acme", aliceCookies);
log(`  Acme tenant ID: ${acmeTenantId}`);

const widgetsTenantId = await ensureTenant(
  "Widget Labs",
  "widgets",
  aliceCookies,
);
log(`  Widgets tenant ID: ${widgetsTenantId}`);

// -- Ensure the workspace-builtins package-registry asset exists --
//
// Agent pins below reference `@intx/tools-*` packages, which the hub's
// scope-routing config maps to the `workspace-builtins` registry. The
// asset row must exist on the Acme tenant so the session-service
// resolver can find it at launch time. `bin/publish-tool-packages.ts`
// (run from `bin/dev.ts` before seed) populates the tarballs; this
// step is a safety net for re-runs of seed against a hub that already
// has the asset row created.

log("Ensuring workspace-builtins package-registry asset...");

const { status: regStatus, data: regData } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/assets`,
  { kind: "package-registry", name: WORKSPACE_BUILTINS_REGISTRY },
  aliceCookies,
);
if (regStatus === 201 || regStatus === 409) {
  log(
    regStatus === 201
      ? "  Created workspace-builtins asset"
      : "  workspace-builtins asset already exists",
  );
} else {
  process.stderr.write(
    `[seed] FAIL ensure workspace-builtins: expected 201 or 409, got ${String(regStatus)}\n`,
  );
  process.stderr.write(`[seed]   ${JSON.stringify(regData)}\n`);
  process.exit(1);
}

// -- Invite Bob to Acme --

log("Inviting Bob to Acme...");

const { data: rolesData } = await api(
  "GET",
  `/api/tenants/${acmeTenantId}/roles`,
  undefined,
  aliceCookies,
);
const rolesList = parse(
  paginatedSchema(RoleResponse),
  rolesData,
  "roles response",
).data;
const memberRole = rolesList.find((r) => r.name === "member");

const { status: inviteStatus, data: inviteData } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/members/invite`,
  { email: "bob@example.com", roleId: memberRole?.id },
  aliceCookies,
);

let bobPrincipalId: string;
if (checkOrSkip("invite bob", inviteStatus, 201, inviteData)) {
  bobPrincipalId = parse(
    PrincipalResponse,
    inviteData,
    "invite bob response",
  ).id;
  await api(
    "PATCH",
    `/api/tenants/${acmeTenantId}/principals/${bobPrincipalId}`,
    { status: "active" },
    aliceCookies,
  );
} else {
  // Already invited -- find Bob's principal
  const { data: acmePrincipals } = await api(
    "GET",
    `/api/tenants/${acmeTenantId}/principals`,
    undefined,
    aliceCookies,
  );
  const acmePrincipalList = parse(
    paginatedSchema(PrincipalResponse),
    acmePrincipals,
    "acme principals response",
  ).data;
  const bobP = acmePrincipalList.find((p) => p.refId === bob.userId);
  bobPrincipalId = bobP?.id ?? "";
}
log(`  Bob principal ID: ${bobPrincipalId}`);

// -- Invite Carol to Widgets --

log("Inviting Carol to Widgets...");

const { data: widgetRoles } = await api(
  "GET",
  `/api/tenants/${widgetsTenantId}/roles`,
  undefined,
  aliceCookies,
);
const widgetRolesList = parse(
  paginatedSchema(RoleResponse),
  widgetRoles,
  "widget roles response",
).data;
const widgetAdminRole = widgetRolesList.find((r) => r.name === "admin");

const { status: carolInviteStatus, data: carolInviteData } = await api(
  "POST",
  `/api/tenants/${widgetsTenantId}/members/invite`,
  { email: "carol@example.com", roleId: widgetAdminRole?.id },
  aliceCookies,
);
if (
  checkOrSkip(
    "invite carol to widgets",
    carolInviteStatus,
    201,
    carolInviteData,
  )
) {
  const carolPrincipalId = parse(
    PrincipalResponse,
    carolInviteData,
    "invite carol response",
  ).id;
  await api(
    "PATCH",
    `/api/tenants/${widgetsTenantId}/principals/${carolPrincipalId}`,
    { status: "active" },
    aliceCookies,
  );
}

// -- Create agent roles in Acme --

log("Creating agent roles in Acme...");

async function ensureRole(
  tenantId: string,
  name: string,
  description: string,
  grants: { resource: string; action: string; effect: string }[],
  cookies: CookieJar,
): Promise<string> {
  const { status, data } = await api(
    "POST",
    `/api/tenants/${tenantId}/roles`,
    { name, description },
    cookies,
  );

  let roleId: string;
  let roleCreated = false;
  if (status === 201) {
    roleId = parse(RoleResponse, data, `create role ${name} response`).id;
    roleCreated = true;
    log(`  Created role ${name}`);
  } else if (status === 409) {
    log(`  SKIP role ${name} (already exists)`);
    const { data: allRoles } = await api(
      "GET",
      `/api/tenants/${tenantId}/roles?limit=200`,
      undefined,
      cookies,
    );
    const found = parse(
      paginatedSchema(RoleResponse),
      allRoles,
      `roles list response`,
    ).data.find((r) => r.name === name);
    if (!found) {
      process.stderr.write(`[seed] FATAL: could not find role ${name}\n`);
      process.exit(1);
    }
    roleId = found.id;
  } else {
    process.stderr.write(
      `[seed] FAIL create role ${name}: expected 201, got ${status}\n`,
    );
    process.stderr.write(`[seed]   ${JSON.stringify(data)}\n`);
    process.exit(1);
  }

  if (roleCreated) {
    for (const g of grants) {
      const { status: grantStatus, data: grantData } = await api(
        "POST",
        `/api/tenants/${tenantId}/grants`,
        {
          roleId,
          resource: g.resource,
          action: g.action,
          effect: g.effect,
          origin: "role",
        },
        cookies,
      );
      check(
        `grant ${g.resource}/${g.action} on role ${name}`,
        grantStatus,
        201,
        grantData,
      );
    }
  }

  return roleId;
}

const researchRoleId = await ensureRole(
  acmeTenantId,
  "research-bot",
  "Grants for the Research Bot agent",
  [
    { resource: "documents:*", action: "read", effect: "allow" },
    { resource: "documents:*", action: "write", effect: "ask" },
    { resource: "tool:mail_*", action: "invoke", effect: "allow" },
  ],
  aliceCookies,
);

const codeReviewRoleId = await ensureRole(
  acmeTenantId,
  "code-review-bot",
  "Grants for the Code Review Bot agent",
  [
    { resource: "repos:*", action: "read", effect: "allow" },
    { resource: "repos:*", action: "comment", effect: "allow" },
    { resource: "tool:mail_*", action: "invoke", effect: "allow" },
  ],
  aliceCookies,
);

// -- Create agents in Acme --

log("Creating agents in Acme...");

const { status: a1Status, data: a1Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/agents/definitions`,
  {
    name: "Research Bot",
    description: "Researches topics and summarizes findings",
    systemPrompt:
      "You are a research assistant. Find and summarize information. When you receive a mail message, reply to it immediately with a helpful response. Do not wait for further instructions.",
    modelConfig: { defaultModel: "kimi-k2.6" },
    modelRequirements: [
      { model: "claude-sonnet-4", capabilities: ["long-context"] },
    ],
    toolPackages: BUILTIN_TOOL_PACKAGES,
    capabilities: { research: true, summarize: true },
    credentialRequirements: [
      { providerName: "OpenCode Go", source: "tenant", scopes: ["chat"] },
    ],
    roleIds: [researchRoleId],
  },
  aliceCookies,
);
checkOrSkip("create research bot", a1Status, 201, a1Data);
const researchBotId =
  a1Status === 201
    ? parse(AgentResponse, a1Data, "research bot response").id
    : null;
if (researchBotId) log(`  Research Bot ID: ${researchBotId}`);

const { status: a2Status, data: a2Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/agents/definitions`,
  {
    name: "Code Review Bot",
    description: "Reviews pull requests and suggests improvements",
    systemPrompt:
      "You are a code reviewer. Analyze code for bugs and improvements.",
    modelConfig: { defaultModel: "kimi-k2.6" },
    toolPackages: BUILTIN_TOOL_PACKAGES,
    capabilities: { codeReview: true },
    credentialRequirements: [
      { providerName: "OpenCode Go", source: "tenant", scopes: ["chat"] },
      { providerName: "GitHub", source: "tenant", scopes: ["repo"] },
    ],
    roleIds: [codeReviewRoleId],
  },
  aliceCookies,
);
checkOrSkip("create code review bot", a2Status, 201, a2Data);
const codeReviewBotId =
  a2Status === 201
    ? parse(AgentResponse, a2Data, "code review bot response").id
    : null;
if (codeReviewBotId) log(`  Code Review Bot ID: ${codeReviewBotId}`);

const codingRoleId = await ensureRole(
  acmeTenantId,
  "coding-agent",
  "Grants for the Coding Agent with full filesystem and LSP access",
  [
    { resource: "tool:read_file", action: "invoke", effect: "allow" },
    { resource: "tool:write_file", action: "invoke", effect: "allow" },
    { resource: "tool:edit_file", action: "invoke", effect: "allow" },
    { resource: "tool:run_shell", action: "invoke", effect: "allow" },
    { resource: "tool:search_files", action: "invoke", effect: "allow" },
    { resource: "tool:grep", action: "invoke", effect: "allow" },
    { resource: "tool:lsp", action: "invoke", effect: "allow" },
    { resource: "tool:mail_*", action: "invoke", effect: "allow" },
  ],
  aliceCookies,
);

const { status: a4Status, data: a4Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/agents/definitions`,
  {
    name: "Coding Agent",
    description:
      "Software engineering agent with filesystem, shell, and language server access",
    systemPrompt: `You are a software engineering agent. You have access to the filesystem, a shell, and a language server for code navigation and diagnostics.

Use the file tools (read_file, write_file, edit_file, search_files, grep) to explore and modify code. Use run_shell to execute build commands, run tests, and interact with version control. Use the lsp tool for code intelligence operations like go-to-definition, find-references, hover information, and symbol search.

When you edit or write files, the language server will automatically report type errors and diagnostics. Pay attention to these diagnostics and fix any issues before declaring your work complete.

When you receive a task via mail, work through it methodically: understand the codebase, plan your approach, implement the changes, verify they build and pass tests, then report back with what you did.`,
    modelConfig: { defaultModel: "kimi-k2.6" },
    toolPackages: BUILTIN_TOOL_PACKAGES,
    capabilities: { coding: true, fileSystem: true, languageServer: true },
    credentialRequirements: [
      { providerName: "OpenCode Go", source: "tenant", scopes: ["chat"] },
    ],
    roleIds: [codingRoleId],
  },
  aliceCookies,
);
checkOrSkip("create coding agent", a4Status, 201, a4Data);
const codingAgentId =
  a4Status === 201
    ? parse(AgentResponse, a4Data, "coding agent response").id
    : null;
if (codingAgentId) log(`  Coding Agent ID: ${codingAgentId}`);

// -- Create agent role and agent in Widgets --

log("Creating agent in Widgets...");

const supportRoleId = await ensureRole(
  widgetsTenantId,
  "support-bot",
  "Grants for the Customer Support Bot agent",
  [
    { resource: "tickets:*", action: "*", effect: "allow" },
    { resource: "billing:*", action: "read", effect: "allow" },
    { resource: "billing:*", action: "refund", effect: "ask" },
    { resource: "tool:mail_*", action: "invoke", effect: "allow" },
  ],
  aliceCookies,
);

const { status: a3Status, data: a3Data } = await api(
  "POST",
  `/api/tenants/${widgetsTenantId}/agents/definitions`,
  {
    name: "Customer Support Bot",
    description: "Handles customer support tickets",
    systemPrompt:
      "You are a customer support agent. Help customers with their issues.",
    modelConfig: { defaultModel: "claude-sonnet-4-6" },
    capabilities: { ticketManagement: true, knowledgeBase: true },
    credentialRequirements: [
      { providerName: "Anthropic", source: "tenant", scopes: ["chat"] },
      {
        providerName: "Stripe",
        source: "tenant",
        scopes: ["charges:read", "refunds:write"],
      },
    ],
    roleIds: [supportRoleId],
  },
  aliceCookies,
);
checkOrSkip("create support bot", a3Status, 201, a3Data);
const supportBotId =
  a3Status === 201
    ? parse(AgentResponse, a3Data, "support bot response").id
    : null;
if (supportBotId) log(`  Support Bot ID: ${supportBotId}`);

// -- Create custom role and grants --

log("Creating custom role in Acme...");

const { status: crStatus, data: crData } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/roles`,
  { name: "reviewer", description: "Can review and comment on documents" },
  aliceCookies,
);
if (checkOrSkip("create reviewer role", crStatus, 201, crData)) {
  const reviewerRoleId = parse(
    RoleResponse,
    crData,
    "reviewer role response",
  ).id;

  await api(
    "POST",
    `/api/tenants/${acmeTenantId}/grants`,
    {
      roleId: reviewerRoleId,
      resource: "documents:*",
      action: "read",
      effect: "allow",
      origin: "role",
    },
    aliceCookies,
  );

  await api(
    "POST",
    `/api/tenants/${acmeTenantId}/principals/${bobPrincipalId}/roles/${reviewerRoleId}`,
    undefined,
    aliceCookies,
  );
}

// -- Set up federation between Acme and Widgets --

log("Setting up federation...");

const { status: fedStatus, data: fedData } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/federation`,
  {
    targetTenantId: widgetsTenantId,
    direction: "bilateral",
  },
  aliceCookies,
);
if (fedStatus === 404) {
  log("  SKIP federation (endpoint not implemented)");
} else {
  checkOrSkip("create federation trust", fedStatus, 201, fedData);
}

// -- Create providers --

log("Creating providers...");

const { status: prv1Status, data: prv1Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/providers`,
  {
    name: "Anthropic",
    plugin: "anthropic",
    metadata: {
      baseURL: "https://api.anthropic.com",
      defaultModel: "claude-sonnet-4-6",
    },
  },
  aliceCookies,
);
checkOrSkip("create anthropic provider", prv1Status, 201, prv1Data);

const { status: prv2Status, data: prv2Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/providers`,
  {
    name: "GitHub",
    plugin: "github",
    authorizationUrl: "https://github.com/login/oauth/authorize",
    tokenUrl: "https://github.com/login/oauth/access_token",
    userInfoUrl: "https://api.github.com/user",
    scopes: ["repo", "read:user"],
    metadata: { baseURL: "https://api.github.com" },
  },
  aliceCookies,
);
checkOrSkip("create github provider", prv2Status, 201, prv2Data);

const { status: prv3Status, data: prv3Data } = await api(
  "POST",
  `/api/tenants/${widgetsTenantId}/providers`,
  {
    name: "Stripe",
    plugin: "stripe",
    metadata: { baseURL: "https://api.stripe.com" },
  },
  aliceCookies,
);
checkOrSkip("create stripe provider", prv3Status, 201, prv3Data);

const { status: prv4Status, data: prv4Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/providers`,
  {
    name: "OpenCode Go",
    plugin: "openai-compatible",
    metadata: {
      baseURL: "https://opencode.ai/zen/go/v1",
      defaultModel: "kimi-k2.6",
    },
  },
  aliceCookies,
);
checkOrSkip("create opencode-go provider", prv4Status, 201, prv4Data);

// Look up provider IDs (handles re-runs where providers already exist)
const { data: acmeProviders } = await api(
  "GET",
  `/api/tenants/${acmeTenantId}/providers`,
  undefined,
  aliceCookies,
);
const providerList = parse(
  paginatedSchema(ProviderResponse),
  acmeProviders,
  "acme providers response",
);
const anthropicProvider = providerList.data.find((p) => p.name === "Anthropic");
const githubProvider = providerList.data.find((p) => p.name === "GitHub");
const opencodeGoProvider = providerList.data.find(
  (p) => p.name === "OpenCode Go",
);

const { data: widgetProviders } = await api(
  "GET",
  `/api/tenants/${widgetsTenantId}/providers`,
  undefined,
  aliceCookies,
);
const widgetProviderList = parse(
  paginatedSchema(ProviderResponse),
  widgetProviders,
  "widget providers response",
);
const stripeProvider = widgetProviderList.data.find((p) => p.name === "Stripe");

// -- Create OAuth clients --

log("Creating OAuth clients...");

if (githubProvider) {
  const { status: oclStatus, data: oclData } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/oauth-clients`,
    {
      providerId: githubProvider.id,
      name: "Acme GitHub App",
      clientId: "fake-github-client-id",
      clientSecret: "fake-github-client-secret",
      redirectUris: ["http://localhost:3000/api/oauth/callback/github"],
      defaultScopes: ["repo", "read:user"],
    },
    aliceCookies,
  );
  if (oclStatus === 404) {
    log("  SKIP oauth client (endpoint not implemented)");
  } else {
    checkOrSkip("create github oauth client", oclStatus, 201, oclData);
  }
}

// -- Create credentials --

log("Creating credentials...");

if (anthropicProvider) {
  const { status: cred1Status, data: cred1Data } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/credentials`,
    {
      name: "Anthropic API Key",
      type: "api_key",
      providerId: anthropicProvider.id,
      description: "Anthropic key for Research Bot",
      secret: "sk-ant-fake-key-for-seed-data",
      scopes: ["chat"],
    },
    aliceCookies,
  );
  checkOrSkip("create anthropic credential", cred1Status, 201, cred1Data);
}

if (githubProvider) {
  const { status: cred2Status, data: cred2Data } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/credentials`,
    {
      name: "GitHub OAuth Token",
      type: "oauth_token",
      providerId: githubProvider.id,
      description: "GitHub access for Code Review Bot",
      secret: "ghp_fake-github-token-for-seed-data",
      scopes: ["repo", "pull_request"],
    },
    aliceCookies,
  );
  checkOrSkip("create github credential", cred2Status, 201, cred2Data);
}

if (stripeProvider) {
  const { status: cred3Status, data: cred3Data } = await api(
    "POST",
    `/api/tenants/${widgetsTenantId}/credentials`,
    {
      name: "Stripe API Key",
      type: "api_key",
      providerId: stripeProvider.id,
      description: "Stripe key for billing operations",
      secret: "sk_test_fake-stripe-key-for-seed-data",
      scopes: ["charges:read", "refunds:write"],
    },
    aliceCookies,
  );
  checkOrSkip("create stripe credential", cred3Status, 201, cred3Data);
}

if (opencodeGoProvider) {
  const { status: cred4Status, data: cred4Data } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/credentials`,
    {
      name: "OpenCode Go API Key",
      type: "api_key",
      providerId: opencodeGoProvider.id,
      description: "OpenCode Go key for coding agents",
      secret: "REPLACE_WITH_YOUR_OPENCODE_GO_KEY",
      scopes: ["chat"],
    },
    aliceCookies,
  );
  checkOrSkip("create opencode-go credential", cred4Status, 201, cred4Data);
}

// -- Create wallets --

log("Creating wallets...");

const { status: w1Status, data: w1Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/wallets`,
  {
    name: "Operating Budget",
    backendType: "credits",
    currency: "USD",
    config: { monthlyLimit: "10000" },
  },
  aliceCookies,
);
checkOrSkip("create acme wallet", w1Status, 201, w1Data);

const { status: w2Status, data: w2Data } = await api(
  "POST",
  `/api/tenants/${widgetsTenantId}/wallets`,
  {
    name: "Support Budget",
    backendType: "credits",
    currency: "USD",
    config: { monthlyLimit: "5000" },
  },
  aliceCookies,
);
checkOrSkip("create widgets wallet", w2Status, 201, w2Data);

// -- Create offerings --

log("Creating offerings...");

// Get agent IDs from listing if we didn't just create them
const { data: acmeAgents } = await api(
  "GET",
  `/api/tenants/${acmeTenantId}/agents/definitions`,
  undefined,
  aliceCookies,
);
const agentList = parse(
  paginatedSchema(AgentResponse),
  acmeAgents,
  "acme agents response",
).data;
const researchBot = agentList.find((a) => a.name === "Research Bot");
const codeReviewBot = agentList.find((a) => a.name === "Code Review Bot");

const { data: widgetAgents } = await api(
  "GET",
  `/api/tenants/${widgetsTenantId}/agents/definitions`,
  undefined,
  aliceCookies,
);
const widgetAgentList = parse(
  paginatedSchema(AgentResponse),
  widgetAgents,
  "widget agents response",
).data;
const supportBot = widgetAgentList.find(
  (a) => a.name === "Customer Support Bot",
);

if (researchBot) {
  const { status: ofr1Status, data: ofr1Data } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/offerings`,
    {
      agentId: researchBot.id,
      name: "Web Research",
      description: "Search the web and summarize findings on any topic",
      pricing: {
        base: { amount: "0.50", currency: "USD" },
        methods: ["credits"],
        negotiable: false,
      },
    },
    aliceCookies,
  );
  checkOrSkip("create web research offering", ofr1Status, 201, ofr1Data);

  const { status: ofr2Status, data: ofr2Data } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/offerings`,
    {
      agentId: researchBot.id,
      name: "Document Summarization",
      description: "Summarize long documents into key takeaways",
      pricing: {
        base: { amount: "0.25", currency: "USD" },
        methods: ["credits"],
        negotiable: true,
        bounds: { min: "0.10", max: "1.00" },
      },
    },
    aliceCookies,
  );
  checkOrSkip("create summarization offering", ofr2Status, 201, ofr2Data);
}

if (codeReviewBot) {
  const { status: ofr3Status, data: ofr3Data } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/offerings`,
    {
      agentId: codeReviewBot.id,
      name: "Pull Request Review",
      description:
        "Automated code review with bug detection and improvement suggestions",
      pricing: {
        base: { amount: "1.00", currency: "USD" },
        methods: ["credits"],
        negotiable: false,
      },
      schema: {
        input: { type: "object", properties: { prUrl: { type: "string" } } },
        output: {
          type: "object",
          properties: { comments: { type: "array" } },
        },
      },
    },
    aliceCookies,
  );
  checkOrSkip("create pr review offering", ofr3Status, 201, ofr3Data);
}

if (supportBot) {
  const { status: ofr4Status, data: ofr4Data } = await api(
    "POST",
    `/api/tenants/${widgetsTenantId}/offerings`,
    {
      agentId: supportBot.id,
      name: "Ticket Resolution",
      description: "Automatically resolve common customer support tickets",
      pricing: {
        base: { amount: "0.75", currency: "USD" },
        methods: ["credits", "fiat"],
        negotiable: true,
        bounds: { min: "0.25", max: "2.00" },
      },
    },
    aliceCookies,
  );
  checkOrSkip("create ticket resolution offering", ofr4Status, 201, ofr4Data);
}

// -- Create model catalog --
//
// The declarative deployment set lives in ./lib/catalog-seed-data; this driver
// walks it. Each catalog provider gets a dedicated old-system provider (so its
// credential has a provider FK) and a credential, then the catalog provider,
// its offerings (carrying explicit quirks), and pricing. Ids are resolved by
// listing after each create so the seed stays idempotent across re-runs where
// a POST returns 409.

log("Creating model catalog...");

const CredentialIdName = type({ id: "string", name: "string" });

for (const m of catalogModels) {
  const { status, data } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/catalog/models`,
    { canonicalName: m.canonicalName, displayName: m.displayName },
    aliceCookies,
  );
  checkOrSkip(`create model ${m.canonicalName}`, status, 201, data);
}

const { data: catalogModelListData } = await api(
  "GET",
  `/api/tenants/${acmeTenantId}/catalog/models`,
  undefined,
  aliceCookies,
);
const modelIdByName = new Map(
  parse(
    paginatedSchema(ModelResponse),
    catalogModelListData,
    "catalog models response",
  ).data.map((m) => [m.canonicalName, m.id]),
);

for (const p of catalogProviders) {
  // Old-system provider that owns the credential. Its plugin mirrors the
  // catalog plugin (the old provider's plugin is free-form) and the metadata
  // baseURL matches the catalog endpoint.
  const { status: intgStatus, data: intgData } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/providers`,
    { name: p.name, plugin: p.plugin, metadata: { baseURL: p.baseURL } },
    aliceCookies,
  );
  checkOrSkip(
    `create integration provider ${p.name}`,
    intgStatus,
    201,
    intgData,
  );

  const { data: intgListData } = await api(
    "GET",
    `/api/tenants/${acmeTenantId}/providers`,
    undefined,
    aliceCookies,
  );
  const integrationProvider = parse(
    paginatedSchema(ProviderResponse),
    intgListData,
    "integration providers response",
  ).data.find((x) => x.name === p.name);
  if (!integrationProvider) fatalMissing(`integration provider ${p.name}`);

  // Every dev deployment authenticates with an API key.
  const { status: credStatus, data: credData } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/credentials`,
    {
      name: p.credentialName,
      type: "api_key",
      providerId: integrationProvider.id,
      secret: p.credentialSecret,
      scopes: ["chat"],
    },
    aliceCookies,
  );
  checkOrSkip(
    `create credential ${p.credentialName}`,
    credStatus,
    201,
    credData,
  );

  const { data: credListData } = await api(
    "GET",
    `/api/tenants/${acmeTenantId}/credentials`,
    undefined,
    aliceCookies,
  );
  const credential = parse(
    paginatedSchema(CredentialIdName),
    credListData,
    "credentials response",
  ).data.find((c) => c.name === p.credentialName);
  if (!credential) fatalMissing(`credential ${p.credentialName}`);

  const { status: provStatus, data: provData } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/catalog/providers`,
    {
      name: p.name,
      plugin: p.plugin,
      baseURL: p.baseURL,
      credentialId: credential.id,
    },
    aliceCookies,
  );
  checkOrSkip(`create catalog provider ${p.name}`, provStatus, 201, provData);

  const { data: provListData } = await api(
    "GET",
    `/api/tenants/${acmeTenantId}/catalog/providers`,
    undefined,
    aliceCookies,
  );
  const catalogProviderRow = parse(
    paginatedSchema(ModelProviderResponse),
    provListData,
    "catalog providers response",
  ).data.find((x) => x.name === p.name);
  if (!catalogProviderRow) fatalMissing(`catalog provider ${p.name}`);

  for (const o of p.offerings) {
    const modelId = modelIdByName.get(o.model);
    if (modelId === undefined) {
      log(`  SKIP offering ${p.name}/${o.model} (model not seeded)`);
      continue;
    }
    const { status, data } = await api(
      "POST",
      `/api/tenants/${acmeTenantId}/catalog/offerings`,
      {
        modelId,
        providerId: catalogProviderRow.id,
        priority: o.priority,
        capabilities: offeringCapabilities(o),
        quirks: o.quirks,
      },
      aliceCookies,
    );
    checkOrSkip(`create offering ${p.name}/${o.model}`, status, 201, data);
  }

  const { data: offeringListData } = await api(
    "GET",
    `/api/tenants/${acmeTenantId}/catalog/offerings`,
    undefined,
    aliceCookies,
  );
  const catalogOfferings = parse(
    paginatedSchema(ModelOfferingResponse),
    offeringListData,
    "catalog offerings response",
  ).data;
  for (const o of p.offerings) {
    const modelId = modelIdByName.get(o.model);
    // The create loop already logged and skipped an offering whose model was
    // not seeded; skip pricing for it too rather than treating it as missing.
    if (modelId === undefined) continue;
    const offering = catalogOfferings.find(
      (x) => x.providerId === catalogProviderRow.id && x.modelId === modelId,
    );
    if (!offering) fatalMissing(`offering ${p.name}/${o.model}`);
    const { status, data } = await api(
      "POST",
      `/api/tenants/${acmeTenantId}/catalog/offerings/${offering.id}/pricing`,
      {
        currency: "USD",
        // Pinned so a seed re-run collides on (offering, currency,
        // effectiveFrom) and skips rather than appending a duplicate.
        effectiveFrom: "2024-01-01T00:00:00.000Z",
        inputTokenPrice: o.price.input,
        outputTokenPrice: o.price.output,
      },
      aliceCookies,
    );
    checkOrSkip(`create pricing for ${p.name}/${o.model}`, status, 201, data);
  }
}

// -- Seed a launchable human-in-the-loop workflow on Acme --
//
// The fixture is a `draft -> awaitSignal{"approve"} -> publish`
// workflow whose step-agents are authored inline (system prompts and
// inference preferences on the definition; no FK to the agent-catalog
// rows above). The definition is created as a `workflow`-kind asset and
// its `workflow.json` is pushed over the asset smart-HTTP route -- the
// only surface that writes asset tree content, since `createAsset` only
// lays down the genesis `.gitignore`. The push uses the system git
// binary with a `GIT_ASKPASS` bearer-token shim, matching the
// established asset-push convention; isomorphic-git over HTTP is not
// used anywhere in the repo for this.

log("Seeding workflow definition on Acme...");

const GitTokenMintResponse = type({ id: "string", secret: "string" });

async function resolveTenantPrincipalId(
  tenantId: string,
  userId: string,
  cookies: CookieJar,
): Promise<string> {
  const { data } = await api(
    "GET",
    `/api/tenants/${tenantId}/principals?limit=200`,
    undefined,
    cookies,
  );
  const principals = parse(
    paginatedSchema(PrincipalResponse),
    data,
    "tenant principals response",
  ).data;
  const match = principals.find((p) => p.refId === userId);
  if (!match) {
    process.stderr.write(
      `[seed] FATAL: could not resolve principal for user ${userId} on tenant ${tenantId}\n`,
    );
    process.exit(1);
  }
  return match.id;
}

async function plantPrincipalGrant(
  tenantId: string,
  principalId: string,
  resource: string,
  action: string,
  cookies: CookieJar,
): Promise<void> {
  // The grants table has no unique constraint and the POST is a plain
  // insert, so re-running the seed would accumulate duplicate grant
  // rows. Check for an equivalent existing grant first and skip the
  // insert when one is present, matching how the role-grant block only
  // plants grants for a freshly created role.
  const { status: listStatus, data: listData } = await api(
    "GET",
    `/api/tenants/${tenantId}/grants?principalId=${encodeURIComponent(principalId)}&resource=${encodeURIComponent(resource)}&limit=200`,
    undefined,
    cookies,
  );
  check(`list grants for principal ${principalId}`, listStatus, 200, listData);
  const existing = parse(
    paginatedSchema(GrantResponse),
    listData,
    `grants list for principal ${principalId}`,
  ).data.find(
    (g) =>
      g.resource === resource &&
      g.action === action &&
      g.effect === "allow" &&
      g.principalId === principalId,
  );
  if (existing) {
    log(`  SKIP grant ${resource}/${action} on principal (already exists)`);
    return;
  }

  const { status, data } = await api(
    "POST",
    `/api/tenants/${tenantId}/grants`,
    { principalId, resource, action, effect: "allow", origin: "creator" },
    cookies,
  );
  check(`grant ${resource}/${action} on principal`, status, 201, data);
}

type GitRunResult = { stdout: string; stderr: string; status: number };

async function runGit(
  args: string[],
  cwd: string,
  extraEnv: Record<string, string>,
): Promise<GitRunResult> {
  return await new Promise<GitRunResult>((resolveRun, reject) => {
    const child = spawn("git", args, {
      cwd,
      env: { ...process.env, ...extraEnv },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (c: Uint8Array) => {
      stdout += new TextDecoder().decode(c);
    });
    child.stderr.on("data", (c: Uint8Array) => {
      stderr += new TextDecoder().decode(c);
    });
    child.on("error", reject);
    child.on("close", (code) => {
      resolveRun({ stdout, stderr, status: code ?? -1 });
    });
  });
}

function withBasicAuth(url: string, user: string, pass: string): string {
  const u = new URL(url);
  u.username = user;
  u.password = pass;
  return u.toString();
}

async function pushWorkflowJson(args: {
  tenantId: string;
  assetName: string;
  tokenSecret: string;
  workflowJson: string;
}): Promise<void> {
  // The bearer token is embedded as the basic-auth password in the
  // smart-HTTP URL (username is an ignored placeholder); a GIT_ASKPASS
  // shim echoes the same token as a belt-and-suspenders fallback so git
  // never blocks on an interactive prompt. `-c credential.helper=`
  // disables any developer-configured credential helper.
  const work = await mkdtemp(join(tmpdir(), "seed-workflow-"));
  const repoDir = join(work, "repo");
  const askpass = join(work, "askpass.sh");
  try {
    await writeFile(
      askpass,
      `#!/bin/sh\nprintf '%s\\n' '${args.tokenSecret.replace(/'/g, "'\\''")}'\n`,
      "utf-8",
    );
    await chmod(askpass, 0o755);
    const gitEnv: Record<string, string> = {
      GIT_ASKPASS: askpass,
      GIT_TERMINAL_PROMPT: "0",
      GIT_AUTHOR_NAME: "Interchange Seed",
      GIT_AUTHOR_EMAIL: "seed@interchange.local",
      GIT_COMMITTER_NAME: "Interchange Seed",
      GIT_COMMITTER_EMAIL: "seed@interchange.local",
    };
    const remote = `${BASE}/api/tenants/${args.tenantId}/assets/workflow/${args.assetName}.git`;
    const authRemote = withBasicAuth(
      remote,
      "x-access-token",
      args.tokenSecret,
    );

    const clone = await runGit(
      ["-c", "credential.helper=", "clone", authRemote, repoDir],
      work,
      gitEnv,
    );
    if (clone.status !== 0) {
      throw new Error(`git clone failed: ${clone.stderr || clone.stdout}`);
    }

    await writeFile(
      join(repoDir, WORKFLOW_JSON_PATH),
      args.workflowJson,
      "utf-8",
    );

    const remaining: { label: string; args: string[] }[] = [
      { label: "add workflow.json", args: ["add", WORKFLOW_JSON_PATH] },
      {
        label: "commit",
        args: ["commit", "-m", "Seed approval-flow workflow definition"],
      },
      {
        label: "push",
        args: ["-c", "credential.helper=", "push", authRemote, "HEAD:main"],
      },
    ];
    for (const stepArgs of remaining) {
      const r = await runGit(stepArgs.args, repoDir, gitEnv);
      if (r.status !== 0) {
        throw new Error(
          `git ${stepArgs.label} failed: ${r.stderr || r.stdout}`,
        );
      }
    }
  } finally {
    await rm(work, { recursive: true, force: true });
  }
}

const aliceAcmePrincipalId = await resolveTenantPrincipalId(
  acmeTenantId,
  alice.userId,
  aliceCookies,
);
log(`  Alice Acme principal ID: ${aliceAcmePrincipalId}`);

// Plant the grants the deploy, listing, and signal routes gate on. Alice
// owns Acme (the owner role already grants `*`/`*`), but the operator
// who delivers the `approve` signal must hold an explicit
// `workflow-run:<deploymentId>`/`manage` grant; the deployment id is
// minted at deploy time, so the planted resource is the
// `workflow-run:*` wildcard the authz glob matcher resolves against any
// concrete deployment. The `workflow:*` create + read grants back the
// deploy and listing routes.
await plantPrincipalGrant(
  acmeTenantId,
  aliceAcmePrincipalId,
  "workflow:*",
  "create",
  aliceCookies,
);
await plantPrincipalGrant(
  acmeTenantId,
  aliceAcmePrincipalId,
  "workflow:*",
  "read",
  aliceCookies,
);
await plantPrincipalGrant(
  acmeTenantId,
  aliceAcmePrincipalId,
  WORKFLOW_RUN_GRANT_RESOURCE,
  WORKFLOW_RUN_GRANT_ACTION,
  aliceCookies,
);

const { status: wfAssetStatus, data: wfAssetData } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/assets`,
  { kind: "workflow", name: WORKFLOW_FIXTURE_ASSET_NAME },
  aliceCookies,
);
const workflowAssetCreated = checkOrSkip(
  "create workflow asset",
  wfAssetStatus,
  201,
  wfAssetData,
);

if (workflowAssetCreated) {
  parse(AssetResponse, wfAssetData, "workflow asset response");

  const tokenExpiresAt = new Date(Date.now() + 10 * 60 * 1000).toISOString();
  const { status: tokenStatus, data: tokenData } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/git-tokens`,
    {
      name: "seed-workflow-push",
      resource: "asset:*",
      refPattern: "**",
      actions: ["can_read", "can_push"],
      expiresAt: tokenExpiresAt,
    },
    aliceCookies,
  );
  check("mint workflow git token", tokenStatus, 201, tokenData);
  const token = parse(
    GitTokenMintResponse,
    tokenData,
    "git token mint response",
  );

  await pushWorkflowJson({
    tenantId: acmeTenantId,
    assetName: WORKFLOW_FIXTURE_ASSET_NAME,
    tokenSecret: token.secret,
    workflowJson: buildWorkflowJson(),
  });
  log(`  Pushed ${WORKFLOW_JSON_PATH} to ${WORKFLOW_FIXTURE_ASSET_NAME}`);
}

// Confirm the workflow asset is listable on the tenant.
const { data: workflowAssetsData } = await api(
  "GET",
  `/api/tenants/${acmeTenantId}/assets?kind=workflow`,
  undefined,
  aliceCookies,
);
const workflowAssets = parse(
  AssetWithOriginResponse.array(),
  workflowAssetsData,
  "workflow assets list response",
);
if (!workflowAssets.some((a) => a.name === WORKFLOW_FIXTURE_ASSET_NAME)) {
  process.stderr.write(
    `[seed] FATAL: workflow asset ${WORKFLOW_FIXTURE_ASSET_NAME} is not listable after seeding\n`,
  );
  process.exit(1);
}
log(
  `  Workflow asset ${WORKFLOW_FIXTURE_ASSET_NAME} is listable (signal: ${WORKFLOW_FIXTURE_SIGNAL_NAME})`,
);

// -- Verify with /me endpoints --

log("Verifying /me endpoints...");

const { status: meStatus, data: meData } = await api(
  "GET",
  "/api/me",
  undefined,
  aliceCookies,
);
check("get alice profile", meStatus, 200, meData);

const { status: mePrincipals, data: mePrinData } = await api(
  "GET",
  "/api/me/principals",
  undefined,
  aliceCookies,
);
check("get alice principals", mePrincipals, 200, mePrinData);
log(
  `  Alice has ${parse(paginatedSchema(PrincipalSummary), mePrinData, "alice principals response").data.length} principal(s) across tenants`,
);

const { status: meAgents, data: meAgentData } = await api(
  "GET",
  "/api/me/agents",
  undefined,
  aliceCookies,
);
check("get alice agents", meAgents, 200, meAgentData);
log(
  `  Alice can see ${parse(paginatedSchema(AgentSummary), meAgentData, "alice agents response").data.length} agent(s)`,
);

// Verify Bob's view
const { data: bobPrinData } = await api(
  "GET",
  "/api/me/principals",
  undefined,
  bobCookies,
);
log(
  `  Bob has ${parse(paginatedSchema(PrincipalSummary), bobPrinData, "bob principals response").data.length} principal(s)`,
);

// Verify Carol's view
const { data: carolPrinData } = await api(
  "GET",
  "/api/me/principals",
  undefined,
  carolCookies,
);
log(
  `  Carol has ${parse(paginatedSchema(PrincipalSummary), carolPrinData, "carol principals response").data.length} principal(s)`,
);

log("Seed completed successfully.");
