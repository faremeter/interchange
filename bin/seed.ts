#!/usr/bin/env bun
/* eslint-disable no-console */

import { readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

import { type, type Type } from "arktype";
import {
  TenantResponse,
  AgentResponse,
  PrincipalResponse,
  PrincipalSummary,
  AgentSummary,
  RoleResponse,
  ProviderResponse,
  ModelResponse,
  ModelProviderResponse,
  ModelOfferingResponse,
  paginatedSchema,
} from "@intx/types";
import { WORKSPACE_BUILTINS_REGISTRY } from "@intx/hub-sessions";
import { extractTarballPackageJSON } from "@intx/tool-packaging";

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
      { model: "claude-sonnet-4", capabilities: ["tool-use"] },
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

log("Creating model catalog...");

const CredentialIdName = type({ id: "string", name: "string" });

// The model provider authenticates with a tenant credential. Reuse the
// Anthropic API key created above; list and find it so the seed is
// idempotent across re-runs.
const { data: acmeCredsData } = await api(
  "GET",
  `/api/tenants/${acmeTenantId}/credentials`,
  undefined,
  aliceCookies,
);
const anthropicCredential = parse(
  paginatedSchema(CredentialIdName),
  acmeCredsData,
  "acme credentials response",
).data.find((c) => c.name === "Anthropic API Key");

if (anthropicCredential) {
  const modelsToSeed = [
    { canonicalName: "claude-sonnet-4", displayName: "Claude Sonnet 4" },
    { canonicalName: "claude-haiku-4", displayName: "Claude Haiku 4" },
  ];
  for (const m of modelsToSeed) {
    const { status, data } = await api(
      "POST",
      `/api/tenants/${acmeTenantId}/catalog/models`,
      m,
      aliceCookies,
    );
    checkOrSkip(`create model ${m.canonicalName}`, status, 201, data);
  }

  const { status: provStatus, data: provData } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/catalog/providers`,
    {
      name: "Anthropic Direct",
      plugin: "anthropic",
      baseURL: "https://api.anthropic.com",
      credentialId: anthropicCredential.id,
    },
    aliceCookies,
  );
  checkOrSkip("create provider Anthropic Direct", provStatus, 201, provData);

  // Resolve catalog ids by listing, so offering/pricing creation works on a
  // re-run where the create calls above returned 409.
  const { data: modelListData } = await api(
    "GET",
    `/api/tenants/${acmeTenantId}/catalog/models`,
    undefined,
    aliceCookies,
  );
  const catalogModels = parse(
    paginatedSchema(ModelResponse),
    modelListData,
    "catalog models response",
  ).data;
  const { data: provListData } = await api(
    "GET",
    `/api/tenants/${acmeTenantId}/catalog/providers`,
    undefined,
    aliceCookies,
  );
  const anthropicProviderRow = parse(
    paginatedSchema(ModelProviderResponse),
    provListData,
    "catalog providers response",
  ).data.find((p) => p.name === "Anthropic Direct");

  const sonnet = catalogModels.find(
    (m) => m.canonicalName === "claude-sonnet-4",
  );
  const haiku = catalogModels.find((m) => m.canonicalName === "claude-haiku-4");

  if (anthropicProviderRow && sonnet && haiku) {
    const offeringSpecs = [
      {
        model: sonnet,
        priority: 0,
        capabilities: ["tool-use", "long-context"],
      },
      { model: haiku, priority: 10, capabilities: ["tool-use"] },
    ];
    for (const spec of offeringSpecs) {
      const { status, data } = await api(
        "POST",
        `/api/tenants/${acmeTenantId}/catalog/offerings`,
        {
          modelId: spec.model.id,
          providerId: anthropicProviderRow.id,
          priority: spec.priority,
          capabilities: spec.capabilities,
        },
        aliceCookies,
      );
      checkOrSkip(
        `create offering ${spec.model.canonicalName}`,
        status,
        201,
        data,
      );
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
    const priceByModel: Record<string, { input: string; output: string }> = {
      [sonnet.id]: { input: "0.000003", output: "0.000015" },
      [haiku.id]: { input: "0.0000008", output: "0.000004" },
    };
    for (const offering of catalogOfferings) {
      const price = priceByModel[offering.modelId];
      if (!price) continue;
      const { status, data } = await api(
        "POST",
        `/api/tenants/${acmeTenantId}/catalog/offerings/${offering.id}/pricing`,
        {
          currency: "USD",
          // Pinned so a seed re-run collides on (offering, currency,
          // effectiveFrom) and skips rather than appending a duplicate.
          effectiveFrom: "2024-01-01T00:00:00.000Z",
          inputTokenPrice: price.input,
          outputTokenPrice: price.output,
        },
        aliceCookies,
      );
      checkOrSkip(`create pricing for ${offering.id}`, status, 201, data);
    }
  }
}

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
