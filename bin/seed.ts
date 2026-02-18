#!/usr/bin/env bun

const BASE = process.env["HUB_URL"] ?? "http://localhost:3000";

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

  if (signUp.cookies.length > 0) {
    const userId = (signUp.data as { user?: { id: string } })?.user?.id ?? "";
    return { cookies: signUp.cookies, userId };
  }

  // User already exists -- sign in instead
  const signIn = await api("POST", "/api/auth/sign-in/email", {
    email,
    password,
  });

  if (signIn.cookies.length === 0) {
    process.stderr.write(`[seed] FATAL: could not authenticate ${email}\n`);
    process.stderr.write(`[seed]   sign-up: ${JSON.stringify(signUp.data)}\n`);
    process.stderr.write(`[seed]   sign-in: ${JSON.stringify(signIn.data)}\n`);
    process.exit(1);
  }

  const userId = (signIn.data as { user?: { id: string } })?.user?.id ?? "";
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

const { status: t1Status, data: t1Data } = await api(
  "POST",
  "/api/tenants",
  { name: "Acme Corp", slug: "acme" },
  aliceCookies,
);
check("create acme tenant", t1Status, 201, t1Data);
const acmeTenantId = (t1Data as { id: string }).id;
log(`  Acme tenant ID: ${acmeTenantId}`);

const { status: t2Status, data: t2Data } = await api(
  "POST",
  "/api/tenants",
  { name: "Widget Labs", slug: "widgets" },
  aliceCookies,
);
check("create widgets tenant", t2Status, 201, t2Data);
const widgetsTenantId = (t2Data as { id: string }).id;
log(`  Widgets tenant ID: ${widgetsTenantId}`);

// -- Invite Bob to Acme --

log("Inviting Bob to Acme...");

// First get acme roles to find member role
const { data: rolesData } = await api(
  "GET",
  `/api/tenants/${acmeTenantId}/roles`,
  undefined,
  aliceCookies,
);
const memberRole = (rolesData as { id: string; name: string }[]).find(
  (r) => r.name === "member",
);

const { status: inviteStatus, data: inviteData } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/members/invite`,
  { email: "bob@example.com", roleId: memberRole?.id },
  aliceCookies,
);
check("invite bob", inviteStatus, 201, inviteData);
const bobPrincipalId = (inviteData as { id: string }).id;
log(`  Bob principal ID: ${bobPrincipalId}`);

// Activate Bob's principal (simulating accepting the invite)
const { status: activateStatus } = await api(
  "PATCH",
  `/api/tenants/${acmeTenantId}/principals/${bobPrincipalId}`,
  { status: "active" },
  aliceCookies,
);
check("activate bob", activateStatus, 200, null);

// -- Invite Carol to Widgets --

log("Inviting Carol to Widgets...");

const { data: widgetRoles } = await api(
  "GET",
  `/api/tenants/${widgetsTenantId}/roles`,
  undefined,
  aliceCookies,
);
const widgetAdminRole = (widgetRoles as { id: string; name: string }[]).find(
  (r) => r.name === "admin",
);

const { status: carolInviteStatus, data: carolInviteData } = await api(
  "POST",
  `/api/tenants/${widgetsTenantId}/members/invite`,
  { email: "carol@example.com", roleId: widgetAdminRole?.id },
  aliceCookies,
);
check("invite carol to widgets", carolInviteStatus, 201, carolInviteData);

// Activate Carol
const carolPrincipalId = (carolInviteData as { id: string }).id;
await api(
  "PATCH",
  `/api/tenants/${widgetsTenantId}/principals/${carolPrincipalId}`,
  { status: "active" },
  aliceCookies,
);

// -- Create agents in Acme --

log("Creating agents in Acme...");

const { status: a1Status, data: a1Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/agents`,
  {
    name: "Research Bot",
    description: "Researches topics and summarizes findings",
    systemPrompt:
      "You are a research assistant. Find and summarize information.",
    capabilities: { research: true, summarize: true },
    initialGrants: [
      { resource: "documents:*", action: "read", effect: "allow" },
      { resource: "documents:*", action: "write", effect: "ask" },
    ],
  },
  aliceCookies,
);
check("create research bot", a1Status, 201, a1Data);
log(`  Research Bot ID: ${(a1Data as { id: string }).id}`);

const { status: a2Status, data: a2Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/agents`,
  {
    name: "Code Review Bot",
    description: "Reviews pull requests and suggests improvements",
    systemPrompt:
      "You are a code reviewer. Analyze code for bugs and improvements.",
    capabilities: { codeReview: true },
    initialGrants: [
      { resource: "repos:*", action: "read", effect: "allow" },
      { resource: "repos:*", action: "comment", effect: "allow" },
    ],
  },
  aliceCookies,
);
check("create code review bot", a2Status, 201, a2Data);
log(`  Code Review Bot ID: ${(a2Data as { id: string }).id}`);

// -- Create agent in Widgets --

log("Creating agent in Widgets...");

const { status: a3Status, data: a3Data } = await api(
  "POST",
  `/api/tenants/${widgetsTenantId}/agents`,
  {
    name: "Customer Support Bot",
    description: "Handles customer support tickets",
    systemPrompt:
      "You are a customer support agent. Help customers with their issues.",
    capabilities: { ticketManagement: true, knowledgeBase: true },
    initialGrants: [
      { resource: "tickets:*", action: "*", effect: "allow" },
      { resource: "billing:*", action: "read", effect: "allow" },
      { resource: "billing:*", action: "refund", effect: "ask" },
    ],
  },
  aliceCookies,
);
check("create support bot", a3Status, 201, a3Data);
log(`  Support Bot ID: ${(a3Data as { id: string }).id}`);

// -- Create custom role and grants --

log("Creating custom role in Acme...");

const { status: crStatus, data: crData } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/roles`,
  { name: "reviewer", description: "Can review and comment on documents" },
  aliceCookies,
);
check("create reviewer role", crStatus, 201, crData);
const reviewerRoleId = (crData as { id: string }).id;

// Grant the reviewer role read access to documents
await api(
  "POST",
  `/api/tenants/${acmeTenantId}/grants`,
  {
    roleId: reviewerRoleId,
    resource: "documents:*",
    action: "read",
    effect: "allow",
    source: "role",
  },
  aliceCookies,
);

// Assign reviewer role to Bob
await api(
  "POST",
  `/api/tenants/${acmeTenantId}/principals/${bobPrincipalId}/roles/${reviewerRoleId}`,
  undefined,
  aliceCookies,
);

// -- Set up federation between Acme and Widgets --

log("Setting up federation...");

const { status: fedStatus } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/federation`,
  {
    targetTenantId: widgetsTenantId,
    direction: "bilateral",
  },
  aliceCookies,
);
check("create federation trust", fedStatus, 201, null);

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
  `  Alice has ${(mePrinData as unknown[]).length} principal(s) across tenants`,
);

const { status: meAgents, data: meAgentData } = await api(
  "GET",
  "/api/me/agents",
  undefined,
  aliceCookies,
);
check("get alice agents", meAgents, 200, meAgentData);
log(`  Alice can see ${(meAgentData as unknown[]).length} agent(s)`);

// Verify Bob's view
const { data: bobPrinData } = await api(
  "GET",
  "/api/me/principals",
  undefined,
  bobCookies,
);
log(`  Bob has ${(bobPrinData as unknown[]).length} principal(s)`);

// Verify Carol's view
const { data: carolPrinData } = await api(
  "GET",
  "/api/me/principals",
  undefined,
  carolCookies,
);
log(`  Carol has ${(carolPrinData as unknown[]).length} principal(s)`);

log("Seed completed successfully.");
