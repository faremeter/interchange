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
    return (data as { id: string }).id;
  }

  // Already exists -- look up via /me/principals
  const { data: principals } = await api(
    "GET",
    "/api/me/principals",
    undefined,
    cookies,
  );
  const principalList = (
    principals as { data: { tenantId: string; tenantSlug: string }[] }
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

// -- Invite Bob to Acme --

log("Inviting Bob to Acme...");

const { data: rolesData } = await api(
  "GET",
  `/api/tenants/${acmeTenantId}/roles`,
  undefined,
  aliceCookies,
);
const rolesList = (rolesData as { data: { id: string; name: string }[] }).data;
const memberRole = rolesList.find((r) => r.name === "member");

const { status: inviteStatus, data: inviteData } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/members/invite`,
  { email: "bob@example.com", roleId: memberRole?.id },
  aliceCookies,
);

let bobPrincipalId: string;
if (checkOrSkip("invite bob", inviteStatus, 201, inviteData)) {
  bobPrincipalId = (inviteData as { id: string }).id;
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
  const acmePrincipalList = (
    acmePrincipals as { data: { id: string; refId: string }[] }
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
const widgetRolesList = (
  widgetRoles as { data: { id: string; name: string }[] }
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
  const carolPrincipalId = (carolInviteData as { id: string }).id;
  await api(
    "PATCH",
    `/api/tenants/${widgetsTenantId}/principals/${carolPrincipalId}`,
    { status: "active" },
    aliceCookies,
  );
}

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
    modelConfig: { defaultModel: "gpt-4o" },
    capabilities: { research: true, summarize: true },
    credentialRequirements: [
      {
        providerName: "OpenAI",
        source: "tenant",
        scopes: ["chat", "embeddings"],
      },
    ],
    initialGrants: [
      { resource: "documents:*", action: "read", effect: "allow" },
      { resource: "documents:*", action: "write", effect: "ask" },
    ],
  },
  aliceCookies,
);
checkOrSkip("create research bot", a1Status, 201, a1Data);
const researchBotId = a1Status === 201 ? (a1Data as { id: string }).id : null;
if (researchBotId) log(`  Research Bot ID: ${researchBotId}`);

const { status: a2Status, data: a2Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/agents`,
  {
    name: "Code Review Bot",
    description: "Reviews pull requests and suggests improvements",
    systemPrompt:
      "You are a code reviewer. Analyze code for bugs and improvements.",
    modelConfig: { defaultModel: "gpt-4o" },
    capabilities: { codeReview: true },
    credentialRequirements: [
      { providerName: "GitHub", source: "tenant", scopes: ["repo"] },
    ],
    initialGrants: [
      { resource: "repos:*", action: "read", effect: "allow" },
      { resource: "repos:*", action: "comment", effect: "allow" },
    ],
  },
  aliceCookies,
);
checkOrSkip("create code review bot", a2Status, 201, a2Data);
const codeReviewBotId = a2Status === 201 ? (a2Data as { id: string }).id : null;
if (codeReviewBotId) log(`  Code Review Bot ID: ${codeReviewBotId}`);

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
    modelConfig: { defaultModel: "gpt-4o" },
    capabilities: { ticketManagement: true, knowledgeBase: true },
    credentialRequirements: [
      {
        providerName: "Stripe",
        source: "tenant",
        scopes: ["charges:read", "refunds:write"],
      },
    ],
    initialGrants: [
      { resource: "tickets:*", action: "*", effect: "allow" },
      { resource: "billing:*", action: "read", effect: "allow" },
      { resource: "billing:*", action: "refund", effect: "ask" },
    ],
  },
  aliceCookies,
);
checkOrSkip("create support bot", a3Status, 201, a3Data);
const supportBotId = a3Status === 201 ? (a3Data as { id: string }).id : null;
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
  const reviewerRoleId = (crData as { id: string }).id;

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
checkOrSkip("create federation trust", fedStatus, 201, fedData);

// -- Create providers --

log("Creating providers...");

const { status: prv1Status, data: prv1Data } = await api(
  "POST",
  `/api/tenants/${acmeTenantId}/providers`,
  {
    name: "OpenAI",
    plugin: "openai",
    metadata: { baseURL: "https://api.openai.com/v1", defaultModel: "gpt-4o" },
  },
  aliceCookies,
);
checkOrSkip("create openai provider", prv1Status, 201, prv1Data);

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
  },
  aliceCookies,
);
checkOrSkip("create stripe provider", prv3Status, 201, prv3Data);

// Look up provider IDs (handles re-runs where providers already exist)
const { data: acmeProviders } = await api(
  "GET",
  `/api/tenants/${acmeTenantId}/providers`,
  undefined,
  aliceCookies,
);
const providerList = acmeProviders as { data: { id: string; name: string }[] };
const openaiProvider = providerList.data.find((p) => p.name === "OpenAI");
const githubProvider = providerList.data.find((p) => p.name === "GitHub");

const { data: widgetProviders } = await api(
  "GET",
  `/api/tenants/${widgetsTenantId}/providers`,
  undefined,
  aliceCookies,
);
const widgetProviderList = widgetProviders as {
  data: { id: string; name: string }[];
};
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
  checkOrSkip("create github oauth client", oclStatus, 201, oclData);
}

// -- Create credentials --

log("Creating credentials...");

if (openaiProvider) {
  const { status: cred1Status, data: cred1Data } = await api(
    "POST",
    `/api/tenants/${acmeTenantId}/credentials`,
    {
      name: "OpenAI API Key",
      type: "api_key",
      providerId: openaiProvider.id,
      description: "Production OpenAI key for Research Bot",
      secret: "sk-fake-openai-key-for-seed-data",
      scopes: ["chat", "embeddings"],
      metadata: { model: "gpt-4" },
    },
    aliceCookies,
  );
  checkOrSkip("create openai credential", cred1Status, 201, cred1Data);
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
  `/api/tenants/${acmeTenantId}/agents`,
  undefined,
  aliceCookies,
);
const agentList = (acmeAgents as { data: { id: string; name: string }[] }).data;
const researchBot = agentList.find((a) => a.name === "Research Bot");
const codeReviewBot = agentList.find((a) => a.name === "Code Review Bot");

const { data: widgetAgents } = await api(
  "GET",
  `/api/tenants/${widgetsTenantId}/agents`,
  undefined,
  aliceCookies,
);
const widgetAgentList = (
  widgetAgents as { data: { id: string; name: string }[] }
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
  `  Alice has ${(mePrinData as { data: unknown[] }).data.length} principal(s) across tenants`,
);

const { status: meAgents, data: meAgentData } = await api(
  "GET",
  "/api/me/agents",
  undefined,
  aliceCookies,
);
check("get alice agents", meAgents, 200, meAgentData);
log(
  `  Alice can see ${(meAgentData as { data: unknown[] }).data.length} agent(s)`,
);

// Verify Bob's view
const { data: bobPrinData } = await api(
  "GET",
  "/api/me/principals",
  undefined,
  bobCookies,
);
log(
  `  Bob has ${(bobPrinData as { data: unknown[] }).data.length} principal(s)`,
);

// Verify Carol's view
const { data: carolPrinData } = await api(
  "GET",
  "/api/me/principals",
  undefined,
  carolCookies,
);
log(
  `  Carol has ${(carolPrinData as { data: unknown[] }).data.length} principal(s)`,
);

log("Seed completed successfully.");
