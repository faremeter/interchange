// End-to-end coverage of the four bearer-middleware failure modes
// that are not covered by `git-asset-clone.test.ts`'s anonymous-clone
// case or the existing `token_revoked` integration test in
// `packages/hub-api/src/routes/git-tokens.test.ts`.
//
// Each test spawns a real hub via `startHub`, drives a clone against
// `/usr/bin/git`, and asserts on three surfaces in parallel:
//
//   1. The exit code of `git clone` (must be non-zero).
//   2. The stderr of `git clone` — pinned to text stock git actually
//      emits when the smart-HTTP endpoint replies 403. This is the
//      string a real user sees in their terminal; pinning the
//      assertion to it makes the test a drift detector for response
//      shape changes that would silently re-route the failure through
//      a different code path in git.
//   3. A raw `fetch` probe of `${assetUrl}/info/refs?service=...`
//      with the same Basic credential, asserting the JSON error body
//      shape (`{ error: { code, message } }`).
//
// The four failure modes mirror the `forbidden(...)` branches in
// `packages/hub-api/src/middleware/git-token-auth.ts`:
//   - `token_expired`
//   - `principal_suspended`
//   - `tenant_mismatch`
//   - `principal_not_found`

import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";
import postgres from "postgres";

import {
  harnessHubEnvAvailable,
  runGit,
  startHub,
  type HubHandle,
} from "./lib/git-harness";
import { loadHarnessDbConfig } from "@intx/test-harness/db-harness";
import { base64Encode } from "@intx/types";
import {
  apiCall,
  assetSmartHttpUrl,
  createAsset,
  createTenant,
  mintTenantGitToken,
  signUpUser,
  tokenEnv,
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
 * Open a per-test postgres connection that targets the spawned hub's
 * schema. The hub-api integration tests use this to inject targeted
 * mutations against state the REST API does not expose (e.g.
 * back-dating `git_token.expires_at`, suspending a principal).
 */
function openSchemaSql(schema: string) {
  const dbConfig = loadHarnessDbConfig();
  return postgres({
    host: dbConfig.host,
    port: dbConfig.port,
    user: dbConfig.user,
    password: dbConfig.password,
    database: dbConfig.database,
    max: 1,
    connection: { search_path: `"${schema.replace(/"/g, '""')}"` },
  });
}

const ErrorBody = type({
  error: {
    code: "string",
    message: "string",
  },
});

const MintPATResponse = type({
  id: "string",
  secret: "string",
});

async function probeInfoRefs(
  assetUrl: string,
  token: string,
): Promise<{ status: number; body: typeof ErrorBody.infer }> {
  const basic = base64Encode(
    new TextEncoder().encode(`x-access-token:${token}`),
  );
  const res = await fetch(`${assetUrl}/info/refs?service=git-upload-pack`, {
    headers: { authorization: `Basic ${basic}` },
  });
  const raw: unknown = await res.json();
  const parsed = ErrorBody(raw);
  if (parsed instanceof type.errors) {
    throw new Error(
      `info/refs probe returned a body without an error envelope (${parsed.summary}): ${JSON.stringify(raw)}`,
    );
  }
  return { status: res.status, body: parsed };
}

describe.skipIf(!harnessHubEnvAvailable())(
  "git-bearer failure modes against /usr/bin/git",
  () => {
    test("token_expired: expired token is rejected with 403", async () => {
      const hub = await startHubTracked();
      const user = await signUpUser(hub.url);
      const tenant = await createTenant(hub.url, user);
      const asset = await createAsset(hub.url, user, tenant);
      const token = await mintTenantGitToken(hub.url, user, tenant, {
        refPattern: "**",
        actions: ["can_read"],
      });

      // The mint floor is `expires_at > now + 60s`, so we cannot mint
      // a pre-expired token. Back-date the row directly.
      const sql = openSchemaSql(hub.schema);
      try {
        await sql`update git_token set expires_at = ${new Date(Date.now() - 60_000)} where id = ${token.tokenId}`;
      } finally {
        await sql.end();
      }

      const env = await tokenEnv(token.secret);
      const cwd = await mkTemp("bearer-token-expired-");
      const cloneTarget = path.join(cwd, "repo");
      const result = await runGit(
        [
          "-c",
          "credential.helper=",
          "clone",
          assetSmartHttpUrl(hub.url, asset),
          cloneTarget,
        ],
        { cwd, env },
      );

      expect(result.status).not.toBe(0);
      expect(result.stderr).toMatch(/The requested URL returned error: 403/);

      const probe = await probeInfoRefs(
        assetSmartHttpUrl(hub.url, asset),
        token.secret,
      );
      expect(probe.status).toBe(403);
      expect(probe.body.error.code).toBe("token_expired");
    }, 90_000);

    test("principal_suspended: suspended principal is rejected with 403", async () => {
      const hub = await startHubTracked();
      const user = await signUpUser(hub.url);
      const tenant = await createTenant(hub.url, user);
      const asset = await createAsset(hub.url, user, tenant);
      const token = await mintTenantGitToken(hub.url, user, tenant, {
        refPattern: "**",
        actions: ["can_read"],
      });

      const sql = openSchemaSql(hub.schema);
      try {
        const rows = await sql<{ principal_id: string | null }[]>`
        select principal_id from git_token where id = ${token.tokenId}
      `;
        const principalId = rows[0]?.principal_id;
        if (principalId === null || principalId === undefined) {
          throw new Error(
            `expected svc token ${token.tokenId} to carry a principal_id, got null`,
          );
        }
        await sql`update principal set status = 'suspended' where id = ${principalId}`;
      } finally {
        await sql.end();
      }

      const env = await tokenEnv(token.secret);
      const cwd = await mkTemp("bearer-principal-suspended-");
      const cloneTarget = path.join(cwd, "repo");
      const result = await runGit(
        [
          "-c",
          "credential.helper=",
          "clone",
          assetSmartHttpUrl(hub.url, asset),
          cloneTarget,
        ],
        { cwd, env },
      );

      expect(result.status).not.toBe(0);
      expect(result.stderr).toMatch(/The requested URL returned error: 403/);

      const probe = await probeInfoRefs(
        assetSmartHttpUrl(hub.url, asset),
        token.secret,
      );
      expect(probe.status).toBe(403);
      expect(probe.body.error.code).toBe("principal_suspended");
    }, 90_000);

    test("tenant_mismatch: svc token bound to tenant A cannot reach tenant B asset", async () => {
      const hub = await startHubTracked();
      const user = await signUpUser(hub.url);
      const tenantA = await createTenant(hub.url, user, { slugPrefix: "a" });
      const tenantB = await createTenant(hub.url, user, { slugPrefix: "b" });
      // Asset lives under B; token is bound to A.
      const assetB = await createAsset(hub.url, user, tenantB);
      const tokenA = await mintTenantGitToken(hub.url, user, tenantA, {
        refPattern: "**",
        actions: ["can_read"],
      });

      const env = await tokenEnv(tokenA.secret);
      const cwd = await mkTemp("bearer-tenant-mismatch-");
      const cloneTarget = path.join(cwd, "repo");
      const result = await runGit(
        [
          "-c",
          "credential.helper=",
          "clone",
          assetSmartHttpUrl(hub.url, assetB),
          cloneTarget,
        ],
        { cwd, env },
      );

      expect(result.status).not.toBe(0);
      expect(result.stderr).toMatch(/The requested URL returned error: 403/);

      const probe = await probeInfoRefs(
        assetSmartHttpUrl(hub.url, assetB),
        tokenA.secret,
      );
      expect(probe.status).toBe(403);
      expect(probe.body.error.code).toBe("tenant_mismatch");
    }, 90_000);

    test("principal_not_found: user B's PAT scoped to user A's tenant has no principal row", async () => {
      const hub = await startHubTracked();
      const userA = await signUpUser(hub.url, { emailPrefix: "alice" });
      const tenantA = await createTenant(hub.url, userA);
      const assetA = await createAsset(hub.url, userA, tenantA);

      const userB = await signUpUser(hub.url, { emailPrefix: "bob" });

      // User B mints a PAT restricted to tenant A. The mint endpoint
      // does not validate that B has any principal in A: it only writes
      // the (userId=B, tenantId=A) tuple onto the row. At bearer time,
      // the middleware resolves the principal via the URL's :tid bound
      // against the token's `userId` and finds no row for (B, A).
      const mintRes = await apiCall(
        hub.url,
        "POST",
        "/api/me/git-tokens",
        {
          name: `cross-tenant-${Date.now()}`,
          resource: "asset:*",
          refPattern: "**",
          actions: ["can_read"],
          expiresAt: new Date(Date.now() + 5 * 60 * 1000).toISOString(),
          tenantId: tenantA.tenantId,
        },
        userB.cookies,
      );
      if (mintRes.status !== 201) {
        throw new Error(
          `mint PAT failed: status=${mintRes.status} body=${JSON.stringify(mintRes.data)}`,
        );
      }
      const mintData = MintPATResponse(mintRes.data);
      if (mintData instanceof type.errors) {
        throw new Error(
          `mint PAT response did not validate (${mintData.summary}): ${JSON.stringify(mintRes.data)}`,
        );
      }
      const patSecret = mintData.secret;

      const env = await tokenEnv(patSecret);
      const cwd = await mkTemp("bearer-principal-not-found-");
      const cloneTarget = path.join(cwd, "repo");
      const result = await runGit(
        [
          "-c",
          "credential.helper=",
          "clone",
          assetSmartHttpUrl(hub.url, assetA),
          cloneTarget,
        ],
        { cwd, env },
      );

      expect(result.status).not.toBe(0);
      expect(result.stderr).toMatch(/The requested URL returned error: 403/);

      const probe = await probeInfoRefs(
        assetSmartHttpUrl(hub.url, assetA),
        patSecret,
      );
      expect(probe.status).toBe(403);
      expect(probe.body.error.code).toBe("principal_not_found");
    }, 90_000);
  },
);
