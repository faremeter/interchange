/* eslint-disable no-console */

// The library half of the tool-package publisher: authenticate against the
// hub, resolve the tenant, ensure the `package-registry` asset, and PUT
// every `*.tgz` under a directory into it.
//
// Workflow (driven by `publishToolPackages`):
//   1. Authenticate against the hub (sign in as the bootstrap admin).
//   2. Resolve the target tenant by slug.
//   3. Find (or create) a `package-registry` asset named <registry>.
//   4. PUT every `*.tgz` under <from> into the asset's tarballs/.
//
// This lives in `bin/lib` rather than in the entry point because the entry
// point `bin/publish-tool-packages.ts` shares a basename with the
// `bin/publish-tool-packages` launcher wrapper; an importer using
// `./publish-tool-packages` would resolve to the extensionless bash wrapper,
// not the `.ts`. A `bin/lib` module has no such twin, so both the CLI
// entry point and `bin/dev.ts`'s in-process publish path import the
// publisher from here.
//
// Auth pattern: the hub's REST endpoints are session-gated; this reuses the
// same sign-in flow that `bin/seed.ts` uses, since there is no pre-existing
// bearer-token convention for human-facing assets. The admin credentials
// default to the seed's `alice@example.com` / `password123`; in dev
// orchestration `bin/dev.ts` and the CLI share these defaults. Operators
// running this against a non-dev hub set `HUB_ADMIN_EMAIL` and
// `HUB_ADMIN_PASSWORD`.
//
// Publishing is idempotent: re-running uploads every tarball under
// `<from>`, overwriting same-name entries in the registry.

import { promises as fs } from "node:fs";
import path from "node:path";
import { type, type Type } from "arktype";

import {
  AssetResponse,
  AssetWithOriginResponse,
  PrincipalSummary,
  TenantResponse,
  paginatedSchema,
} from "@intx/types";

// `Headers#getSetCookie` (used by mergeSetCookies below) requires Bun
// or Node >= 18. Fail loudly with a runtime-shape message so an
// operator running this on an unsupported runtime gets an actionable
// error instead of an inscrutable "headers.getSetCookie is not a
// function" deep in the auth flow. The check runs at module load so it
// fires for every importer — the CLI, bin/dev.ts, and any future one.
assertSupportedRuntime();

function assertSupportedRuntime(): void {
  if (typeof Bun !== "undefined") return;
  const versionString = process.versions.node;
  const major = Number.parseInt(versionString.split(".")[0] ?? "0", 10);
  if (Number.isFinite(major) && major >= 18) return;
  throw new Error(
    `publish-tool-packages requires Bun (preferred) or Node >= 18; this process is running Node ${versionString}`,
  );
}

const TarballPutResponse = type({
  commit: "string",
  integrity: "string",
});

const AuthResponse = type({ "user?": { id: "string" } });

export type PublishOptions = {
  hubURL: string;
  adminEmail: string;
  adminPassword: string;
  tenantSlug: string;
  tenantName: string;
  registryName: string;
  fromDir: string;
};

// The dev seed's publishing identity and target tenant. Both callers —
// `bin/dev.ts`'s in-process publish and the CLI entry point — resolve these
// from the environment, defaulting to the seed, so the defaults live in one
// place and the two edges cannot authenticate as different identities.
export const PUBLISH_SEED_DEFAULTS = {
  adminEmail: "alice@example.com",
  adminPassword: "password123",
  tenantSlug: "acme",
  tenantName: "Acme Corp",
} as const;

type CookieJar = string[];

type ApiResult = {
  status: number;
  data: unknown;
  cookies: CookieJar;
};

function parseSchema<T extends Type>(
  schema: T,
  data: unknown,
  label: string,
): T["infer"] {
  const result = schema(data);
  if (result instanceof type.errors) {
    throw new Error(
      `publish-tool-packages: validation failed for ${label}: ${result.summary}`,
    );
  }
  return result;
}

function mergeSetCookies(prev: CookieJar, setCookies: string[]): CookieJar {
  const next = [...prev];
  for (const sc of setCookies) {
    const name = sc.split("=")[0];
    if (name === undefined) continue;
    const value = sc.split(";")[0];
    if (value === undefined) continue;
    const idx = next.findIndex((c) => c.startsWith(`${name}=`));
    if (idx >= 0) {
      next[idx] = value;
    } else {
      next.push(value);
    }
  }
  return next;
}

async function jsonApi(
  hubURL: string,
  method: string,
  apiPath: string,
  body: unknown,
  cookies: CookieJar,
): Promise<ApiResult> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (cookies.length > 0) {
    headers["Cookie"] = cookies.join("; ");
  }
  let res: Response;
  try {
    res = await fetch(`${hubURL}${apiPath}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      redirect: "manual",
    });
  } catch (cause) {
    throw new Error(
      `publish-tool-packages: network error on ${method} ${apiPath}`,
      { cause },
    );
  }
  const nextCookies = mergeSetCookies(cookies, res.headers.getSetCookie());
  let data: unknown = null;
  const ct = res.headers.get("content-type") ?? "";
  if (ct.includes("json")) {
    data = await res.json();
  }
  return { status: res.status, data, cookies: nextCookies };
}

async function putTarball(
  hubURL: string,
  tenantId: string,
  assetId: string,
  filename: string,
  bytes: Uint8Array,
  cookies: CookieJar,
): Promise<{ commit: string; integrity: string }> {
  const headers: Record<string, string> = {
    "Content-Type": "application/octet-stream",
  };
  if (cookies.length > 0) {
    headers["Cookie"] = cookies.join("; ");
  }
  const url = `${hubURL}/api/tenants/${tenantId}/assets/${assetId}/tarballs/${filename}`;
  // Retry once on a 5xx response. The hub's tarball upload path is
  // idempotent (the asset's tip is keyed by integrity, not by
  // request id), so a single immediate retry is safe and matches the
  // honest-error-surfaces pattern: transient 5xx survives, persistent
  // failures still surface. No exponential backoff — this is a dev
  // workflow tool, not a hot-path producer.
  let res: Response | undefined;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      res = await fetch(url, {
        method: "PUT",
        headers,
        body: bytes,
        redirect: "manual",
      });
    } catch (cause) {
      throw new Error(
        `publish-tool-packages: network error uploading ${filename}`,
        { cause },
      );
    }
    if (res.status < 500 || attempt === 1) break;
    // Drain the response so the connection can be reused for the
    // retry, then loop. The first-attempt body is not useful — only
    // the second attempt's outcome flows downstream.
    await res.text();
  }
  if (res === undefined) {
    throw new Error(
      `publish-tool-packages: upload for ${filename} produced no response`,
    );
  }
  if (res.status !== 200) {
    const text = await res.text();
    throw new Error(
      `publish-tool-packages: upload failed for ${filename}: ${String(res.status)} ${text}`,
    );
  }
  const data: unknown = await res.json();
  return parseSchema(TarballPutResponse, data, `PUT ${filename}`);
}

async function authenticate(
  hubURL: string,
  email: string,
  password: string,
): Promise<CookieJar> {
  const signUp = await jsonApi(
    hubURL,
    "POST",
    "/api/auth/sign-up/email",
    { name: "Publish Admin", email, password },
    [],
  );
  // better-auth's sign-up returns 200 on success (with the new
  // session's cookies) and 422 UNPROCESSABLE_ENTITY with body
  // `{ code: "USER_ALREADY_EXISTS_USE_ANOTHER_EMAIL", ... }` when the
  // address is already registered. Anything else is a hub-side fault
  // that must not silently fall through to sign-in; that fall-through
  // would mask a real misconfiguration as an authentication failure.
  if (signUp.status === 200) {
    parseSchema(AuthResponse, signUp.data, "sign-up response");
    return signUp.cookies;
  }
  if (signUp.status !== 422) {
    throw new Error(
      `publish-tool-packages: unexpected sign-up response for ${email}: ${String(signUp.status)} ${JSON.stringify(signUp.data)}`,
    );
  }
  const signIn = await jsonApi(
    hubURL,
    "POST",
    "/api/auth/sign-in/email",
    { email, password },
    [],
  );
  if (signIn.status !== 200) {
    throw new Error(
      `publish-tool-packages: authentication failed for ${email}: sign-up=${String(signUp.status)} ${JSON.stringify(signUp.data)} sign-in=${String(signIn.status)} ${JSON.stringify(signIn.data)}`,
    );
  }
  parseSchema(AuthResponse, signIn.data, "sign-in response");
  return signIn.cookies;
}

async function ensureTenant(
  hubURL: string,
  cookies: CookieJar,
  slug: string,
  name: string,
): Promise<string> {
  const create = await jsonApi(
    hubURL,
    "POST",
    "/api/tenants",
    { name, slug },
    cookies,
  );
  if (create.status === 201) {
    return parseSchema(TenantResponse, create.data, "create tenant response")
      .id;
  }
  // 409 is the documented "tenant already exists" response: fall
  // through to the slug lookup. Anything else (400 validation, 403
  // permission, 5xx) is surfaced explicitly so a hub-side regression
  // does not silently degrade into a misleading "tenant slug not
  // visible" error from the fallback path.
  if (create.status !== 409) {
    throw new Error(
      `publish-tool-packages: failed to create tenant ${slug}: ${String(create.status)} ${JSON.stringify(create.data)}`,
    );
  }
  const list = await jsonApi(
    hubURL,
    "GET",
    "/api/me/principals",
    undefined,
    cookies,
  );
  if (list.status !== 200) {
    throw new Error(
      `publish-tool-packages: failed to look up tenant by slug ${slug}: ${String(list.status)} ${JSON.stringify(list.data)}`,
    );
  }
  const principals = parseSchema(
    paginatedSchema(PrincipalSummary),
    list.data,
    "me/principals response",
  );
  const match = principals.data.find((p) => p.tenantSlug === slug);
  if (match === undefined) {
    throw new Error(
      `publish-tool-packages: tenant slug ${slug} not visible to authenticated principal`,
    );
  }
  return match.tenantId;
}

async function ensureRegistryAsset(
  hubURL: string,
  cookies: CookieJar,
  tenantId: string,
  registryName: string,
): Promise<string> {
  const list = await jsonApi(
    hubURL,
    "GET",
    `/api/tenants/${tenantId}/assets?kind=package-registry&inherited=false`,
    undefined,
    cookies,
  );
  if (list.status !== 200) {
    throw new Error(
      `publish-tool-packages: failed to list assets: ${String(list.status)} ${JSON.stringify(list.data)}`,
    );
  }
  const rows = parseSchema(
    AssetWithOriginResponse.array(),
    list.data,
    "list assets response",
  );
  const existing = rows.find((r) => r.name === registryName);
  if (existing !== undefined) {
    return existing.id;
  }
  const create = await jsonApi(
    hubURL,
    "POST",
    `/api/tenants/${tenantId}/assets`,
    { kind: "package-registry", name: registryName },
    cookies,
  );
  if (create.status !== 201) {
    throw new Error(
      `publish-tool-packages: failed to create asset ${registryName}: ${String(create.status)} ${JSON.stringify(create.data)}`,
    );
  }
  return parseSchema(AssetResponse, create.data, "create asset response").id;
}

async function listTarballs(fromDir: string): Promise<string[]> {
  let entries: string[];
  try {
    entries = await fs.readdir(fromDir);
  } catch (cause) {
    throw new Error(
      `publish-tool-packages: cannot read --from directory ${fromDir}`,
      { cause },
    );
  }
  const tarballs = entries.filter((f) => f.endsWith(".tgz")).sort();
  if (tarballs.length === 0) {
    throw new Error(
      `publish-tool-packages: no *.tgz files found under ${fromDir}`,
    );
  }
  return tarballs;
}

/**
 * Publish every tarball in `opts.fromDir` to the registry. Returns one
 * record per uploaded file so callers (including `bin/dev.ts`) can log
 * a summary without re-deriving the data.
 */
export async function publishToolPackages(opts: PublishOptions): Promise<
  {
    filename: string;
    commit: string;
    integrity: string;
  }[]
> {
  console.log(`[publish] authenticating as ${opts.adminEmail}`);
  const cookies = await authenticate(
    opts.hubURL,
    opts.adminEmail,
    opts.adminPassword,
  );

  console.log(`[publish] resolving tenant ${opts.tenantSlug}`);
  const tenantId = await ensureTenant(
    opts.hubURL,
    cookies,
    opts.tenantSlug,
    opts.tenantName,
  );
  console.log(`[publish]   tenant ${opts.tenantSlug} -> ${tenantId}`);

  console.log(
    `[publish] ensuring package-registry asset ${opts.registryName} on ${opts.tenantSlug}`,
  );
  const assetId = await ensureRegistryAsset(
    opts.hubURL,
    cookies,
    tenantId,
    opts.registryName,
  );
  console.log(`[publish]   asset ${opts.registryName} -> ${assetId}`);

  const tarballs = await listTarballs(opts.fromDir);
  console.log(
    `[publish] uploading ${String(tarballs.length)} tarball(s) from ${opts.fromDir}`,
  );

  const summaries: { filename: string; commit: string; integrity: string }[] =
    [];
  for (const filename of tarballs) {
    const abs = path.join(opts.fromDir, filename);
    const bytes = await fs.readFile(abs);
    const result = await putTarball(
      opts.hubURL,
      tenantId,
      assetId,
      filename,
      new Uint8Array(bytes),
      cookies,
    );
    console.log(
      `[publish]   ${filename} commit=${result.commit} integrity=${result.integrity}`,
    );
    summaries.push({ filename, ...result });
  }
  return summaries;
}
