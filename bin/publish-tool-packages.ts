#!/usr/bin/env bun
/* eslint-disable no-console */

// Command-line front end for the tool-package publisher. Parses flags and
// environment, then delegates to `publishToolPackages` in
// `bin/lib/publish-tool-packages.ts` (which `bin/dev.ts` also calls in its
// in-process publish path). See that module for the publish workflow and
// the auth pattern.

import path from "node:path";
import { parseArgs } from "node:util";

import { WORKSPACE_BUILTINS_REGISTRY } from "@intx/hub-sessions";

import {
  PUBLISH_SEED_DEFAULTS,
  publishToolPackages,
} from "./lib/publish-tool-packages";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    throw new Error(
      `publish-tool-packages: required environment variable ${name} is not set`,
    );
  }
  return value;
}

function optionalEnv(name: string, fallback: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") return fallback;
  return value;
}

async function runCLI(): Promise<void> {
  const { values } = parseArgs({
    options: {
      registry: { type: "string", default: WORKSPACE_BUILTINS_REGISTRY },
      from: { type: "string", default: "dist/builtins" },
      tenant: { type: "string" },
      "tenant-name": { type: "string" },
      help: { type: "boolean", default: false },
    },
    strict: true,
  });

  if (values.help) {
    console.log(
      "Usage: bin/publish-tool-packages [--registry <name>] [--from <dir>] [--tenant <slug>] [--tenant-name <name>]",
    );
    console.log("");
    console.log("Environment:");
    console.log("  HUB_URL              hub base URL (required)");
    console.log(
      "  HUB_ADMIN_EMAIL      admin email for sign-in (default: alice@example.com)",
    );
    console.log("  HUB_ADMIN_PASSWORD   admin password (default: password123)");
    console.log("  HUB_TENANT_SLUG      target tenant slug (default: acme)");
    console.log("  HUB_TENANT_NAME      target tenant display name");
    return;
  }

  const hubURL = requireEnv("HUB_URL");
  // Warn whenever the admin credentials still match the dev seed.
  // The tenant slug is a routing concern (the wrong slug fails the
  // launch loudly upstream), but default admin credentials let any
  // operator authenticated against a seeded hub act as the
  // publishing admin — that risk does not depend on whether the
  // operator picked a custom tenant. Either default credential is
  // enough to fire the warning.
  const usingDefaultEmail = process.env.HUB_ADMIN_EMAIL === undefined;
  const usingDefaultPassword = process.env.HUB_ADMIN_PASSWORD === undefined;
  if (usingDefaultEmail || usingDefaultPassword) {
    console.warn(
      "[publish] running with dev seed credentials — set HUB_ADMIN_EMAIL and HUB_ADMIN_PASSWORD to override",
    );
  }
  const adminEmail = optionalEnv(
    "HUB_ADMIN_EMAIL",
    PUBLISH_SEED_DEFAULTS.adminEmail,
  );
  const adminPassword = optionalEnv(
    "HUB_ADMIN_PASSWORD",
    PUBLISH_SEED_DEFAULTS.adminPassword,
  );
  const tenantSlug =
    values.tenant ??
    optionalEnv("HUB_TENANT_SLUG", PUBLISH_SEED_DEFAULTS.tenantSlug);
  const tenantName =
    values["tenant-name"] ??
    optionalEnv("HUB_TENANT_NAME", PUBLISH_SEED_DEFAULTS.tenantName);
  // parseArgs above declares defaults for `registry` and `from`, so
  // `values.registry` and `values.from` are always strings here.
  const registryName = values.registry;
  const fromRaw = values.from;
  const fromDir = path.isAbsolute(fromRaw)
    ? fromRaw
    : path.resolve(process.cwd(), fromRaw);

  await publishToolPackages({
    hubURL,
    adminEmail,
    adminPassword,
    tenantSlug,
    tenantName,
    registryName,
    fromDir,
  });
}

if (import.meta.main) {
  try {
    await runCLI();
  } catch (err) {
    if (err instanceof Error) {
      console.error(err.message);
      if (err.cause !== undefined) {
        console.error(`  cause: ${String(err.cause)}`);
      }
    } else {
      console.error(String(err));
    }
    process.exit(1);
  }
}
