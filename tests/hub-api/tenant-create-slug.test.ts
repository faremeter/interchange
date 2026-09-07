// The tenant-create route normalizes case so a mail sender (whose From is
// lowercased when parsed) maps to exactly one tenant: it derives the domain from
// a lowercased slug and rejects a case-variant slug as a clean 409 rather than
// letting the `lower(domain)` unique index surface a 500. Driven against a real
// spawned hub through the production HTTP route.

import { afterEach, describe, expect, test } from "bun:test";

import {
  harnessHubEnvAvailable,
  startHub,
  type HubHandle,
} from "./lib/git-harness";
import { apiCall, signUpUser } from "./lib/git-asset-fixtures";

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

describe.skipIf(!harnessHubEnvAvailable())(
  "tenant create slug case-insensitivity",
  () => {
    test("lowercases the domain and rejects a case-variant slug with 409", async () => {
      const hub = await startHubTracked();
      const user = await signUpUser(hub.url);

      const created = await apiCall(
        hub.url,
        "POST",
        "/api/tenants",
        { name: "Acme", slug: "AcmeCiTest" },
        user.cookies,
      );
      expect(created.status).toBe(201);
      const body = created.data;
      if (typeof body !== "object" || body === null) {
        throw new Error(`unexpected create body: ${JSON.stringify(body)}`);
      }
      // The slug is stored as provided; the domain is derived from a lowercased
      // slug so a lowercased sender address resolves to it.
      expect(body).toMatchObject({
        slug: "AcmeCiTest",
        domain: "acmecitest.localhost",
      });

      // A slug differing only in case collides on lower(slug): a clean 409, not
      // a 500 from the lower(domain) unique-index violation.
      const conflict = await apiCall(
        hub.url,
        "POST",
        "/api/tenants",
        { name: "Acme Two", slug: "acmecitest" },
        user.cookies,
      );
      expect(conflict.status).toBe(409);
    });
  },
);
