// The tenant-create route normalizes case so a mail sender (whose From is
// lowercased when parsed) maps to exactly one tenant: it derives the domain from
// a lowercased slug and rejects a case-variant slug as a clean 409 rather than
// letting the `lower(domain)` unique index surface a 500. The slug also becomes
// a DNS label in the derived domain, so the route rejects a slug that is not a
// legal label. Driven against a real spawned hub through the production HTTP
// route.

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

// Every entry violates RFC 1035 section 2.3.1 as relaxed by RFC 1123 section
// 2.1, so none of them is a legal DNS label. The derived domain is both the
// sender stamp and the recipient run address, and the recipient address is the
// key into the inbound mail policy lookup and the mail router, so a slug that
// is not a label corrupts the admission policy key and the routing key.
const ILLEGAL_SLUGS: { label: string; slug: string }[] = [
  { label: "empty", slug: "" },
  { label: "leading hyphen", slug: "-acme" },
  { label: "trailing hyphen", slug: "acme-" },
  { label: "underscore", slug: "acme_co" },
  { label: "dot", slug: "acme.co" },
  { label: "space", slug: "acme co" },
  { label: "at sign", slug: "acme@evil" },
  // The address parser accepts these three, so only the label grammar stops
  // them from reaching the derived domain.
  { label: "comma", slug: "acme,evil" },
  { label: "semicolon", slug: "acme;evil" },
  { label: "greater than", slug: "acme>evil" },
  { label: "newline", slug: "acme\nevil" },
  { label: "trailing newline", slug: "acme\n" },
  { label: "over 63 characters", slug: "a".repeat(64) },
];

// Each entry is a legal DNS label. RFC 1035 defines the letter set as all 52
// characters in both cases and attaches no significance to case, so a
// mixed-case slug is conformant and must stay accepted.
const LEGAL_SLUGS: { label: string; slug: string }[] = [
  { label: "lowercase letters", slug: "acme" },
  { label: "mixed case", slug: "AcmeMixed" },
  { label: "leading digit", slug: "7acme" },
  { label: "interior hyphen", slug: "acme-two" },
  { label: "exactly 63 characters", slug: "b".repeat(63) },
];

// Collects every case before asserting, so one failing run names every slug the
// route got wrong rather than only the first.
async function createEach(
  hub: HubHandle,
  cookies: string[],
  cases: { label: string; slug: string }[],
) {
  const seen: { label: string; status: number }[] = [];
  for (const { label, slug } of cases) {
    const res = await apiCall(
      hub.url,
      "POST",
      "/api/tenants",
      { name: `Acme ${label}`, slug },
      cookies,
    );
    seen.push({ label, status: res.status });
  }
  return seen;
}

describe.skipIf(!harnessHubEnvAvailable())("tenant create slug grammar", () => {
  test("rejects a slug that is not a legal DNS label", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);

    const seen = await createEach(hub, user.cookies, ILLEGAL_SLUGS);
    expect(seen).toEqual(
      ILLEGAL_SLUGS.map(({ label }) => ({ label, status: 400 })),
    );
  });

  test("accepts a slug that is a legal DNS label", async () => {
    const hub = await startHubTracked();
    const user = await signUpUser(hub.url);

    const seen = await createEach(hub, user.cookies, LEGAL_SLUGS);
    expect(seen).toEqual(
      LEGAL_SLUGS.map(({ label }) => ({ label, status: 201 })),
    );
  });
});
