import { describe, test, expect } from "bun:test";
import { toolConsumer, type GrantRule } from "@intx/authz";
import type { CredentialProvider, CredentialShapeContext } from "@intx/types";

import { createCredentialProviderRegistry } from "./credential-providers";
import {
  createCredentialCapability,
  type ResolvedCredentialBinding,
} from "./credential-capability";

function grant(
  overrides: Partial<GrantRule> &
    Pick<GrantRule, "resource" | "action" | "effect">,
): GrantRule {
  return {
    id: "grt_test",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
    ...overrides,
  };
}

// A provider that records every shape context it is handed and counts disposes,
// so the capability's threading and teardown can be asserted.
function trackingProvider(): {
  provider: CredentialProvider;
  shapes: CredentialShapeContext[];
  disposeCount: () => number;
} {
  const shapes: CredentialShapeContext[] = [];
  let disposed = 0;
  const provider: CredentialProvider = {
    key: "fake",
    shape(ctx) {
      shapes.push(ctx);
      return {
        kind: "http",
        fetch: async () => new Response(),
        dispose: () => {
          disposed += 1;
        },
      };
    },
  };
  return { provider, shapes, disposeCount: () => disposed };
}

const CONSUMER = toolConsumer("@intx/tools-example");

function bindingsFor(track: { provider: CredentialProvider }) {
  return new Map<string, ResolvedCredentialBinding>([
    [
      "gh",
      {
        credentialId: "cred_gh",
        providerKey: track.provider.key,
        origin: "https://api.github.com",
        readCurrentMaterial: () => ({ secret: "sk-1" }),
      },
    ],
  ]);
}

describe("createCredentialCapability (Gate 2)", () => {
  test("resolves a handle the consumer is authorized to use", async () => {
    const track = trackingProvider();
    const cap = createCredentialCapability({
      consumer: CONSUMER,
      bindings: bindingsFor(track),
      providers: createCredentialProviderRegistry([track.provider]),
      grants: [
        grant({
          resource: "credential:cred_gh",
          action: "use",
          effect: "allow",
          conditions: { tool: CONSUMER },
        }),
      ],
    });

    const handle = await cap.resolve("gh");
    expect(handle.kind).toBe("http");
    // The binding's provider was chosen and handed the right origin + material.
    expect(track.shapes).toHaveLength(1);
    expect(track.shapes[0]?.origin).toBe("https://api.github.com");
    expect(track.shapes[0]?.readCurrentMaterial()).toEqual({ secret: "sk-1" });
  });

  test("a coarse credential:* / use grant (no condition) authorizes", async () => {
    const track = trackingProvider();
    const cap = createCredentialCapability({
      consumer: CONSUMER,
      bindings: bindingsFor(track),
      providers: createCredentialProviderRegistry([track.provider]),
      grants: [
        grant({ resource: "credential:*", action: "use", effect: "allow" }),
      ],
    });
    expect((await cap.resolve("gh")).kind).toBe("http");
  });

  test("refuses when the grant's { tool } condition names another consumer", async () => {
    const track = trackingProvider();
    const cap = createCredentialCapability({
      consumer: CONSUMER,
      bindings: bindingsFor(track),
      providers: createCredentialProviderRegistry([track.provider]),
      grants: [
        grant({
          resource: "credential:cred_gh",
          action: "use",
          effect: "allow",
          conditions: { tool: toolConsumer("@intx/tools-other") },
        }),
      ],
    });
    await expect(cap.resolve("gh")).rejects.toThrow(/not authorized/);
    // The credential was never shaped on the deny path.
    expect(track.shapes).toHaveLength(0);
  });

  test("refuses when no grant matches", async () => {
    const track = trackingProvider();
    const cap = createCredentialCapability({
      consumer: CONSUMER,
      bindings: bindingsFor(track),
      providers: createCredentialProviderRegistry([track.provider]),
      grants: [],
    });
    await expect(cap.resolve("gh")).rejects.toThrow(/not authorized/);
  });

  test("fails closed when the consumer identity is empty", async () => {
    const track = trackingProvider();
    const cap = createCredentialCapability({
      consumer: "",
      bindings: bindingsFor(track),
      providers: createCredentialProviderRegistry([track.provider]),
      grants: [
        grant({
          resource: "credential:cred_gh",
          action: "use",
          effect: "allow",
          conditions: { tool: CONSUMER },
        }),
      ],
    });
    await expect(cap.resolve("gh")).rejects.toThrow(/not authorized/);
  });

  test("throws for a handle no binding covers", async () => {
    const track = trackingProvider();
    const cap = createCredentialCapability({
      consumer: CONSUMER,
      bindings: bindingsFor(track),
      providers: createCredentialProviderRegistry([track.provider]),
      grants: [
        grant({ resource: "credential:*", action: "use", effect: "allow" }),
      ],
    });
    await expect(cap.resolve("nope")).rejects.toThrow(
      /no credential is bound to handle "nope"/,
    );
  });

  test("memoizes: a second resolve returns the same instance, shaping once", async () => {
    const track = trackingProvider();
    const cap = createCredentialCapability({
      consumer: CONSUMER,
      bindings: bindingsFor(track),
      providers: createCredentialProviderRegistry([track.provider]),
      grants: [
        grant({ resource: "credential:*", action: "use", effect: "allow" }),
      ],
    });
    const a = await cap.resolve("gh");
    const b = await cap.resolve("gh");
    expect(a).toBe(b);
    expect(track.shapes).toHaveLength(1);
  });

  test("dispose releases every shaped handle", async () => {
    const track = trackingProvider();
    const cap = createCredentialCapability({
      consumer: CONSUMER,
      bindings: bindingsFor(track),
      providers: createCredentialProviderRegistry([track.provider]),
      grants: [
        grant({ resource: "credential:*", action: "use", effect: "allow" }),
      ],
    });
    await cap.resolve("gh");
    await cap.dispose();
    expect(track.disposeCount()).toBe(1);
  });

  test("concurrent resolves of one handle shape once and share the instance", async () => {
    const track = trackingProvider();
    const cap = createCredentialCapability({
      consumer: CONSUMER,
      bindings: bindingsFor(track),
      providers: createCredentialProviderRegistry([track.provider]),
      grants: [
        grant({ resource: "credential:*", action: "use", effect: "allow" }),
      ],
    });
    const [a, b] = await Promise.all([cap.resolve("gh"), cap.resolve("gh")]);
    expect(a).toBe(b);
    expect(track.shapes).toHaveLength(1);
  });

  test("dispose isolates a throwing handle, releasing the rest and surfacing the error", async () => {
    let goodDisposed = 0;
    const badProvider: CredentialProvider = {
      key: "bad",
      shape: () => ({
        kind: "http",
        fetch: async () => new Response(),
        dispose: () => {
          throw new Error("boom");
        },
      }),
    };
    const goodProvider: CredentialProvider = {
      key: "good",
      shape: () => ({
        kind: "http",
        fetch: async () => new Response(),
        dispose: () => {
          goodDisposed += 1;
        },
      }),
    };
    const bindings = new Map<string, ResolvedCredentialBinding>([
      [
        "bad",
        {
          credentialId: "cred_bad",
          providerKey: "bad",
          origin: "https://a.example.com",
          readCurrentMaterial: () => ({ secret: "s" }),
        },
      ],
      [
        "good",
        {
          credentialId: "cred_good",
          providerKey: "good",
          origin: "https://b.example.com",
          readCurrentMaterial: () => ({ secret: "s" }),
        },
      ],
    ]);
    const cap = createCredentialCapability({
      consumer: CONSUMER,
      bindings,
      providers: createCredentialProviderRegistry([badProvider, goodProvider]),
      grants: [
        grant({ resource: "credential:*", action: "use", effect: "allow" }),
      ],
    });
    await cap.resolve("bad");
    await cap.resolve("good");

    await expect(cap.dispose()).rejects.toThrow(/failed to dispose/);
    // The healthy handle was still released despite the bad one throwing.
    expect(goodDisposed).toBe(1);
  });
});
