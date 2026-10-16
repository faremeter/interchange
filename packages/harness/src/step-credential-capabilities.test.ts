import { describe, test, expect } from "bun:test";
import { toolConsumer, type GrantRule } from "@intx/authz";
import type { CredentialProvider, CredentialShapeContext } from "@intx/types";
import type { CredentialDelivery } from "@intx/types/credential-delivery";
import type { ToolCredentialDeclaration } from "@intx/types/package-json";

import { createCredentialProviderRegistry } from "./credential-providers";
import {
  buildConsumerCredentialCapability,
  buildCredentialCapabilities,
} from "./step-credential-capabilities";

// A provider that records every shape context it is handed, so a test can
// reach the `readCurrentMaterial` closure a shaped handle reads through.
function trackingProvider(): {
  provider: CredentialProvider;
  shapes: CredentialShapeContext[];
} {
  const shapes: CredentialShapeContext[] = [];
  const provider: CredentialProvider = {
    key: "fake",
    shape(ctx) {
      shapes.push(ctx);
      return {
        kind: "http",
        fetch: async () => new Response(),
        dispose() {
          /* no-op: the fake http handle holds nothing to release */
        },
      };
    },
  };
  return { provider, shapes };
}

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

// The assembly reads only `packageName` and `declaredCredentials`.
function fac(
  packageName: string,
  declaredCredentials: ToolCredentialDeclaration[],
) {
  return { packageName, declaredCredentials };
}

const ORIGIN = "https://api.example.com";

// A one-credential delivery that binds `handle` to each named consumer.
function deliveryFor(
  providerKey: string,
  bound: readonly { consumer: string; handle: string }[],
): { current: CredentialDelivery } {
  return {
    current: {
      bindings: bound.map(({ consumer, handle }) => ({
        handle,
        credentialId: "c1",
        consumer,
      })),
      materials: [
        { credentialId: "c1", providerKey, origin: ORIGIN, secret: "sk-1" },
      ],
    },
  };
}

function useGrant(consumer: string): GrantRule {
  return grant({
    resource: "credential:c1",
    action: "use",
    effect: "allow",
    conditions: { tool: consumer },
  });
}

describe("buildCredentialCapabilities", () => {
  test("a credential granted to one package cannot be resolved by another (confused deputy)", async () => {
    const track = trackingProvider();
    // The same credential is bound for both packages, but only pkg-a holds the
    // credential:c1/use grant scoped to its consumer.
    const cell = {
      current: {
        bindings: [
          {
            handle: "shared",
            credentialId: "c1",
            consumer: toolConsumer("@intx/pkg-a"),
          },
          {
            handle: "shared",
            credentialId: "c1",
            consumer: toolConsumer("@intx/pkg-b"),
          },
        ],
        materials: [
          {
            credentialId: "c1",
            providerKey: track.provider.key,
            origin: ORIGIN,
            secret: "sk-1",
          },
        ],
      },
    };
    const caps = buildCredentialCapabilities(
      [fac("@intx/pkg-a", []), fac("@intx/pkg-b", [])],
      {
        materialCell: cell,
        resolveGrants: () => [
          grant({
            resource: "credential:c1",
            action: "use",
            effect: "allow",
            conditions: { tool: toolConsumer("@intx/pkg-a") },
          }),
        ],
        providers: createCredentialProviderRegistry([track.provider]),
      },
    );

    const capA = caps.get("@intx/pkg-a");
    const capB = caps.get("@intx/pkg-b");
    if (capA === undefined || capB === undefined) {
      throw new Error("expected a capability for both packages");
    }

    // pkg-a's grant authorizes it; pkg-b binds the same credential but holds no
    // grant for it, so Gate 2 refuses -- the credential does not leak sideways.
    await expect(capA.resolve("shared")).resolves.toMatchObject({
      kind: "http",
    });
    await expect(capB.resolve("shared")).rejects.toThrow(/not authorized/);
  });

  test("a declared handle no binding resolves fails the build closed", () => {
    const track = trackingProvider();
    const cell = { current: { bindings: [], materials: [] } };
    expect(() =>
      buildCredentialCapabilities(
        [fac("@intx/pkg-a", [{ handle: "needed" }])],
        {
          materialCell: cell,
          resolveGrants: () => [],
          providers: createCredentialProviderRegistry([track.provider]),
        },
      ),
    ).toThrow(
      /declares credential handle\(s\) that no binding resolves: needed/,
    );
  });

  test("dropping a credential's material starves an already-shaped handle", async () => {
    const track = trackingProvider();
    const cell: { current: CredentialDelivery | null } = {
      current: {
        bindings: [
          {
            handle: "cred",
            credentialId: "c1",
            consumer: toolConsumer("@intx/pkg-a"),
          },
        ],
        materials: [
          {
            credentialId: "c1",
            providerKey: track.provider.key,
            origin: ORIGIN,
            secret: "sk-1",
          },
        ],
      },
    };
    const caps = buildCredentialCapabilities([fac("@intx/pkg-a", [])], {
      materialCell: cell,
      resolveGrants: () => [
        grant({
          resource: "credential:c1",
          action: "use",
          effect: "allow",
          conditions: { tool: toolConsumer("@intx/pkg-a") },
        }),
      ],
      providers: createCredentialProviderRegistry([track.provider]),
    });
    const capA = caps.get("@intx/pkg-a");
    if (capA === undefined) throw new Error("expected a capability for pkg-a");

    await capA.resolve("cred");
    const shaped = track.shapes[0];
    if (shaped === undefined)
      throw new Error("expected the handle to be shaped");

    // Live read works while the material is delivered.
    expect(shaped.readCurrentMaterial()).toEqual({ secret: "sk-1" });

    // A re-push that revokes pkg-a drops c1's material: the already-shaped
    // handle starves on its next read rather than serving a stale secret.
    cell.current = { bindings: [], materials: [] };
    expect(() => shaped.readCurrentMaterial()).toThrow(/no longer delivered/);

    // An emptied cell (no delivery at all) fails closed the same way.
    cell.current = null;
    expect(() => shaped.readCurrentMaterial()).toThrow(/cell is empty/);
  });

  test("a material whose provider or origin drifted under a shaped handle is refused", async () => {
    const track = trackingProvider();
    const bindings = [
      {
        handle: "cred",
        credentialId: "c1",
        consumer: toolConsumer("@intx/pkg-a"),
      },
    ];
    const cell: { current: CredentialDelivery | null } = {
      current: {
        bindings,
        materials: [
          {
            credentialId: "c1",
            providerKey: track.provider.key,
            origin: ORIGIN,
            secret: "sk-1",
          },
        ],
      },
    };
    const caps = buildCredentialCapabilities([fac("@intx/pkg-a", [])], {
      materialCell: cell,
      resolveGrants: () => [
        grant({
          resource: "credential:c1",
          action: "use",
          effect: "allow",
          conditions: { tool: toolConsumer("@intx/pkg-a") },
        }),
      ],
      providers: createCredentialProviderRegistry([track.provider]),
    });
    const capA = caps.get("@intx/pkg-a");
    if (capA === undefined) throw new Error("expected a capability for pkg-a");
    await capA.resolve("cred");
    const shaped = track.shapes[0];
    if (shaped === undefined)
      throw new Error("expected the handle to be shaped");

    // A rotation may change the secret, never the provider/origin. A live entry
    // whose origin drifted is refused rather than followed.
    cell.current = {
      bindings,
      materials: [
        {
          credentialId: "c1",
          providerKey: track.provider.key,
          origin: "https://api.attacker.example",
          secret: "sk-2",
        },
      ],
    };
    expect(() => shaped.readCurrentMaterial()).toThrow(
      /changed provider\/origin/,
    );
  });

  test("a descriptor with no backing material fails the build closed", () => {
    const track = trackingProvider();
    const cell = {
      current: {
        bindings: [
          {
            handle: "cred",
            credentialId: "c1",
            consumer: toolConsumer("@intx/pkg-a"),
          },
        ],
        materials: [],
      },
    };
    expect(() =>
      buildCredentialCapabilities([fac("@intx/pkg-a", [])], {
        materialCell: cell,
        resolveGrants: () => [],
        providers: createCredentialProviderRegistry([track.provider]),
      }),
    ).toThrow(/delivery is malformed/);
  });

  test("a package that declares and binds no credential gets no capability", () => {
    const track = trackingProvider();
    // A credential is delivered, but only for a different package's consumer.
    const cell = {
      current: {
        bindings: [
          {
            handle: "x",
            credentialId: "c1",
            consumer: toolConsumer("@intx/other"),
          },
        ],
        materials: [
          {
            credentialId: "c1",
            providerKey: track.provider.key,
            origin: ORIGIN,
            secret: "sk-1",
          },
        ],
      },
    };
    const caps = buildCredentialCapabilities([fac("@intx/pkg-a", [])], {
      materialCell: cell,
      resolveGrants: () => [],
      providers: createCredentialProviderRegistry([track.provider]),
    });
    expect(caps.has("@intx/pkg-a")).toBe(false);
  });

  // The grants thunk must be read only when a package actually needs a
  // capability. This pins the self-discovery-resume fix: a toolless resume
  // precedes the grants barrier, so resolving grants for it would fault on a
  // snapshot that is not present yet. The "no capability" test above passes a
  // benign `() => []`, so it stays green whether grants resolve lazily or
  // eagerly -- these two use a throwing thunk to catch a laziness regression.
  const throwingGrants = (): readonly GrantRule[] => {
    throw new Error("resolveGrants must not be called for a step needing none");
  };

  test("a step whose packages need no capability never resolves grants", () => {
    const track = trackingProvider();
    const cell = { current: { bindings: [], materials: [] } };
    expect(() =>
      buildCredentialCapabilities([fac("@intx/pkg-a", [])], {
        materialCell: cell,
        resolveGrants: throwingGrants,
        providers: createCredentialProviderRegistry([track.provider]),
      }),
    ).not.toThrow();
  });

  test("grants are resolved once a package needs a capability", () => {
    const track = trackingProvider();
    const cell = {
      current: {
        bindings: [
          {
            handle: "cred",
            credentialId: "c1",
            consumer: toolConsumer("@intx/pkg-a"),
          },
        ],
        materials: [
          {
            credentialId: "c1",
            providerKey: track.provider.key,
            origin: ORIGIN,
            secret: "sk-1",
          },
        ],
      },
    };
    expect(() =>
      buildCredentialCapabilities([fac("@intx/pkg-a", [])], {
        materialCell: cell,
        resolveGrants: throwingGrants,
        providers: createCredentialProviderRegistry([track.provider]),
      }),
    ).toThrow(/resolveGrants must not be called/);
  });
});

describe("buildCredentialCapabilities grants reads", () => {
  test("a step reads its grants once, whatever the package order", () => {
    const track = trackingProvider();
    const cell = deliveryFor(track.provider.key, [
      { consumer: toolConsumer("@intx/pkg-a"), handle: "cred" },
      { consumer: toolConsumer("@intx/pkg-b"), handle: "cred" },
    ]);
    const packages = ["@intx/pkg-none", "@intx/pkg-a", "@intx/pkg-b"];
    const rotations = packages.map((_unused, i) => [
      ...packages.slice(i),
      ...packages.slice(0, i),
    ]);
    for (const order of rotations) {
      let reads = 0;
      const caps = buildCredentialCapabilities(
        order.map((name) => fac(name, [])),
        {
          materialCell: cell,
          resolveGrants: () => {
            reads += 1;
            return [];
          },
          providers: createCredentialProviderRegistry([track.provider]),
        },
      );
      expect([...caps.keys()].sort()).toEqual(["@intx/pkg-a", "@intx/pkg-b"]);
      expect(reads).toBe(1);
    }
  });
});

describe("buildConsumerCredentialCapability", () => {
  const consumer = toolConsumer("@intx/pkg-a");

  test("a bound and granted consumer resolves its handle", async () => {
    const track = trackingProvider();
    const capability = buildConsumerCredentialCapability(consumer, [], {
      materialCell: deliveryFor(track.provider.key, [
        { consumer, handle: "cred" },
      ]),
      resolveGrants: () => [useGrant(consumer)],
      providers: createCredentialProviderRegistry([track.provider]),
    });
    if (capability === undefined) throw new Error("expected a capability");
    await expect(capability.resolve("cred")).resolves.toMatchObject({
      kind: "http",
    });
    expect(track.shapes[0]?.readCurrentMaterial()).toEqual({ secret: "sk-1" });
  });

  test("a declared handle no binding resolves throws at build", () => {
    const track = trackingProvider();
    expect(() =>
      buildConsumerCredentialCapability(consumer, [{ handle: "needed" }], {
        materialCell: deliveryFor(track.provider.key, []),
        resolveGrants: () => [],
        providers: createCredentialProviderRegistry([track.provider]),
      }),
    ).toThrow(
      /declares credential handle\(s\) that no binding resolves: needed/,
    );
  });

  test("a consumer with no declarations and no bindings gets no capability and reads no grants", () => {
    const track = trackingProvider();
    const capability = buildConsumerCredentialCapability(consumer, [], {
      materialCell: deliveryFor(track.provider.key, [
        { consumer: toolConsumer("@intx/pkg-other"), handle: "cred" },
      ]),
      resolveGrants: () => {
        throw new Error("resolveGrants must not be called");
      },
      providers: createCredentialProviderRegistry([track.provider]),
    });
    expect(capability).toBeUndefined();
  });

  test("each call reads the grants it is given and caches nothing", async () => {
    const track = trackingProvider();
    const wiring = (grants: readonly GrantRule[]) => ({
      materialCell: deliveryFor(track.provider.key, [
        { consumer, handle: "cred" },
      ]),
      resolveGrants: () => grants,
      providers: createCredentialProviderRegistry([track.provider]),
    });
    const granted = buildConsumerCredentialCapability(
      consumer,
      [],
      wiring([useGrant(consumer)]),
    );
    const ungranted = buildConsumerCredentialCapability(
      consumer,
      [],
      wiring([]),
    );
    if (granted === undefined || ungranted === undefined) {
      throw new Error("expected a capability for both calls");
    }
    await expect(granted.resolve("cred")).resolves.toMatchObject({
      kind: "http",
    });
    await expect(ungranted.resolve("cred")).rejects.toThrow(/not authorized/);
  });
});
