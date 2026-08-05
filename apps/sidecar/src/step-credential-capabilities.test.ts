import { describe, test, expect } from "bun:test";
import { toolConsumer, type GrantRule } from "@intx/authz";
import { defineTool, type ToolBundle } from "@intx/agent";
import type { CredentialProvider, CredentialShapeContext } from "@intx/types";
import type { CredentialDelivery } from "@intx/types/sidecar";
import type { ToolCredentialDeclaration } from "@intx/types/package-json";
import { createCredentialProviderRegistry } from "@intx/harness";

import { buildCredentialCapabilities } from "./step-credential-capabilities";
import type { StepToolFactory } from "./tool-materialization";

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

// A StepToolFactory whose `factory` is a never-invoked stub: the assembly reads
// only `packageName` and `declaredCredentials`.
function fac(
  packageName: string,
  declaredCredentials: ToolCredentialDeclaration[],
): StepToolFactory {
  return {
    packageName,
    declaredCredentials,
    factory: defineTool({
      id: `${packageName}/bundle`,
      requires: [],
      definitions: [],
      factory: (): ToolBundle => ({
        definitions: [],
        // Never invoked: the assembly reads only packageName + declarations.
        run: () => Promise.reject(new Error("stub tool bundle: run is unused")),
      }),
    }),
  };
}

const ORIGIN = "https://api.example.com";

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
