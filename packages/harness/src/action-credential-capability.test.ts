// The action credentials factory: a capability built per invocation for the
// consumer `action:<handler>` from the live material cell and the grants of
// the invocation's step.

import { describe, test, expect } from "bun:test";

import { actionConsumer, type GrantRule } from "@intx/authz";
import type { CredentialProvider, CredentialShapeContext } from "@intx/types";
import type { CredentialDelivery } from "@intx/types/credential-delivery";

import {
  createActionCredentialsFactory,
  type ActionCredentialWiring,
} from "./action-credential-capability";
import { createCredentialProviderRegistry } from "./credential-providers";

const ORIGIN = "https://api.example.com";

// A provider that records every shape context, so a test can read the secret
// a shaped handle would send.
function trackingProvider(): {
  provider: CredentialProvider;
  shapes: CredentialShapeContext[];
} {
  const shapes: CredentialShapeContext[] = [];
  return {
    shapes,
    provider: {
      key: "fake",
      shape(ctx) {
        shapes.push(ctx);
        return {
          kind: "http",
          fetch: async () => new Response(),
          dispose() {
            /* the fake handle holds nothing to release */
          },
        };
      },
    },
  };
}

function useGrant(consumer: string): GrantRule {
  return {
    id: "grt_test",
    resource: "credential:c1",
    action: "use",
    effect: "allow",
    origin: "system",
    conditions: { tool: consumer },
    expiresAt: null,
    roleId: null,
    principalId: null,
  };
}

// One credential, bound to `handle` for each named handler.
function cellFor(
  bound: readonly { handler: string; handle: string }[],
  secret = "sk-1",
): { current: CredentialDelivery } {
  return {
    current: {
      bindings: bound.map(({ handler, handle }) => ({
        handle,
        credentialId: "c1",
        consumer: actionConsumer(handler),
      })),
      materials: [
        { credentialId: "c1", providerKey: "fake", origin: ORIGIN, secret },
      ],
    },
  };
}

function contextFor(opts: {
  cell: { current: CredentialDelivery | null };
  grantsByStep: Readonly<Record<string, readonly GrantRule[]>>;
  provider: CredentialProvider;
  stepReads?: string[];
}): ActionCredentialWiring {
  return {
    materialCell: opts.cell,
    resolveStepGrants: ({ runId, stepId }) => {
      opts.stepReads?.push(runId === undefined ? stepId : `${runId}/${stepId}`);
      return opts.grantsByStep[stepId] ?? [];
    },
    providers: createCredentialProviderRegistry([opts.provider]),
  };
}

function request(
  handler: string,
  credentials: readonly string[],
  stepId = "push",
) {
  return {
    handler,
    credentials,
    authzContext: { stepId, runId: "run-1" },
  };
}

describe("createActionCredentialsFactory", () => {
  test("a bound and granted handle resolves to the live secret", async () => {
    const track = trackingProvider();
    const cell = cellFor([{ handler: "push", handle: "github" }]);
    const factory = createActionCredentialsFactory(
      contextFor({
        cell,
        grantsByStep: { push: [useGrant(actionConsumer("push"))] },
        provider: track.provider,
      }),
    );
    const capability = factory(request("push", ["github"]));
    await expect(capability.resolve("github")).resolves.toMatchObject({
      kind: "http",
    });
    expect(track.shapes[0]?.readCurrentMaterial()).toEqual({ secret: "sk-1" });

    // A rotation in the cell reaches the already-shaped handle.
    cell.current = cellFor(
      [{ handler: "push", handle: "github" }],
      "sk-2",
    ).current;
    expect(track.shapes[0]?.readCurrentMaterial()).toEqual({ secret: "sk-2" });
  });

  test("a bound handle without the step's use grant fails closed", async () => {
    const track = trackingProvider();
    const factory = createActionCredentialsFactory(
      contextFor({
        cell: cellFor([{ handler: "push", handle: "github" }]),
        grantsByStep: {},
        provider: track.provider,
      }),
    );
    await expect(
      factory(request("push", ["github"])).resolve("github"),
    ).rejects.toThrow(
      "consumer action:push is not authorized to use credential c1",
    );
    expect(track.shapes).toEqual([]);
  });

  test("a declared handle with no binding fails at build", () => {
    const factory = createActionCredentialsFactory(
      contextFor({
        cell: cellFor([]),
        grantsByStep: {},
        provider: trackingProvider().provider,
      }),
    );
    expect(() => factory(request("push", ["github"]))).toThrow(
      "consumer action:push declares credential handle(s) that no binding resolves: github",
    );
  });

  test("one handler cannot resolve a handle bound only for another", async () => {
    const factory = createActionCredentialsFactory(
      contextFor({
        cell: cellFor([{ handler: "push", handle: "github" }]),
        grantsByStep: {
          release: [
            useGrant(actionConsumer("push")),
            useGrant(actionConsumer("release")),
          ],
        },
        provider: trackingProvider().provider,
      }),
    );
    await expect(
      factory(request("release", [], "release")).resolve("github"),
    ).rejects.toThrow(
      'no credential is bound to handle "github" for consumer action:release',
    );
  });

  test("an action with no declaration and no binding gets a capability that fails closed", async () => {
    const stepReads: string[] = [];
    const factory = createActionCredentialsFactory(
      contextFor({
        cell: cellFor([]),
        grantsByStep: {},
        provider: trackingProvider().provider,
        stepReads,
      }),
    );
    await expect(
      factory(request("push", [])).resolve("github"),
    ).rejects.toThrow(
      'no credential is bound to handle "github" for consumer action:push',
    );
    expect(stepReads).toEqual([]);
  });

  test("each invocation reads its own step's grants", async () => {
    const stepReads: string[] = [];
    const factory = createActionCredentialsFactory(
      contextFor({
        cell: cellFor([
          { handler: "push", handle: "github" },
          { handler: "release", handle: "github" },
        ]),
        grantsByStep: {
          first: [useGrant(actionConsumer("push"))],
          second: [useGrant(actionConsumer("release"))],
        },
        provider: trackingProvider().provider,
        stepReads,
      }),
    );
    await factory(request("push", ["github"], "first")).resolve("github");
    await factory(request("release", ["github"], "second")).resolve("github");
    expect(stepReads).toEqual(["run-1/first", "run-1/second"]);
  });

  test("the grants resolver receives the invocation's run id", async () => {
    const stepReads: string[] = [];
    const factory = createActionCredentialsFactory(
      contextFor({
        cell: cellFor([{ handler: "push", handle: "github" }]),
        grantsByStep: { push: [useGrant(actionConsumer("push"))] },
        provider: trackingProvider().provider,
        stepReads,
      }),
    );
    await factory({
      handler: "push",
      credentials: ["github"],
      authzContext: { stepId: "push", runId: "run-child" },
    }).resolve("github");
    await factory({
      handler: "push",
      credentials: ["github"],
      authzContext: { stepId: "push" },
    }).resolve("github");
    expect(stepReads).toEqual(["run-child/push", "push"]);
  });

  test("an authz context with no step id fails naming the handler", () => {
    const factory = createActionCredentialsFactory(
      contextFor({
        cell: cellFor([]),
        grantsByStep: {},
        provider: trackingProvider().provider,
      }),
    );
    expect(() =>
      factory({
        handler: "push",
        credentials: [],
        authzContext: { runId: "run-1" },
      }),
    ).toThrow(
      "action push cannot build its credentials capability: the authz context carries no step id",
    );
  });
});
