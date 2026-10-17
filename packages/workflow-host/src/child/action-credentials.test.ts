// A deployed action handler's credentials capability: the run child's adapter
// over the harness action credentials factory, and its use through the
// default invoker. The factory's own cases live with it in the harness.

import { describe, test, expect } from "bun:test";

import { actionConsumer, type GrantRule } from "@intx/authz";
import { createCredentialProviderRegistry } from "@intx/harness";
import type {
  CredentialCapability,
  CredentialProvider,
  CredentialShapeContext,
  HostCredentialCapability,
} from "@intx/types";
import type { CredentialDelivery } from "@intx/types/credential-delivery";
import {
  createDefaultActionInvoker,
  createInMemoryEffectLedger,
  type ActionCredentialsFactory,
  type ActionInvokeRequest,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

import {
  createSidecarActionCredentialsFactory,
  type SidecarStepCredentialContext,
} from "./action-credentials";

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
}): SidecarStepCredentialContext {
  return {
    materialCell: opts.cell,
    resolveStepGrants: (stepId) => {
      opts.stepReads?.push(stepId);
      return opts.grantsByStep[stepId] ?? [];
    },
    providers: createCredentialProviderRegistry([opts.provider]),
  };
}

function request(handler: string, credentials: readonly string[]) {
  return {
    handler,
    credentials,
    authzContext: { stepId: "push", attempt: 1, runId: "run-1" },
  };
}

describe("createSidecarActionCredentialsFactory", () => {
  test("reads the step's grants from the run child's context by step id", async () => {
    const stepReads: string[] = [];
    const track = trackingProvider();
    const factory = createSidecarActionCredentialsFactory(
      contextFor({
        cell: cellFor([{ handler: "push", handle: "github" }]),
        grantsByStep: { push: [useGrant(actionConsumer("push"))] },
        provider: track.provider,
        stepReads,
      }),
    );
    await expect(
      factory(request("push", ["github"])).resolve("github"),
    ).resolves.toMatchObject({ kind: "http" });
    expect(stepReads).toEqual(["push"]);
    expect(track.shapes[0]?.readCurrentMaterial()).toEqual({ secret: "sk-1" });
  });
});

describe("action credentials through the default invoker", () => {
  const allow: WorkflowAuthorizeFn = async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });

  test("each invocation gets a fresh capability, disposed once, and a closed scope refuses", async () => {
    const built = createSidecarActionCredentialsFactory(
      contextFor({
        cell: cellFor([{ handler: "push", handle: "github" }]),
        grantsByStep: { push: [useGrant(actionConsumer("push"))] },
        provider: trackingProvider().provider,
      }),
    );
    const capabilities: HostCredentialCapability[] = [];
    const disposals = new Map<HostCredentialCapability, number>();
    const factory: ActionCredentialsFactory = (req) => {
      const capability = built(req);
      capabilities.push(capability);
      return {
        resolve: (handle) => capability.resolve(handle),
        dispose: async () => {
          disposals.set(capability, (disposals.get(capability) ?? 0) + 1);
          await capability.dispose();
        },
      };
    };
    const views: CredentialCapability[] = [];
    const invoke = createDefaultActionInvoker(
      allow,
      createInMemoryEffectLedger(),
      () => async (_input, ctx) =>
        ctx.perform({
          effectId: "resolve",
          capability: "resolve",
          run: async ({ credentials }) => {
            views.push(credentials);
            return (await credentials.resolve("github")).kind;
          },
        }),
      factory,
    );
    const invocation: ActionInvokeRequest = {
      handler: "push",
      input: null,
      requires: ["resolve"],
      credentials: ["github"],
      authzContext: { stepId: "push", attempt: 1, runId: "run-1" },
      signal: new AbortController().signal,
    };

    // A distinct input keys a distinct effect, so the second run is not a
    // ledger hit and resolves again.
    await expect(invoke(invocation)).resolves.toEqual({ output: "http" });
    await expect(invoke({ ...invocation, input: 2 })).resolves.toEqual({
      output: "http",
    });
    expect(views).toHaveLength(2);
    expect(capabilities).toHaveLength(2);
    expect(capabilities[0]).not.toBe(capabilities[1]);
    expect([...disposals.values()]).toEqual([1, 1]);
    await expect(views[0]?.resolve("github")).rejects.toThrow(
      "outside the run of its effect resolve",
    );
  });
});
