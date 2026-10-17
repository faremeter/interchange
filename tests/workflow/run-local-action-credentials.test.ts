// runLocal with the harness action credentials factory: a local run gives its
// action handlers the same mediated credentials a deployed run does.
//
// A top-level action and an action in a childWorkflow child each resolve a
// bound handle inside `perform` and fetch through it. The provider's injected
// fetch records the bearer, so the test reads the exact delivered secret. The
// grants resolver records the run id of each read, which shows that one factory
// serves the parent run and its child run. A run whose grants hold no use grant
// fails closed before any request.

import { describe, test, expect } from "bun:test";

import { actionConsumer, type GrantRule } from "@intx/authz";
import {
  createActionCredentialsFactory,
  createCredentialProviderRegistry,
  createHttpCredentialProvider,
} from "@intx/harness";
import type { CredentialDelivery } from "@intx/types/credential-delivery";
import {
  action,
  childWorkflow,
  defineWorkflow,
  runLocal,
  type ActionHandler,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

const ORIGIN = "https://api.example.com";
const SECRET = "sk-local-delta-51c0";

const allow: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

function useGrant(consumer: string): GrantRule {
  return {
    id: `grant-use-${consumer}`,
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

const material: { current: CredentialDelivery } = {
  current: {
    bindings: ["push", "pushChild"].map((handler) => ({
      handle: "gh",
      credentialId: "c1",
      consumer: actionConsumer(handler),
    })),
    materials: [
      {
        credentialId: "c1",
        providerKey: "http",
        origin: ORIGIN,
        secret: SECRET,
      },
    ],
  },
};

// Fetch `path` through the `gh` handle inside one `perform`.
function fetching(path: string): ActionHandler {
  return (_input, ctx) =>
    ctx.perform({
      effectId: path,
      capability: "call",
      run: async ({ credentials }) => {
        const gh = await credentials.resolve("gh");
        return (await gh.fetch(path)).status;
      },
    });
}

const handlers: Record<string, ActionHandler> = {
  push: fetching("/push"),
  pushChild: fetching("/child"),
};

const definition = defineWorkflow({
  id: "local-action-credentials",
  trigger: { type: "manual" },
  steps: {
    push: action({
      handler: "push",
      credentials: ["gh"],
      effect: { requires: ["call"] },
    }),
    spawn: childWorkflow({
      definition: defineWorkflow({
        id: "local-action-credentials-child",
        trigger: { type: "manual" },
        steps: {
          pushChild: action({
            handler: "pushChild",
            credentials: ["gh"],
            effect: { requires: ["call"] },
          }),
        },
      }),
    }),
  },
});

function runWith(grants: readonly GrantRule[]) {
  const requests: { url: string; authorization: string | null }[] = [];
  const grantReads: { runId?: string; stepId: string }[] = [];
  const run = runLocal(definition, {
    authorize: allow,
    hasUpstreamSignalResolver: true,
    actionResolver: (ref) => {
      const handler = handlers[ref];
      if (handler === undefined) throw new Error(`no handler ${ref}`);
      return handler;
    },
    actionCredentials: createActionCredentialsFactory({
      materialCell: material,
      resolveStepGrants: (step) => {
        grantReads.push(step);
        return grants;
      },
      providers: createCredentialProviderRegistry([
        createHttpCredentialProvider({
          fetch: async (input, init) => {
            const request =
              input instanceof Request
                ? new Request(input, init)
                : new Request(String(input), init);
            requests.push({
              url: request.url,
              authorization: request.headers.get("authorization"),
            });
            return new Response("ok");
          },
        }),
      ]),
    }),
  });
  return { run, requests, grantReads };
}

describe("runLocal action credentials", () => {
  test("a top-level action and a childWorkflow action authenticate with the bound secret", async () => {
    const { run, requests, grantReads } = runWith([
      useGrant(actionConsumer("push")),
      useGrant(actionConsumer("pushChild")),
    ]);
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(requests).toHaveLength(2);
    expect(requests).toContainEqual({
      url: `${ORIGIN}/push`,
      authorization: `Bearer ${SECRET}`,
    });
    expect(requests).toContainEqual({
      url: `${ORIGIN}/child`,
      authorization: `Bearer ${SECRET}`,
    });
    // One factory served two runs: the child's grants read carries its own
    // run id.
    expect(grantReads.map((read) => read.stepId).sort()).toEqual([
      "push",
      "pushChild",
    ]);
    const runIds = new Set(grantReads.map((read) => read.runId));
    expect(runIds.size).toBe(2);
    expect(runIds.has(undefined)).toBe(false);
  });

  test("a run without the use grant fails closed before any request", async () => {
    const { run, requests } = runWith([]);
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
    const failure = result.events.find(
      (event) => event.kind === "StepFailed" && event.stepId === "push",
    );
    if (failure === undefined || failure.kind !== "StepFailed") {
      throw new Error("expected the push step to fail");
    }
    expect(failure.error.message).toContain(
      "consumer action:push is not authorized to use credential c1",
    );
    expect(requests).toEqual([]);
  });
});
