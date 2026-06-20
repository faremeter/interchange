/* eslint-disable @typescript-eslint/no-unsafe-type-assertion -- Transport.fetch<T> is a generic interface method; mock implementations must use `as T` to satisfy the return type contract */
import { describe, expect, test } from "bun:test";

import type { InferenceSource } from "@intx/types/runtime";

import { createBrowserTransport, type Transport } from "./transport";
import {
  deliverWorkflowSignal,
  deployWorkflow,
  listWorkflowDeployments,
  listWorkflowRuns,
  readWorkflowRunEvents,
  triggerWorkflowRun,
  type WorkflowDeployment,
} from "./workflows";

const TENANT_ID = "ten_1";
const DEPLOYMENT_ID = "deployment_abc123";

type FetchCall = { method: string; path: string; body?: unknown };

function createMockTransport(fetchHandler: (call: FetchCall) => unknown) {
  const calls: FetchCall[] = [];
  const transport: Transport = {
    async fetch<T>(method: string, path: string, body?: unknown): Promise<T> {
      const call = { method, path, body };
      calls.push(call);
      return fetchHandler(call) as T;
    },
    subscribe(): () => void {
      throw new Error("subscribe is not used by workflow client methods");
    },
  };
  return { transport, calls };
}

function makeDeployment(
  overrides: Partial<WorkflowDeployment> = {},
): WorkflowDeployment {
  return {
    id: DEPLOYMENT_ID,
    tenantId: TENANT_ID,
    definitionAssetId: "asset_wf1",
    status: "deployed",
    createdAt: "2024-01-01T00:00:00.000Z",
    ...overrides,
  };
}

function makeSource(): InferenceSource {
  return {
    id: "src_1",
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    model: "claude-3-5-sonnet",
    apiKey: "sk-test",
  };
}

describe("listWorkflowDeployments", () => {
  test("GETs the instances endpoint and returns parsed deployments", async () => {
    const rows = [
      makeDeployment({ id: "deployment_2" }),
      makeDeployment({ id: "deployment_1" }),
    ];
    const { transport, calls } = createMockTransport(() => rows);

    const result = await listWorkflowDeployments(transport, TENANT_ID);

    expect(calls).toHaveLength(1);
    expect(calls[0]).toEqual({
      method: "GET",
      path: `/api/tenants/${TENANT_ID}/workflows/instances`,
      body: undefined,
    });
    expect(result).toEqual(rows);
  });

  test("rejects a response that does not match the deployment shape", async () => {
    const { transport } = createMockTransport(() => [{ id: "deployment_1" }]);
    await expect(listWorkflowDeployments(transport, TENANT_ID)).rejects.toThrow(
      /Invalid workflow deployment list response/,
    );
  });
});

describe("deployWorkflow", () => {
  test("POSTs the deploy body and returns the parsed deployment", async () => {
    const deployment = makeDeployment();
    const { transport, calls } = createMockTransport(() => deployment);
    const sources = [makeSource()];

    const result = await deployWorkflow(transport, TENANT_ID, {
      assetId: "asset_wf1",
      sources,
      defaultSource: "src_1",
    });

    expect(calls).toHaveLength(1);
    expect(calls[0]).toEqual({
      method: "POST",
      path: `/api/tenants/${TENANT_ID}/workflows/instances`,
      body: {
        assetId: "asset_wf1",
        sources,
        defaultSource: "src_1",
      },
    });
    expect(result).toEqual(deployment);
  });

  test("rejects a response that does not match the deployment shape", async () => {
    const { transport } = createMockTransport(() => ({ id: "deployment_1" }));
    await expect(
      deployWorkflow(transport, TENANT_ID, {
        assetId: "asset_wf1",
        sources: [makeSource()],
        defaultSource: "src_1",
      }),
    ).rejects.toThrow(/Invalid workflow deploy response/);
  });
});

describe("deliverWorkflowSignal", () => {
  test("POSTs the signal body passing signalId through verbatim", async () => {
    const { transport, calls } = createMockTransport(() => undefined);

    await deliverWorkflowSignal(transport, TENANT_ID, DEPLOYMENT_ID, {
      runId: "run_1",
      signalName: "approve",
      signalId: "caller-supplied-signal-id",
      payload: { ok: true },
    });

    expect(calls).toHaveLength(1);
    expect(calls[0]).toEqual({
      method: "POST",
      path: `/api/tenants/${TENANT_ID}/workflows/${DEPLOYMENT_ID}/signals`,
      body: {
        runId: "run_1",
        signalName: "approve",
        signalId: "caller-supplied-signal-id",
        payload: { ok: true },
      },
    });
  });

  test("omits payload from the body when the caller does not supply one", async () => {
    const { transport, calls } = createMockTransport(() => undefined);

    await deliverWorkflowSignal(transport, TENANT_ID, DEPLOYMENT_ID, {
      runId: "run_1",
      signalName: "approve",
      signalId: "sig_1",
    });

    expect(calls[0]?.body).toEqual({
      runId: "run_1",
      signalName: "approve",
      signalId: "sig_1",
    });
  });

  // The signal route returns 202 with an empty body. Driving the call
  // through the real browser transport (rather than a mock that returns
  // undefined) exercises the no-content path: a transport that tried to
  // JSON-parse the empty body would reject with "Unexpected end of JSON
  // input" even though the hub accepted the signal.
  test("resolves on a real 202 empty-body response without throwing", async () => {
    const originalFetch = globalThis.fetch;
    const seen: { url: string; method: string | undefined }[] = [];
    globalThis.fetch = Object.assign(
      (input: string | URL | Request, init?: RequestInit) => {
        seen.push({ url: String(input), method: init?.method });
        return Promise.resolve(new Response(null, { status: 202 }));
      },
      { preconnect: () => undefined },
    );
    try {
      const transport = createBrowserTransport();
      await deliverWorkflowSignal(transport, TENANT_ID, DEPLOYMENT_ID, {
        runId: "run_1",
        signalName: "approve",
        signalId: "sig_1",
        payload: { ok: true },
      });
      expect(seen).toHaveLength(1);
      expect(seen[0]?.method).toBe("POST");
      expect(seen[0]?.url).toBe(
        `/api/tenants/${TENANT_ID}/workflows/${DEPLOYMENT_ID}/signals`,
      );
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});

describe("triggerWorkflowRun", () => {
  test("POSTs the mail body and returns the parsed trigger ack", async () => {
    const ack = {
      deploymentId: DEPLOYMENT_ID,
      address: "wf@deployments.example.com",
      messageId: "<msg_1@example.com>",
    };
    const { transport, calls } = createMockTransport(() => ack);

    const result = await triggerWorkflowRun(
      transport,
      TENANT_ID,
      DEPLOYMENT_ID,
      {
        content: "kick off",
        attachments: [
          { mimeType: "text/plain", data: "aGk=", name: "note.txt" },
        ],
      },
    );

    expect(calls).toHaveLength(1);
    expect(calls[0]).toEqual({
      method: "POST",
      path: `/api/tenants/${TENANT_ID}/workflows/${DEPLOYMENT_ID}/mail`,
      body: {
        content: "kick off",
        attachments: [
          { mimeType: "text/plain", data: "aGk=", name: "note.txt" },
        ],
      },
    });
    expect(result).toEqual(ack);
  });

  test("omits attachments from the body when the caller does not supply any", async () => {
    const ack = {
      deploymentId: DEPLOYMENT_ID,
      address: "wf@deployments.example.com",
      messageId: "<msg_1@example.com>",
    };
    const { transport, calls } = createMockTransport(() => ack);

    await triggerWorkflowRun(transport, TENANT_ID, DEPLOYMENT_ID, {
      content: "kick off",
    });

    expect(calls[0]?.body).toEqual({ content: "kick off" });
  });

  test("rejects a response that does not match the trigger ack shape", async () => {
    const { transport } = createMockTransport(() => ({
      deploymentId: DEPLOYMENT_ID,
    }));
    await expect(
      triggerWorkflowRun(transport, TENANT_ID, DEPLOYMENT_ID, {
        content: "kick off",
      }),
    ).rejects.toThrow(/Invalid workflow run trigger response/);
  });

  // The trigger route returns 202 WITH a JSON acknowledgement body, unlike
  // the signal route's empty 202. Driving the call through the real browser
  // transport (rather than a mock) exercises the 202-with-body path: a
  // transport that discarded every 202 body would resolve to undefined and
  // lose the deploymentId/address/messageId the caller needs.
  test("returns the parsed ack from a real 202-with-body response", async () => {
    const ack = {
      deploymentId: DEPLOYMENT_ID,
      address: "wf@deployments.example.com",
      messageId: "<msg_1@example.com>",
    };
    const originalFetch = globalThis.fetch;
    const seen: { url: string; method: string | undefined; body: unknown }[] =
      [];
    globalThis.fetch = Object.assign(
      (input: string | URL | Request, init?: RequestInit) => {
        seen.push({
          url: String(input),
          method: init?.method,
          body: init?.body,
        });
        return Promise.resolve(
          new Response(JSON.stringify(ack), {
            status: 202,
            headers: { "Content-Type": "application/json" },
          }),
        );
      },
      { preconnect: () => undefined },
    );
    try {
      const transport = createBrowserTransport();
      const result = await triggerWorkflowRun(
        transport,
        TENANT_ID,
        DEPLOYMENT_ID,
        { content: "kick off" },
      );
      expect(result).toEqual(ack);
      expect(seen).toHaveLength(1);
      expect(seen[0]?.method).toBe("POST");
      expect(seen[0]?.url).toBe(
        `/api/tenants/${TENANT_ID}/workflows/${DEPLOYMENT_ID}/mail`,
      );
      expect(seen[0]?.body).toBe(JSON.stringify({ content: "kick off" }));
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});

describe("listWorkflowRuns", () => {
  test("GETs the runs endpoint and returns the run id array", async () => {
    const { transport, calls } = createMockTransport(() => ({
      runIds: ["run_1", "run_2"],
    }));

    const result = await listWorkflowRuns(transport, TENANT_ID, DEPLOYMENT_ID);

    expect(calls).toHaveLength(1);
    expect(calls[0]).toEqual({
      method: "GET",
      path: `/api/tenants/${TENANT_ID}/workflows/${DEPLOYMENT_ID}/runs`,
      body: undefined,
    });
    expect(result).toEqual(["run_1", "run_2"]);
  });

  test("rejects a response that does not match the run list shape", async () => {
    const { transport } = createMockTransport(() => ({ runIds: [1, 2] }));
    await expect(
      listWorkflowRuns(transport, TENANT_ID, DEPLOYMENT_ID),
    ).rejects.toThrow(/Invalid workflow run list response/);
  });
});

describe("readWorkflowRunEvents", () => {
  test("GETs the run events endpoint and returns the parsed projection", async () => {
    const projection = {
      runId: "run_1",
      events: [
        { seq: 0, type: "RunStarted", body: { at: "2024-01-01T00:00:00Z" } },
        { seq: 1, type: "RunCompleted", body: { ok: true } },
      ],
    };
    const { transport, calls } = createMockTransport(() => projection);

    const result = await readWorkflowRunEvents(
      transport,
      TENANT_ID,
      DEPLOYMENT_ID,
      "run_1",
    );

    expect(calls).toHaveLength(1);
    expect(calls[0]).toEqual({
      method: "GET",
      path: `/api/tenants/${TENANT_ID}/workflows/${DEPLOYMENT_ID}/runs/run_1/events`,
      body: undefined,
    });
    expect(result).toEqual(projection);
  });

  test("rejects a response that does not match the run events shape", async () => {
    const { transport } = createMockTransport(() => ({
      runId: "run_1",
      events: [{ seq: "0", type: "RunStarted", body: {} }],
    }));
    await expect(
      readWorkflowRunEvents(transport, TENANT_ID, DEPLOYMENT_ID, "run_1"),
    ).rejects.toThrow(/Invalid workflow run events response/);
  });
});
