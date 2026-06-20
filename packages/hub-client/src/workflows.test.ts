/* eslint-disable @typescript-eslint/no-unsafe-type-assertion -- Transport.fetch<T> is a generic interface method; mock implementations must use `as T` to satisfy the return type contract */
import { describe, expect, test } from "bun:test";

import type { InferenceSource } from "@intx/types/runtime";

import type { Transport } from "./transport";
import {
  deliverWorkflowSignal,
  deployWorkflow,
  listWorkflowDeployments,
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
});
