/* eslint-disable @typescript-eslint/no-unsafe-type-assertion -- Transport.fetch<T> mocks must satisfy the caller-selected generic return type */
import { expect, test } from "bun:test";

import type { Transport } from "./transport";
import { enrollExecutionHost } from "./execution-hosts";

test("enrollExecutionHost posts the host and validates its credential", async () => {
  const calls: unknown[] = [];
  const transport: Transport = {
    async fetch<T>(method: string, path: string, body?: unknown): Promise<T> {
      calls.push({ method, path, body });
      return {
        host: {
          id: "hst_1",
          tenantId: "tnt_1",
          principalId: "prn_host_1",
          ownerPrincipalId: "prn_owner_1",
          displayName: "Browser tab",
          createdAt: "2026-09-03T12:00:00.000Z",
          updatedAt: "2026-09-03T12:00:00.000Z",
        },
        secret: "intx_host_secret",
      } as T;
    },
    subscribe() {
      return () => undefined;
    },
  };

  const enrollment = await enrollExecutionHost(transport, "tnt_1", {
    displayName: "Browser tab",
  });

  expect(calls).toEqual([
    {
      method: "POST",
      path: "/api/tenants/tnt_1/hosts",
      body: { displayName: "Browser tab" },
    },
  ]);
  expect(enrollment.host.principalId).toBe("prn_host_1");
  expect(enrollment.secret).toBe("intx_host_secret");
});
