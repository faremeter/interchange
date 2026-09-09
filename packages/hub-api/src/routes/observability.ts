import { Hono } from "hono";
import { describeRoute } from "hono-openapi";
import {
  LogEntry,
  ErrorResponse,
  MetricsResponse,
  TraceResponse,
  SpanResponse,
} from "@intx/types";

import type { AppEnv } from "../context";
import { errorResponse } from "../error-response";
import { jsonResponse } from "../openapi";

export function createObservabilityRoutes(): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.get(
    "/agents/:agentId/logs",
    describeRoute({
      tags: ["Observability"],
      summary: "Get agent logs",
      description:
        "Structured logs for an agent. Filterable by level and time range.",
      parameters: [
        {
          name: "level",
          in: "query",
          schema: { type: "string", enum: ["debug", "info", "warn", "error"] },
        },
        { name: "startTime", in: "query", schema: { type: "string" } },
        { name: "endTime", in: "query", schema: { type: "string" } },
      ],
      responses: {
        200: jsonResponse("Log entries", LogEntry.array()),
        404: jsonResponse("Agent not found", ErrorResponse),
      },
    }),
    (c) => errorResponse(c, "not_implemented", "Not implemented"),
  );

  app.get(
    "/agents/:agentId/metrics",
    describeRoute({
      tags: ["Observability"],
      summary: "Get agent metrics",
      description:
        "Returns throughput, latency, error rates, token usage, and cost metrics.",
      responses: {
        200: jsonResponse("Agent metrics", MetricsResponse),
        404: jsonResponse("Agent not found", ErrorResponse),
      },
    }),
    (c) => errorResponse(c, "not_implemented", "Not implemented"),
  );

  app.get(
    "/traces",
    describeRoute({
      tags: ["Observability"],
      summary: "Query distributed traces",
      description:
        "Searches traces within the tenant. Filterable by agent, session, time range, and trace ID.",
      parameters: [
        { name: "agentId", in: "query", schema: { type: "string" } },
        { name: "sessionId", in: "query", schema: { type: "string" } },
        { name: "traceId", in: "query", schema: { type: "string" } },
        { name: "startTime", in: "query", schema: { type: "string" } },
        { name: "endTime", in: "query", schema: { type: "string" } },
      ],
      responses: {
        200: jsonResponse("List of traces", SpanResponse.array()),
      },
    }),
    (c) => errorResponse(c, "not_implemented", "Not implemented"),
  );

  app.get(
    "/traces/:traceId",
    describeRoute({
      tags: ["Observability"],
      summary: "Get a full trace",
      description: "Returns all spans in a trace across agent boundaries.",
      responses: {
        200: jsonResponse("Trace with spans", TraceResponse),
        404: jsonResponse("Trace not found", ErrorResponse),
      },
    }),
    (c) => errorResponse(c, "not_implemented", "Not implemented"),
  );

  return app;
}
