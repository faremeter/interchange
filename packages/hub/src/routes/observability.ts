import { Hono } from "hono";
import { describeRoute, resolver } from "hono-openapi";
import {
  LogEntry,
  MetricsResponse,
  TraceResponse,
  SpanResponse,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

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
      200: {
        description: "Log entries",
        content: {
          "application/json": {
            schema: resolver(LogEntry.array()),
          },
        },
      },
      404: {
        description: "Agent not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/agents/:agentId/metrics",
  describeRoute({
    tags: ["Observability"],
    summary: "Get agent metrics",
    description:
      "Returns throughput, latency, error rates, token usage, and cost metrics.",
    responses: {
      200: {
        description: "Agent metrics",
        content: {
          "application/json": { schema: resolver(MetricsResponse) },
        },
      },
      404: {
        description: "Agent not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
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
      200: {
        description: "List of traces",
        content: {
          "application/json": {
            schema: resolver(SpanResponse.array()),
          },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/traces/:traceId",
  describeRoute({
    tags: ["Observability"],
    summary: "Get a full trace",
    description: "Returns all spans in a trace across agent boundaries.",
    responses: {
      200: {
        description: "Trace with spans",
        content: {
          "application/json": { schema: resolver(TraceResponse) },
        },
      },
      404: {
        description: "Trace not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

export { app as observabilityRoutes };
