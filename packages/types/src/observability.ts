import { type } from "arktype";

export const LogEntry = type({
  timestamp: "string",
  level: "'debug' | 'info' | 'warn' | 'error'",
  message: "string",
  "metadata?": "Record<string, unknown> | null",
});

export const LogQuery = type({
  "level?": "'debug' | 'info' | 'warn' | 'error'",
  "startTime?": "string",
  "endTime?": "string",
});

export const MetricsResponse = type({
  agentId: "string",
  "messageCount?": "number",
  "tokenUsage?": {
    "input?": "number",
    "output?": "number",
    "total?": "number",
  },
  "cost?": "string",
  "avgLatencyMs?": "number",
  "errorRate?": "number",
});

export const TraceQuery = type({
  "agentId?": "string",
  "sessionId?": "string",
  "traceId?": "string",
  "startTime?": "string",
  "endTime?": "string",
});

export const SpanResponse = type({
  spanId: "string",
  traceId: "string",
  "parentSpanId?": "string | null",
  name: "string",
  "agentId?": "string | null",
  startTime: "string",
  "endTime?": "string | null",
  "durationMs?": "number | null",
  "status?": "'ok' | 'error'",
  "attributes?": "Record<string, unknown> | null",
});

export const TraceResponse = type({
  traceId: "string",
  spans: type({
    spanId: "string",
    traceId: "string",
    "parentSpanId?": "string | null",
    name: "string",
    "agentId?": "string | null",
    startTime: "string",
    "endTime?": "string | null",
    "durationMs?": "number | null",
    "status?": "'ok' | 'error'",
    "attributes?": "Record<string, unknown> | null",
  }).array(),
});
