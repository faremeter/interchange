import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import {
  CreateSession,
  SessionResponse,
  SessionStatus,
  SendMessage,
  MessageResponse,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

app.post(
  "/",
  describeRoute({
    tags: ["Sessions"],
    summary: "Create a session",
    description:
      "Creates a new session with the specified agent. Returns a session ID and session token (JWT for WebSocket authentication). Invoker-granted capabilities become grants with source 'invoker' scoped to the session lifetime.",
    responses: {
      201: {
        description: "Session created",
        content: {
          "application/json": { schema: resolver(SessionResponse) },
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
  validator("json", CreateSession),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/",
  describeRoute({
    tags: ["Sessions"],
    summary: "List sessions in the tenant",
    description:
      "Lists the caller's sessions within this tenant. Filterable by agentId.",
    parameters: [{ name: "agentId", in: "query", schema: { type: "string" } }],
    responses: {
      200: {
        description: "List of sessions",
        content: {
          "application/json": {
            schema: resolver(SessionResponse.array()),
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
  "/:sessionId",
  describeRoute({
    tags: ["Sessions"],
    summary: "Get session metadata",
    description:
      "Returns session status, agent, creation time, and last activity.",
    responses: {
      200: {
        description: "Session details",
        content: {
          "application/json": { schema: resolver(SessionResponse) },
        },
      },
      404: {
        description: "Session not found",
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

app.delete(
  "/:sessionId",
  describeRoute({
    tags: ["Sessions"],
    summary: "End a session",
    description:
      "Ends the session and expires all invoker-granted capabilities associated with it.",
    responses: {
      204: {
        description: "Session ended",
      },
      404: {
        description: "Session not found",
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

app.post(
  "/:sessionId/messages",
  describeRoute({
    tags: ["Sessions"],
    summary: "Send a message to the agent",
    description:
      "Persists the user message and returns it. The agent's response streams over the session channel (WebSocket or SSE), not in this HTTP response.",
    responses: {
      201: {
        description: "Message sent",
        content: {
          "application/json": { schema: resolver(MessageResponse) },
        },
      },
      400: {
        description: "Validation error",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", SendMessage),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:sessionId/messages",
  describeRoute({
    tags: ["Sessions"],
    summary: "List messages in a session",
    description:
      "Returns messages with all parts (text, reasoning, tool calls, etc.). Cursor-paginated.",
    responses: {
      200: {
        description: "List of messages",
        content: {
          "application/json": {
            schema: resolver(MessageResponse.array()),
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
  "/:sessionId/messages/:messageId",
  describeRoute({
    tags: ["Sessions"],
    summary: "Get a single message",
    description: "Returns a message with all its parts.",
    responses: {
      200: {
        description: "Message details",
        content: {
          "application/json": { schema: resolver(MessageResponse) },
        },
      },
      404: {
        description: "Message not found",
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

app.post(
  "/:sessionId/abort",
  describeRoute({
    tags: ["Sessions"],
    summary: "Abort current operation",
    description: "Aborts the agent's current inference or tool execution.",
    responses: {
      204: {
        description: "Abort signal sent",
      },
      404: {
        description: "Session not found",
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
  "/:sessionId/status",
  describeRoute({
    tags: ["Sessions"],
    summary: "Get session status",
    description:
      "Returns the current session status: idle, busy, retry (with attempt info), or waiting_approval.",
    responses: {
      200: {
        description: "Session status",
        content: {
          "application/json": { schema: resolver(SessionStatus) },
        },
      },
      404: {
        description: "Session not found",
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
  "/:sessionId/events",
  describeRoute({
    tags: ["Sessions"],
    summary: "SSE event stream (fallback)",
    description:
      "Server-Sent Events stream for clients that cannot use WebSocket. Same event types as the WebSocket session channel. Server-to-client only; use POST .../messages for client-to-server.",
    responses: {
      200: {
        description: "SSE event stream",
        content: {
          "text/event-stream": {},
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

export { app as sessionRoutes };
