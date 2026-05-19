import { Hono } from "hono";
import { describeRoute, resolver } from "hono-openapi";
import {
  FileEntry,
  FileContent,
  HistoryEntry,
  CommitDetail,
  BranchInfo,
  ErrorResponse,
} from "@intx/types";

import type { AppEnv } from "../context";

export function createAgentDataRoutes(): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.get(
    "/data",
    describeRoute({
      tags: ["Agent Data"],
      summary: "List files in agent working directory",
      responses: {
        200: {
          description: "File listing",
          content: {
            "application/json": {
              schema: resolver(FileEntry.array()),
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
    "/data/*",
    describeRoute({
      tags: ["Agent Data"],
      summary: "Read a file from agent storage",
      description: "Reads a file by path from the agent's local storage.",
      responses: {
        200: {
          description: "File content",
          content: {
            "application/json": { schema: resolver(FileContent) },
          },
        },
        404: {
          description: "File or agent not found",
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
    "/history",
    describeRoute({
      tags: ["Agent Data"],
      summary: "List commits and checkpoints",
      description:
        "Returns the agent's change history with commit messages and timestamps.",
      responses: {
        200: {
          description: "History entries",
          content: {
            "application/json": {
              schema: resolver(HistoryEntry.array()),
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
    "/history/:ref",
    describeRoute({
      tags: ["Agent Data"],
      summary: "Show changes in a commit",
      description:
        "Returns the files changed in a specific commit with additions/deletions counts.",
      responses: {
        200: {
          description: "Commit details",
          content: {
            "application/json": { schema: resolver(CommitDetail) },
          },
        },
        404: {
          description: "Commit not found",
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
    "/branches",
    describeRoute({
      tags: ["Agent Data"],
      summary: "List branches",
      description: "Lists branches in the agent's data repository.",
      responses: {
        200: {
          description: "List of branches",
          content: {
            "application/json": {
              schema: resolver(BranchInfo.array()),
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

  app.post(
    "/history/:ref/restore",
    describeRoute({
      tags: ["Agent Data"],
      summary: "Restore agent data to a previous state",
      description:
        "Restores the agent's working directory to the state at the specified commit.",
      responses: {
        204: {
          description: "Data restored",
        },
        404: {
          description: "Commit not found",
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

  return app;
}
