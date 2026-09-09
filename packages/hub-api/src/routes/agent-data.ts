import { Hono } from "hono";
import { describeRoute } from "hono-openapi";
import {
  FileEntry,
  ErrorResponse,
  FileContent,
  HistoryEntry,
  CommitDetail,
  BranchInfo,
} from "@intx/types";

import type { AppEnv } from "../context";
import { errorResponse } from "../error-response";
import { jsonResponse } from "../openapi";

export function createAgentDataRoutes(): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.get(
    "/data",
    describeRoute({
      tags: ["Agent Data"],
      summary: "List files in agent working directory",
      responses: {
        200: jsonResponse("File listing", FileEntry.array()),
        404: jsonResponse("Agent not found", ErrorResponse),
      },
    }),
    (c) => errorResponse(c, "not_implemented", "Not implemented"),
  );

  app.get(
    "/data/*",
    describeRoute({
      tags: ["Agent Data"],
      summary: "Read a file from agent storage",
      description: "Reads a file by path from the agent's local storage.",
      responses: {
        200: jsonResponse("File content", FileContent),
        404: jsonResponse("File or agent not found", ErrorResponse),
      },
    }),
    (c) => errorResponse(c, "not_implemented", "Not implemented"),
  );

  app.get(
    "/history",
    describeRoute({
      tags: ["Agent Data"],
      summary: "List commits and checkpoints",
      description:
        "Returns the agent's change history with commit messages and timestamps.",
      responses: {
        200: jsonResponse("History entries", HistoryEntry.array()),
      },
    }),
    (c) => errorResponse(c, "not_implemented", "Not implemented"),
  );

  app.get(
    "/history/:ref",
    describeRoute({
      tags: ["Agent Data"],
      summary: "Show changes in a commit",
      description:
        "Returns the files changed in a specific commit with additions/deletions counts.",
      responses: {
        200: jsonResponse("Commit details", CommitDetail),
        404: jsonResponse("Commit not found", ErrorResponse),
      },
    }),
    (c) => errorResponse(c, "not_implemented", "Not implemented"),
  );

  app.get(
    "/branches",
    describeRoute({
      tags: ["Agent Data"],
      summary: "List branches",
      description: "Lists branches in the agent's data repository.",
      responses: {
        200: jsonResponse("List of branches", BranchInfo.array()),
      },
    }),
    (c) => errorResponse(c, "not_implemented", "Not implemented"),
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
        404: jsonResponse("Commit not found", ErrorResponse),
      },
    }),
    (c) => errorResponse(c, "not_implemented", "Not implemented"),
  );

  return app;
}
