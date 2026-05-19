import { eq } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import type { Handler } from "hono";

import { sidecar } from "@intx/db/schema";
import { parseSidecarStatus } from "@intx/db";
import type { DB } from "@intx/db";
import { CreateSidecar, SidecarResponse, ErrorResponse } from "@intx/types";

import type { AppEnv } from "../context";
import { first, ts } from "../format";

function formatSidecar(row: typeof sidecar.$inferSelect) {
  return {
    id: row.id,
    url: row.url,
    status: parseSidecarStatus(row.status),
    lastHeartbeat: row.lastHeartbeat ? ts(row.lastHeartbeat) : null,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

export type CreateSidecarRoutesDeps = {
  db: DB["db"];
  wsHandler?: Handler<AppEnv>;
};

// Sidecar management routes are system-level (not tenant-scoped) and
// authenticated by the sidecar's registration token over the WebSocket
// channel. The REST endpoints here are for internal tooling and are not
// exposed through tenant authorization grants.
export function createSidecarRoutes({
  db,
  wsHandler,
}: CreateSidecarRoutesDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  if (wsHandler) {
    app.get("/ws", wsHandler);
  }

  app.post(
    "/",
    describeRoute({
      tags: ["Sidecars"],
      summary: "Register or update a sidecar",
      description:
        "Upserts a sidecar record. If an id is provided and already exists, the record is updated. Used for idempotent sidecar registration by a known stable identifier.",
      responses: {
        201: {
          description: "Sidecar registered",
          content: {
            "application/json": { schema: resolver(SidecarResponse) },
          },
        },
      },
    }),
    validator("json", CreateSidecar),
    async (c) => {
      const body = c.req.valid("json");

      const resolvedStatus = body.status ?? "online";
      const created = first(
        await db
          .insert(sidecar)
          .values({
            id: body.id || crypto.randomUUID(),
            url: body.url,
            status: resolvedStatus,
            lastHeartbeat: new Date(),
          })
          .onConflictDoUpdate({
            target: sidecar.id,
            set: {
              url: body.url,
              status: resolvedStatus,
              lastHeartbeat: new Date(),
              updatedAt: new Date(),
            },
          })
          .returning(),
      );

      return c.json(formatSidecar(created), 201);
    },
  );

  app.get(
    "/",
    describeRoute({
      tags: ["Sidecars"],
      summary: "List all sidecars",
      responses: {
        200: {
          description: "List of sidecars",
          content: {
            "application/json": {
              schema: resolver(SidecarResponse.array()),
            },
          },
        },
      },
    }),
    async (c) => {
      const sidecars = await db.select().from(sidecar);
      return c.json(sidecars.map(formatSidecar));
    },
  );

  app.get(
    "/:id",
    describeRoute({
      tags: ["Sidecars"],
      summary: "Get a sidecar by ID",
      responses: {
        200: {
          description: "Sidecar detail",
          content: {
            "application/json": { schema: resolver(SidecarResponse) },
          },
        },
        404: {
          description: "Sidecar not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const id = c.req.param("id");
      const [sc] = await db.select().from(sidecar).where(eq(sidecar.id, id));

      if (!sc) {
        return c.json(
          { error: { code: "not_found", message: "Sidecar not found" } },
          404,
        );
      }

      return c.json(formatSidecar(sc));
    },
  );

  app.delete(
    "/:id",
    describeRoute({
      tags: ["Sidecars"],
      summary: "Deregister a sidecar",
      responses: {
        204: { description: "Sidecar deregistered" },
        404: {
          description: "Sidecar not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const id = c.req.param("id");
      const deleted = await db
        .delete(sidecar)
        .where(eq(sidecar.id, id))
        .returning();

      if (deleted.length === 0) {
        return c.json(
          { error: { code: "not_found", message: "Sidecar not found" } },
          404,
        );
      }

      return c.body(null, 204);
    },
  );

  app.post(
    "/:id/heartbeat",
    describeRoute({
      tags: ["Sidecars"],
      summary: "Record a sidecar heartbeat",
      description:
        "Updates the sidecar's last heartbeat timestamp and sets status to online.",
      responses: {
        204: { description: "Heartbeat recorded" },
        404: {
          description: "Sidecar not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const id = c.req.param("id");
      const updated = await db
        .update(sidecar)
        .set({
          lastHeartbeat: new Date(),
          status: "online",
          updatedAt: new Date(),
        })
        .where(eq(sidecar.id, id))
        .returning();

      if (updated.length === 0) {
        return c.json(
          { error: { code: "not_found", message: "Sidecar not found" } },
          404,
        );
      }

      return c.body(null, 204);
    },
  );

  return app;
}
