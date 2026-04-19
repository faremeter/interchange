import { eq } from "drizzle-orm";
import { Hono } from "hono";
import type { Handler } from "hono";

import { sidecar } from "@interchange/db/schema";

import type { AppEnv } from "../context";

interface SidecarRegisterBody {
  id?: string;
  url: string;
  status?: string;
}

export function createSidecarRoutes(wsHandler?: Handler<AppEnv>) {
  const app = new Hono<AppEnv>();

  if (wsHandler) {
    app.get("/ws", wsHandler);
  }

  app.post("/", async (c) => {
    const db = c.get("db");
    const body = await c.req.json<SidecarRegisterBody>();
    const { id, url, status } = body;

    const [created] = await db
      .insert(sidecar)
      .values({
        id: id || crypto.randomUUID(),
        url,
        status: (status as "online" | "offline" | "error") || "online",
        lastHeartbeat: new Date(),
      })
      .onConflictDoUpdate({
        target: sidecar.id,
        set: {
          url,
          status: (status as "online" | "offline" | "error") || "online",
          lastHeartbeat: new Date(),
        },
      })
      .returning();

    return c.json({ data: created }, 201);
  });

  app.get("/", async (c) => {
    const db = c.get("db");
    const sidecars = await db.select().from(sidecar);
    return c.json({ data: sidecars });
  });

  app.get("/:id", async (c) => {
    const db = c.get("db");
    const id = c.req.param("id");
    const [sc] = await db.select().from(sidecar).where(eq(sidecar.id, id));

    if (!sc) {
      return c.json({ error: "Sidecar not found" }, { status: 404 });
    }

    return c.json({ data: sc });
  });

  app.delete("/:id", async (c) => {
    const db = c.get("db");
    const id = c.req.param("id");
    await db.delete(sidecar).where(eq(sidecar.id, id));
    return c.json({ success: true });
  });

  app.post("/:id/heartbeat", async (c) => {
    const db = c.get("db");
    const id = c.req.param("id");
    await db
      .update(sidecar)
      .set({ lastHeartbeat: new Date(), status: "online" })
      .where(eq(sidecar.id, id));

    return c.json({ success: true });
  });

  return app;
}
