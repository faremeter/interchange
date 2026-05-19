import type { Context, Next } from "hono";

import type { AppEnv } from "../context";
import type { GetSession } from "../session";

export function createSessionMiddleware(getSession: GetSession) {
  return async function sessionMiddleware(c: Context<AppEnv>, next: Next) {
    const result = await getSession(c.req.raw.headers);
    c.set("user", result?.user ?? null);
    c.set("session", result?.session ?? null);
    await next();
  };
}
