import type { DB } from "@interchange/db";
import type { Env } from "hono";

export type AppEnv = Env & {
  Variables: {
    db: DB["db"];
  };
};
