import type { DB } from "@interchange/db";
import type { Env } from "hono";

import type { Auth } from "./auth";

export type AppEnv = Env & {
  Variables: {
    db: DB["db"];
    user: Auth["$Infer"]["Session"]["user"] | null;
    session: Auth["$Infer"]["Session"]["session"] | null;
  };
};
