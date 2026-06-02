import { type } from "arktype";

export const DBConfig = type({
  host: "string",
  port: "number.integer > 0",
  user: "string",
  password: "string",
  database: "string",
  "ssl?": "boolean",
  "max?": "number.integer > 0",
  // Postgres schema name. When set, the connection's `search_path` is
  // pinned to this schema and migrations apply into it. This is the
  // mechanism the integration-test harness uses to give each spawned
  // hub a dedicated, droppable schema. When unset, the connection uses
  // postgres' default `search_path` (which begins with `public`).
  "schema?": "string",
});

export type DBConfig = typeof DBConfig.infer;
