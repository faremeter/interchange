import { type } from "arktype";

export const DBConfig = type({
  host: "string",
  port: "number.integer > 0",
  user: "string",
  password: "string",
  database: "string",
  "ssl?": "boolean",
  "max?": "number.integer > 0",
});

export type DBConfig = typeof DBConfig.infer;
