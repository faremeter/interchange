import { createDB } from "@interchange/db";
import { createApp } from "@interchange/hub";

const { db } = createDB({
  host: process.env["DB_HOST"] ?? "localhost",
  port: Number(process.env["DB_PORT"] ?? 5432),
  user: process.env["DB_USER"] ?? "postgres",
  password: process.env["DB_PASSWORD"] ?? "postgres",
  database: process.env["DB_NAME"] ?? "interchange",
});

const app = createApp();

app.use(async (c, next) => {
  c.set("db", db);
  await next();
});

const port = Number(process.env["PORT"] ?? 3000);

export default {
  fetch: app.fetch,
  port,
};
