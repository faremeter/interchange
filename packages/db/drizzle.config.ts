import { defineConfig } from "drizzle-kit";

function env(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

export default defineConfig({
  dialect: "postgresql",
  schema: "./src/schema/index.ts",
  out: "./migrations",
  dbCredentials: {
    host: env("DB_HOST"),
    port: Number(process.env["DB_PORT"] ?? 5432),
    user: env("DB_USER"),
    password: process.env["DB_PASSWORD"] ?? "",
    database: env("DB_NAME"),
    ssl: process.env["DB_SSL"] === "true",
  },
});
