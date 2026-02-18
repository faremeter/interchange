import type { DB } from "@interchange/db";
import { betterAuth } from "better-auth";
import { drizzleAdapter } from "better-auth/adapters/drizzle";

export function createAuth(db: DB["db"]) {
  return betterAuth({
    database: drizzleAdapter(db, { provider: "pg" }),
    emailAndPassword: {
      enabled: true,
    },
    socialProviders: {
      google: {
        clientId: process.env["GOOGLE_CLIENT_ID"] ?? "",
        clientSecret: process.env["GOOGLE_CLIENT_SECRET"] ?? "",
      },
    },
  });
}

export type Auth = ReturnType<typeof createAuth>;
